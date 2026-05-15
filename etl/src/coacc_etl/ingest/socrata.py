"""Generic YAML-driven Socrata ingester.

Reads a :class:`~coacc_etl.catalog.DatasetSpec`, pulls new rows from Socrata
since the last lake watermark, enforces coverage, writes parquet partitions,
and advances the watermark — never to wall-clock, always to
``max(batch[watermark_column])``.

One ingester replaces the hand-rolled ``pipelines/<name>.py`` bodies for every
Socrata-backed ``tier: core`` dataset. Bespoke non-Socrata adapters live under
``ingest.custom`` and are out of scope.
"""
from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

from coacc_etl.ingest.coverage import (
    CoverageFailure,
    CoverageReport,
    write_coverage_report,
    write_failure_report,
)
from coacc_etl.lakehouse import watermark as wm
from coacc_etl.lakehouse.paths import meta_path, raw_path, raw_snapshot_path
from coacc_etl.lakehouse.writer import write_parquet_to_dir

if TYPE_CHECKING:
    from pathlib import Path

    from coacc_etl.catalog import DatasetSpec

LOG = logging.getLogger(__name__)

DEFAULT_DOMAIN = "www.datos.gov.co"
DEFAULT_PAGE_SIZE = 10_000
DEFAULT_MAX_PAGES = 10_000
DEFAULT_TIMEOUT = 60.0


class IngestError(RuntimeError):
    """Raised for unrecoverable ingest failures (HTTP, schema, watermark)."""


def _positive_int(value: int, *, name: str) -> int:
    if value < 1:
        msg = f"{name} must be >= 1, got {value}"
        raise IngestError(msg)
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        msg = f"{name} must be an integer, got {raw!r}"
        raise IngestError(msg) from exc
    return _positive_int(value, name=name)


@dataclass
class IngestResult:
    dataset_id: str
    rows: int
    partitions: list[tuple[int, int]]
    parquet_paths: list[Path]
    watermark_delta: wm.WatermarkDelta | None
    coverage: CoverageReport | None
    batch_id: str
    started_at: datetime
    finished_at: datetime
    skipped_reason: str = ""

    @property
    def ingested(self) -> bool:
        return self.rows > 0 and not self.skipped_reason


def _build_default_client(timeout: float = DEFAULT_TIMEOUT) -> httpx.Client:
    headers = {"User-Agent": "coacc-etl-ingest/1.0"}
    app_token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    key_id = os.environ.get("SOCRATA_KEY_ID", "").strip()
    key_secret = os.environ.get("SOCRATA_KEY_SECRET", "").strip()
    if app_token and app_token != key_id:
        headers["X-App-Token"] = app_token
    auth = (key_id, key_secret) if key_id and key_secret else None
    return httpx.Client(
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
        auth=auth,
    )


@dataclass
class SocrataClient:
    """Minimal paginating Socrata client with retry + auth.

    Use ``from_env`` for the production client (reads ``SOCRATA_APP_TOKEN``,
    ``SOCRATA_KEY_ID``, ``SOCRATA_KEY_SECRET``); tests may construct a
    ``SocrataClient`` directly around a fake ``httpx.Client``.
    """

    http: httpx.Client
    domain: str = DEFAULT_DOMAIN
    page_size: int = DEFAULT_PAGE_SIZE
    max_pages: int = DEFAULT_MAX_PAGES
    max_retries: int = 5
    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    sleep_fn: Callable[[float], None] = field(default=time.sleep)

    @classmethod
    def from_env(
        cls,
        *,
        domain: str = DEFAULT_DOMAIN,
        timeout: float = DEFAULT_TIMEOUT,
        page_size: int | None = None,
        max_pages: int | None = None,
    ) -> SocrataClient:
        resolved_page_size = (
            _positive_int(page_size, name="page_size")
            if page_size is not None
            else _env_int("COACC_SOCRATA_PAGE_SIZE", DEFAULT_PAGE_SIZE)
        )
        resolved_max_pages = (
            _positive_int(max_pages, name="max_pages")
            if max_pages is not None
            else _env_int("COACC_SOCRATA_MAX_PAGES", DEFAULT_MAX_PAGES)
        )
        return cls(
            http=_build_default_client(timeout=timeout),
            domain=domain,
            page_size=resolved_page_size,
            max_pages=resolved_max_pages,
        )

    def close(self) -> None:
        self.http.close()

    def __enter__(self) -> SocrataClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, url: str, params: dict[str, str | int]) -> list[dict[str, object]]:
        backoff = self.initial_backoff
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.http.get(url, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError(
                        f"retryable {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    msg = f"{url}: expected JSON array, got {type(payload).__name__}"
                    raise IngestError(msg)
                return payload
            except (
                httpx.TimeoutException,
                httpx.TransportError,
                httpx.HTTPStatusError,
            ) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                LOG.warning(
                    "ingest retry %s attempt=%s/%s (%s)",
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )
                self.sleep_fn(backoff)
                backoff = min(backoff * 2, self.max_backoff)
        msg = f"exhausted retries for {url}: {last_error}"
        raise IngestError(msg) from last_error

    def fetch(
        self,
        dataset_id: str,
        *,
        where: str | None = None,
        order: str,
    ) -> Iterator[list[dict[str, object]]]:
        """Yield successive pages of rows for ``dataset_id``."""
        url = f"https://{self.domain}/resource/{dataset_id}.json"
        offset = 0
        for _page in range(self.max_pages):
            params: dict[str, str | int] = {
                "$limit": self.page_size,
                "$offset": offset,
                "$order": order,
            }
            if where:
                params["$where"] = where
            batch = self._get(url, params)
            if not batch:
                return
            yield batch
            if len(batch) < self.page_size:
                return
            offset += self.page_size
        msg = f"{dataset_id}: exceeded max_pages={self.max_pages}"
        raise IngestError(msg)


def _parse_ts(value: object) -> datetime | None:
    if value is None:
        return None
    # pandas StringDtype/Float64Dtype use pd.NA which raises on truthy/eq checks.
    try:
        if pd.isna(value):  # type: ignore[call-overload]
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        # Some Colombian sources use slashes instead of dashes and pad
        # fractional seconds with extra digits (e.g. "2020/11/14 05:28:48.583000000").
        # Truncate nanoseconds to microseconds and try ISO again with the swap.
        normalized = raw.replace("Z", "+00:00")
        if "/" in normalized and "-" not in normalized.split("/", 1)[0]:
            normalized = normalized.replace("/", "-", 2)
        if "." in normalized:
            head, _, frac = normalized.rpartition(".")
            digits = frac[:6]
            tail = frac[len(digits):]
            # If trailing chars after frac are all digits (extra precision), drop them.
            if tail.isdigit():
                normalized = f"{head}.{digits}"
        parsed: datetime | None
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            parsed = _parse_dmy(raw)
        if parsed is None:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


_FALLBACK_FORMATS = (
    "%d/%m/%Y",         # DD/MM/YYYY (Colombian gov)
    "%d-%m-%Y",         # DD-MM-YYYY
    "%d/%m/%Y %H:%M:%S",
    "%Y%m%d",           # YYYYMMDD compact (e.g. SGR period codes)
    "%Y%m",             # YYYYMM
    "%Y",               # YYYY only (annual datasets like enrollment)
)


def _parse_dmy(raw: str) -> datetime | None:
    """Fallback for non-ISO date formats common in Colombian Socrata data."""
    for fmt in _FALLBACK_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _iso_for_where(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000")


def _rows_to_frame(rows: Iterable[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return frame
    # Socrata returns strings by default; keep them as strings and let the
    # caller parse watermark/partition columns. Cast to string dtype so
    # coverage's empty-string handling is consistent.
    return frame.astype("string")


def _assign_partitions(
    frame: pd.DataFrame, partition_column: str, *, require_parseable: bool = True
) -> tuple[pd.DataFrame, list[tuple[int, int]]]:
    """Assign year/month partitions; rows without a parseable partition_column
    are routed to the sentinel ``year=0/month=0`` partition so they survive
    incremental ingest. Rejecting them entirely would lose data permanently —
    Socrata's ``$where`` on a date column never returns rows whose value is
    NULL, so the next incremental run cannot re-pull them.
    """
    parsed = frame[partition_column].map(_parse_ts)
    if require_parseable and parsed.notna().sum() == 0:
        msg = (
            f"{partition_column} is unparseable for every row "
            f"({len(frame)}) — likely a column-name typo in the YAML"
        )
        raise IngestError(msg)
    years = parsed.map(lambda d: d.year if d is not None else 0).fillna(0).astype("int32")
    months = parsed.map(lambda d: d.month if d is not None else 0).fillna(0).astype("int32")
    out = frame.copy()
    out["__year"] = years
    out["__month"] = months
    partitions = sorted({(int(y), int(m)) for y, m in zip(years, months, strict=True)})
    return out, partitions


def _apply_columns_map(
    frame: pd.DataFrame, columns_map: dict[str, str]
) -> pd.DataFrame:
    """Rename source columns to canonical names per ``{canonical: source}``."""
    if not columns_map:
        return frame
    rename = {
        source: canonical
        for canonical, source in columns_map.items()
        if source in frame.columns
    }
    return frame.rename(columns=rename)


@dataclass(frozen=True)
class _StagedParquet:
    path: Path
    final_dir: Path


@dataclass
class _CoverageAccumulator:
    dataset_id: str
    required_coverage: dict[str, float]
    rows: int = 0
    non_null: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.non_null = {column: 0 for column in self.required_coverage}

    def update(self, frame: pd.DataFrame) -> None:
        self.rows += len(frame)
        for column in self.required_coverage:
            if column not in frame.columns:
                continue
            series = frame[column]
            present = series.notna() & (series.astype("string").str.len() > 0)
            self.non_null[column] += int(present.sum())

    def report(self) -> CoverageReport:
        coverage = {
            column: (float(self.non_null[column]) / float(self.rows) if self.rows else 0.0)
            for column in self.required_coverage
        }
        failures = {
            column: actual
            for column, actual in coverage.items()
            if actual < self.required_coverage[column]
        }
        return CoverageReport(
            dataset_id=self.dataset_id,
            rows=self.rows,
            coverage=coverage,
            thresholds=dict(self.required_coverage),
            failures=failures,
            checked_at=datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    def assert_pass(self) -> CoverageReport:
        report = self.report()
        if report.failures:
            write_failure_report(report)
            failures = {
                column: (actual, self.required_coverage[column])
                for column, actual in report.failures.items()
            }
            raise CoverageFailure(self.dataset_id, failures)
        return report


def _stage_root(batch_id: str) -> Path:
    return meta_path() / "ingest_staging" / batch_id


def _stage_incremental_dir(source: str, batch_id: str, year: int, month: int) -> Path:
    return (
        _stage_root(batch_id)
        / f"source={source}"
        / f"year={int(year)}"
        / f"month={int(month):02d}"
    )


def _stage_snapshot_dir(source: str, batch_id: str, snapshot: str) -> Path:
    return _stage_root(batch_id) / f"source={source}" / f"snapshot={snapshot}"


def _cleanup_stage(batch_id: str) -> None:
    shutil.rmtree(_stage_root(batch_id), ignore_errors=True)


def _finalize_staged(staged: list[_StagedParquet]) -> list[Path]:
    final_paths: list[Path] = []
    for item in staged:
        item.final_dir.mkdir(parents=True, exist_ok=True)
        final = item.final_dir / item.path.name
        if final.exists():
            final = item.final_dir / f"{item.path.stem}-{uuid.uuid4().hex[:8]}.parquet"
        item.path.rename(final)
        final_paths.append(final)
    return final_paths


def _parsed_timestamps(frame: pd.DataFrame, column: str) -> list[datetime | None]:
    if column not in frame.columns:
        return [None] * len(frame)
    return frame[column].map(_parse_ts).tolist()


def _stage_incremental_frame(
    spec: DatasetSpec,
    frame: pd.DataFrame,
    batch_id: str,
) -> tuple[list[tuple[int, int]], list[_StagedParquet]]:
    assert spec.partition_column is not None
    frame, partitions = _assign_partitions(
        frame, spec.partition_column, require_parseable=False
    )

    sort_cols: list[str] = []
    if spec.watermark_column and spec.watermark_column in frame.columns:
        sort_cols.append(spec.watermark_column)
    sort_cols.extend(column for column in (":id", "id") if column in frame.columns)
    if sort_cols:
        frame = frame.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    renamed = _apply_columns_map(
        frame.drop(columns=["__year", "__month"]), spec.columns_map
    )
    renamed["__year"] = frame["__year"].values
    renamed["__month"] = frame["__month"].values

    staged: list[_StagedParquet] = []
    for year, month in partitions:
        mask = (renamed["__year"] == year) & (renamed["__month"] == month)
        chunk = renamed.loc[mask].drop(columns=["__year", "__month"])
        if chunk.empty:
            continue
        path = write_parquet_to_dir(
            chunk,
            _stage_incremental_dir(spec.id, batch_id, year, month),
        )
        staged.append(
            _StagedParquet(path=path, final_dir=raw_path(spec.id, year, month))
        )
    return partitions, staged


def ingest(
    spec: DatasetSpec,
    *,
    client: SocrataClient | None = None,
    full_refresh: bool = False,
    page_size: int | None = None,
    max_pages: int | None = None,
) -> IngestResult:
    """Run one ingest pass for ``spec`` against Socrata.

    - Loads last watermark from ``lakehouse.watermark``.
    - Queries Socrata with ``$where`` + ``$order`` on ``spec.watermark_column``.
    - Enforces ``spec.required_coverage``; on fail, writes a failure report
      under ``lake/meta/failures/<id>/`` and raises without advancing the
      watermark.
    - Writes parquet to ``lake/raw/source=<id>/year=YYYY/month=MM/`` via
      ``lakehouse.writer.append_parquet``.
    - Advances the watermark to ``max(batch[watermark_column])`` — never to
      wall-clock ``now()``.
    """
    if not spec.is_ingest_ready():
        msg = (
            f"{spec.id} is not ingest-ready: needs columns_map plus either "
            f"(watermark_column + partition_column) for incremental mode "
            f"or (full_refresh_only=True) for snapshot mode (got "
            f"watermark_column={spec.watermark_column!r}, "
            f"partition_column={spec.partition_column!r}, "
            f"full_refresh_only={spec.full_refresh_only}, "
            f"columns_map_keys={list(spec.columns_map)!r})"
        )
        raise IngestError(msg)

    started_at = datetime.now(tz=UTC)
    batch_id = uuid.uuid4().hex
    owned_client = False
    if client is None:
        client = SocrataClient.from_env(page_size=page_size, max_pages=max_pages)
        owned_client = True

    try:
        if spec.full_refresh_only:
            return _ingest_snapshot(spec, client, started_at, batch_id)

        # is_ingest_ready guarantees these are non-None for incremental mode.
        assert spec.watermark_column is not None
        assert spec.partition_column is not None

        current = wm.get(spec.id) if not full_refresh else None
        where = (
            f"{spec.watermark_column} > '{_iso_for_where(current.last_seen_ts)}'"
            if current is not None
            else None
        )
        order = f"{spec.watermark_column} ASC, :id ASC"

        rows = 0
        parseable_count = 0
        unparseable = 0
        max_ts: datetime | None = None
        partitions_seen: set[tuple[int, int]] = set()
        staged: list[_StagedParquet] = []
        parquet_paths: list[Path] = []
        coverage: CoverageReport | None = None
        delta: wm.WatermarkDelta | None = None
        coverage_acc = _CoverageAccumulator(spec.id, spec.required_coverage)

        try:
            for page in client.fetch(spec.id, where=where, order=order):
                frame = _rows_to_frame(page)
                if frame.empty:
                    continue

                rows += len(frame)
                coverage_acc.update(frame)

                parsed_wm = _parsed_timestamps(frame, spec.watermark_column)
                page_parseable = [ts for ts in parsed_wm if ts is not None]
                parseable_count += len(page_parseable)
                unparseable += len(parsed_wm) - len(page_parseable)
                if page_parseable:
                    page_max = max(page_parseable)
                    max_ts = page_max if max_ts is None else max(max_ts, page_max)

                partitions, page_staged = _stage_incremental_frame(
                    spec, frame, batch_id
                )
                partitions_seen.update(partitions)
                staged.extend(page_staged)

            if rows == 0:
                _cleanup_stage(batch_id)
                LOG.info("ingest %s: no new rows since %s", spec.id, current)
                return IngestResult(
                    dataset_id=spec.id,
                    rows=0,
                    partitions=[],
                    parquet_paths=[],
                    watermark_delta=None,
                    coverage=None,
                    batch_id=batch_id,
                    started_at=started_at,
                    finished_at=datetime.now(tz=UTC),
                    skipped_reason="no_new_rows",
                )

            coverage = coverage_acc.assert_pass()
            write_coverage_report(coverage)

            # Rows with unparseable watermark survive in sentinel partitions,
            # but at least one parseable row is required to advance safely.
            if parseable_count == 0 or max_ts is None:
                msg = (
                    f"{spec.id}: zero rows have a parseable "
                    f"{spec.watermark_column!r} — likely a column-name typo"
                )
                raise IngestError(msg)
            if unparseable:
                LOG.warning(
                    "ingest %s: %s of %s rows have unparseable %s and will land "
                    "in the year=0/month=0 sentinel partition",
                    spec.id,
                    unparseable,
                    rows,
                    spec.watermark_column,
                )

            parquet_paths = _finalize_staged(staged)
            delta = wm.advance(
                source=spec.id,
                rows=rows,
                batch_id=batch_id,
                last_seen_ts=max_ts,
                force=full_refresh,
            )
        except Exception:
            _cleanup_stage(batch_id)
            raise
        else:
            _cleanup_stage(batch_id)

        finished_at = datetime.now(tz=UTC)
        partitions = sorted(partitions_seen)
        assert max_ts is not None
        assert coverage is not None
        assert delta is not None
        LOG.info(
            "ingest %s: %s rows across %s partitions (watermark %s)",
            spec.id,
            rows,
            len(partitions),
            max_ts.isoformat(),
        )
        return IngestResult(
            dataset_id=spec.id,
            rows=rows,
            partitions=partitions,
            parquet_paths=parquet_paths,
            watermark_delta=delta,
            coverage=coverage,
            batch_id=batch_id,
            started_at=started_at,
            finished_at=finished_at,
        )
    finally:
        if owned_client:
            client.close()


def _ingest_snapshot(
    spec: DatasetSpec,
    client: SocrataClient,
    started_at: datetime,
    batch_id: str,
) -> IngestResult:
    """Snapshot-mode ingest for ``full_refresh_only`` datasets.

    Pulls every row in ``:id`` order, enforces coverage, writes to
    ``lake/raw/source=<id>/snapshot=YYYYMMDDTHHMMSSZ/``. No watermark is
    advanced — these datasets have no row-level timestamp.
    """
    snapshot_id = started_at.strftime("%Y%m%dT%H%M%SZ")

    rows = 0
    staged: list[_StagedParquet] = []
    parquet_paths: list[Path] = []
    coverage: CoverageReport | None = None
    coverage_acc = _CoverageAccumulator(spec.id, spec.required_coverage)

    try:
        for page in client.fetch(spec.id, where=None, order=":id ASC"):
            frame = _rows_to_frame(page)
            if frame.empty:
                continue
            rows += len(frame)
            coverage_acc.update(frame)

            sort_cols = [column for column in (":id", "id") if column in frame.columns]
            if sort_cols:
                frame = frame.sort_values(sort_cols, kind="mergesort").reset_index(
                    drop=True
                )
            renamed = _apply_columns_map(frame, spec.columns_map)
            path = write_parquet_to_dir(
                renamed,
                _stage_snapshot_dir(spec.id, batch_id, snapshot_id),
            )
            staged.append(
                _StagedParquet(
                    path=path,
                    final_dir=raw_snapshot_path(spec.id, snapshot_id),
                )
            )

        if rows == 0:
            _cleanup_stage(batch_id)
            LOG.info("ingest %s: snapshot returned 0 rows", spec.id)
            return IngestResult(
                dataset_id=spec.id,
                rows=0,
                partitions=[],
                parquet_paths=[],
                watermark_delta=None,
                coverage=None,
                batch_id=batch_id,
                started_at=started_at,
                finished_at=datetime.now(tz=UTC),
                skipped_reason="empty_snapshot",
            )

        coverage = coverage_acc.assert_pass()
        write_coverage_report(coverage)
        parquet_paths = _finalize_staged(staged)
    except Exception:
        _cleanup_stage(batch_id)
        raise
    else:
        _cleanup_stage(batch_id)

    finished_at = datetime.now(tz=UTC)
    LOG.info(
        "ingest %s: snapshot %s wrote %s rows",
        spec.id,
        snapshot_id,
        rows,
    )
    return IngestResult(
        dataset_id=spec.id,
        rows=rows,
        partitions=[],
        parquet_paths=parquet_paths,
        watermark_delta=None,
        coverage=coverage,
        batch_id=batch_id,
        started_at=started_at,
        finished_at=finished_at,
    )
