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
    CoverageReport,
    assert_coverage,
    write_coverage_report,
)
from coacc_etl.lakehouse import watermark as wm
from coacc_etl.lakehouse.writer import append_parquet

if TYPE_CHECKING:
    from pathlib import Path

    from coacc_etl.catalog import DatasetSpec

LOG = logging.getLogger(__name__)

DEFAULT_DOMAIN = "www.datos.gov.co"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_MAX_PAGES = 10_000
DEFAULT_TIMEOUT = 60.0


class IngestError(RuntimeError):
    """Raised for unrecoverable ingest failures (HTTP, schema, watermark)."""


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
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> SocrataClient:
        return cls(
            http=_build_default_client(timeout=timeout),
            domain=domain,
            page_size=page_size,
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
        if pd.isna(value):  # type: ignore[arg-type]
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
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
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
    frame: pd.DataFrame, partition_column: str
) -> tuple[pd.DataFrame, list[tuple[int, int]]]:
    """Assign year/month partitions; rows without a parseable partition_column
    are routed to the sentinel ``year=0/month=0`` partition so they survive
    incremental ingest. Rejecting them entirely would lose data permanently —
    Socrata's ``$where`` on a date column never returns rows whose value is
    NULL, so the next incremental run cannot re-pull them.
    """
    parsed = frame[partition_column].map(_parse_ts)
    if parsed.notna().sum() == 0:
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


def ingest(
    spec: DatasetSpec,
    *,
    client: SocrataClient | None = None,
    full_refresh: bool = False,
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
            f"{spec.id} is not ingest-ready: needs tier=core, watermark_column,"
            f" partition_column, and columns_map (got tier={spec.tier!r},"
            f" watermark_column={spec.watermark_column!r},"
            f" partition_column={spec.partition_column!r},"
            f" columns_map_keys={list(spec.columns_map)!r})"
        )
        raise IngestError(msg)
    # is_ingest_ready guarantees these are non-None, narrow for the type checker.
    assert spec.watermark_column is not None
    assert spec.partition_column is not None

    started_at = datetime.now(tz=UTC)
    batch_id = uuid.uuid4().hex
    owned_client = False
    if client is None:
        client = SocrataClient.from_env()
        owned_client = True

    try:
        current = wm.get(spec.id) if not full_refresh else None
        where = (
            f"{spec.watermark_column} > '{_iso_for_where(current.last_seen_ts)}'"
            if current is not None
            else None
        )
        order = f"{spec.watermark_column} ASC, :id ASC"

        collected: list[dict[str, object]] = []
        for page in client.fetch(spec.id, where=where, order=order):
            collected.extend(page)

        if not collected:
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

        frame = _rows_to_frame(collected)

        coverage = assert_coverage(spec.id, frame, spec.required_coverage)
        write_coverage_report(coverage)

        # Parse watermark column — we need the max BEFORE column rename.
        # Rows with unparseable watermark survive (see _assign_partitions) but
        # don't contribute to max(); if every row is unparseable, refuse to
        # advance because the column is almost certainly misnamed.
        parsed_wm = frame[spec.watermark_column].map(_parse_ts)
        parseable = [ts for ts in parsed_wm.tolist() if ts is not None]
        if not parseable:
            msg = (
                f"{spec.id}: zero rows have a parseable "
                f"{spec.watermark_column!r} — likely a column-name typo"
            )
            raise IngestError(msg)
        unparseable = len(parsed_wm) - len(parseable)
        if unparseable:
            LOG.warning(
                "ingest %s: %s of %s rows have unparseable %s and will land in "
                "the year=0/month=0 sentinel partition",
                spec.id,
                unparseable,
                len(parsed_wm),
                spec.watermark_column,
            )
        max_ts = max(parseable)

        frame, partitions = _assign_partitions(frame, spec.partition_column)

        # Deterministic sort: by watermark column asc, then :id if present.
        sort_cols = [spec.watermark_column]
        if ":id" in frame.columns:
            sort_cols.append(":id")
        elif "id" in frame.columns:
            sort_cols.append("id")
        frame = frame.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

        renamed = _apply_columns_map(
            frame.drop(columns=["__year", "__month"]), spec.columns_map
        )
        renamed["__year"] = frame["__year"].values
        renamed["__month"] = frame["__month"].values

        parquet_paths: list[Path] = []
        for year, month in partitions:
            mask = (renamed["__year"] == year) & (renamed["__month"] == month)
            chunk = renamed.loc[mask].drop(columns=["__year", "__month"])
            if chunk.empty:
                continue
            path = append_parquet(chunk, source=spec.id, year=year, month=month)
            parquet_paths.append(path)

        delta = wm.advance(
            source=spec.id,
            rows=len(frame),
            batch_id=batch_id,
            last_seen_ts=max_ts,
            force=full_refresh,
        )

        finished_at = datetime.now(tz=UTC)
        LOG.info(
            "ingest %s: %s rows across %s partitions (watermark %s)",
            spec.id,
            len(frame),
            len(partitions),
            max_ts.isoformat(),
        )
        return IngestResult(
            dataset_id=spec.id,
            rows=len(frame),
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
