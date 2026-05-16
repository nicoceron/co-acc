"""Phase 7 ingest orchestration.

This module turns the post-refactor plan's manual Phase 7 checklist into a
bounded operator command. It intentionally keeps the orchestration small:
dataset sequencing, disk safety, smoke watermark seeding, ingest invocation,
and append-only run logging.
"""
from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from coacc_etl.catalog import DatasetSpec, load_catalog
from coacc_etl.ingest.socrata import IngestResult, SocrataClient, _parse_ts, ingest
from coacc_etl.lakehouse import watermark as wm
from coacc_etl.lakehouse.paths import lake_root

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

Phase7Mode = Literal["smoke", "full"]

PHASE7_DATASET_IDS: tuple[str, ...] = (
    "2jzx-383z",
    "jbjy-vk9h",
    "qddk-cgux",
    "p6dx-8zbt",
    "c82u-588k",
    "rpmr-utcd",
    "wi7w-2nvm",
)

DEFAULT_SMOKE_MIN_FREE_GB = 1.0
DEFAULT_FULL_MIN_FREE_GB = 80.0
DEFAULT_SMOKE_PAGE_SIZE = 1_000
DEFAULT_SMOKE_MAX_PAGES = 5
DEFAULT_FULL_PAGE_SIZE = 10_000
DEFAULT_FULL_MAX_PAGES = 10_000
DEFAULT_SMOKE_DAYS = 7

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_LOG_PATH = _REPO_ROOT / "docs" / "runbooks" / "ingest_log.md"

_SMOKE_FALLBACK_SINCE: dict[str, datetime] = {
    "2jzx-383z": datetime(2025, 12, 10, tzinfo=UTC),
    "jbjy-vk9h": datetime(2026, 4, 27, tzinfo=UTC),
    "qddk-cgux": datetime(2017, 12, 24, tzinfo=UTC),
    "p6dx-8zbt": datetime(2026, 5, 6, tzinfo=UTC),
    "c82u-588k": datetime(2026, 4, 27, tzinfo=UTC),
    "rpmr-utcd": datetime(2026, 4, 20, tzinfo=UTC),
    "wi7w-2nvm": datetime(2026, 4, 20, tzinfo=UTC),
    "7y2j-43cv": datetime(2025, 3, 28, tzinfo=UTC),
}


class DiskBudgetError(RuntimeError):
    """Raised when the lake root does not have enough free space."""


class Phase7RunError(RuntimeError):
    """Raised after a dataset fails and the runner is configured to stop."""

    def __init__(self, record: Phase7RunRecord) -> None:
        super().__init__(f"{record.dataset_id}: {record.note}")
        self.record = record


@dataclass(frozen=True)
class DiskStatus:
    path: Path
    free_bytes: int
    required_bytes: int

    @property
    def free_gb(self) -> float:
        return self.free_bytes / 1024**3

    @property
    def required_gb(self) -> float:
        return self.required_bytes / 1024**3


@dataclass(frozen=True)
class Phase7RunRecord:
    dataset_id: str
    mode: Phase7Mode
    status: Literal["ok", "skipped", "failed"]
    started_at: datetime
    finished_at: datetime
    rows: int
    coverage: str
    watermark: str
    note: str


def default_log_path() -> Path:
    return _DEFAULT_LOG_PATH


def check_disk_budget(min_free_gb: float, *, path: Path | None = None) -> DiskStatus:
    root = path or lake_root()
    root.mkdir(parents=True, exist_ok=True)
    required = int(min_free_gb * 1024**3)
    usage = shutil.disk_usage(root)
    status = DiskStatus(path=root, free_bytes=usage.free, required_bytes=required)
    if status.free_bytes < status.required_bytes:
        msg = (
            f"{root} has {status.free_gb:.1f} GiB free; "
            f"{status.required_gb:.1f} GiB required"
        )
        raise DiskBudgetError(msg)
    return status


def fetch_max_watermark(spec: DatasetSpec, client: SocrataClient) -> object | None:
    if spec.watermark_column is None:
        return None
    url = f"https://{client.domain}/resource/{spec.id}.json"
    payload = client._get(url, {"$select": f"max({spec.watermark_column})"})
    if not payload:
        return None
    row = payload[0]
    if not row:
        return None
    return next(iter(row.values()))


def smoke_seed_watermark(
    spec: DatasetSpec,
    *,
    smoke_days: int = DEFAULT_SMOKE_DAYS,
    max_lookup: Callable[[DatasetSpec], object | None] | None = None,
) -> str:
    existing = wm.get(spec.id)
    if existing is not None:
        return f"smoke: using existing watermark {existing.last_seen_ts.isoformat()}"

    max_value = max_lookup(spec) if max_lookup is not None else None
    parsed_max = _parse_ts(max_value)
    now = datetime.now(tz=UTC)
    if parsed_max is not None and 2000 <= parsed_max.year <= now.year + 1:
        since = parsed_max - timedelta(days=smoke_days)
        source = f"max({spec.watermark_column})"
    else:
        since = _SMOKE_FALLBACK_SINCE.get(spec.id, now - timedelta(days=smoke_days))
        source = "fallback"

    wm.set(
        wm.Watermark(
            source=spec.id,
            last_seen_ts=since,
            last_batch_id=f"phase7-smoke-seed-{uuid.uuid4().hex[:8]}",
            row_count=0,
        ),
        force=True,
    )
    return f"smoke: seeded watermark {since.isoformat()} from {source}"


def append_ingest_log(record: Phase7RunRecord, *, log_path: Path | None = None) -> None:
    path = log_path or default_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(
            "# Phase 7 ingest log\n\n"
            "| finished_at_utc | dataset | mode | status | rows | coverage | "
            "watermark | note |\n"
            "|---|---|---|---:|---:|---|---|---|\n",
            encoding="utf-8",
        )
    note = record.note.replace("|", "/").replace("\r", " ").replace("\n", " ")
    line = (
        f"| {record.finished_at.astimezone(UTC).isoformat()} "
        f"| `{record.dataset_id}` | {record.mode} | {record.status} "
        f"| {record.rows} | {record.coverage} | {record.watermark} "
        f"| {note} |\n"
    )
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def _coverage_summary(result: IngestResult) -> str:
    if result.coverage is None:
        return "-"
    if result.coverage.failures:
        return "fail"
    return "pass"


def _watermark_summary(result: IngestResult) -> str:
    if result.watermark_delta is not None:
        return result.watermark_delta.last_seen_ts.isoformat()
    return "-"


def _dataset_ids(dataset_ids: Iterable[str] | None) -> tuple[str, ...]:
    if dataset_ids is None:
        return PHASE7_DATASET_IDS
    ids = tuple(dataset_ids)
    return ids or PHASE7_DATASET_IDS


def run_phase7(
    *,
    mode: Phase7Mode,
    dataset_ids: Iterable[str] | None = None,
    continue_on_error: bool = False,
    min_free_gb: float | None = None,
    page_size: int | None = None,
    max_pages: int | None = None,
    smoke_days: int = DEFAULT_SMOKE_DAYS,
    log_path: Path | None = None,
    ingest_fn: Callable[..., IngestResult] = ingest,
    max_lookup: Callable[[DatasetSpec], object | None] | None = None,
) -> list[Phase7RunRecord]:
    min_free = (
        min_free_gb
        if min_free_gb is not None
        else (DEFAULT_FULL_MIN_FREE_GB if mode == "full" else DEFAULT_SMOKE_MIN_FREE_GB)
    )
    check_disk_budget(min_free)

    specs = load_catalog()
    ids = _dataset_ids(dataset_ids)
    records: list[Phase7RunRecord] = []

    owned_client: SocrataClient | None = None
    if mode == "smoke" and max_lookup is None:
        owned_client = SocrataClient.from_env()
        max_lookup = lambda spec: fetch_max_watermark(spec, owned_client)  # noqa: E731

    try:
        for dataset_id in ids:
            started = datetime.now(tz=UTC)
            note = ""
            try:
                spec = specs[dataset_id]
                if not spec.is_ingest_ready():
                    raise ValueError(f"{dataset_id} is not ingest-ready")
                if mode == "smoke":
                    note = smoke_seed_watermark(
                        spec,
                        smoke_days=smoke_days,
                        max_lookup=max_lookup,
                    )
                    full_refresh = False
                    resolved_page_size = page_size or DEFAULT_SMOKE_PAGE_SIZE
                    resolved_max_pages = max_pages or DEFAULT_SMOKE_MAX_PAGES
                else:
                    full_refresh = True
                    resolved_page_size = page_size or DEFAULT_FULL_PAGE_SIZE
                    resolved_max_pages = max_pages or DEFAULT_FULL_MAX_PAGES

                result = ingest_fn(
                    spec,
                    full_refresh=full_refresh,
                    page_size=resolved_page_size,
                    max_pages=resolved_max_pages,
                )
                status: Literal["ok", "skipped", "failed"] = (
                    "ok" if result.ingested else "skipped"
                )
                if result.skipped_reason:
                    note = f"{note}; {result.skipped_reason}" if note else result.skipped_reason
                record = Phase7RunRecord(
                    dataset_id=dataset_id,
                    mode=mode,
                    status=status,
                    started_at=started,
                    finished_at=datetime.now(tz=UTC),
                    rows=result.rows,
                    coverage=_coverage_summary(result),
                    watermark=_watermark_summary(result),
                    note=note or "-",
                )
            except Exception as exc:  # noqa: BLE001 - operator log must record any failure.
                record = Phase7RunRecord(
                    dataset_id=dataset_id,
                    mode=mode,
                    status="failed",
                    started_at=started,
                    finished_at=datetime.now(tz=UTC),
                    rows=0,
                    coverage="-",
                    watermark="-",
                    note=str(exc),
                )
                records.append(record)
                append_ingest_log(record, log_path=log_path)
                if not continue_on_error:
                    raise Phase7RunError(record) from exc
                continue

            records.append(record)
            append_ingest_log(record, log_path=log_path)
    finally:
        if owned_client is not None:
            owned_client.close()

    return records
