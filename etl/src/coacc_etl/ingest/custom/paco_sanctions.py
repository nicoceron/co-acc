"""PACO public sanctions adapter.

PACO publishes a small bundle of public CSV/ZIP files rather than a Socrata
API. This adapter snapshots those files into the raw lake with a minimal
canonical overlay so later DuckDB curation can join by document/NIT without
loading the whole source into memory.
"""

from __future__ import annotations

import json
import logging
import math
import re
import shutil
import unicodedata
import uuid
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

import httpx
import pandas as pd

from coacc_etl.ingest.coverage import (
    CoverageFailure,
    CoverageReport,
    write_coverage_report,
    write_failure_report,
)
from coacc_etl.ingest.socrata import IngestError, IngestResult
from coacc_etl.lakehouse.paths import meta_path, raw_snapshot_path
from coacc_etl.lakehouse.writer import write_parquet_to_dir

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from coacc_etl.catalog import DatasetSpec

LOG = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 50_000
DEFAULT_TIMEOUT = 600.0


@dataclass(frozen=True)
class PacoFeed:
    name: str
    url: str
    zipped: bool
    columns: tuple[str, ...]
    canonical: dict[str, str]


PACO_FEEDS: tuple[PacoFeed, ...] = (
    PacoFeed(
        name="antecedentes_siri_sanciones",
        url=(
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/antecedentes_SIRI_sanciones_Cleaned.zip"
        ),
        zipped=True,
        columns=(
            "paco_record_id",
            "source_domain",
            "subject_kind",
            "document_type_code",
            "document_type",
            "subject_document_id",
            "last_name_1",
            "last_name_2",
            "first_name_1",
            "first_name_2",
            "role",
            "department",
            "municipality",
            "sanction_type",
            "duration_years",
            "duration_months",
            "duration_days",
            "instance",
            "issuing_authority",
            "sanction_date",
            "reference",
            "affected_entity",
            "affected_department",
            "affected_municipality",
            "sanction_year",
            "sanction_month",
            "sanction_day",
            "duration_text",
        ),
        canonical={
            "subject_document_id": "subject_document_id",
            "subject_name": "first_name_1",
            "subject_name_extra": "first_name_2",
            "subject_last_name": "last_name_1",
            "subject_last_name_extra": "last_name_2",
            "subject_type": "subject_kind",
            "sanction_type": "sanction_type",
            "sanction_date": "sanction_date",
            "reference": "reference",
            "affected_entity": "affected_entity",
        },
    ),
    PacoFeed(
        name="colusiones_en_contratacion",
        url=(
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/colusiones_en_contratacion_SIC.csv"
        ),
        zipped=False,
        columns=(),
        canonical={
            "subject_document_id": "Identificacion",
            "subject_name": "Personas Sancionadas",
            "subject_type": "Tipo de Persona Sancionada",
            "sanction_type": "Falta que origina la sancion",
            "sanction_date": "Fecha de Radicacion",
            "reference": "Radicado",
            "amount": "Multa Inicial",
        },
    ),
    PacoFeed(
        name="multas_secop",
        url=(
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/multas_SECOP_Cleaned.zip"
        ),
        zipped=True,
        columns=(
            "buyer_entity",
            "buyer_document_id",
            "buyer_region",
            "buyer_order",
            "reference",
            "subject_document_id",
            "subject_name",
            "contract_id",
            "amount",
            "sanction_date",
            "record_url",
            "raw_11",
            "raw_12",
            "department",
            "municipality",
        ),
        canonical={
            "subject_document_id": "subject_document_id",
            "subject_name": "subject_name",
            "subject_type": "buyer_order",
            "sanction_type": "reference",
            "sanction_date": "sanction_date",
            "reference": "reference",
            "contract_id": "contract_id",
            "amount": "amount",
            "affected_entity": "buyer_entity",
        },
    ),
    PacoFeed(
        name="responsabilidades_fiscales",
        url=(
            "https://paco7public7info7prod.blob.core.windows.net/"
            "paco-pulic-info/responsabilidades_fiscales.csv"
        ),
        zipped=False,
        columns=(),
        canonical={
            "subject_document_id": "Tipo y Num Docuemento",
            "subject_name": "Responsable Fiscal",
            "subject_type": "TR",
            "sanction_type": "R",
            "reference": "Ente que Reporta",
            "affected_entity": "Entidad Afectada",
        },
    ),
)


def ingest(
    spec: DatasetSpec,
    *,
    full_refresh: bool = False,
    chunk_size: int | None = None,
    timeout: float | None = None,
    feeds: Iterable[PacoFeed] = PACO_FEEDS,
) -> IngestResult:
    """Snapshot PACO feeds into ``lake/raw/source=paco_sanctions``."""
    if spec.adapter != "paco_sanctions":
        msg = f"{spec.id}: paco_sanctions adapter cannot ingest adapter={spec.adapter!r}"
        raise IngestError(msg)
    if not spec.full_refresh_only:
        msg = f"{spec.id}: paco_sanctions adapter requires full_refresh_only=true"
        raise IngestError(msg)
    if not spec.is_ingest_ready():
        msg = f"{spec.id}: paco_sanctions adapter requires columns_map"
        raise IngestError(msg)
    if not full_refresh:
        LOG.info("%s: PACO is snapshot-only; running full snapshot ingest", spec.id)

    started_at = datetime.now(tz=UTC)
    snapshot_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    batch_id = uuid.uuid4().hex
    size = chunk_size or DEFAULT_CHUNK_SIZE
    request_timeout = timeout or DEFAULT_TIMEOUT
    stage_dir = _stage_snapshot_dir(spec.id, batch_id, snapshot_id)
    download_dir = _stage_download_dir(batch_id)
    coverage = _CoverageAccumulator(spec.id, spec.required_coverage)
    staged_paths: list[Path] = []

    try:
        for feed in feeds:
            source_path = _download_feed(feed, download_dir, timeout=request_timeout)
            for chunk in _read_feed_chunks(source_path, feed, chunk_size=size):
                normalized = _normalize_chunk(chunk, feed)
                if normalized.empty:
                    continue
                coverage.update(normalized)
                staged_paths.append(write_parquet_to_dir(normalized, stage_dir))

        if coverage.rows == 0:
            _cleanup_stage(batch_id)
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

        report = coverage.assert_pass()
        write_coverage_report(report)
        final_paths = _finalize_snapshot(staged_paths, spec.id, snapshot_id)
    except Exception:
        _cleanup_stage(batch_id)
        raise
    else:
        _cleanup_stage(batch_id)

    finished_at = datetime.now(tz=UTC)
    LOG.info(
        "ingest %s: PACO snapshot %s wrote %s rows",
        spec.id,
        snapshot_id,
        coverage.rows,
    )
    return IngestResult(
        dataset_id=spec.id,
        rows=coverage.rows,
        partitions=[],
        parquet_paths=final_paths,
        watermark_delta=None,
        coverage=report,
        batch_id=batch_id,
        started_at=started_at,
        finished_at=finished_at,
    )


def _stage_root(batch_id: str) -> Path:
    return meta_path() / "ingest_staging" / batch_id


def _stage_snapshot_dir(source: str, batch_id: str, snapshot: str) -> Path:
    return _stage_root(batch_id) / f"source={source}" / f"snapshot={snapshot}"


def _stage_download_dir(batch_id: str) -> Path:
    return _stage_root(batch_id) / "downloads"


def _cleanup_stage(batch_id: str) -> None:
    shutil.rmtree(_stage_root(batch_id), ignore_errors=True)


def _download_feed(feed: PacoFeed, download_dir: Path, *, timeout: float) -> Path:
    download_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".zip" if feed.zipped else ".csv"
    downloaded = download_dir / f"{feed.name}{suffix}"
    _download_file(feed.url, downloaded, timeout=timeout)
    if not feed.zipped:
        return downloaded

    extract_dir = download_dir / f"{feed.name}_extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(downloaded) as archive:
        candidates = [name for name in archive.namelist() if not name.endswith("/")]
        if not candidates:
            msg = f"{feed.name}: PACO archive contained no files"
            raise IngestError(msg)
        archive.extract(candidates[0], extract_dir)
        return extract_dir / candidates[0]


def _download_file(url: str, target: Path, *, timeout: float) -> None:
    parsed = urlparse(url)
    if parsed.scheme == "file":
        shutil.copyfile(Path(unquote(parsed.path)), target)
        return

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_bytes():
                if chunk:
                    handle.write(chunk)


def _read_feed_chunks(path: Path, feed: PacoFeed, *, chunk_size: int) -> Iterator[pd.DataFrame]:
    if feed.columns:
        reader = pd.read_csv(
            path,
            chunksize=chunk_size,
            dtype="string",
            encoding="utf-8-sig",
            header=None,
            names=list(feed.columns),
        )
    else:
        reader = pd.read_csv(
            path,
            chunksize=chunk_size,
            dtype="string",
            encoding="utf-8-sig",
        )
    yield from reader


def _normalize_chunk(frame: pd.DataFrame, feed: PacoFeed) -> pd.DataFrame:
    frame = frame.astype("string")
    out = pd.DataFrame(index=frame.index)
    out["source_id"] = "paco_sanctions"
    out["paco_feed"] = feed.name
    out["source_url"] = feed.url
    out["subject_document_id"] = _canonical_series(frame, feed, "subject_document_id")
    out["subject_name"] = _subject_name(frame, feed)
    out["subject_type"] = _canonical_series(frame, feed, "subject_type")
    out["sanction_type"] = _canonical_series(frame, feed, "sanction_type")
    out["sanction_date"] = _canonical_series(frame, feed, "sanction_date")
    out["reference"] = _canonical_series(frame, feed, "reference")
    out["contract_id"] = _canonical_series(frame, feed, "contract_id")
    out["amount"] = _canonical_series(frame, feed, "amount")
    out["affected_entity"] = _canonical_series(frame, feed, "affected_entity")
    out["raw_record_json"] = _raw_record_json(frame)

    for column in frame.columns:
        raw_name = f"raw_{_slug(str(column))}"
        if raw_name not in out.columns:
            out[raw_name] = frame[column]
    return out.reset_index(drop=True).astype("string")


def _raw_record_json(frame: pd.DataFrame) -> pd.Series:
    raw_records = [
        {str(key): _json_safe_value(value) for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    return pd.Series(
        [json.dumps(record, ensure_ascii=False, sort_keys=True) for record in raw_records],
        index=frame.index,
        dtype="string",
    )


def _json_safe_value(value: object) -> object | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _canonical_series(frame: pd.DataFrame, feed: PacoFeed, key: str) -> pd.Series:
    column = feed.canonical.get(key)
    if column and column in frame.columns:
        return frame[column]
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="string")


def _subject_name(frame: pd.DataFrame, feed: PacoFeed) -> pd.Series:
    primary = _canonical_series(frame, feed, "subject_name")
    parts = [primary]
    for key in ("subject_name_extra", "subject_last_name", "subject_last_name_extra"):
        column = feed.canonical.get(key)
        if column and column in frame.columns:
            parts.append(frame[column])
    if len(parts) == 1:
        return primary
    combined = parts[0].fillna("")
    for part in parts[1:]:
        combined = (combined + " " + part.fillna("")).str.strip()
    return combined.replace("", pd.NA)


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", normalized).strip("_").lower()
    return slug or "column"


def _finalize_snapshot(staged_paths: list[Path], source: str, snapshot: str) -> list[Path]:
    final_dir = raw_snapshot_path(source, snapshot)
    final_dir.mkdir(parents=True, exist_ok=True)
    final_paths: list[Path] = []
    for path in staged_paths:
        final = final_dir / path.name
        if final.exists():
            final = final_dir / f"{path.stem}-{uuid.uuid4().hex[:8]}.parquet"
        path.rename(final)
        final_paths.append(final)
    return final_paths


@dataclass
class _CoverageAccumulator:
    dataset_id: str
    required_coverage: dict[str, float]
    rows: int = 0
    non_null: dict[str, int] | None = None

    def __post_init__(self) -> None:
        self.non_null = {column: 0 for column in self.required_coverage}

    def update(self, frame: pd.DataFrame) -> None:
        self.rows += len(frame)
        assert self.non_null is not None
        for column in self.required_coverage:
            if column not in frame.columns:
                continue
            series = frame[column]
            present = series.notna() & (series.astype("string").str.len() > 0)
            self.non_null[column] += int(present.sum())

    def assert_pass(self) -> CoverageReport:
        assert self.non_null is not None
        coverage = {
            column: (float(self.non_null[column]) / float(self.rows) if self.rows else 0.0)
            for column in self.required_coverage
        }
        failures = {
            column: actual
            for column, actual in coverage.items()
            if actual < self.required_coverage[column]
        }
        report = CoverageReport(
            dataset_id=self.dataset_id,
            rows=self.rows,
            coverage=coverage,
            thresholds=dict(self.required_coverage),
            failures=failures,
            checked_at=datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        if failures:
            typed_failures = {
                column: (actual, self.required_coverage[column])
                for column, actual in failures.items()
            }
            write_failure_report(report)
            raise CoverageFailure(self.dataset_id, typed_failures)
        return report
