"""Coverage gate for ingested batches.

Fails loud when any tracked column falls below its declared non-null ratio.
Callers are expected to block watermark advancement on failure and write the
failure report returned by :func:`write_failure_report` to
``lake/meta/failures/<id>/<iso_ts>.json`` for auditability.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from coacc_etl.lakehouse.paths import meta_path

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd


class CoverageFailure(RuntimeError):  # noqa: N818  # historical name — "Failure" reads cleaner at call sites than "Error"
    """Raised when one or more required columns are under their threshold."""

    def __init__(self, dataset_id: str, failures: dict[str, tuple[float, float]]) -> None:
        self.dataset_id = dataset_id
        self.failures = failures
        details = ", ".join(
            f"{col}: {actual:.3f} < {threshold:.3f}"
            for col, (actual, threshold) in sorted(failures.items())
        )
        super().__init__(f"{dataset_id}: coverage below threshold — {details}")


@dataclass(frozen=True)
class CoverageReport:
    dataset_id: str
    rows: int
    coverage: dict[str, float]
    thresholds: dict[str, float]
    failures: dict[str, float]
    checked_at: str


def _non_null_ratio(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return 0.0
    series = frame[column]
    total = len(series)
    if total == 0:
        return 0.0
    # Socrata often ships empty strings rather than NULLs. Treat both as missing.
    non_null = series.notna() & (series.astype("string").str.len() > 0)
    return float(non_null.sum()) / float(total)


def assert_coverage(
    dataset_id: str,
    frame: pd.DataFrame,
    required_coverage: dict[str, float],
) -> CoverageReport:
    """Check ``frame`` against ``required_coverage``.

    Raises :class:`CoverageFailure` on any column under its threshold and
    otherwise returns a :class:`CoverageReport` the caller can persist.
    """
    coverage: dict[str, float] = {}
    failures: dict[str, tuple[float, float]] = {}
    for column, threshold in required_coverage.items():
        actual = _non_null_ratio(frame, column)
        coverage[column] = actual
        if actual < threshold:
            failures[column] = (actual, threshold)

    report = CoverageReport(
        dataset_id=dataset_id,
        rows=len(frame),
        coverage=coverage,
        thresholds=dict(required_coverage),
        failures={col: actual for col, (actual, _) in failures.items()},
        checked_at=datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    if failures:
        write_failure_report(report)
        raise CoverageFailure(dataset_id, failures)
    return report


def write_failure_report(report: CoverageReport) -> Path:
    """Persist a failed :class:`CoverageReport` under ``lake/meta/failures/``."""
    out_dir = meta_path() / "failures" / report.dataset_id
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}-{uuid.uuid4().hex[:8]}.json"
    path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_coverage_report(report: CoverageReport) -> Path:
    """Persist a passed :class:`CoverageReport` under ``lake/meta/coverage/``."""
    out_dir = meta_path() / "coverage" / report.dataset_id
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}-{uuid.uuid4().hex[:8]}.json"
    path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
    return path
