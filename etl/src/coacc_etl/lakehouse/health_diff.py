from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from coacc_etl.lakehouse.reality import CuratedTableHealth, DatasetHealth

Severity = Literal["info", "warn", "fail"]


class RealityThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_count_drop_ratio: float = 0.01
    null_rate_margin: float = 0.01
    dup_ratio_rise: float = 0.005
    sentinel_fraction_rise: float = 0.01
    partition_skew_rise_ratio: float = 0.20
    partition_count_drop: int = 0
    max_freshness_seconds: float | None = None


class Finding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    metric: str
    severity: Severity
    message: str
    current: str | int | float | None = None
    previous: str | int | float | None = None
    threshold: str | int | float | None = None


class ThresholdConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    defaults: RealityThresholds = Field(default_factory=RealityThresholds)
    datasets: dict[str, RealityThresholds] = Field(default_factory=dict)


def load_thresholds(path: Path | None = None) -> ThresholdConfig:
    if path is None:
        path = Path(__file__).resolve().parents[4] / "config" / "reality_thresholds.yml"
    if not path.exists():
        return ThresholdConfig()

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        msg = f"{path} must contain a YAML mapping"
        raise ValueError(msg)

    default_values = raw.get("defaults") or {}
    if not isinstance(default_values, dict):
        msg = f"{path}: defaults must be a mapping"
        raise ValueError(msg)
    defaults = RealityThresholds.model_validate(default_values)

    dataset_values = raw.get("datasets") or {}
    if not isinstance(dataset_values, dict):
        msg = f"{path}: datasets must be a mapping"
        raise ValueError(msg)

    datasets = {
        str(dataset_id): RealityThresholds.model_validate({**defaults.model_dump(), **values})
        for dataset_id, values in dataset_values.items()
        if isinstance(values, dict)
    }
    return ThresholdConfig(defaults=defaults, datasets=datasets)


def thresholds_for(config: ThresholdConfig, dataset_id: str) -> RealityThresholds:
    return config.datasets.get(dataset_id, config.defaults)


def current_health_findings(
    current: DatasetHealth,
    thresholds: RealityThresholds,
) -> list[Finding]:
    findings: list[Finding] = []
    if current.row_count == 0:
        findings.append(
            Finding(
                dataset_id=current.dataset_id,
                metric="row_count",
                severity="fail",
                message="lake dataset has zero rows",
                current=0,
                threshold=">0",
            )
        )

    for column, expected_coverage in current.required_coverage.items():
        actual_null_rate = current.null_rate.get(column, 1.0)
        allowed_null_rate = max(0.0, 1.0 - expected_coverage + thresholds.null_rate_margin)
        if actual_null_rate > allowed_null_rate:
            findings.append(
                Finding(
                    dataset_id=current.dataset_id,
                    metric=f"null_rate.{column}",
                    severity="fail",
                    message=(
                        f"{column} null rate exceeds required coverage "
                        f"({actual_null_rate:.4f} > {allowed_null_rate:.4f})"
                    ),
                    current=round(actual_null_rate, 6),
                    threshold=round(allowed_null_rate, 6),
                )
            )

    if (
        thresholds.max_freshness_seconds is not None
        and current.freshness_seconds is not None
        and current.freshness_seconds > thresholds.max_freshness_seconds
    ):
        findings.append(
            Finding(
                dataset_id=current.dataset_id,
                metric="freshness_seconds",
                severity="warn",
                message="latest parquet file is older than the configured freshness budget",
                current=round(current.freshness_seconds, 2),
                threshold=thresholds.max_freshness_seconds,
            )
        )

    return findings


def diff_health(
    previous: DatasetHealth | None,
    current: DatasetHealth,
    thresholds: RealityThresholds,
) -> list[Finding]:
    findings = current_health_findings(current, thresholds)
    if previous is None:
        findings.append(
            Finding(
                dataset_id=current.dataset_id,
                metric="baseline",
                severity="info",
                message="no previous baseline was available for this dataset",
            )
        )
        return findings

    if previous.row_count > 0:
        drop_ratio = (previous.row_count - current.row_count) / previous.row_count
        if drop_ratio > thresholds.row_count_drop_ratio:
            findings.append(
                Finding(
                    dataset_id=current.dataset_id,
                    metric="row_count",
                    severity="fail",
                    message=f"row count dropped by {drop_ratio:.4%}",
                    current=current.row_count,
                    previous=previous.row_count,
                    threshold=thresholds.row_count_drop_ratio,
                )
            )

    partition_drop = previous.partition_count - current.partition_count
    if partition_drop > thresholds.partition_count_drop:
        findings.append(
            Finding(
                dataset_id=current.dataset_id,
                metric="partition_count",
                severity="fail",
                message="partition count regressed",
                current=current.partition_count,
                previous=previous.partition_count,
                threshold=thresholds.partition_count_drop,
            )
        )

    _append_rise_finding(
        findings,
        dataset_id=current.dataset_id,
        metric="dup_ratio",
        previous=previous.dup_ratio,
        current=current.dup_ratio,
        threshold=thresholds.dup_ratio_rise,
        message="duplicate ratio rose above threshold",
    )
    _append_rise_finding(
        findings,
        dataset_id=current.dataset_id,
        metric="sentinel_fraction",
        previous=previous.sentinel_fraction,
        current=current.sentinel_fraction,
        threshold=thresholds.sentinel_fraction_rise,
        message="sentinel partition fraction rose above threshold",
    )

    if previous.partition_skew is not None and current.partition_skew is not None:
        allowed = previous.partition_skew * (1.0 + thresholds.partition_skew_rise_ratio)
        if current.partition_skew > allowed and current.partition_skew > previous.partition_skew:
            findings.append(
                Finding(
                    dataset_id=current.dataset_id,
                    metric="partition_skew",
                    severity="fail",
                    message="partition skew rose above threshold",
                    current=round(current.partition_skew, 6),
                    previous=round(previous.partition_skew, 6),
                    threshold=round(allowed, 6),
                )
            )

    if current.schema_hash != previous.schema_hash:
        findings.append(
            Finding(
                dataset_id=current.dataset_id,
                metric="schema_hash",
                severity="warn",
                message="schema hash changed",
                current=current.schema_hash,
                previous=previous.schema_hash,
            )
        )

    return findings


def current_curated_health_findings(
    current: CuratedTableHealth,
    thresholds: RealityThresholds,
) -> list[Finding]:
    findings: list[Finding] = []
    dataset_id = _curated_id(current.table)
    if current.row_count == 0:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="row_count",
                severity="fail",
                message="curated table has zero rows",
                current=0,
                threshold=">0",
            )
        )

    if current.latest_manifest_path is None:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="manifest",
                severity="warn",
                message="curated table has no manifest entry",
                current=current.manifest_entry_count,
                threshold=">=1",
            )
        )
    elif current.manifest_row_count_matches is False:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="manifest.rows",
                severity="fail",
                message="latest curated manifest row count does not match parquet rows",
                current=current.row_count,
                previous=current.latest_manifest_rows,
            )
        )

    if current.table.startswith("signal_feature_"):
        if current.evidence_ref_coverage is None:
            findings.append(
                Finding(
                    dataset_id=dataset_id,
                    metric="evidence_refs",
                    severity="fail",
                    message="signal feature table is missing evidence_refs",
                    current=None,
                    threshold="present",
                )
            )
        elif current.evidence_ref_coverage < 1.0:
            findings.append(
                Finding(
                    dataset_id=dataset_id,
                    metric="evidence_ref_coverage",
                    severity="fail",
                    message="signal feature rows must carry at least one evidence ref",
                    current=round(current.evidence_ref_coverage, 6),
                    threshold=1.0,
                )
            )

    if (
        thresholds.max_freshness_seconds is not None
        and current.freshness_seconds is not None
        and current.freshness_seconds > thresholds.max_freshness_seconds
    ):
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="freshness_seconds",
                severity="warn",
                message="latest curated parquet file is older than the configured freshness budget",
                current=round(current.freshness_seconds, 2),
                threshold=thresholds.max_freshness_seconds,
            )
        )

    return findings


def diff_curated_health(
    previous: CuratedTableHealth | None,
    current: CuratedTableHealth,
    thresholds: RealityThresholds,
) -> list[Finding]:
    findings = current_curated_health_findings(current, thresholds)
    dataset_id = _curated_id(current.table)
    if previous is None:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="baseline",
                severity="info",
                message="no previous baseline was available for this curated table",
            )
        )
        return findings

    if previous.row_count > 0:
        drop_ratio = (previous.row_count - current.row_count) / previous.row_count
        if drop_ratio > thresholds.row_count_drop_ratio:
            findings.append(
                Finding(
                    dataset_id=dataset_id,
                    metric="row_count",
                    severity="fail",
                    message=f"curated row count dropped by {drop_ratio:.4%}",
                    current=current.row_count,
                    previous=previous.row_count,
                    threshold=thresholds.row_count_drop_ratio,
                )
            )

    if current.schema_hash != previous.schema_hash:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="schema_hash",
                severity="warn",
                message="curated schema hash changed",
                current=current.schema_hash,
                previous=previous.schema_hash,
            )
        )

    if (
        previous.evidence_ref_coverage is not None
        and current.evidence_ref_coverage is not None
        and current.evidence_ref_coverage + thresholds.null_rate_margin
        < previous.evidence_ref_coverage
    ):
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric="evidence_ref_coverage",
                severity="fail",
                message="curated evidence ref coverage regressed",
                current=round(current.evidence_ref_coverage, 6),
                previous=round(previous.evidence_ref_coverage, 6),
                threshold=round(previous.evidence_ref_coverage - thresholds.null_rate_margin, 6),
            )
        )

    return findings


def findings_have_failures(findings: list[Finding]) -> bool:
    return any(finding.severity == "fail" for finding in findings)


def render_diff_markdown(
    *,
    snapshot_date: str,
    generated_at: datetime | None,
    baseline_date: str | None,
    datasets: list[DatasetHealth],
    curated_tables: list[CuratedTableHealth] | None = None,
    findings: list[Finding],
) -> str:
    curated_tables = curated_tables or []
    generated_at = generated_at or datetime.now(tz=UTC)
    lines = [
        f"# Lake reality diff: {snapshot_date}",
        "",
        f"- Generated at: `{generated_at.isoformat()}`",
        f"- Baseline: `{baseline_date or 'none'}`",
        f"- Datasets: `{len(datasets)}`",
        f"- Curated tables: `{len(curated_tables)}`",
        f"- Failures: `{sum(1 for finding in findings if finding.severity == 'fail')}`",
        f"- Warnings: `{sum(1 for finding in findings if finding.severity == 'warn')}`",
        "",
        "## Dataset Summary",
        "",
        "| dataset | rows | partitions | files | watermark max | dup ratio | sentinel | schema |",
        "| --- | ---: | ---: | ---: | --- | ---: | ---: | --- |",
    ]
    for dataset in sorted(datasets, key=lambda item: item.dataset_id):
        dup_ratio = _format_ratio(dataset.dup_ratio)
        sentinel = _format_ratio(dataset.sentinel_fraction)
        watermark = dataset.watermark_max or ""
        lines.append(
            f"| `{dataset.dataset_id}` | {dataset.row_count} | {dataset.partition_count} | "
            f"{dataset.parquet_file_count} | `{watermark}` | {dup_ratio} | {sentinel} | "
            f"`{dataset.schema_hash[:12]}` |"
        )

    lines.extend(["", "## Curated Table Summary", ""])
    if not curated_tables:
        lines.append("No curated tables probed.")
    else:
        lines.append(
            "| table | rows | files | manifest rows | evidence coverage | schema |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | --- |")
        for table in sorted(curated_tables, key=lambda item: item.table):
            manifest_rows = "" if table.latest_manifest_rows is None else table.latest_manifest_rows
            lines.append(
                f"| `{table.table}` | {table.row_count} | {table.parquet_file_count} | "
                f"{manifest_rows} | {_format_ratio(table.evidence_ref_coverage)} | "
                f"`{table.schema_hash[:12]}` |"
            )

    lines.extend(["", "## Findings", ""])
    if not findings:
        lines.append("No regressions found.")
    else:
        lines.append("| severity | dataset | metric | message | current | previous | threshold |")
        lines.append("| --- | --- | --- | --- | ---: | ---: | ---: |")
        for finding in sorted(
            findings, key=lambda item: (item.severity, item.dataset_id, item.metric)
        ):
            lines.append(
                f"| `{finding.severity}` | `{finding.dataset_id}` | `{finding.metric}` | "
                f"{finding.message} | {_cell(finding.current)} | {_cell(finding.previous)} | "
                f"{_cell(finding.threshold)} |"
            )
    lines.append("")
    return "\n".join(lines)


def _append_rise_finding(
    findings: list[Finding],
    *,
    dataset_id: str,
    metric: str,
    previous: float | None,
    current: float | None,
    threshold: float,
    message: str,
) -> None:
    if previous is None or current is None:
        return
    rise = current - previous
    if rise > threshold:
        findings.append(
            Finding(
                dataset_id=dataset_id,
                metric=metric,
                severity="fail",
                message=message,
                current=round(current, 6),
                previous=round(previous, 6),
                threshold=threshold,
            )
        )


def _curated_id(table: str) -> str:
    return f"curated:{table}"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}"


def _cell(value: str | int | float | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    if isinstance(value, str):
        return "`" + value.replace("`", "'") + "`"
    return str(value)
