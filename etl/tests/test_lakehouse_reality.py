from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd

from coacc_etl.catalog import load_catalog
from coacc_etl.lakehouse.health_diff import (
    RealityThresholds,
    current_health_findings,
    diff_health,
)
from coacc_etl.lakehouse.reality import compute_dataset_health
from coacc_etl.lakehouse.writer import append_parquet

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _write_fiscal_findings_lake() -> None:
    frame = pd.DataFrame(
        {
            "nit": ["9001", "9002", "9003"],
            "entity_name": ["Alpha SA", "Beta SAS", "Gamma Ltda"],
            "received_date": ["2026-04-01", "2026-04-02", "2026-04-03"],
            "radicado": ["R-1", "R-2", "R-3"],
        }
    )
    append_parquet(frame, "8qxx-ubmq", 2026, 4)


def test_compute_dataset_health_resolves_source_coverage_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_fiscal_findings_lake()
    spec = load_catalog()["8qxx-ubmq"]

    health = compute_dataset_health(
        spec,
        now=datetime(2026, 5, 23, 12, 0, tzinfo=UTC),
    )

    assert health.dataset_id == "8qxx-ubmq"
    assert health.row_count == 3
    assert health.partition_count == 1
    assert health.coverage_columns["nombre_sujeto"] == "entity_name"
    assert health.coverage_columns["fecha_recibo_traslado"] == "received_date"
    assert health.null_rate["nombre_sujeto"] == 0.0
    assert health.watermark_column == "received_date"
    assert health.watermark_max == "2026-04-03"
    assert health.join_key_class == "nit"
    assert health.dup_ratio == 0.0
    assert health.sentinel_fraction == 0.0
    assert health.schema_hash


def test_current_health_findings_fail_when_required_coverage_regresses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_fiscal_findings_lake()
    health = compute_dataset_health(load_catalog()["8qxx-ubmq"])
    degraded = health.model_copy(
        update={"null_rate": {"nombre_sujeto": 0.5}, "required_coverage": {"nombre_sujeto": 0.95}}
    )

    findings = current_health_findings(degraded, RealityThresholds())

    assert any(
        finding.severity == "fail" and finding.metric == "null_rate.nombre_sujeto"
        for finding in findings
    )


def test_diff_health_fails_on_row_count_drop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_fiscal_findings_lake()
    current = compute_dataset_health(load_catalog()["8qxx-ubmq"]).model_copy(
        update={"row_count": 90}
    )
    previous = current.model_copy(update={"row_count": 100})

    findings = diff_health(previous, current, RealityThresholds(row_count_drop_ratio=0.01))

    assert any(finding.severity == "fail" and finding.metric == "row_count" for finding in findings)


def test_diff_health_warns_on_schema_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_fiscal_findings_lake()
    current = compute_dataset_health(load_catalog()["8qxx-ubmq"]).model_copy(
        update={"schema_hash": "new"}
    )
    previous = current.model_copy(update={"schema_hash": "old"})

    findings = diff_health(previous, current, RealityThresholds())

    assert any(
        finding.severity == "warn" and finding.metric == "schema_hash" for finding in findings
    )
