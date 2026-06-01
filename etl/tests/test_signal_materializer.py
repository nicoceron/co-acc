from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.signals import SignalMaterializationError, materialize_signals
from coacc_etl.signals.contracts import EvidenceBundleRow, SignalHitRow

if TYPE_CHECKING:
    from pathlib import Path


def _write_feature_rows(
    root: Path,
    signal_id: str,
    rows: list[dict[str, object]],
) -> None:
    out = root / "curated" / f"table=signal_feature_{signal_id}"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), out / "part-00000.parquet")


def _write_demo_feature_tables(root: Path) -> None:
    _write_feature_rows(
        root,
        "procurement_sanctioned_supplier_awarded",
        [
            {
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1:paco-1",
                "scope_type": "sanction_record",
                "risk_signal": 1.0,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "https://paco.example/paco-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_supplier_concentration_across_entities",
        [
            {
                "signal_id": "procurement_supplier_concentration_across_entities",
                "entity_id": "doc:900765432",
                "entity_key": "900765432",
                "entity_label": "Company",
                "scope_key": "supplier:900765432",
                "scope_type": "supplier",
                "risk_signal": 0.8,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CX-1",
                    "https://secop.example/CX-2",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_repeat_awards_same_supplier",
        [
            {
                "signal_id": "procurement_repeat_awards_same_supplier",
                "entity_id": "doc:901000111",
                "entity_key": "901000111",
                "entity_label": "Company",
                "scope_key": "buyer:800999888",
                "scope_type": "buyer",
                "risk_signal": 0.75,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CR-1",
                    "https://secop.example/CR-2",
                ],
            }
        ],
    )


def test_materialize_signals_writes_hits_evidence_and_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = materialize_signals(run_id="test-run")

    assert result.run_id == "test-run"
    assert result.hit_count == 3
    assert result.evidence_count == 6
    assert {row.signal_id: row.hit_count for row in result.signal_results} == {
        "procurement_sanctioned_supplier_awarded": 1,
        "procurement_supplier_concentration_across_entities": 1,
        "procurement_repeat_awards_same_supplier": 1,
    }

    con = duckdb.connect()
    try:
        hit_rows = con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY signal_id",
            [str(tmp_path / "curated" / "signal_hits" / "run_id=test-run" / "*.parquet")],
        ).fetchall()
        hit_columns = [item[0] for item in con.description or []]
        evidence_rows = con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY signal_id, item_index",
            [
                str(
                    tmp_path
                    / "curated"
                    / "evidence_bundles"
                    / "run_id=test-run"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        evidence_columns = [item[0] for item in con.description or []]
    finally:
        con.close()

    hit_payloads = [dict(zip(hit_columns, row, strict=False)) for row in hit_rows]
    evidence_payloads = [
        dict(zip(evidence_columns, row, strict=False)) for row in evidence_rows
    ]
    SignalHitRow.model_validate(hit_payloads[0])
    EvidenceBundleRow.model_validate(evidence_payloads[0])
    assert [row["signal_id"] for row in hit_payloads] == [
        "procurement_repeat_awards_same_supplier",
        "procurement_sanctioned_supplier_awarded",
        "procurement_supplier_concentration_across_entities",
    ]
    assert all(row["evidence_count"] == 2 for row in hit_payloads)
    paco_evidence = next(
        row for row in evidence_payloads if row["url"] == "https://paco.example/paco-1"
    )
    assert paco_evidence["source_id"] == "paco_sanctions"
    assert "scope_key=C-1:paco-1" in paco_evidence["row_selector"]

    manifest = json.loads((tmp_path / "meta" / "signal_runs" / "test-run.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["hit_count"] == 3
    assert manifest["evidence_count"] == 6


def test_materialize_signal_accepts_registry_alias(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = materialize_signals(["split_contracts_below_threshold"], run_id="alias-run")

    assert [(row.signal_id, row.hit_count) for row in result.signal_results] == [
        ("procurement_repeat_awards_same_supplier", 1)
    ]


def test_materialize_signals_errors_when_feature_table_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    with pytest.raises(SignalMaterializationError, match="missing curated signal feature"):
        materialize_signals(["procurement_repeat_awards_same_supplier"], run_id="missing")


def test_signals_materialize_cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = CliRunner().invoke(cli, ["signals", "materialize", "--all", "--run-id", "cli-run"])

    assert result.exit_code == 0, result.output
    assert "signal run cli-run: wrote 3 hits and 6 evidence rows" in result.output
    assert (tmp_path / "meta" / "signal_runs" / "cli-run.json").exists()
