from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.models.anomaly import build_anomaly_features, train_anomaly_model

if TYPE_CHECKING:
    from pathlib import Path


def _write_anomaly_inputs(root: Path) -> None:
    awards_out = root / "curated" / "table=fct_procurement_contract_awards"
    sanctions_out = (
        root
        / "curated"
        / "table=signal_feature_procurement_sanctioned_supplier_awarded"
    )
    awards_out.mkdir(parents=True)
    sanctions_out.mkdir(parents=True)
    award_rows = []
    for index in range(8):
        supplier_key = "900123456" if index in {0, 3, 7} else f"90000000{index}"
        award_rows.append({
            "contract_id": f"C-{index}",
            "contract_reference": f"C-{index}",
            "process_id": f"P-{index}",
            "process_url": f"https://secop.example/C-{index}",
            "supplier_document_digits": supplier_key,
            "supplier_nit_base": supplier_key,
            "supplier_nit_canonical": f"{supplier_key}8",
            "supplier_document_key": supplier_key,
            "supplier_entity_id": f"doc:{supplier_key}",
            "supplier_name": f"Proveedor {index}",
            "supplier_doc_type": "NIT",
            "buyer_document_digits": "800123456",
            "buyer_nit_canonical": "8001234561",
            "buyer_document_id": "800123456",
            "buyer_name": "Entidad Compradora",
            "department": "Bogota",
            "city": "Bogota",
            "sector": "Gobierno",
            "procurement_modality": "Contratacion directa" if index < 4 else "Minima cuantia",
            "contract_type": "Servicios",
            "contract_value": float(1_000_000 * (index + 1)),
            "signing_date": date(2026, 1, index + 1),
            "contract_start_date": date(2026, 1, index + 2),
            "contract_end_date": date(2026, 2, index + 2),
            "last_update_at": None,
            "source_id": "secop_ii_contracts",
            "table": "fct_procurement_contract_awards",
        })
    pq.write_table(pa.Table.from_pylist(award_rows), awards_out / "part-00000.parquet")
    pq.write_table(
        pa.Table.from_pylist([
            {
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-0:paco-1",
                "scope_type": "sanction_record",
                "risk_signal": 1.0,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "contract_id": "C-0",
                "contract_reference": "C-0",
                "process_id": "P-0",
                "process_url": "https://secop.example/C-0",
                "supplier_name": "Proveedor 0",
                "supplier_doc_type": "NIT",
                "buyer_document_id": "800123456",
                "buyer_name": "Entidad Compradora",
                "department": "Bogota",
                "city": "Bogota",
                "sector": "Gobierno",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_value": 1_000_000.0,
                "signing_date": date(2026, 1, 1),
                "paco_record_id": "paco-1",
                "paco_feed": "antecedentes_siri_sanciones",
                "paco_source_url": "https://paco.example/paco-1",
                "sanction_subject_document_digits": "900123456",
                "sanction_subject_name": "Proveedor 0",
                "sanction_subject_type": "PERSONA JURIDICA",
                "sanction_type": "SANCION",
                "sanction_date": date(2025, 12, 1),
                "sanction_reference": "S-1",
                "sanction_contract_id": None,
                "sanction_amount": None,
                "affected_entity": "Entidad",
                "join_rule": "document_exact",
                "evidence_refs": ["https://secop.example/C-0", "https://paco.example/paco-1"],
                "table": "signal_feature_procurement_sanctioned_supplier_awarded",
            }
        ]),
        sanctions_out / "part-00000.parquet",
    )


def test_build_anomaly_features_writes_contract_features(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_anomaly_inputs(tmp_path)

    result = build_anomaly_features(run_id="feature-test")

    assert result.rows == 8
    con = duckdb.connect()
    try:
        row = con.execute(
            """
            SELECT prior_sanction_supplier, n_prior_contracts_12mo_supplier
            FROM read_parquet(?)
            WHERE contract_id = 'C-3'
            """,
            [str(tmp_path / "curated" / "anomaly_features" / "run_id=feature-test" / "*.parquet")],
        ).fetchone()
    finally:
        con.close()
    assert row == (True, 1)


def test_train_anomaly_model_writes_model_scores_and_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_anomaly_inputs(tmp_path)

    result = train_anomaly_model(
        run_id="model-test",
        max_training_rows=8,
        batch_size=3,
    )

    assert result.training_rows == 8
    assert result.scored_rows == 8
    assert (tmp_path / "models" / "anomaly" / "model-test" / "iforest.joblib").exists()
    assert (tmp_path / "models" / "anomaly" / "model-test" / "metrics.json").exists()
    assert (tmp_path / "models" / "anomaly" / "current.json").exists()
    assert (
        tmp_path
        / "curated"
        / "anomaly_scores"
        / "run_id=model-test"
        / "part-00000.parquet"
    ).exists()


def test_anomaly_model_cli_train(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_anomaly_inputs(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "model",
            "train",
            "anomaly",
            "--run-id",
            "cli-model",
            "--max-training-rows",
            "8",
            "--batch-size",
            "4",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "anomaly model cli-model" in result.output
    assert (tmp_path / "models" / "anomaly" / "current.json").exists()
