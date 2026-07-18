from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.models.anomaly import build_anomaly_features, train_anomaly_model
from coacc_etl.models.anomaly.common import FEATURE_NAMES


def _write_anomaly_inputs(root: Path, *, include_offers: bool = True) -> None:
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
    if include_offers:
        offers_out = root / "raw" / "source=wi7w-2nvm" / "year=2026" / "month=01"
        offers_out.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist([
                {
                    "fecha_de_registro": "2026-01-01T00:00:00.000",
                    "referencia_de_la_oferta": "O-C-0",
                    "identificador_de_la_oferta": "O-C-0",
                    "valor_de_la_oferta": "1000000",
                    "entidad_compradora": "Entidad Compradora",
                    "nit_entidad_compradora": "800123456",
                    "moneda": "COP",
                    "descripcion_del_procedimiento": "Proceso C-0",
                    "referencia_del_proceso": "P-0",
                    "id_del_proceso_de_compra": "P-0",
                    "modalidad": "Contratacion directa",
                    "invitacion_directa": "No",
                    "nombre_proveedor": "Proveedor 0",
                    "nit_del_proveedor": "900123456",
                    "c_digo_entidad": "buyer-0",
                    "c_digo_proveedor": "supplier-0",
                },
                {
                    "fecha_de_registro": "2026-01-02T00:00:00.000",
                    "referencia_de_la_oferta": "O-C-1-A",
                    "identificador_de_la_oferta": "O-C-1-A",
                    "valor_de_la_oferta": "2000000",
                    "entidad_compradora": "Entidad Compradora",
                    "nit_entidad_compradora": "800123456",
                    "moneda": "COP",
                    "descripcion_del_procedimiento": "Proceso C-1",
                    "referencia_del_proceso": "P-1",
                    "id_del_proceso_de_compra": "P-1",
                    "modalidad": "Contratacion directa",
                    "invitacion_directa": "No",
                    "nombre_proveedor": "Proveedor 1",
                    "nit_del_proveedor": "900000001",
                    "c_digo_entidad": "buyer-1",
                    "c_digo_proveedor": "supplier-1",
                },
                {
                    "fecha_de_registro": "2026-01-02T00:00:00.000",
                    "referencia_de_la_oferta": "O-C-1-B",
                    "identificador_de_la_oferta": "O-C-1-B",
                    "valor_de_la_oferta": "2100000",
                    "entidad_compradora": "Entidad Compradora",
                    "nit_entidad_compradora": "800123456",
                    "moneda": "COP",
                    "descripcion_del_procedimiento": "Proceso C-1",
                    "referencia_del_proceso": "P-1",
                    "id_del_proceso_de_compra": "P-1",
                    "modalidad": "Contratacion directa",
                    "invitacion_directa": "No",
                    "nombre_proveedor": "Proveedor Alterno",
                    "nit_del_proveedor": "900000099",
                    "c_digo_entidad": "buyer-1",
                    "c_digo_proveedor": "supplier-99",
                },
            ]),
            offers_out / "part-00000.parquet",
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
            SELECT prior_sanction_supplier, n_prior_contracts_12mo_supplier, single_bidder
            FROM read_parquet(?)
            WHERE contract_id = 'C-3'
            """,
            [str(tmp_path / "curated" / "anomaly_features" / "run_id=feature-test" / "*.parquet")],
        ).fetchone()
        offer_rows = con.execute(
            """
            SELECT contract_id, single_bidder
            FROM read_parquet(?)
            WHERE contract_id IN ('C-0', 'C-1')
            ORDER BY contract_id
            """,
            [
                str(
                    tmp_path
                    / "curated"
                    / "anomaly_features"
                    / "run_id=feature-test"
                    / "*.parquet"
                )
            ],
        ).fetchall()
    finally:
        con.close()
    assert row == (True, 1, False)
    assert offer_rows == [("C-0", True), ("C-1", False)]


def test_build_anomaly_features_without_offers_keeps_single_bidder_false(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_anomaly_inputs(tmp_path, include_offers=False)

    result = build_anomaly_features(run_id="feature-no-offers")

    con = duckdb.connect()
    try:
        row = con.execute(
            """
            SELECT bool_or(single_bidder)
            FROM read_parquet(?)
            """,
            [
                str(
                    tmp_path
                    / "curated"
                    / "anomaly_features"
                    / "run_id=feature-no-offers"
                    / "*.parquet"
                )
            ],
        ).fetchone()
    finally:
        con.close()

    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert row == (False,)
    assert manifest["offer_features"] == {"available": False, "source": "secop_offers"}


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

    assert 0 < result.training_rows < 8
    assert result.scored_rows == 8
    assert (tmp_path / "models" / "anomaly" / "model-test" / "iforest.joblib").exists()
    assert not (
        tmp_path / "models" / "anomaly" / "model-test" / "supervised_hgb.joblib"
    ).exists()
    assert (tmp_path / "models" / "anomaly" / "model-test" / "metrics.json").exists()
    assert (tmp_path / "models" / "anomaly" / "current.json").exists()
    assert (
        tmp_path
        / "curated"
        / "anomaly_scores"
        / "run_id=model-test"
        / "part-00000.parquet"
    ).exists()
    metrics = json.loads(
        (tmp_path / "models" / "anomaly" / "model-test" / "metrics.json").read_text(
            encoding="utf-8"
        )
    )
    assert metrics["model_kind"] == "iforest"
    assert metrics["random_state"] == 42
    assert "supervised_topup" not in metrics
    assert metrics["metrics"]["holdout_unit"] == "supplier_entity_uid"
    assert metrics["metrics"]["base_rate"] is not None
    assert metrics["metrics"]["average_precision"] is not None
    assert metrics["metrics"]["roc_auc"] is not None
    assert "prior_sanction_supplier" not in FEATURE_NAMES
    con = duckdb.connect()
    try:
        row = con.execute(
            """
            SELECT score, iforest_score, top_features
            FROM read_parquet(?)
            ORDER BY score DESC
            LIMIT 1
            """,
            [
                str(
                    tmp_path
                    / "curated"
                    / "anomaly_scores"
                    / "run_id=model-test"
                    / "*.parquet"
                )
            ],
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row[0] == row[1]
    assert "prior_sanction_supplier" not in row[2]


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
    assert "Isolation Forest only" in result.output
    assert (tmp_path / "models" / "anomaly" / "current.json").exists()
