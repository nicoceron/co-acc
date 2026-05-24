from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc_etl.curated import CuratedBuildError, build_curated

if TYPE_CHECKING:
    from pathlib import Path


def _write_rows(root: Path, source: str, rows: list[dict[str, object]]) -> None:
    out = root / "raw" / f"source={source}" / "year=2026" / "month=05"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), out / "fixture.parquet")


def test_build_curated_procurement_signal_uses_catalog_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_rows(
        tmp_path,
        "jbjy-vk9h",
        [
            {
                "contract_id": "C-1",
                "contract_reference": "REF-1",
                "procurement_process": "P-1",
                "process_url": "{'url': 'https://secop.example/C-1'}",
                "supplier_document": "900123456-7",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Sancionado SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Prestacion de servicios",
                "contract_value": "125000000",
                "signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-03T00:00:00",
            },
            {
                "contract_id": "C-2",
                "contract_reference": "REF-2",
                "procurement_process": "P-2",
                "process_url": "{'url': 'https://secop.example/C-2'}",
                "supplier_document": "1001234567",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Persona No Nit",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Licitacion",
                "contract_type": "Suministro",
                "contract_value": "45000000",
                "signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-03T00:00:00",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "paco_sanctions",
        [
            {
                "source_id": "paco-1",
                "raw_paco_record_id": "paco-1",
                "paco_feed": "responsabilidades_fiscales",
                "source_url": "https://paco.example/1",
                "subject_document_id": "900123456",
                "subject_name": "Proveedor Sancionado SAS",
                "subject_type": "PERSONA JURIDICA",
                "sanction_type": "Responsabilidad fiscal",
                "sanction_date": "2025-12-31",
                "reference": "RF-1",
                "contract_id": "",
                "amount": "5000000",
                "affected_entity": "Comprador Uno",
                "raw_record_json": "{}",
            }
        ],
    )

    results = build_curated()

    assert {result.table for result in results} == {
        "dim_subject_document",
        "fct_procurement_contract_awards",
        "signal_feature_procurement_sanctioned_supplier_awarded",
    }
    rows_by_table = {result.table: result.rows for result in results}
    assert rows_by_table["fct_procurement_contract_awards"] == 2
    assert rows_by_table["signal_feature_procurement_sanctioned_supplier_awarded"] == 1

    con = duckdb.connect()
    try:
        signal_rows = con.execute(
            "SELECT entity_key, contract_id, paco_record_id, join_rule, risk_signal, "
            "evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_sanctioned_supplier_awarded"
                    / "*.parquet"
                )
            ],
        ).fetchall()
    finally:
        con.close()
    assert signal_rows == [
        (
            "900123456",
            "C-1",
            "paco-1",
            "nit_base_to_subject",
            1.0,
            "https://secop.example/C-1",
        )
    ]


def test_build_curated_errors_when_required_sources_are_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    with pytest.raises(CuratedBuildError, match="missing required lake source"):
        build_curated()
