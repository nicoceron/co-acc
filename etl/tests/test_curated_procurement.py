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
                "supplier_document": "900123456-8",
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
            *[
                {
                    "contract_id": f"CX-{index}",
                    "contract_reference": f"REF-X-{index}",
                    "procurement_process": f"PX-{index}",
                    "process_url": f"https://secop.example/CX-{index}",
                    "supplier_document": "900765432-6",
                    "supplier_doc_type": "NIT",
                    "awarded_supplier": "Proveedor Concentrado SAS",
                    "entity_nit": f"800{index:06d}",
                    "entity_name": f"Comprador {index}",
                    "department": "NARINO" if index % 2 else "CAUCA",
                    "city": "TUMACO",
                    "sector": "Infraestructura",
                    "procurement_modality": "Licitacion",
                    "contract_type": "Obra",
                    "contract_value": "100000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for index in range(50)
            ],
            *[
                {
                    "contract_id": f"CR-{index}",
                    "contract_reference": f"REF-R-{index}",
                    "procurement_process": f"PR-{index}",
                    "process_url": f"https://secop.example/CR-{index}",
                    "supplier_document": "901000111-8",
                    "supplier_doc_type": "NIT",
                    "awarded_supplier": "Proveedor Recurrente SAS",
                    "entity_nit": "800999888",
                    "entity_name": "Comprador Recurrente",
                    "department": "ANTIOQUIA",
                    "city": "MEDELLIN",
                    "sector": "Tecnologia",
                    "procurement_modality": "Contratacion directa",
                    "contract_type": "Servicios",
                    "contract_value": "200000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for index in range(10)
            ],
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
    _write_rows(
        tmp_path,
        "5u9e-g5w9",
        [
            {
                "document_type": "CC",
                "funcionario_id": "123.456.789",
                "full_name": "Servidora Publica",
                "institution_id": "INST-1",
                "institution_name": "Entidad Uno",
                "start_date": "2025-01-15",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "8tz7-h3eu",
        [
            {
                "document_type": "CC",
                "document_id": "123456789",
                "declarant_first_name": "Servidora",
                "declarant_second_name": "",
                "declarant_first_lastname": "Publica",
                "declarant_second_lastname": "",
                "entity_name": "Entidad Uno",
                "publication_date": "2026-01-10",
            }
        ],
    )

    results = build_curated()

    assert {result.table for result in results} == {
        "dim_subject_document",
        "dim_company",
        "dim_buyer",
        "dim_person",
        "fct_procurement_contract_awards",
        "signal_feature_procurement_sanctioned_supplier_awarded",
        "signal_feature_procurement_supplier_concentration_across_entities",
        "signal_feature_procurement_repeat_awards_same_supplier",
    }
    rows_by_table = {result.table: result.rows for result in results}
    assert rows_by_table["fct_procurement_contract_awards"] == 62
    assert rows_by_table["dim_company"] == 3
    assert rows_by_table["dim_buyer"] == 52
    assert rows_by_table["dim_person"] == 1
    assert rows_by_table["signal_feature_procurement_sanctioned_supplier_awarded"] == 1
    assert rows_by_table["signal_feature_procurement_supplier_concentration_across_entities"] == 1
    assert rows_by_table["signal_feature_procurement_repeat_awards_same_supplier"] == 1

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
        concentration_rows = con.execute(
            "SELECT entity_key, contract_count, distinct_buyer_count, total_contract_value, "
            "evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_supplier_concentration_across_entities"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        repeat_rows = con.execute(
            "SELECT entity_key, buyer_document_id, contract_count, total_contract_value, "
            "scope_key, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_repeat_awards_same_supplier"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        company_rows = con.execute(
            "SELECT nit_canonical, sources FROM read_parquet(?) ORDER BY nit_canonical",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=dim_company"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        person_rows = con.execute(
            "SELECT cedula_canonical, nit_canonical, sources FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=dim_person"
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
    assert concentration_rows == [
        ("900765432", 50, 50, 5_000_000_000.0, "https://secop.example/CX-0")
    ]
    assert repeat_rows == [
        (
            "901000111",
            "800999888",
            10,
            2_000_000_000.0,
            "buyer:800999888",
            "https://secop.example/CR-0",
        )
    ]
    assert company_rows == [
        ("9001234568", ["paco_sanctions", "secop_ii_contracts"]),
        ("9007654326", ["secop_ii_contracts"]),
        ("9010001118", ["secop_ii_contracts"]),
    ]
    assert person_rows == [("123456789", None, ["5u9e-g5w9", "8tz7-h3eu"])]


def test_build_curated_errors_when_required_sources_are_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    with pytest.raises(CuratedBuildError, match="missing required lake source"):
        build_curated()


def test_build_dim_person_without_procurement_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_rows(
        tmp_path,
        "5u9e-g5w9",
        [
            {
                "document_type": "CC",
                "funcionario_id": "987654321",
                "full_name": "Funcionario Uno",
                "institution_id": "INST-2",
                "institution_name": "Entidad Dos",
                "start_date": "2025-02-01",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "8tz7-h3eu",
        [
            {
                "document_type": "CC",
                "document_id": "987654321",
                "declarant_first_name": "Funcionario",
                "declarant_second_name": "",
                "declarant_first_lastname": "Uno",
                "declarant_second_lastname": "",
                "entity_name": "Entidad Dos",
                "publication_date": "2026-02-01",
            }
        ],
    )

    results = build_curated(["dim_person"])

    assert [(result.table, result.rows) for result in results] == [("dim_person", 1)]
