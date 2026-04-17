from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc.services import lakehouse_query
from coacc.services.public_guard import public_signal_payload_leaks_identity
from coacc.services.signal_registry import clear_signal_registry_cache, get_signal_definition

if TYPE_CHECKING:
    from pathlib import Path

WAVE_B_SIGNALS = [
    "pida_full30_meta",
    "reincorporacion_sirr_vs_contractor",
    "damnificados_emergency_contract_overlap",
    "pnis_familia_municipality_vs_award",
    "dane_pobreza_vs_award_ratio",
    "catastro_igac_vs_vendor_address",
    "anim_inmueble_vs_vendor_domicilio",
    "iniciativas_33007_municipal_vs_award",
    "mineria_titulo_vs_anla_license_vs_contractor",
    "upme_subsidio_energia_vs_vendor",
    "dane_micronegocio_vs_secop_subcontract",
    "registro_actores_deporte_vs_funcionario_pdet",
    "bpin_dnp_vs_pida27_obras_prioritarias",
    "pida5_pida27_pida4_chain",
]

FIXTURE_ROWS: dict[str, list[dict[str, object]]] = {
    "secop_integrado": [
        {
            "supplier_document_id": "9001",
            "supplier_name": "Proveedor Uno",
            "buyer_document_id": "8001",
            "municipality": "TUMACO",
            "department": "NARINO",
            "contract_value": "12000000",
            "contract_id": "C-1",
            "process_id": "P-1",
            "modality": "Urgencia manifiesta",
            "object": "Atencion emergencia",
            "pida_category": "PIDA5",
            "bpin_code": "BPIN-1",
        },
        {
            "supplier_document_id": "9001",
            "supplier_name": "Proveedor Uno",
            "buyer_document_id": "8002",
            "municipality": "TUMACO",
            "department": "NARINO",
            "contract_value": "14000000",
            "contract_id": "C-2",
            "process_id": "P-2",
            "modality": "Contratacion directa",
            "object": "Obra prioritaria",
            "pida_category": "PIDA27",
            "bpin_code": "BPIN-1",
        },
        {
            "supplier_document_id": "9002",
            "supplier_name": "Proveedor Dos",
            "buyer_document_id": "8003",
            "municipality": "TUMACO",
            "department": "NARINO",
            "contract_value": "18000000",
            "contract_id": "C-3",
            "process_id": "P-3",
            "modality": "Licitacion",
            "object": "Servicio",
            "pida_category": "PIDA4",
            "bpin_code": "BPIN-2",
        },
    ],
    "pida_category_hits": [{"municipality": "TUMACO", "pida_category": "PIDA99"}],
    "secop_suppliers": [
        {
            "supplier_document_id": "9001",
            "municipality": "TUMACO",
            "department": "NARINO",
            "parcel_id": "PARCEL-1",
            "address_key": "ADDR-1",
            "domicile_property_id": "PROP-1",
        },
        {
            "supplier_document_id": "9002",
            "municipality": "TUMACO",
            "department": "NARINO",
            "parcel_id": "PARCEL-1",
            "address_key": "ADDR-1",
            "domicile_property_id": "PROP-2",
        },
    ],
    "sirr_reincorporacion": [
        {"supplier_document_id": "9001", "municipality": "TUMACO", "participant_count": "3"}
    ],
    "ungrd_damnificados": [{
        "municipality": "TUMACO",
        "department": "NARINO",
        "affected_count": "500",
        "decree_id": "D-1",
    }],
    "secop_contract_additions": [{"contract_id": "C-1", "addition_value": "3000000"}],
    "secop_budget_commitments": [{"contract_id": "C-1", "commitment_value": "5000000"}],
    "pnis_beneficiarios": [{"municipality": "TUMACO", "family_count": "90"}],
    "pdet_municipios": [{"municipality": "TUMACO", "is_pdet": "true"}],
    "dane_pobreza_monetaria": [{
        "municipality": "TUMACO",
        "poverty_rate": "0.45",
        "population": "1000",
    }],
    "dane_ipm": [{"municipality": "TUMACO", "ipm": "45"}],
    "igac_parcelas": [{
        "parcel_id": "PARCEL-1",
        "address_key": "ADDR-1",
        "municipality": "TUMACO",
    }],
    "anim_inmuebles": [{
        "property_id": "PROP-1",
        "address_key": "ADDR-1",
        "municipality": "TUMACO",
    }],
    "presidencia_iniciativas_33007": [{"municipality": "TUMACO", "initiative_id": "I-1"}],
    "anm_titulos": [{
        "supplier_document_id": "9001",
        "mining_title_id": "M-1",
        "municipality": "TUMACO",
    }],
    "anla_licencias": [{
        "supplier_document_id": "9001",
        "license_id": "L-1",
        "municipality": "TUMACO",
    }],
    "upme_subsidios": [{
        "supplier_document_id": "9001",
        "subsidy_id": "U-1",
        "municipality": "TUMACO",
    }],
    "secop_contract_execution": [
        {
            "subcontractor_document_id": "9001",
            "municipality": "TUMACO",
            "department": "NARINO",
            "award_value": "2000000",
            "contract_id": "C-1",
        }
    ],
    "dane_micronegocios": [{"supplier_document_id": "9001", "municipality": "TUMACO"}],
    "sigep_public_servants": [
        {"person_document_id": "1001", "municipality": "TUMACO", "department": "NARINO"}
    ],
    "mindeporte_actores": [
        {"person_document_id": "1001", "municipality": "TUMACO", "actor_id": "A-1"}
    ],
    "secop_process_bpin": [
        {
            "bpin_code": "BPIN-1",
            "supplier_document_id": "9001",
            "municipality": "TUMACO",
            "department": "NARINO",
            "contract_id": "C-1",
        }
    ],
    "dnp_obras_prioritarias": [
        {"bpin_code": "BPIN-1", "municipality": "TUMACO", "priority_id": "O-1"}
    ],
    "secop_sanctions": [{"supplier_document_id": "9001", "sanction_id": "S-1"}],
}


def _write_fixture_lake(root: Path) -> None:
    for source, rows in FIXTURE_ROWS.items():
        out = root / "raw" / f"source={source}" / "year=2026" / "month=04"
        out.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), out / "fixture.parquet")


@pytest.fixture()
def fixture_lake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_fixture_lake(tmp_path)
    return tmp_path


@pytest.mark.parametrize("signal_id", WAVE_B_SIGNALS)
def test_wave_b_sql_returns_public_safe_shape(fixture_lake: Path, signal_id: str) -> None:
    clear_signal_registry_cache()
    definition = get_signal_definition(signal_id)
    assert definition is not None
    assert definition.runner.kind == "duckdb"
    assert definition.public_safe is True
    assert definition.public_presentation == "territorial"

    con = duckdb.connect()
    for source in definition.sources:
        lakehouse_query.register_source(con, source)
    sql = lakehouse_query.signal_sql_path(signal_id).read_text(encoding="utf-8")
    cursor = con.execute(sql)
    columns = [item[0] for item in cursor.description or []]
    rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]

    assert rows, signal_id
    for row in rows:
        assert "entity_id" in row
        assert "scope_key" in row
        assert "risk_signal" in row
        assert public_signal_payload_leaks_identity(row) is False
