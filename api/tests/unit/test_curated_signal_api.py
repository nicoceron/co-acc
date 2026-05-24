from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc.main import app

if TYPE_CHECKING:
    from pathlib import Path

    from httpx import AsyncClient


def _write_curated_signal(root: Path) -> None:
    out = (
        root
        / "curated"
        / "table=signal_feature_procurement_sanctioned_supplier_awarded"
    )
    out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1:paco-1",
                "scope_type": "sanction_record",
                "risk_signal": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "contract_id": "C-1",
                "supplier_name": "Proveedor Sancionado SAS",
                "paco_record_id": "paco-1",
                "paco_feed": "responsabilidades_fiscales",
                "sanction_type": "Responsabilidad fiscal",
                "join_rule": "document_exact",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "https://paco.example/paco-1",
                ],
            }
        ]),
        out / "part-00000.parquet",
    )


def _write_curated_supplier_concentration(root: Path) -> None:
    out = (
        root
        / "curated"
        / "table=signal_feature_procurement_supplier_concentration_across_entities"
    )
    out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "signal_id": "procurement_supplier_concentration_across_entities",
                "entity_id": "doc:900765432",
                "entity_key": "900765432",
                "entity_label": "Company",
                "scope_key": "supplier:900765432",
                "scope_type": "supplier",
                "risk_signal": 0.8,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "supplier_name": "Proveedor Concentrado SAS",
                "contract_count": 50,
                "distinct_buyer_count": 50,
                "total_contract_value": 5_000_000_000.0,
                "evidence_refs": [
                    "https://secop.example/CX-1",
                    "https://secop.example/CX-2",
                ],
            }
        ]),
        out / "part-00000.parquet",
    )


@pytest.mark.anyio
async def test_signal_detail_reads_curated_samples_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)

    response = await client.get(
        "/api/v1/signals/procurement_sanctioned_supplier_awarded?limit=1"
    )

    assert response.status_code == 200
    data = response.json()
    assert data["definition"]["id"] == "procurement_sanctioned_supplier_awarded"
    assert data["sample_hits"][0]["entity_key"] == "900123456"
    assert data["sample_hits"][0]["sources"] == [
        {"database": "paco_sanctions", "record_id": None, "extracted_at": None},
        {"database": "fiscal_findings", "record_id": None, "extracted_at": None},
        {"database": "fiscal_responsibility", "record_id": None, "extracted_at": None},
        {"database": "secop_ii_contracts", "record_id": None, "extracted_at": None},
    ]
    assert data["sample_hits"][0]["evidence_refs"] == [
        "https://secop.example/C-1",
        "https://paco.example/paco-1",
    ]


@pytest.mark.anyio
async def test_signal_list_counts_curated_hits_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)
    _write_curated_supplier_concentration(tmp_path)

    response = await client.get("/api/v1/signals/")

    assert response.status_code == 200
    signals = {row["id"]: row for row in response.json()["signals"]}
    assert signals["procurement_sanctioned_supplier_awarded"]["hit_count"] == 1
    assert signals["procurement_supplier_concentration_across_entities"]["hit_count"] == 1


@pytest.mark.anyio
async def test_supplier_concentration_detail_reads_curated_sample_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_supplier_concentration(tmp_path)

    response = await client.get(
        "/api/v1/signals/procurement_supplier_concentration_across_entities?limit=1"
    )

    assert response.status_code == 200
    hit = response.json()["sample_hits"][0]
    assert hit["entity_key"] == "900765432"
    assert hit["sources"] == [
        {"database": "secop_ii_contracts", "record_id": None, "extracted_at": None}
    ]
    assert hit["evidence_refs"] == [
        "https://secop.example/CX-1",
        "https://secop.example/CX-2",
    ]


@pytest.mark.anyio
async def test_health_reports_ok_when_only_curated_lake_is_available(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["neo4j"] == "unavailable"
