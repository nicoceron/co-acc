from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import AsyncClient

from coacc.main import app
from coacc.services.baseline_service import BASELINE_QUERIES


def _write_lake_baseline_tables(root: Path) -> None:
    company_out = root / "curated" / "table=dim_company"
    awards_out = root / "curated" / "table=fct_procurement_contract_awards"
    company_out.mkdir(parents=True)
    awards_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "entity_uid": "company:9001234568",
                "nit_canonical": "9001234568",
                "nit_variants": ["900123456", "9001234568"],
                "name_canonical": "Proveedor Baseline SAS",
                "name_variants": ["Proveedor Baseline SAS"],
                "first_seen": "2026-01-01",
                "last_seen": "2026-06-01",
                "sources": ["secop_ii_contracts"],
                "source_row_count": 2,
            }
        ]),
        company_out / "part-00000.parquet",
    )
    rows = [
        {
            "award_row_id": 1,
            "contract_id": "target-1",
            "supplier_nit_canonical": "9001234568",
            "supplier_document_key": "900123456",
            "supplier_document_digits": "900123456",
            "supplier_entity_id": "doc:900123456",
            "supplier_name": "Proveedor Baseline SAS",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 100.0,
        },
        {
            "award_row_id": 2,
            "contract_id": "target-2",
            "supplier_nit_canonical": "9001234568",
            "supplier_document_key": "900123456",
            "supplier_document_digits": "900123456",
            "supplier_entity_id": "doc:900123456",
            "supplier_name": "Proveedor Baseline SAS",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 200.0,
        },
        {
            "award_row_id": 3,
            "contract_id": "peer-1",
            "supplier_nit_canonical": "8000000011",
            "supplier_document_key": "800000001",
            "supplier_document_digits": "800000001",
            "supplier_entity_id": "doc:800000001",
            "supplier_name": "Peer Uno",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 100.0,
        },
        {
            "award_row_id": 4,
            "contract_id": "peer-2",
            "supplier_nit_canonical": "8000000022",
            "supplier_document_key": "800000002",
            "supplier_document_digits": "800000002",
            "supplier_entity_id": "doc:800000002",
            "supplier_name": "Peer Dos",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 300.0,
        },
        {
            "award_row_id": 5,
            "contract_id": "peer-3",
            "supplier_nit_canonical": "8000000022",
            "supplier_document_key": "800000002",
            "supplier_document_digits": "800000002",
            "supplier_entity_id": "doc:800000002",
            "supplier_name": "Peer Dos",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 300.0,
        },
        {
            "award_row_id": 6,
            "contract_id": "peer-4",
            "supplier_nit_canonical": "8000000022",
            "supplier_document_key": "800000002",
            "supplier_document_digits": "800000002",
            "supplier_entity_id": "doc:800000002",
            "supplier_name": "Peer Dos",
            "sector": "Salud",
            "department": "Bogota",
            "city": "Bogota",
            "contract_value": 300.0,
        },
    ]
    pq.write_table(pa.Table.from_pylist(rows), awards_out / "part-00000.parquet")


def test_baseline_query_files_exist() -> None:
    from coacc.services.neo4j_service import CypherLoader

    for dimension, query_name in BASELINE_QUERIES.items():
        try:
            CypherLoader.load(query_name)
        except FileNotFoundError:
            pytest.fail(
                f"Missing .cypher file for baseline {dimension}: {query_name}.cypher"
            )
        finally:
            CypherLoader.clear_cache()


def test_baseline_queries_cover_dimensions() -> None:
    assert "sector" in BASELINE_QUERIES
    assert "region" in BASELINE_QUERIES


@pytest.mark.anyio
async def test_baseline_invalid_dimension(client: AsyncClient) -> None:
    response = await client.get("/api/v1/baseline/test-id?dimension=invalid")
    assert response.status_code == 400
    assert "Invalid dimension" in response.json()["detail"]


@pytest.mark.anyio
async def test_baseline_endpoint_returns_200(client: AsyncClient) -> None:
    response = await client.get("/api/v1/baseline/test-id")
    assert response.status_code == 200
    data = response.json()
    assert "comparisons" in data
    assert "total" in data
    assert data["entity_id"] == "test-id"


@pytest.mark.anyio
async def test_baseline_with_sector_dimension(client: AsyncClient) -> None:
    response = await client.get("/api/v1/baseline/test-id?dimension=sector")
    assert response.status_code == 200
    data = response.json()
    assert data["entity_id"] == "test-id"


@pytest.mark.anyio
async def test_baseline_with_region_dimension(client: AsyncClient) -> None:
    response = await client.get("/api/v1/baseline/test-id?dimension=region")
    assert response.status_code == 200
    data = response.json()
    assert data["entity_id"] == "test-id"


@pytest.mark.anyio
async def test_baseline_reads_lake_procurement_awards_when_neo4j_is_connected(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_lake_baseline_tables(tmp_path)

    response = await client.get("/api/v1/baseline/900123456")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entity_id"] == "900123456"
    assert payload["total"] == 2
    assert {item["comparison_dimension"] for item in payload["comparisons"]} == {
        "sector",
        "region",
    }
    sector = next(
        item for item in payload["comparisons"] if item["comparison_dimension"] == "sector"
    )
    assert sector["company_id"] == "company:9001234568"
    assert sector["company_name"] == "Proveedor Baseline SAS"
    assert sector["company_document_id"] == "9001234568"
    assert sector["comparison_key"] == "Salud"
    assert sector["contract_count"] == 2
    assert sector["total_value"] == 300.0
    assert sector["peer_count"] == 3
    assert sector["peer_avg_contracts"] == 2.0
    assert sector["sources"][0]["database"] == "lake_curated_procurement"


@pytest.mark.anyio
async def test_baseline_reads_lake_procurement_awards_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_lake_baseline_tables(tmp_path)

    response = await client.get("/api/v1/baseline/900123456?dimension=region")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["comparisons"][0]["comparison_dimension"] == "region"
    assert payload["comparisons"][0]["comparison_key"] == "Bogota"
