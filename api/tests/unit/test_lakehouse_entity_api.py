from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc.config import settings
from coacc.main import app
from coacc.services.lakehouse_signal_service import materialized_signal_counts

if TYPE_CHECKING:
    from pathlib import Path

    from httpx import AsyncClient


def _write_dim_tables(root: Path) -> None:
    company_out = root / "curated" / "table=dim_company"
    person_out = root / "curated" / "table=dim_person"
    company_out.mkdir(parents=True)
    person_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "entity_uid": "company:9001234568",
                "nit_canonical": "9001234568",
                "nit_variants": ["900123456", "9001234568"],
                "name_canonical": "Proveedor Sancionado SAS",
                "name_variants": ["Proveedor Sancionado SAS"],
                "first_seen": "2026-01-01",
                "last_seen": "2026-06-01",
                "sources": ["secop_ii_contracts", "paco_sanctions"],
                "source_row_count": 2,
            }
        ]),
        company_out / "part-00000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([
            {
                "entity_uid": "person:123456789",
                "cedula_canonical": "123456789",
                "nit_canonical": None,
                "document_variants": ["123456789"],
                "name_canonical": "Persona Teste",
                "name_variants": ["Persona Teste"],
                "document_types": ["CC"],
                "first_seen": "2026-01-01",
                "last_seen": "2026-06-01",
                "institution_names": ["Entidad Uno"],
                "sources": ["5u9e-g5w9"],
                "source_row_count": 1,
            }
        ]),
        person_out / "part-00000.parquet",
    )


def _write_signal_run(root: Path) -> None:
    run_id = "lake-entity-run"
    hits_out = root / "curated" / "signal_hits" / f"run_id={run_id}"
    evidence_out = root / "curated" / "evidence_bundles" / f"run_id={run_id}"
    hits_out.mkdir(parents=True)
    evidence_out.mkdir(parents=True)
    hit_row = {
        "run_id": run_id,
        "signal_id": "procurement_sanctioned_supplier_awarded",
        "signal_version": 1,
        "hit_id": "hit-entity-1",
        "entity_uid": "doc:900123456",
        "entity_key": "900123456",
        "entity_label": "Company",
        "scope_key": "C-1:paco-1",
        "scope_type": "sanction_record",
        "severity": "critical",
        "score": 1.0,
        "title": "Award to supplier with official sanctions or findings",
        "description": "Supplier holds sanction records.",
        "public_safe": True,
        "reviewer_only": False,
        "identity_confidence": 1.0,
        "identity_match_type": "EXACT_COMPANY_NIT",
        "identity_quality": "exact",
        "evidence_count": 1,
        "evidence_bundle_id": "bundle:hit-entity-1",
        "evidence_refs": ["https://secop.example/C-1"],
        "created_at": "2026-06-01T01:00:00+00:00",
        "first_seen_at": "2026-06-01T01:00:00+00:00",
        "last_seen_at": "2026-06-01T01:01:00+00:00",
    }
    pq.write_table(
        pa.Table.from_pylist([hit_row, dict(hit_row)]),
        hits_out / "part-00000.parquet",
    )
    evidence_row = {
        "run_id": run_id,
        "bundle_id": "bundle:hit-entity-1",
        "hit_id": "hit-entity-1",
        "signal_id": "procurement_sanctioned_supplier_awarded",
        "item_index": 1,
        "source_id": "secop_ii_contracts",
        "record_id": None,
        "parquet_path": "lake/curated/signal_feature",
        "row_selector": "scope_key=C-1:paco-1",
        "label": "https://secop.example/C-1",
        "url": "https://secop.example/C-1",
        "observed_at": "2026-06-01T01:01:00+00:00",
        "public_safe": True,
        "identity_match_type": "EXACT_COMPANY_NIT",
        "identity_quality": "exact",
    }
    pq.write_table(
        pa.Table.from_pylist([evidence_row, dict(evidence_row)]),
        evidence_out / "part-00000.parquet",
    )
    manifest_out = root / "meta" / "signal_runs"
    manifest_out.mkdir(parents=True)
    (manifest_out / f"{run_id}.json").write_text(
        json.dumps({
            "run_id": run_id,
            "generated_at": "2026-06-01T01:00:00+00:00",
            "finished_at": "2026-06-01T01:01:00+00:00",
            "status": "completed",
            "hit_count": 1,
            "evidence_count": 1,
            "outputs": {
                "signal_hits": str(hits_out),
                "evidence_bundles": str(evidence_out),
            },
        }),
        encoding="utf-8",
    )


def _write_anomaly_run(root: Path) -> None:
    run_id = "anomaly-run"
    scores_out = root / "curated" / "anomaly_scores" / f"run_id={run_id}"
    scores_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": run_id,
                "model_run_id": "model-run",
                "feature_run_id": "feature-run",
                "scored_at": "2026-06-01T02:00:00+00:00",
                "contract_id": "C-1",
                "entity_uid": "company:9001234568",
                "score": 0.92,
                "score_confidence": "low",
                "top_features": ["log_value_z_buyer", "timing_anomaly_score"],
                "prior_sanction_supplier": True,
                "process_url": "https://secop.example/C-1",
            },
            {
                "run_id": run_id,
                "model_run_id": "model-run",
                "feature_run_id": "feature-run",
                "scored_at": "2026-06-01T02:00:00+00:00",
                "contract_id": "C-2",
                "entity_uid": "company:800000000",
                "score": 0.44,
                "score_confidence": "standard",
                "top_features": ["buyer_supplier_concentration"],
                "prior_sanction_supplier": False,
                "process_url": "https://secop.example/C-2",
            },
        ]),
        scores_out / "part-00000.parquet",
    )
    current_out = root / "models" / "anomaly"
    current_out.mkdir(parents=True)
    (current_out / "current.json").write_text(
        json.dumps({
            "run_id": "model-run",
            "score_run_id": run_id,
            "feature_run_id": "feature-run",
            "trained_at": "2026-06-01T01:55:00+00:00",
            "metrics": {"precision_at_100": 0.03},
        }),
        encoding="utf-8",
    )


def _write_lake_narrative(root: Path, case_id: str, markdown: str) -> None:
    safe = "".join(ch if ch.isalnum() or ch in "_.=-" else "_" for ch in case_id).strip("._")
    out = root / "curated" / "narratives"
    out.mkdir(parents=True)
    (out / f"{safe or 'case'}.md").write_text(markdown, encoding="utf-8")


@pytest.mark.anyio
async def test_entity_lookup_reads_curated_dimension_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/entity/by-element-id/company:9001234568")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "company:9001234568"
    assert payload["type"] == "company"
    assert payload["properties"]["nit"] == "9001234568"
    assert payload["properties"]["razon_social"] == "Proveedor Sancionado SAS"


@pytest.mark.anyio
async def test_entity_lookup_prefers_curated_dimension_when_neo4j_is_connected(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/entity/9001234568")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == "company:9001234568"
    assert payload["properties"]["razon_social"] == "Proveedor Sancionado SAS"


@pytest.mark.anyio
async def test_search_reads_curated_dimensions_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/search?q=Proveedor&type=company")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["results"][0]["id"] == "company:9001234568"
    assert payload["results"][0]["name"] == "Proveedor Sancionado SAS"


@pytest.mark.anyio
async def test_search_prefers_curated_dimensions_when_neo4j_is_connected(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/search?q=Proveedor&type=company")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["results"][0]["id"] == "company:9001234568"


@pytest.mark.anyio
async def test_search_matches_nit_root_against_curated_dimensions(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/search?q=900123456&type=company")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["results"][0]["document"] == "9001234568"


@pytest.mark.anyio
async def test_entity_signals_reads_materialized_hits_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)
    _write_signal_run(tmp_path)

    response = await client.get("/api/v1/entity/900123456/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entity_key"] == "900123456"
    assert payload["total"] == 1
    assert payload["signals"][0]["hit_id"] == "hit-entity-1"
    assert len(payload["signals"][0]["evidence_items"]) == 1
    assert payload["signals"][0]["evidence_items"][0]["url"] == "https://secop.example/C-1"

    linked_response = await client.get("/api/v1/entity/company:9001234568/signals")
    assert linked_response.status_code == 200
    assert linked_response.json()["total"] == 1


@pytest.mark.anyio
async def test_entity_signals_prefers_materialized_hits_when_neo4j_is_connected(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_dim_tables(tmp_path)
    _write_signal_run(tmp_path)

    response = await client.get("/api/v1/entity/900123456/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["signals"][0]["hit_id"] == "hit-entity-1"


@pytest.mark.anyio
async def test_lake_case_routes_expose_anomaly_scores_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_anomaly_run(tmp_path)
    _write_lake_narrative(tmp_path, "C-1", "# Lead\nNarrativa verificada.")

    list_response = await client.get("/api/v1/cases/")

    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["total"] == 2
    assert payload["cases"][0]["anomaly_score"]["contract_id"] == "C-1"
    assert payload["cases"][0]["anomaly_score"]["score_confidence"] == "low"

    detail_response = await client.get(f"/api/v1/cases/{payload['cases'][0]['id']}")

    assert detail_response.status_code == 200
    case = detail_response.json()
    assert case["signal_count"] == 0
    assert case["anomaly_score"]["score"] == 0.92
    assert case["narrative_markdown"] == "# Lead\nNarrativa verificada."
    assert case["narrative_generated_at"] is not None
    assert case["events"][0]["type"] == "anomaly_score"
    assert case["evidence_bundles"][0]["evidence_items"][0]["url"] == "https://secop.example/C-1"


@pytest.mark.anyio
async def test_lake_signal_case_detail_attaches_matching_anomaly_score(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_signal_run(tmp_path)
    _write_anomaly_run(tmp_path)
    _write_lake_narrative(tmp_path, "hit-entity-1", "# Lead\nSignal narrative.")

    list_response = await client.get("/api/v1/cases/")
    response = await client.get("/api/v1/cases/hit-entity-1")

    assert list_response.status_code == 200
    assert list_response.json()["cases"][0]["id"] == "hit-entity-1"
    assert list_response.json()["cases"][0]["signal_count"] == 1
    assert response.status_code == 200
    payload = response.json()
    assert payload["signals"][0]["hit_id"] == "hit-entity-1"
    assert payload["anomaly_score"]["contract_id"] == "C-1"
    assert payload["narrative_markdown"] == "# Lead\nSignal narrative."
    assert payload["anomaly_score"]["prior_sanction_supplier"] is True


@pytest.mark.anyio
async def test_entity_anomaly_scores_read_lake_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_anomaly_run(tmp_path)

    response = await client.get("/api/v1/entity/company:9001234568/anomaly-scores")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entity_id"] == "company:9001234568"
    assert payload["total"] == 1
    assert payload["scores"][0]["contract_id"] == "C-1"


def test_materialized_signal_counts_deduplicate_lake_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_signal_run(tmp_path)

    counts = materialized_signal_counts()

    assert counts["procurement_sanctioned_supplier_awarded"][0] == 1


@pytest.mark.anyio
async def test_patterns_read_materialized_lake_hits_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "patterns_enabled", True)
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)
    _write_signal_run(tmp_path)

    response = await client.get("/api/v1/patterns/company:9001234568")
    specific_response = await client.get(
        "/api/v1/patterns/company:9001234568/sanctioned_supplier_record"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["patterns"][0]["pattern_id"] == "sanctioned_supplier_record"
    assert payload["patterns"][0]["data"]["hit_id"] == "hit-entity-1"
    assert specific_response.status_code == 200
    assert specific_response.json()["total"] == 1


@pytest.mark.anyio
async def test_patterns_prefer_materialized_lake_hits_when_neo4j_is_connected(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "patterns_enabled", True)
    _write_dim_tables(tmp_path)
    _write_signal_run(tmp_path)

    response = await client.get("/api/v1/patterns/company:9001234568")
    specific_response = await client.get(
        "/api/v1/patterns/company:9001234568/sanctioned_supplier_record"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["patterns"][0]["pattern_id"] == "sanctioned_supplier_record"
    assert specific_response.status_code == 200
    assert specific_response.json()["total"] == 1


@pytest.mark.anyio
async def test_public_patterns_read_lake_company_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "patterns_enabled", True)
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)
    _write_signal_run(tmp_path)

    response = await client.get("/api/v1/public/patterns/company/9001234568")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entity_id"] == "company:9001234568"
    assert payload["total"] == 1
    assert payload["patterns"][0]["sources"][0]["database"] == "secop_ii_contracts"
    assert "document_id" not in str(payload).lower()


@pytest.mark.anyio
async def test_lake_search_hides_people_in_public_mode(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "public_mode", True)
    monkeypatch.setattr(settings, "public_allow_person", False)
    app.state.neo4j_driver = None
    _write_dim_tables(tmp_path)

    response = await client.get("/api/v1/search?q=Persona")

    assert response.status_code == 200
    assert response.json()["total"] == 0
