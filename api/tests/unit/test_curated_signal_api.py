from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc.main import app
from coacc.services import lakehouse_signal_service

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


def _write_signal_run_manifest(root: Path) -> None:
    out = root / "meta" / "signal_runs"
    out.mkdir(parents=True)
    (out / "test-run.json").write_text(
        json.dumps({
            "run_id": "test-run",
            "generated_at": "2026-06-01T01:00:00+00:00",
            "finished_at": "2026-06-01T01:01:00+00:00",
            "status": "completed",
            "hit_count": 1,
        }),
        encoding="utf-8",
    )


def _write_signal_run_manifest_payload(
    root: Path,
    *,
    run_id: str,
    finished_at: str,
) -> None:
    out = root / "meta" / "signal_runs"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{run_id}.json").write_text(
        json.dumps({
            "run_id": run_id,
            "generated_at": finished_at,
            "finished_at": finished_at,
            "status": "completed",
            "hit_count": 1,
            "evidence_count": 1,
        }),
        encoding="utf-8",
    )


def _write_materialized_signal_run(root: Path) -> None:
    run_id = "lake-run"
    hits_out = root / "curated" / "signal_hits" / f"run_id={run_id}"
    evidence_out = root / "curated" / "evidence_bundles" / f"run_id={run_id}"
    feature_path = "lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded"
    selector_prefix = "signal_id=procurement_sanctioned_supplier_awarded;scope_key=C-1:paco-1"
    hits_out.mkdir(parents=True)
    evidence_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": run_id,
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "signal_version": 1,
                "hit_id": "hit-lake-1",
                "entity_uid": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1:paco-1",
                "scope_type": "sanction_record",
                "severity": "critical",
                "score": 1.0,
                "title": "Award to supplier with official sanctions or findings",
                "description": "Supplier holds sanction or fiscal finding records.",
                "public_safe": True,
                "reviewer_only": False,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_count": 2,
                "evidence_bundle_id": "bundle:hit-lake-1",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "https://paco.example/paco-1",
                ],
                "created_at": "2026-06-01T01:00:00+00:00",
                "first_seen_at": "2026-06-01T01:00:00+00:00",
                "last_seen_at": "2026-06-01T01:01:00+00:00",
            }
        ]),
        hits_out / "part-00000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": run_id,
                "bundle_id": "bundle:hit-lake-1",
                "hit_id": "hit-lake-1",
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "item_index": 1,
                "source_id": "secop_ii_contracts",
                "record_id": None,
                "parquet_path": feature_path,
                "row_selector": f"{selector_prefix};evidence_index=1",
                "label": "https://secop.example/C-1",
                "url": "https://secop.example/C-1",
                "observed_at": "2026-06-01T01:01:00+00:00",
                "public_safe": True,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
            },
            {
                "run_id": run_id,
                "bundle_id": "bundle:hit-lake-1",
                "hit_id": "hit-lake-1",
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "item_index": 2,
                "source_id": "paco_sanctions",
                "record_id": None,
                "parquet_path": feature_path,
                "row_selector": f"{selector_prefix};evidence_index=2",
                "label": "https://paco.example/paco-1",
                "url": "https://paco.example/paco-1",
                "observed_at": "2026-06-01T01:01:00+00:00",
                "public_safe": True,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
            },
        ]),
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
            "evidence_count": 2,
            "outputs": {
                "signal_hits": str(hits_out),
                "evidence_bundles": str(evidence_out),
            },
        }),
        encoding="utf-8",
    )


def _write_materialized_partial_signal_run(root: Path) -> None:
    run_id = "partial-lake-run"
    hits_out = root / "curated" / "signal_hits" / f"run_id={run_id}"
    evidence_out = root / "curated" / "evidence_bundles" / f"run_id={run_id}"
    hits_out.mkdir(parents=True)
    evidence_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": run_id,
                "signal_id": "procurement_single_bidder_high_value",
                "signal_version": 1,
                "hit_id": "hit-partial-1",
                "entity_uid": "doc:902000111",
                "entity_key": "902000111",
                "entity_label": "Company",
                "scope_key": "PV-1",
                "scope_type": "procurement_process",
                "severity": "medium",
                "score": 0.78,
                "title": "Single bidder on high-value procurement process",
                "description": "Partial contract-only signal.",
                "public_safe": True,
                "reviewer_only": False,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_count": 1,
                "evidence_bundle_id": "bundle:hit-partial-1",
                "evidence_refs": ["https://secop.example/CV-1"],
                "created_at": "2026-06-01T01:00:00+00:00",
                "first_seen_at": "2026-06-01T01:00:00+00:00",
                "last_seen_at": "2026-06-01T01:01:00+00:00",
            }
        ]),
        hits_out / "part-00000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": run_id,
                "bundle_id": "bundle:hit-partial-1",
                "hit_id": "hit-partial-1",
                "signal_id": "procurement_single_bidder_high_value",
                "item_index": 1,
                "source_id": "secop_ii_contracts",
                "record_id": None,
                "parquet_path": (
                    "lake/curated/table=signal_feature_procurement_single_bidder_high_value"
                ),
                "row_selector": "scope_key=PV-1;evidence_index=1",
                "label": "https://secop.example/CV-1",
                "url": "https://secop.example/CV-1",
                "observed_at": "2026-06-01T01:01:00+00:00",
                "public_safe": True,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
            }
        ]),
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


def _write_curated_repeat_awards(root: Path) -> None:
    out = (
        root
        / "curated"
        / "table=signal_feature_procurement_repeat_awards_same_supplier"
    )
    out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "signal_id": "procurement_repeat_awards_same_supplier",
                "entity_id": "doc:901000111",
                "entity_key": "901000111",
                "entity_label": "Company",
                "scope_key": "buyer:800999888",
                "scope_type": "buyer",
                "risk_signal": 0.75,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "supplier_name": "Proveedor Recurrente SAS",
                "buyer_document_id": "800999888",
                "buyer_name": "Comprador Recurrente",
                "contract_count": 10,
                "total_contract_value": 2_000_000_000.0,
                "evidence_refs": [
                    "https://secop.example/CR-1",
                    "https://secop.example/CR-2",
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
    _write_curated_repeat_awards(tmp_path)

    response = await client.get("/api/v1/signals/")

    assert response.status_code == 200
    signals = {row["id"]: row for row in response.json()["signals"]}
    assert signals["procurement_sanctioned_supplier_awarded"]["hit_count"] == 1
    assert signals["procurement_sanctioned_supplier_awarded"]["materialized"] is True
    assert (
        signals["procurement_sanctioned_supplier_awarded"]["materialization_state"]
        == "materialized"
    )
    assert signals["procurement_single_bidder_high_value"]["materialized"] is False
    assert (
        signals["procurement_single_bidder_high_value"]["materialization_state"]
        == "registered_only"
    )
    assert signals["procurement_supplier_concentration_across_entities"]["hit_count"] == 1
    assert signals["procurement_repeat_awards_same_supplier"]["hit_count"] == 1


@pytest.mark.anyio
async def test_signal_list_uses_materialized_lake_severity(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_materialized_partial_signal_run(tmp_path)

    response = await client.get("/api/v1/signals/")
    detail_response = await client.get(
        "/api/v1/signals/procurement_single_bidder_high_value?limit=1"
    )

    assert response.status_code == 200
    assert detail_response.status_code == 200
    signals = {row["id"]: row for row in response.json()["signals"]}
    partial = signals["procurement_single_bidder_high_value"]
    assert partial["materialized"] is True
    assert partial["hit_count"] == 1
    assert partial["severity"] == "medium"
    detail = detail_response.json()
    assert detail["definition"]["severity"] == "medium"
    assert detail["sample_hits"][0]["severity"] == "medium"


@pytest.mark.anyio
async def test_signal_list_can_require_materialized_public_scope(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    from coacc.config import settings

    monkeypatch.setattr(settings, "coacc_signals_require_materialized", True)
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)

    response = await client.get("/api/v1/signals/")

    assert response.status_code == 200
    signal_ids = {row["id"] for row in response.json()["signals"]}
    assert signal_ids == {"procurement_sanctioned_supplier_awarded"}


@pytest.mark.anyio
async def test_signal_list_reports_latest_lake_signal_run(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)
    _write_signal_run_manifest(tmp_path)

    response = await client.get("/api/v1/signals/")

    assert response.status_code == 200
    data = response.json()
    assert data["last_run_id"] == "test-run"
    assert data["last_refreshed_at"] == "2026-06-01T01:01:00+00:00"


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
async def test_repeat_awards_detail_reads_curated_sample_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_repeat_awards(tmp_path)

    response = await client.get(
        "/api/v1/signals/procurement_repeat_awards_same_supplier?limit=1"
    )

    assert response.status_code == 200
    hit = response.json()["sample_hits"][0]
    assert hit["entity_key"] == "901000111"
    assert hit["scope_key"] == "buyer:800999888"
    assert hit["sources"] == [
        {"database": "secop_ii_contracts", "record_id": None, "extracted_at": None}
    ]
    assert hit["evidence_refs"] == [
        "https://secop.example/CR-1",
        "https://secop.example/CR-2",
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


@pytest.mark.anyio
async def test_health_reports_completed_lake_signal_run(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_curated_signal(tmp_path)
    _write_signal_run_manifest(tmp_path)

    response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["last_signal_run_id"] == "test-run"
    assert data["last_signal_run_status"] == "completed"


@pytest.mark.anyio
async def test_signal_routes_read_materialized_signal_run(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_materialized_signal_run(tmp_path)

    list_response = await client.get("/api/v1/signals/")
    detail_response = await client.get(
        "/api/v1/signals/procurement_sanctioned_supplier_awarded?limit=1"
    )

    assert list_response.status_code == 200
    signals = {row["id"]: row for row in list_response.json()["signals"]}
    assert signals["procurement_sanctioned_supplier_awarded"]["hit_count"] == 1
    assert list_response.json()["last_run_id"] == "lake-run"
    assert detail_response.status_code == 200
    hit = detail_response.json()["sample_hits"][0]
    assert hit["run_id"] == "lake-run"
    assert hit["hit_id"] == "hit-lake-1"
    assert hit["evidence_items"][1]["source_id"] == "paco_sanctions"


@pytest.mark.anyio
async def test_case_routes_read_materialized_lake_evidence_without_neo4j(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_materialized_signal_run(tmp_path)

    list_response = await client.get("/api/v1/cases/")
    detail_response = await client.get("/api/v1/cases/hit-lake-1")

    assert list_response.status_code == 200
    assert list_response.json()["total"] == 1
    assert list_response.json()["cases"][0]["id"] == "hit-lake-1"
    assert detail_response.status_code == 200
    case = detail_response.json()
    assert case["id"] == "hit-lake-1"
    assert case["signal_count"] == 1
    assert case["signals"][0]["signal_id"] == "procurement_sanctioned_supplier_awarded"
    assert case["evidence_bundles"][0]["bundle_id"] == "bundle:hit-lake-1"
    assert case["evidence_bundles"][0]["evidence_items"][1]["url"] == "https://paco.example/paco-1"


def test_latest_lake_signal_run_uses_finished_at_not_filename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_signal_run_manifest_payload(
        tmp_path,
        run_id="zz-old-run",
        finished_at="2026-06-01T01:00:00+00:00",
    )
    _write_signal_run_manifest_payload(
        tmp_path,
        run_id="aa-new-run",
        finished_at="2026-06-01T02:00:00+00:00",
    )

    run = lakehouse_signal_service.latest_signal_run()

    assert run is not None
    assert run.run_id == "aa-new-run"
