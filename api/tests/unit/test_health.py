from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from coacc.services.source_registry import load_source_registry, source_registry_summary


@pytest.mark.anyio
async def test_health_returns_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "default-src 'none'" in response.headers["content-security-policy"]


@pytest.mark.anyio
async def test_meta_health_has_security_headers(client: AsyncClient) -> None:
    with patch(
        "coacc.routers.meta.execute_query_single",
        new_callable=AsyncMock,
        return_value={"ok": 1},
    ):
        response = await client.get("/api/v1/meta/health")

    assert response.status_code == 200
    assert response.json() == {"neo4j": "connected"}
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"


@pytest.mark.anyio
async def test_meta_sources(client: AsyncClient) -> None:
    response = await client.get("/api/v1/meta/sources")
    assert response.status_code == 200
    data = response.json()
    summary = source_registry_summary(load_source_registry())
    assert "sources" in data
    assert len(data["sources"]) == summary["universe_v1_sources"]
    source_ids = [s["id"] for s in data["sources"]]
    assert "secop_integrado" in source_ids
    assert "secop_sanctions" in source_ids
    assert "paco_sanctions" in source_ids
    assert "pte_sector_commitments" in source_ids
    assert "pte_top_contracts" in source_ids
    assert "secop_ii_processes" in source_ids
    assert "secop_ii_contracts" in source_ids
    assert "secop_suppliers" in source_ids
    assert "sigep_public_servants" in source_ids
    assert "sigep_sensitive_positions" in source_ids
    assert "asset_disclosures" in source_ids
    assert "conflict_disclosures" in source_ids
    assert "sgr_projects" in source_ids
    assert "sgr_expense_execution" in source_ids
    assert "health_providers" in source_ids
    assert "higher_ed_enrollment" in source_ids
    assert "igac_property_transactions" in source_ids
    assert "mapa_inversiones_projects" in source_ids
    assert "registraduria_death_status_checks" in source_ids
    assert "rues_chambers" in source_ids
    assert "cuentas_claras_income_2019" in source_ids
    assert "supersoc_top_companies" in source_ids
    first = data["sources"][0]
    assert "status" in first
    assert "implementation_state" in first
    assert "load_state" in first
    assert "in_universe_v1" in first
    assert "discovery_status" in first
    assert "last_seen_url" in first
    assert "public_access_mode" in first
    assert "quality_status" in first


@pytest.mark.anyio
async def test_meta_stats(client: AsyncClient) -> None:
    mock_record = {
        "total_nodes": 87_500_000,
        "total_relationships": 53_100_000,
        "person_count": 2_450_000,
        "company_count": 58_500_000,
        "health_count": 602_000,
        "finance_count": 24_000_000,
        "contract_count": 1_080_000,
        "sanction_count": 69_000,
        "election_count": 17_000,
        "amendment_count": 98_000,
        "embargo_count": 79_000,
        "education_count": 224_000,
        "convenio_count": 67_000,
        "laborstats_count": 29_500,
        "offshore_entity_count": 810_000,
        "offshore_officer_count": 500_000,
        "global_pep_count": 15_000,
        "cvm_proceeding_count": 5_000,
        "expense_count": 2_000_000,
        "pep_record_count": 100_000,
        "expulsion_count": 10_000,
        "leniency_count": 34,
        "international_sanction_count": 12_000,
        "gov_card_expense_count": 500_000,
        "gov_travel_count": 150_000,
        "bid_count": 2_000_000,
        "fund_count": 30_000,
        "dou_act_count": 50_000,
        "tax_waiver_count": 800_000,
        "municipal_finance_count": 100_000,
        "declared_asset_count": 5_000_000,
        "party_membership_count": 15_000_000,
        "barred_ngo_count": 5_000,
        "bcb_penalty_count": 10_000,
        "labor_movement_count": 2_000_000,
        "legal_case_count": 100_000,
        "judicial_case_count": 50_000,
        "cpi_count": 500,
        "inquiry_requirement_count": 2_500,
        "inquiry_session_count": 1_400,
        "municipal_bid_count": 8_000_000,
        "municipal_contract_count": 6_000_000,
        "municipal_gazette_act_count": 4_000_000,
    }

    # Reset the stats cache between tests
    import coacc.routers.meta as meta_module
    meta_module._stats_cache = None
    meta_module._stats_cache_time = 0.0

    with patch(
        "coacc.routers.meta.execute_query_single",
        new_callable=AsyncMock,
        return_value=mock_record,
    ):
        response = await client.get("/api/v1/meta/stats")

    assert response.status_code == 200
    data = response.json()

    assert data["total_nodes"] == 87_500_000
    assert data["total_relationships"] == 53_100_000
    assert data["person_count"] == 2_450_000
    assert data["company_count"] == 58_500_000
    assert data["health_count"] == 602_000
    assert data["finance_count"] == 24_000_000
    assert data["contract_count"] == 1_080_000
    assert data["sanction_count"] == 69_000
    assert data["election_count"] == 17_000
    assert data["amendment_count"] == 98_000
    assert data["embargo_count"] == 79_000
    assert data["education_count"] == 224_000
    assert data["convenio_count"] == 67_000
    assert data["laborstats_count"] == 29_500
    assert data["offshore_entity_count"] == 810_000
    assert data["offshore_officer_count"] == 500_000
    assert data["global_pep_count"] == 15_000
    assert data["cvm_proceeding_count"] == 5_000
    assert data["expense_count"] == 2_000_000
    assert data["pep_record_count"] == 100_000
    assert data["expulsion_count"] == 10_000
    assert data["leniency_count"] == 34
    assert data["international_sanction_count"] == 12_000
    assert data["gov_card_expense_count"] == 500_000
    assert data["gov_travel_count"] == 150_000
    assert data["bid_count"] == 2_000_000
    assert data["fund_count"] == 30_000
    assert data["dou_act_count"] == 50_000
    assert data["tax_waiver_count"] == 800_000
    assert data["municipal_finance_count"] == 100_000
    assert data["declared_asset_count"] == 5_000_000
    assert data["party_membership_count"] == 15_000_000
    assert data["barred_ngo_count"] == 5_000
    assert data["bcb_penalty_count"] == 10_000
    assert data["labor_movement_count"] == 2_000_000
    assert data["legal_case_count"] == 100_000
    assert data["judicial_case_count"] == 50_000
    assert data["cpi_count"] == 500
    assert data["inquiry_requirement_count"] == 2_500
    assert data["inquiry_session_count"] == 1_400
    assert data["municipal_bid_count"] == 8_000_000
    assert data["municipal_contract_count"] == 6_000_000
    assert data["municipal_gazette_act_count"] == 4_000_000
    assert data["source_document_count"] == 0
    assert data["ingestion_run_count"] == 0
    assert data["temporal_violation_count"] == 0
    summary = source_registry_summary(load_source_registry())
    assert data["data_sources"] == summary["universe_v1_sources"]
    assert data["implemented_sources"] == summary["implemented_sources"]
    assert data["loaded_sources"] == summary["loaded_sources"]
    assert data["healthy_sources"] == summary["healthy_sources"]
    assert data["stale_sources"] == summary["stale_sources"]
    assert data["blocked_external_sources"] == summary["blocked_external_sources"]
    assert data["quality_fail_sources"] == summary["quality_fail_sources"]
    assert data["discovered_uningested_sources"] == summary["discovered_uningested_sources"]


@pytest.mark.anyio
async def test_meta_suspicious_people_watchlist(client: AsyncClient) -> None:
    import coacc.routers.meta as meta_module

    meta_module._watchlist_cache = {}

    with patch(
        "coacc.routers.meta.execute_query",
        new_callable=AsyncMock,
        return_value=[
            {
                "entity_id": "4:abc",
                "name": "Adriana Maria Mejia Aguado",
                "document_id": "31862756",
                "suspicion_score": 13,
                "signal_types": 5,
                "office_count": 1,
                "donation_count": 22,
                "donation_value": 329_997_800.0,
                "candidacy_count": 3,
                "asset_count": 1,
                "asset_value": 0.0,
                "finance_count": 1,
                "finance_value": 0.0,
                "supplier_contract_count": 4,
                "supplier_contract_value": 1_276_000_000.0,
                "conflict_disclosure_count": 1,
                "disclosure_reference_count": 3,
                "corporate_activity_disclosure_count": 1,
                "donor_vendor_loop_count": 4,
                "offices": ["Gerente"],
            }
        ],
    ):
        response = await client.get("/api/v1/meta/watchlist/people?limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["people"][0]["entity_id"] == "4:abc"
    assert data["people"][0]["name"] == "Adriana Maria Mejia Aguado"
    assert data["people"][0]["suspicion_score"] == 13
    assert data["people"][0]["signal_types"] == 5
    assert data["people"][0]["donation_count"] == 22
    assert data["people"][0]["supplier_contract_count"] == 4
    assert data["people"][0]["disclosure_reference_count"] == 3
    assert data["people"][0]["donor_vendor_loop_count"] == 4
    assert data["people"][0]["offices"] == ["Gerente"]
    assert data["people"][0]["alerts"]
    assert data["people"][0]["alerts"][0]["finding_class"] == "incompatibility"
    assert data["people"][0]["alerts"][0]["human_review_needed"] is True


@pytest.mark.anyio
async def test_meta_suspicious_company_watchlist(client: AsyncClient) -> None:
    import coacc.routers.meta as meta_module

    meta_module._company_watchlist_cache = {}

    with patch(
        "coacc.routers.meta.execute_query",
        new_callable=AsyncMock,
        return_value=[
            {
                "entity_id": "4:def",
                "name": "Consorcio Andino",
                "document_id": "900123456",
                "suspicion_score": 11,
                "signal_types": 4,
                "contract_count": 9,
                "contract_value": 245_000_000.0,
                "buyer_count": 3,
                "sanction_count": 1,
                "official_officer_count": 2,
                "official_role_count": 2,
                "low_competition_bid_count": 5,
                "low_competition_bid_value": 110_000_000.0,
                "direct_invitation_bid_count": 2,
                "funding_overlap_event_count": 12,
                "funding_overlap_total": 930_000_000.0,
                "capacity_mismatch_contract_count": 9,
                "capacity_mismatch_contract_value": 245_000_000.0,
                "capacity_mismatch_revenue_ratio": 6.2,
                "capacity_mismatch_asset_ratio": 3.1,
                "execution_gap_contract_count": 3,
                "execution_gap_invoice_total": 47_000_000.0,
                "commitment_gap_contract_count": 1,
                "commitment_gap_total": 8_000_000.0,
                "official_names": ["Ana Perez"],
            }
        ],
    ):
        response = await client.get("/api/v1/meta/watchlist/companies?limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["companies"][0]["entity_id"] == "4:def"
    assert data["companies"][0]["name"] == "Consorcio Andino"
    assert data["companies"][0]["document_id"] == "900123456"
    assert data["companies"][0]["suspicion_score"] == 11
    assert data["companies"][0]["signal_types"] == 4
    assert data["companies"][0]["low_competition_bid_count"] == 5
    assert data["companies"][0]["funding_overlap_event_count"] == 12
    assert data["companies"][0]["capacity_mismatch_revenue_ratio"] == 6.2
    assert data["companies"][0]["official_names"] == ["Ana Perez"]
    assert data["companies"][0]["alerts"]
    assert data["companies"][0]["alerts"][0]["human_review_needed"] is True


@pytest.mark.anyio
async def test_meta_suspicious_buyer_watchlist(client: AsyncClient) -> None:
    import coacc.routers.meta as meta_module

    meta_module._buyer_watchlist_cache = {}
    meta_module._snapshot_cache = {}

    with patch(
        "coacc.routers.meta.execute_query",
        new_callable=AsyncMock,
        return_value=[
            {
                "buyer_id": "830000001",
                "buyer_name": "Alcaldia de Prueba",
                "buyer_document_id": "830000001",
                "suspicion_score": 12,
                "signal_types": 3,
                "contract_count": 14,
                "contract_value": 900_000_000.0,
                "supplier_count": 4,
                "top_supplier_name": "Consorcio Andino",
                "top_supplier_document_id": "900123456",
                "top_supplier_share": 0.62,
                "low_competition_contract_count": 0,
                "direct_invitation_contract_count": 0,
                "sanctioned_supplier_contract_count": 2,
                "sanctioned_supplier_value": 120_000_000.0,
                "official_overlap_contract_count": 1,
                "official_overlap_supplier_count": 1,
                "capacity_mismatch_supplier_count": 1,
                "discrepancy_contract_count": 2,
                "discrepancy_value": 35_000_000.0,
            }
        ],
    ), patch("coacc.routers.meta._load_watchlist_snapshot", return_value=None):
        response = await client.get("/api/v1/meta/watchlist/buyers?limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["buyers"][0]["buyer_id"] == "830000001"
    assert data["buyers"][0]["top_supplier_share"] == 0.62
    assert data["buyers"][0]["alerts"]
    assert data["buyers"][0]["alerts"][0]["alert_type"] == "buyer_supplier_concentration"


@pytest.mark.anyio
async def test_meta_suspicious_territory_watchlist(client: AsyncClient) -> None:
    import coacc.routers.meta as meta_module

    meta_module._territory_watchlist_cache = {}
    meta_module._snapshot_cache = {}

    with patch(
        "coacc.routers.meta.execute_query",
        new_callable=AsyncMock,
        return_value=[
            {
                "territory_id": "Bogota|Cundinamarca",
                "territory_name": "Bogota, Cundinamarca",
                "department": "Cundinamarca",
                "municipality": "Bogota",
                "suspicion_score": 10,
                "signal_types": 3,
                "contract_count": 18,
                "contract_value": 1_400_000_000.0,
                "buyer_count": 5,
                "supplier_count": 7,
                "top_supplier_name": "Consorcio Andino",
                "top_supplier_share": 0.48,
                "low_competition_contract_count": 0,
                "direct_invitation_contract_count": 0,
                "sanctioned_supplier_contract_count": 3,
                "sanctioned_supplier_value": 200_000_000.0,
                "official_overlap_contract_count": 2,
                "capacity_mismatch_supplier_count": 1,
                "discrepancy_contract_count": 2,
                "discrepancy_value": 80_000_000.0,
            }
        ],
    ), patch("coacc.routers.meta._load_watchlist_snapshot", return_value=None):
        response = await client.get("/api/v1/meta/watchlist/territories?limit=5")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["territories"][0]["territory_id"] == "Bogota|Cundinamarca"
    assert data["territories"][0]["territory_name"] == "Bogota, Cundinamarca"
    assert data["territories"][0]["alerts"]
    assert data["territories"][0]["alerts"][0]["alert_type"] == "territory_supplier_concentration"
