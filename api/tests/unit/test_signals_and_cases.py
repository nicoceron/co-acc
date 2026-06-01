from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from coacc.main import app
from coacc.models.case import CaseListResponse, CaseResponse, CaseSummary
from coacc.models.entity import EntityResponse, SourceAttribution
from coacc.models.investigation import InvestigationResponse
from coacc.models.signal import EntitySignalsResponse, EvidenceItemResponse, SignalHitResponse
from coacc.services.signal_registry import clear_signal_registry_cache, load_signal_registry


def _mock_record(data: dict[str, object]) -> MagicMock:
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.get = lambda key, default=None: data.get(key, default)
    record.keys.return_value = list(data.keys())
    return record


def _fake_result(records: list[MagicMock]) -> AsyncMock:
    result = AsyncMock()

    async def _iter(self: object) -> object:  # noqa: ANN001
        for record in records:
            yield record

    result.__aiter__ = _iter
    result.single = AsyncMock(return_value=records[0] if records else None)
    return result


def _user_record() -> MagicMock:
    return _mock_record({
        "id": "test-user-id",
        "email": "test@example.com",
        "created_at": "2026-01-01T00:00:00Z",
        "role": "reviewer",
    })


def _lake_signal_hit() -> SignalHitResponse:
    return SignalHitResponse(
        hit_id="lake-hit-1",
        run_id="lake-run-1",
        signal_id="procurement_sanctioned_supplier_awarded",
        signal_version=1,
        title="Sanctioned supplier awarded",
        description="Lake signal",
        category="procurement",
        severity="high",
        public_safe=True,
        reviewer_only=False,
        entity_id="company:8605246546",
        entity_key="8605246546",
        entity_label="Company",
        scope_key="CO1.PCCNTR.8731701",
        scope_type="contract",
        dedup_key="lake-hit-dedup",
        score=7.0,
        identity_confidence=1.0,
        identity_match_type="EXACT_COMPANY_NIT",
        identity_quality="exact",
        evidence_count=1,
        evidence_bundle_id="bundle:lake-hit-1",
        evidence_refs=["https://example.com/evidence"],
        data={},
        sources=[SourceAttribution(database="lake_signal_hits")],
        evidence_items=[
            EvidenceItemResponse(
                item_id="lake-hit-1:1",
                source_id="lake_signal_hits",
                url="https://example.com/evidence",
                label="Evidence",
                observed_at="2026-06-01T00:00:00Z",
            )
        ],
        created_at="2026-06-01T00:00:00Z",
        first_seen_at="2026-06-01T00:00:00Z",
        last_seen_at="2026-06-01T00:00:00Z",
    )


@pytest.mark.anyio
async def test_list_signals_returns_registry(client: AsyncClient) -> None:
    clear_signal_registry_cache()
    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/signals/")
    assert response.status_code == 200
    data = response.json()
    assert data["registry_version"] == 1
    assert any(row["id"] == "tvec_multi_entity_capture" for row in data["signals"])


@pytest.mark.anyio
async def test_get_signal_detail_returns_definition_and_samples(client: AsyncClient) -> None:
    from coacc.main import app

    sample_record = _mock_record({
        "hit_id": "hit-1",
        "run_id": "run-1",
        "signal_id": "split_contracts_below_threshold",
        "signal_version": 1,
        "title": "Fraccionamiento contractual bajo tope",
        "description": "Descripción",
        "category": "procurement",
        "severity": "high",
        "public_safe": True,
        "reviewer_only": False,
        "entity_id": "8601",
        "entity_key": "8601",
        "entity_label": "Company",
        "scope_key": "ref-1",
        "scope_type": "contract",
        "dedup_key": "signal:split:entity:8601",
        "score": 5.0,
        "identity_confidence": 1.0,
        "identity_match_type": "EXACT_COMPANY_NIT",
        "identity_quality": "exact",
        "evidence_count": 2,
        "evidence_bundle_id": "bundle-1",
        "evidence_refs": ["https://example.com/ref"],
        "data_json": "{\"risk_signal\": 5}",
        "sources": ["neo4j_public"],
        "created_at": "2026-03-31T00:00:00+00:00",
        "first_seen_at": "2026-03-31T00:00:00+00:00",
        "last_seen_at": "2026-03-31T00:00:00+00:00",
        "evidence_items": [
            {
                "item_id": "hit-1:1",
                "source_id": "neo4j_public",
                "record_id": None,
                "url": "https://example.com/ref",
                "label": "https://example.com/ref",
                "item_type": "reference",
                "node_ref": "Document:https://example.com/ref",
                "observed_at": "2026-03-31T00:00:00+00:00",
                "public_safe": True,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
            }
        ],
    })

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([sample_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/signals/split_contracts_below_threshold")
    assert response.status_code == 200
    data = response.json()
    assert data["definition"]["id"] == "procurement_repeat_awards_same_supplier"
    assert data["sample_hits"][0]["hit_id"] == "hit-1"
    assert data["sample_hits"][0]["signal_id"] == "procurement_repeat_awards_same_supplier"


@pytest.mark.anyio
async def test_list_cases_uses_existing_investigations(
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    from coacc.main import app

    user_rec = _user_record()
    list_record = _mock_record({
        "total": 1,
        "id": "case-1",
        "title": "Caso 1",
        "description": "",
        "status": "new",
        "created_at": "2026-03-31T00:00:00Z",
        "updated_at": "2026-03-31T00:00:00Z",
        "share_token": None,
        "entity_ids": ["8601"],
    })
    count_record = _mock_record({
        "signal_count": 0,
        "public_signal_count": 0,
        "last_refreshed_at": None,
        "last_run_id": None,
        "stale": True,
    })

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    call_count = 0

    async def _run_side_effect(*args: object, **kwargs: object) -> AsyncMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _fake_result([user_rec])
        if call_count == 2:
            return _fake_result([list_record])
        return _fake_result([count_record])

    mock_session.run = AsyncMock(side_effect=_run_side_effect)
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/cases/", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["cases"][0]["id"] == "case-1"
    assert data["cases"][0]["stale"] is True


@pytest.mark.anyio
async def test_list_cases_prefers_lake_cases_when_graph_is_available(
    client: AsyncClient,
) -> None:
    lake_response = CaseListResponse(
        cases=[
            CaseSummary(
                id="lake-case-1",
                title="Lake case",
                description="Public lake case",
                status="new",
                created_at="2026-06-01T00:00:00Z",
                updated_at="2026-06-01T00:00:00Z",
                entity_ids=["8601"],
                signal_count=1,
                public_signal_count=1,
                last_refreshed_at="2026-06-01T00:00:00Z",
                last_run_id="lake-run-1",
                stale=False,
            )
        ],
        total=1,
    )

    with (
        patch("coacc.routers.cases.list_lake_cases", return_value=lake_response),
        patch("coacc.routers.cases.list_cases", new=AsyncMock()) as graph_list,
    ):
        response = await client.get("/api/v1/cases/")

    assert response.status_code == 200
    assert response.json()["cases"][0]["id"] == "lake-case-1"
    graph_list.assert_not_called()


@pytest.mark.anyio
async def test_case_detail_prefers_lake_case_when_graph_is_available(
    client: AsyncClient,
) -> None:
    lake_case = CaseResponse(
        id="lake-case-1",
        title="Lake case",
        description="Public lake case",
        status="new",
        created_at="2026-06-01T00:00:00Z",
        updated_at="2026-06-01T00:00:00Z",
        entity_ids=["8601"],
        signal_count=1,
        public_signal_count=1,
        last_refreshed_at="2026-06-01T00:00:00Z",
        last_run_id="lake-run-1",
        stale=False,
        signals=[],
        evidence_bundles=[],
        events=[],
    )

    with (
        patch("coacc.routers.cases.get_lake_case", return_value=lake_case),
        patch("coacc.routers.cases.get_case", new=AsyncMock()) as graph_get,
    ):
        response = await client.get("/api/v1/cases/lake-case-1")

    assert response.status_code == 200
    assert response.json()["id"] == "lake-case-1"
    graph_get.assert_not_called()


def test_signal_registry_v2_definitions_are_loaded() -> None:
    clear_signal_registry_cache()
    registry = load_signal_registry()
    signal = next(
        row for row in registry.signals if row.id == "procurement_repeat_awards_same_supplier"
    )

    assert registry.registry_version == 1
    assert (
        registry.aliases["split_contracts_below_threshold"]
        == "procurement_repeat_awards_same_supplier"
    )
    assert signal.runner.kind == "cypher"
    assert signal.runner.ref == "procurement_repeat_awards_same_supplier"
    assert signal.scope_type
    assert signal.dedup_fields
    assert signal.public_policy.allow_public is True


@pytest.mark.anyio
async def test_refresh_entity_signals_endpoint_returns_persisted_payload(
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    from coacc.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([_user_record()]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    entity_record = _mock_record({"entity_labels": ["Company"]})
    payload = {
        "entity_id": "8601",
        "entity_key": "8601",
        "total": 1,
        "last_run_id": "run-entity-1",
        "last_refreshed_at": "2026-03-31T00:00:00Z",
        "stale": False,
        "signals": [],
    }

    with (
        patch(
            "coacc.routers.entity._lookup_entity_record",
            new=AsyncMock(return_value=entity_record),
        ),
        patch("coacc.routers.entity.refresh_entity_signals", new=AsyncMock(return_value=payload)),
    ):
        response = await client.post(
            "/api/v1/entity/8601/signals/refresh?lang=es",
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["entity_id"] == "8601"
    assert data["last_run_id"] == "run-entity-1"
    assert data["stale"] is False


@pytest.mark.anyio
async def test_refresh_entity_signals_endpoint_returns_lake_payload_when_graph_missing(
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    from coacc.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([_user_record()]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    lake_entity = EntityResponse(
        id="company:8605246546",
        type="company",
        entity_label="Company",
        identity_quality="exact",
        properties={"name": "Lake company", "nit": "8605246546"},
        sources=[SourceAttribution(database="lake_curated")],
    )
    lake_payload = EntitySignalsResponse(
        entity_id="company:8605246546",
        entity_key="8605246546",
        total=0,
        last_run_id="lake-run-1",
        last_refreshed_at="2026-06-01T00:00:00Z",
        stale=False,
        signals=[],
    )

    with (
        patch("coacc.routers.entity._lookup_entity_record", new=AsyncMock(return_value=None)),
        patch("coacc.routers.entity.get_lake_entity", return_value=lake_entity),
        patch(
            "coacc.routers.entity.materialized_entity_signals",
            return_value=lake_payload,
        ) as lake_signals,
        patch("coacc.routers.entity.refresh_entity_signals", new=AsyncMock()) as graph_refresh,
    ):
        response = await client.post(
            "/api/v1/entity/860524654/signals/refresh?lang=es",
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["entity_id"] == "company:8605246546"
    assert data["last_run_id"] == "lake-run-1"
    lake_signals.assert_called_once_with("company:8605246546", public_only=False)
    graph_refresh.assert_not_called()


@pytest.mark.anyio
async def test_refresh_case_endpoint_returns_case_payload(
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    from coacc.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([_user_record()]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    payload = {
        "id": "case-1",
        "title": "Caso 1",
        "description": "",
        "status": "new",
        "created_at": "2026-03-31T00:00:00Z",
        "updated_at": "2026-03-31T00:00:00Z",
        "entity_ids": ["8601"],
        "signal_count": 2,
        "public_signal_count": 1,
        "last_refreshed_at": "2026-03-31T00:00:00Z",
        "last_run_id": "run-case-1",
        "stale": False,
        "signals": [],
        "evidence_bundles": [],
        "events": [],
    }

    with (
        patch("coacc.routers.cases.get_lake_case", return_value=None),
        patch("coacc.routers.cases.refresh_case", new=AsyncMock(return_value=payload)),
    ):
        response = await client.post(
            "/api/v1/cases/case-1/refresh?lang=es",
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "case-1"
    assert data["last_run_id"] == "run-case-1"
    assert data["signal_count"] == 2


@pytest.mark.anyio
async def test_refresh_case_endpoint_prefers_lake_case_when_graph_is_available(
    client: AsyncClient,
    auth_headers: dict[str, str],
) -> None:
    from coacc.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([_user_record()]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    lake_case = CaseResponse(
        id="lake-case-1",
        title="Lake case",
        description="Public lake case",
        status="new",
        created_at="2026-06-01T00:00:00Z",
        updated_at="2026-06-01T00:00:00Z",
        entity_ids=["8601"],
        signal_count=1,
        public_signal_count=1,
        last_refreshed_at="2026-06-01T00:00:00Z",
        last_run_id="lake-run-1",
        stale=False,
        signals=[],
        evidence_bundles=[],
        events=[],
    )

    with (
        patch("coacc.routers.cases.get_lake_case", return_value=lake_case),
        patch("coacc.routers.cases.refresh_case", new=AsyncMock()) as graph_refresh,
    ):
        response = await client.post(
            "/api/v1/cases/lake-case-1/refresh?lang=es",
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "lake-case-1"
    assert data["last_run_id"] == "lake-run-1"
    graph_refresh.assert_not_called()


@pytest.mark.anyio
async def test_get_case_merges_lake_signals_for_investigation_entities() -> None:
    from coacc.services import case_service

    investigation = InvestigationResponse(
        id="case-1",
        title="Investigation",
        description="",
        status="new",
        created_at="2026-06-01T00:00:00Z",
        updated_at="2026-06-01T00:00:00Z",
        entity_ids=["8605246546"],
    )
    hit = _lake_signal_hit()
    lake_response = EntitySignalsResponse(
        entity_id="8605246546",
        entity_key="8605246546",
        total=1,
        last_run_id="lake-run-1",
        last_refreshed_at="2026-06-01T00:00:00Z",
        stale=False,
        signals=[hit],
    )

    with (
        patch(
            "coacc.services.case_service.investigation_service.get_investigation",
            new=AsyncMock(return_value=investigation),
        ),
        patch(
            "coacc.services.case_service._case_summary_meta",
            new=AsyncMock(return_value=(0, 0, "2026-06-01T00:00:00Z", "case-run-1", False)),
        ),
        patch("coacc.services.case_service.execute_query", new=AsyncMock(side_effect=[[], []])),
        patch(
            "coacc.services.case_service.materialized_entity_signals",
            return_value=lake_response,
        ) as lake_signals,
    ):
        case = await case_service.get_case(AsyncMock(), "case-1", "test-user-id")

    assert case is not None
    assert case.signal_count == 1
    assert case.public_signal_count == 1
    assert case.signals[0].hit_id == "lake-hit-1"
    assert case.evidence_bundles[0].bundle_id == "bundle:lake-hit-1"
    assert case.events[0].signal_hit_id == "lake-hit-1"
    lake_signals.assert_called_once_with("8605246546", public_only=False)


@pytest.mark.anyio
async def test_refresh_case_uses_lake_signals_without_graph_materialization() -> None:
    from coacc.services import case_service

    investigation = InvestigationResponse(
        id="case-1",
        title="Investigation",
        description="",
        status="new",
        created_at="2026-06-01T00:00:00Z",
        updated_at="2026-06-01T00:00:00Z",
        entity_ids=["8605246546"],
    )
    hit = _lake_signal_hit()
    lake_response = EntitySignalsResponse(
        entity_id="8605246546",
        entity_key="8605246546",
        total=1,
        last_run_id="lake-run-1",
        last_refreshed_at="2026-06-01T00:00:00Z",
        stale=False,
        signals=[hit],
    )
    case_response = CaseResponse(
        id="case-1",
        title="Investigation",
        description="",
        status="new",
        created_at="2026-06-01T00:00:00Z",
        updated_at="2026-06-01T00:00:00Z",
        entity_ids=["8605246546"],
        signal_count=1,
        public_signal_count=1,
        last_refreshed_at="2026-06-01T00:00:00Z",
        last_run_id="case-run-1",
        stale=False,
        signals=[hit],
        evidence_bundles=[],
        events=[],
    )

    with (
        patch(
            "coacc.services.case_service.investigation_service.get_investigation",
            new=AsyncMock(return_value=investigation),
        ),
        patch(
            "coacc.services.case_service.materialized_entity_signals",
            return_value=lake_response,
        ),
        patch(
            "coacc.services.case_service.refresh_entity_signals",
            new=AsyncMock(),
        ) as graph_refresh,
        patch(
            "coacc.services.case_service.execute_query_single",
            new=AsyncMock(return_value=_mock_record({"case_id": "case-1"})),
        ),
        patch("coacc.services.case_service.execute_query", new=AsyncMock()),
        patch("coacc.services.case_service.get_case", new=AsyncMock(return_value=case_response)),
    ):
        response = await case_service.refresh_case(
            AsyncMock(),
            "case-1",
            "test-user-id",
            MagicMock(),
        )

    assert response == case_response
    graph_refresh.assert_not_called()
