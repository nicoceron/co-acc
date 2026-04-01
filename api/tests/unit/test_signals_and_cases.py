from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from coacc.main import app
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

    with patch("coacc.routers.cases.refresh_case", new=AsyncMock(return_value=payload)):
        response = await client.post(
            "/api/v1/cases/case-1/refresh?lang=es",
            headers=auth_headers,
        )

    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "case-1"
    assert data["last_run_id"] == "run-case-1"
    assert data["signal_count"] == 2
