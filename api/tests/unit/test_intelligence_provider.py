from unittest.mock import AsyncMock, patch

import pytest
from pytest import MonkeyPatch

from coacc.config import settings
from coacc.services import intelligence_provider as provider_module


def test_falls_back_to_community_when_full_modules_missing(
    monkeypatch: MonkeyPatch,
) -> None:
    original_tier = settings.product_tier
    try:
        monkeypatch.setattr(settings, "product_tier", "full")
        monkeypatch.setattr(
            provider_module,
            "_full_modules_available",
            lambda: False,
        )
        provider_module._PROVIDER_CACHE.clear()

        provider = provider_module.get_default_provider()

        assert isinstance(provider, provider_module.CommunityIntelligenceProvider)
    finally:
        provider_module._PROVIDER_CACHE.clear()
        settings.product_tier = original_tier


def test_keeps_full_when_modules_are_available(
    monkeypatch: MonkeyPatch,
) -> None:
    original_tier = settings.product_tier
    try:
        monkeypatch.setattr(settings, "product_tier", "full")
        monkeypatch.setattr(
            provider_module,
            "_full_modules_available",
            lambda: True,
        )
        provider_module._PROVIDER_CACHE.clear()

        provider = provider_module.get_default_provider()

        assert isinstance(provider, provider_module.FullIntelligenceProvider)
    finally:
        provider_module._PROVIDER_CACHE.clear()
        settings.product_tier = original_tier


def test_community_provider_exposes_exactly_17_patterns() -> None:
    provider = provider_module.CommunityIntelligenceProvider()
    pattern_ids = [row["id"] for row in provider.list_patterns()]
    assert len(pattern_ids) == 17
    assert set(pattern_ids) == set(provider_module.COMMUNITY_PATTERN_IDS)


def test_default_analysis_pattern_subset_stays_within_declared_patterns() -> None:
    assert set(provider_module.COMMUNITY_ANALYSIS_PATTERN_IDS).issubset(
        set(provider_module.COMMUNITY_PATTERN_IDS)
    )


def test_community_patterns_have_query_files() -> None:
    from coacc.services.neo4j_service import CypherLoader

    for query_name in provider_module.COMMUNITY_PATTERN_QUERIES.values():
        try:
            CypherLoader.load(query_name)
        finally:
            CypherLoader.clear_cache()


def test_sanitize_public_pattern_data_supports_neo4j_record_shape() -> None:
    class FakeRecord:
        def __init__(self) -> None:
            self._data = {
                "pattern_id": "sanctioned_supplier_record",
                "company_name": "Empresa Teste",
                "evidence_refs": ["sanction:1"],
                "risk_signal": 3.0,
            }

        def keys(self) -> list[str]:
            return list(self._data.keys())

        def __getitem__(self, key: str) -> object:
            return self._data[key]

    payload = provider_module._sanitize_public_pattern_data(FakeRecord())

    assert payload["company_name"] == "Empresa Teste"
    assert payload["evidence_refs"] == ["sanction:1"]
    assert payload["risk_signal"] == 3.0


@pytest.mark.anyio
async def test_community_provider_enforces_public_evidence_fields() -> None:
    provider = provider_module.CommunityIntelligenceProvider()
    fake_session = object()

    with (
        patch(
            "coacc.services.intelligence_provider.execute_query_single",
            new_callable=AsyncMock,
            return_value={
                "entity_labels": ["Company"],
                "e": {"cnpj": "11.111.111/0001-11"},
            },
        ),
        patch(
            "coacc.services.intelligence_provider.execute_query",
            new_callable=AsyncMock,
            return_value=[
                {
                    "pattern_id": "debtor_contracts",
                    "cnpj": "11.111.111/0001-11",
                    "company_name": "Empresa Teste",
                    "amount_total": 1000.0,
                    "window_start": "2024-01-01",
                    "window_end": "2024-12-31",
                    "evidence_refs": ["contract:1", "debt:2"],
                }
            ],
        ),
    ):
        results = await provider.run_pattern(
            fake_session,  # type: ignore[arg-type]
            pattern_id="debtor_contracts",
            entity_id="c1",
            lang="pt",
        )

    assert len(results) == 1
    payload = results[0].data
    assert payload["evidence_refs"]
    assert "risk_signal" in payload
