from __future__ import annotations

from coacc.services import dependency_registry


def test_signals_to_rerun_empty_returns_empty() -> None:
    dependency_registry.clear_dependency_registry_cache()
    assert dependency_registry.signals_to_rerun(set()) == []


def test_signals_to_rerun_finds_secop_integrado_dependents() -> None:
    dependency_registry.clear_dependency_registry_cache()
    signals = dependency_registry.signals_to_rerun({"secop_integrado"})

    assert "damnificados_emergency_contract_overlap" in signals
    assert "pida5_pida27_pida4_chain" in signals
    assert "procurement_single_bidder_high_value" not in signals


def test_can_materialize_requires_required_sources(monkeypatch) -> None:
    dependency_registry.clear_dependency_registry_cache()
    present = {"secop_integrado", "secop_contract_additions"}
    monkeypatch.setattr(
        dependency_registry.lakehouse_query,
        "watermark_exists",
        lambda source: source in present,
    )

    can_run, missing = dependency_registry.can_materialize(
        "damnificados_emergency_contract_overlap"
    )

    assert can_run is True
    assert missing == []


def test_can_materialize_reports_missing_required_source(monkeypatch) -> None:
    dependency_registry.clear_dependency_registry_cache()
    monkeypatch.setattr(dependency_registry.lakehouse_query, "watermark_exists", lambda _: False)

    can_run, missing = dependency_registry.can_materialize(
        "damnificados_emergency_contract_overlap"
    )

    assert can_run is False
    assert missing == ["secop_integrado", "secop_contract_additions"]


def test_severity_degrades_when_optional_sources_are_absent(monkeypatch) -> None:
    dependency_registry.clear_dependency_registry_cache()
    monkeypatch.setattr(
        dependency_registry.lakehouse_query,
        "watermark_exists",
        lambda source: source == "secop_suppliers",
    )

    severity = dependency_registry.severity_for_sources(
        "reincorporacion_sirr_vs_contractor",
        "high",
    )

    assert severity == "low"


def test_dependency_registry_validate_has_no_config_errors() -> None:
    dependency_registry.clear_dependency_registry_cache()
    assert dependency_registry.validate() == []
