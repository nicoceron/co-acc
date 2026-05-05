"""Catches drift between ``signal_source_deps.yml`` and the signed catalog.

Every source name a signal depends on must either:

1. Map to an ``ingest-ready`` YAML in ``etl/datasets/`` (incremental or
   snapshot). The bridge from the legacy ``pipeline_id`` string to a
   Socrata 4x4 dataset id goes through ``docs/source_registry_co_v1.csv``
   ``primary_url``.

2. Or be in :data:`_KNOWN_DEFERRED_SOURCES` — sources we have explicitly
   parked (no ingest path today; the dependent signals must declare
   ``status: parked`` in ``config/signal_registry.yml``).

3. Or be in :data:`_ASPIRATIONAL_SOURCES` — sources referenced by signal
   designs that pre-date Wave 4 and have never been wired (no
   ``pipeline_id`` row, no YAML). They survive in deps because the
   signal author's intent is to add them later. They must not appear
   in any signal's ``required`` set.

Wave 6 retires ``source_registry_co_v1.csv`` and migrates the
``pipeline_id`` → ``dataset_id`` map directly into the YAML catalog;
this test stays correct across that migration because the resolution
goes through ``primary_url`` which lives on the YAML itself.
"""
from __future__ import annotations

import csv
from pathlib import Path

import yaml

from coacc_etl.catalog import load_catalog

REPO_ROOT = Path(__file__).resolve().parents[2]


_KNOWN_DEFERRED_SOURCES = {
    # Custom adapters (non-Socrata) we know we still need but haven't
    # yet shipped under ``coacc_etl.ingest.custom``. Signals that
    # *require* one of these stay marked ``status: parked`` in
    # ``config/signal_registry.yml``.
    "adverse_media",
    "judicial_providencias",
    "actos_administrativos",
    "gacetas_territoriales",
    "control_politico",
    "environmental_files_corantioquia",
    "rub_beneficial_owners",
    "tvec_orders_consolidated",
    "anim_inmuebles",
    "pnis_beneficiarios",
    # In the legacy CSV but with non-Socrata URLs — bridge to a future
    # custom adapter, currently unwired.
    "dnp_project_contract_links",      # local://graph derivation
    "official_case_bulletins",         # local://official_case_bulletins
    "paco_sanctions",                  # portal.paco.gov.co (web scrape)
}


_ASPIRATIONAL_SOURCES = {
    # Source names referenced in legacy signal designs but never wired
    # to a Socrata dataset or a custom adapter. They may only appear in
    # ``optional`` sets — never in ``required`` — until we either
    # promote them to a YAML or remove the dependent signal.
    "anla_licencias",
    "anm_titulos",
    "dane_ipm",
    "dane_micronegocios",
    "dane_pobreza_monetaria",
    "dnp_obras_prioritarias",
    "igac_parcelas",
    "mindeporte_actores",
    "pdet_municipios",
    "pida_category_hits",
    "presidencia_iniciativas_33007",
    "sirr_reincorporacion",
    "ungrd_damnificados",
    "upme_subsidios",
}


def _load_pipeline_id_to_dataset_id() -> dict[str, str | None]:
    """pipeline_id (legacy) → Socrata dataset_id (or None for non-Socrata)."""
    catalog = load_catalog()
    url_to_dataset = {spec.url: spec.id for spec in catalog.values()}

    registry = REPO_ROOT / "docs" / "source_registry_co_v1.csv"
    out: dict[str, str | None] = {}
    with registry.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            pid = (row.get("pipeline_id") or "").strip()
            if not pid:
                continue
            url = (row.get("primary_url") or "").strip()
            out[pid] = url_to_dataset.get(url)
    return out


def _load_signal_deps() -> dict[str, dict]:
    payload = yaml.safe_load(
        (REPO_ROOT / "config" / "signal_source_deps.yml").read_text(encoding="utf-8")
    )
    return payload.get("signals") or {}


def _load_signal_registry_statuses() -> dict[str, str]:
    payload = yaml.safe_load(
        (REPO_ROOT / "config" / "signal_registry.yml").read_text(encoding="utf-8")
    )
    return {sig["id"]: sig.get("status", "active") for sig in (payload.get("signals") or [])}


def test_every_signal_source_resolves_to_yaml_or_known_backlog() -> None:
    """No signal should reference a source we don't recognise."""
    catalog = load_catalog()
    ds_to_ready = {ds_id: spec.is_ingest_ready() for ds_id, spec in catalog.items()}
    pid_to_ds = _load_pipeline_id_to_dataset_id()

    deps = _load_signal_deps()
    referenced = set()
    for sig_id, payload in deps.items():
        for key in ("sources", "required", "optional"):
            referenced.update(payload.get(key) or [])

    unknown: set[str] = set()
    for source in referenced:
        if source in _KNOWN_DEFERRED_SOURCES or source in _ASPIRATIONAL_SOURCES:
            continue
        ds_id = pid_to_ds.get(source)
        if ds_id and ds_to_ready.get(ds_id):
            continue
        unknown.add(source)

    assert not unknown, (
        "signal_source_deps.yml references sources that are neither in the "
        f"signed catalog nor explicitly deferred: {sorted(unknown)}"
    )


def test_signals_required_only_known_or_parked() -> None:
    """A signal whose ``required`` set contains a deferred or aspirational
    source must declare ``status: parked`` in the registry."""
    pid_to_ds = _load_pipeline_id_to_dataset_id()
    catalog = load_catalog()
    ds_to_ready = {ds_id: spec.is_ingest_ready() for ds_id, spec in catalog.items()}

    deps = _load_signal_deps()
    statuses = _load_signal_registry_statuses()

    expected_parked: set[str] = set()
    for sig_id, payload in deps.items():
        required = payload.get("required") or payload.get("sources") or []
        for source in required:
            ds_id = pid_to_ds.get(source)
            if ds_id and ds_to_ready.get(ds_id):
                continue
            expected_parked.add(sig_id)
            break

    missing_parked = sorted(
        sig
        for sig in expected_parked
        if statuses.get(sig) not in {"parked", "deferred"}
        # Signals only present in deps but not in the registry are out of
        # scope — Wave 6 may add them later.
        if sig in statuses
    )
    assert not missing_parked, (
        "These signals require a source that is not ingest-ready, but the "
        f"registry does not mark them status: parked: {missing_parked}"
    )


def test_aspirational_sources_only_in_optional() -> None:
    """Aspirational sources (no pipeline, no YAML) must not appear in
    any signal's ``required`` set."""
    deps = _load_signal_deps()
    leaks: list[str] = []
    for sig_id, payload in deps.items():
        required = set(payload.get("required") or [])
        leaked = sorted(required & _ASPIRATIONAL_SOURCES)
        if leaked:
            leaks.append(f"{sig_id}: {leaked}")
    assert not leaks, (
        "Aspirational sources may only appear in `optional`, never in "
        f"`required`: {leaks}"
    )
