from __future__ import annotations

import csv
from pathlib import Path

from coacc_etl.catalog import load_catalog

REPO_ROOT = Path(__file__).resolve().parents[2]


def _migrated_pipeline_ids() -> set[str]:
    """pipeline_ids whose dataset already migrated to a YAML contract.

    Wave 4 deletes the bespoke ``pipelines/<name>.py`` once a dataset is
    served by the generic Socrata ingester. The legacy
    ``docs/source_registry_co_v1.csv`` keeps its row (the API still reads it)
    until Wave 6 retires the CSV entirely, so this set lets the alignment
    test ignore those rows in the meantime.
    """
    catalog = load_catalog()
    primary_url_to_id = {spec.url: spec.id for spec in catalog.values()}
    registry_path = REPO_ROOT / "docs" / "source_registry_co_v1.csv"
    migrated: set[str] = set()
    with registry_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            url = (row.get("primary_url") or "").strip()
            pipeline_id = (row.get("pipeline_id") or "").strip()
            if not pipeline_id or not url:
                continue
            dataset_id = primary_url_to_id.get(url)
            if dataset_id and spec_is_ingest_ready(catalog, dataset_id):
                migrated.add(pipeline_id)
    return migrated


def spec_is_ingest_ready(catalog: dict, dataset_id: str) -> bool:
    spec = catalog.get(dataset_id)
    return bool(spec and spec.is_ingest_ready())


_DEFERRED_BACKLOG = {
    # Not Socrata; lives at mapainversiones.dnp.gov.co. Will land under
    # ingest/custom/ in Wave 4.B as a NotImplementedError shell, or be
    # purged from the legacy CSV in Wave 6.
    "mapa_inversiones_projects",
    # Socrata muyy-6yw9 directors of higher-ed institutions — low signal
    # value (each row is just a contact entry for an existing university),
    # not in catalog.signed.csv. Bespoke pipeline retired with no YAML
    # contract; can be promoted to a YAML if a downstream signal needs it.
    "higher_ed_directors",
    # Wave 4.B retired the bespoke Pipeline stack entirely. The following
    # pipeline_ids are non-Socrata custom adapters — the bespoke .py files
    # are gone and they have no YAML contract. The legacy CSV row stays so
    # the API service keeps reporting them as "known" sources; Wave 6
    # retires the CSV.
    "dnp_project_contract_links",      # local://graph
    "official_case_bulletins",         # local://official_case_bulletins
    "paco_sanctions",                  # portal.paco.gov.co (custom)
    "pte_sector_commitments",          # pte-prueba.azurewebsites.net (custom)
    "pte_top_contracts",               # pte-prueba.azurewebsites.net (custom)
    "registraduria_death_status_checks",  # registraduria.gov.co (custom)
    "rues_chambers",                   # rues.org.co (custom)
    "siri_antecedents",                # iaeu-rcn6 (DD/MM/YYYY string-cmp)
    "supersoc_top_companies",          # supersociedades.gov.co (custom)
}


def test_source_registry_pipeline_ids_resolve_to_known_pipelines() -> None:
    registry_path = REPO_ROOT / "docs" / "source_registry_co_v1.csv"
    with registry_path.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    documented_pipeline_ids = {
        (row.get("pipeline_id") or "").strip()
        for row in rows
        if (row.get("pipeline_id") or "").strip()
    }
    # Wave 4.B retired the bespoke Pipeline stack — there is no
    # ``list_pipeline_names`` anymore. Every legacy CSV row must either be
    # migrated to a YAML contract or explicitly parked in _DEFERRED_BACKLOG.
    migrated = _migrated_pipeline_ids()

    missing = sorted(documented_pipeline_ids - migrated - _DEFERRED_BACKLOG)
    assert missing == [], (
        f"source_registry rows with no YAML contract or deferred entry: {missing}"
    )
