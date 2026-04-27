from __future__ import annotations

import csv
from pathlib import Path

from coacc_etl.catalog import load_catalog
from coacc_etl.pipeline_registry import list_pipeline_names

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
    # Socrata mzgh-shtp but has no per-row timestamp column — cannot be
    # made ingest-ready under the current generic-Socrata model. Pending a
    # full-refresh-only mode for snapshot-style datasets.
    "sgr_projects",
    # Socrata muyy-6yw9 directors of higher-ed institutions — low signal
    # value (each row is just a contact entry for an existing university),
    # not in catalog.signed.csv. Bespoke pipeline retired with no YAML
    # contract; can be promoted to a YAML if a downstream signal needs it.
    "higher_ed_directors",
    # SECOP extension/relationship tables: snapshots tied to contracts
    # but without their own row-level timestamp. Need a full_refresh_only
    # mode (planned) before the YAML can be made ingest-ready. YAMLs
    # remain placeholders in catalog.signed.csv.
    "secop_additional_locations",     # wwhe-4sq8
    "secop_budget_commitments",       # skc9-met7
    "secop_budget_items",             # cwhv-7fnp
    "secop_cdp_requests",             # a86w-fh92
    "secop_execution_locations",      # gra4-pcp2
    "secop_i_resource_origins",       # 3xwx-53wt
    "secop_process_bpin",             # d9na-abhe
    # secop_offers and secop_payment_plans had Socrata IDs that timed out
    # on probe; YAMLs left as placeholders pending a re-probe.
    "secop_offers",                   # wi7w-2nvm
    "secop_payment_plans",            # uymx-8p3j
    # DNP project tables: snapshot-style relational tables on a project
    # without their own row-level timestamp. Each row links a project (BPIN)
    # to a beneficiary / location / executor; updates re-emit the snapshot.
    # Pending the full_refresh_only mode.
    "dnp_project_beneficiary_characterization",  # tmmn-mpqc
    "dnp_project_beneficiary_locations",         # iuc2-3r6h
    "dnp_project_executors",                     # epzv-8ck4
    "dnp_project_locations",                     # xikz-44ja
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
    available_pipelines = set(list_pipeline_names())
    migrated = _migrated_pipeline_ids()

    missing = sorted(
        documented_pipeline_ids - available_pipelines - migrated - _DEFERRED_BACKLOG
    )
    assert missing == [], (
        f"source_registry rows with no pipeline and no YAML contract: {missing}"
    )
