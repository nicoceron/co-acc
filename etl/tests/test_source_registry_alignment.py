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
