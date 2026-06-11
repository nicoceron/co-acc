from __future__ import annotations

import csv
from pathlib import Path

from coacc_etl.catalog import load_catalog

REPO_ROOT = Path(__file__).resolve().parents[2]

_DEFERRED_SOURCE_REFS = {
    "higher_ed_directors",
}


def _catalog_source_refs() -> dict[str, str]:
    catalog_path = REPO_ROOT / "docs" / "datasets" / "catalog.signed.csv"
    out: dict[str, str] = {}
    with catalog_path.open(encoding="utf-8", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            dataset_id = (row.get("dataset_id") or "").strip()
            for source_ref in (row.get("source_refs") or "").split("|"):
                source_ref = source_ref.strip()
                if source_ref:
                    out[source_ref] = dataset_id
    return out


def test_catalog_source_refs_resolve_to_yaml_contracts_or_deferred_sources() -> None:
    catalog = load_catalog()
    source_refs = _catalog_source_refs()

    missing: list[str] = []
    not_ready: list[str] = []
    for source_ref, dataset_id in source_refs.items():
        if source_ref in _DEFERRED_SOURCE_REFS:
            continue
        spec = catalog.get(dataset_id)
        if spec is None:
            missing.append(f"{source_ref}:{dataset_id}")
        elif not spec.is_ingest_ready():
            not_ready.append(f"{source_ref}:{dataset_id}")

    assert missing == []
    assert not_ready == []


def test_custom_adapter_sources_are_yaml_backed() -> None:
    catalog = load_catalog()
    custom_sources = {
        dataset_id
        for dataset_id, spec in catalog.items()
        if spec.adapter != "socrata"
    }

    assert custom_sources == {"paco_sanctions"}
    assert catalog["paco_sanctions"].is_ingest_ready()
