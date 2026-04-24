"""Catalog YAML contracts match the signed CSV and validate on load."""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

import pytest

from coacc_etl.catalog import DatasetSpec, clear_cache, datasets_dir, load_catalog

REPO_ROOT = Path(__file__).resolve().parents[2]
PROVEN_CSV = REPO_ROOT / "docs" / "datasets" / "catalog.proven.csv"


def _proven_ids() -> set[str]:
    with PROVEN_CSV.open(encoding="utf-8", newline="") as fh:
        return {
            row["dataset_id"].strip()
            for row in csv.DictReader(fh)
            if row.get("dataset_id", "").strip()
        }


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    clear_cache()


def test_every_proven_row_has_a_yaml_contract() -> None:
    proven_ids = _proven_ids()
    yaml_ids = {p.stem for p in datasets_dir().glob("*.yml") if not p.name.startswith("_")}

    missing = proven_ids - yaml_ids
    orphan = yaml_ids - proven_ids

    assert not missing, f"catalog.proven.csv ids without YAML contract: {sorted(missing)}"
    assert not orphan, f"YAML contracts without catalog.proven.csv row: {sorted(orphan)}"


def test_catalog_loads_and_validates_every_yaml() -> None:
    specs = load_catalog()

    assert len(specs) == len(_proven_ids())
    for dataset_id, spec in specs.items():
        assert isinstance(spec, DatasetSpec)
        assert spec.id == dataset_id
        assert spec.name
        assert spec.url.startswith("https://www.datos.gov.co/d/")


def test_tier_distribution_matches_report() -> None:
    tiers = Counter(spec.tier for spec in load_catalog().values())

    assert tiers["core"] == 118
    assert tiers["context"] == 30
    assert tiers["backlog"] == 0


def test_core_specs_have_at_least_one_core_join_key() -> None:
    core_join_classes = {"nit", "contract", "process", "entity"}
    for spec in load_catalog().values():
        if spec.tier != "core":
            continue
        classes = set(spec.join_keys.keys())
        assert classes & core_join_classes, (
            f"{spec.id} tier=core has no core join key; join_keys={spec.join_keys}"
        )


def test_context_specs_have_only_context_join_keys() -> None:
    context_join_classes = {"bpin", "divipola"}
    for spec in load_catalog().values():
        if spec.tier != "context":
            continue
        classes = set(spec.join_keys.keys())
        assert classes, f"{spec.id} tier=context has no join keys at all"
        assert classes <= context_join_classes | {"entity"}, (
            f"{spec.id} tier=context leaks non-context classes; join_keys={spec.join_keys}"
        )


def test_no_ingest_ready_yaml_yet_wave2_placeholder() -> None:
    """Wave 2 only bootstraps placeholders; Wave 4 populates watermark/columns."""
    ready = [spec.id for spec in load_catalog().values() if spec.is_ingest_ready()]
    assert ready == [], f"unexpected ingest-ready specs this early: {ready}"
