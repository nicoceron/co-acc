from __future__ import annotations

import csv
from pathlib import Path

from coacc_etl.pipeline_registry import list_pipeline_names


def test_source_registry_pipeline_ids_resolve_to_known_pipelines() -> None:
    registry_path = Path(__file__).resolve().parents[2] / "docs" / "source_registry_co_v1.csv"
    with registry_path.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))

    documented_pipeline_ids = {
        (row.get("pipeline_id") or "").strip()
        for row in rows
        if (row.get("pipeline_id") or "").strip()
    }
    available_pipelines = set(list_pipeline_names())

    missing = sorted(documented_pipeline_ids - available_pipelines)
    assert missing == []
