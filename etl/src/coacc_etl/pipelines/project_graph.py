from __future__ import annotations

from typing import Any

from coacc_etl.entity_resolution.canonical import canonical_project_key
from coacc_etl.loader import Neo4jBatchLoader


def build_project_row(project_id: str, **properties: Any) -> dict[str, Any]:
    normalized_project_id = canonical_project_key(project_id)
    row = {
        "project_id": normalized_project_id,
        "convenio_id": normalized_project_id,
        "bpin_code": normalized_project_id,
        "identity_quality": "exact",
    }
    row.update(properties)
    return row


def load_project_nodes(loader: Neo4jBatchLoader, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    return loader.load_nodes("Project", rows, key_field="project_id")


def load_project_relationships(
    loader: Neo4jBatchLoader,
    *,
    rel_type: str,
    rows: list[dict[str, Any]],
    source_label: str,
    source_key: str,
    properties: list[str] | None = None,
) -> int:
    if not rows:
        return 0
    return loader.load_relationships(
        rel_type=rel_type,
        rows=rows,
        source_label=source_label,
        source_key=source_key,
        target_label="Project",
        target_key="project_id",
        properties=properties,
    )
