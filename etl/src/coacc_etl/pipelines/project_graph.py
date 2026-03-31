from __future__ import annotations

from typing import Any

from coacc_etl.loader import Neo4jBatchLoader


def build_project_row(project_id: str, **properties: Any) -> dict[str, Any]:
    row = {
        "project_id": project_id,
        "convenio_id": project_id,
        "bpin_code": project_id,
    }
    row.update(properties)
    return row


def load_project_nodes(loader: Neo4jBatchLoader, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    loaded = 0
    loaded += loader.load_nodes("Project", rows, key_field="project_id")
    loaded += loader.load_nodes("Convenio", rows, key_field="convenio_id")
    return loaded


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
    loaded = 0
    loaded += loader.load_relationships(
        rel_type=rel_type,
        rows=rows,
        source_label=source_label,
        source_key=source_key,
        target_label="Project",
        target_key="project_id",
        properties=properties,
    )
    loaded += loader.load_relationships(
        rel_type=rel_type,
        rows=rows,
        source_label=source_label,
        source_key=source_key,
        target_label="Convenio",
        target_key="convenio_id",
        properties=properties,
    )
    return loaded
