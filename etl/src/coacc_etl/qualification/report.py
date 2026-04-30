"""Catalog read/write + human-readable report rendering."""
# ruff: noqa: E501
from __future__ import annotations

import csv
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from coacc_etl.qualification.promotion import (
    CATALOG_FIELDS,
    TriageCatalogRow,
    classify_dataset,
)

if TYPE_CHECKING:
    from pathlib import Path

LOG = logging.getLogger("source_qualification")


def read_catalog(path: Path) -> list[TriageCatalogRow]:
    if not path.exists():
        LOG.warning("catalog CSV not found for --llm-only: %s", path)
        return []
    rows: list[TriageCatalogRow] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            probe_notes = [
                part.strip()
                for part in (r.get("probe_notes") or "").split(";")
                if part.strip()
            ]
            row_kwargs = {
                field_name: r.get(field_name, "")
                for field_name in CATALOG_FIELDS
                if field_name != "probe_notes"
            }
            for int_field in (
                "rows",
                "last_update_days",
                "n_columns",
                "n_meaningful_columns",
                "join_keys_found",
            ):
                try:
                    row_kwargs[int_field] = int(row_kwargs.get(int_field) or -1)
                except (TypeError, ValueError):
                    row_kwargs[int_field] = -1
            rows.append(TriageCatalogRow(**row_kwargs, probe_notes=probe_notes))
    return rows


def write_catalog(rows: list[TriageCatalogRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def write_proven(rows: list[TriageCatalogRow], out_path: Path) -> None:
    proven = [r for r in rows if r.join_keys_found > 0]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "dataset_id", "name", "sector", "recommendation", "relevance",
        "rows", "join_key_classes", "join_key_columns",
        "sample_join_density", "ingest_class", "source_refs", "origin_refs",
        "signal_refs", "url",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in proven:
            d = row.as_csv_row()
            d["ingest_class"] = classify_dataset(row)
            writer.writerow({f: d.get(f, "") for f in fields})


def write_report(rows: list[TriageCatalogRow], report_path: Path) -> None:
    classified: dict[str, list[TriageCatalogRow]] = {}
    for row in rows:
        cls = classify_dataset(row)
        classified.setdefault(cls, []).append(row)

    class_order = [
        "ingest_priority",
        "ingest",
        "ingest_if_useful",
        "context_enrichment",
        "context_if_useful",
        "schema_core_join",
        "schema_context_join",
        "weak_join",
        "large_no_join",
        "no_join",
        "unknown",
        "unreachable",
    ]

    lines: list[str] = [
        "# Dataset join-key triage report",
        "",
        f"Generated: {datetime.now(tz=UTC).isoformat(timespec='seconds')}",
        "Input: configured dataset source inventories",
        f"Total probed: {len(rows)}",
        f"Datasets with proven join keys: {sum(1 for r in rows if r.join_keys_found > 0)}",
        "",
        "## Ingest class counts",
        "",
    ]
    for cls in class_order:
        lines.append(f"- {cls}: {len(classified.get(cls, []))}")
    lines.append("")

    class_descriptions = {
        "ingest_priority": "2+ core join keys (NIT/contract/process/entity) + >=1K rows; highest priority for pipeline ingestion",
        "ingest": "1+ core join key + >=5K rows; strong candidate for pipeline ingestion",
        "ingest_if_useful": "1+ core join key but <5K rows; ingest only if fills a signal dependency gap",
        "context_enrichment": "BPIN/divipola join key + >=1K rows; useful as enrichment/context layer",
        "context_if_useful": "BPIN/divipola join key but <1K rows; ingest only if context fills a gap",
        "schema_core_join": "Metadata-only pass found a core join key; needs row count/freshness before promotion",
        "schema_context_join": "Metadata-only pass found only BPIN/divipola context keys; needs explicit signal use",
        "weak_join": "Join key found but no core keys and no strong context keys; low priority",
        "large_no_join": ">10K rows and readable schema but no recognized join key; needs manual column review",
        "no_join": "No recognized join key found; likely not useful for entity-linked pipeline",
        "unknown": "Metadata fetch failed; cannot classify",
        "unreachable": "Dataset is 404/403 and cannot be probed",
    }

    for cls in class_order:
        group = classified.get(cls, [])
        if not group:
            continue
        lines.append(f"## {cls}")
        lines.append("")
        lines.append(f"{class_descriptions.get(cls, '')}")
        lines.append("")
        if cls in (
            "ingest_priority",
            "ingest",
            "ingest_if_useful",
            "context_enrichment",
            "schema_core_join",
            "schema_context_join",
        ):
            lines.append("| dataset_id | rows | join_keys | join_columns | density_sample | name |")
            lines.append("|---|---:|---|---|---|---|")
            for r in sorted(group, key=lambda x: -max(x.rows, 0)):
                lines.append(
                    f"| `{r.dataset_id}` | {r.rows:,} | {r.join_key_classes} | "
                    f"{r.join_key_columns} | {r.sample_join_density or '—'} | {r.name} |"
                )
        elif cls in ("weak_join", "large_no_join", "no_join"):
            lines.append("| dataset_id | rows | cols | join_keys | name |")
            lines.append("|---|---:|---:|---|---|")
            for r in sorted(group, key=lambda x: -max(x.rows, 0)):
                lines.append(
                    f"| `{r.dataset_id}` | {r.rows:,} | {r.n_meaningful_columns} | "
                    f"{r.join_key_classes or '—'} | {r.name} |"
                )
        lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
