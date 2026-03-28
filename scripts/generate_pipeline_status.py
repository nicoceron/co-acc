#!/usr/bin/env python3
"""Generate docs/pipeline_status.md from the source registry."""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def status_bucket(row: dict[str, str]) -> str:
    implementation_state = (row.get("implementation_state") or "").strip()
    status = (row.get("status") or "").strip()
    load_state = (row.get("load_state") or "").strip()

    if implementation_state != "implemented":
        return "not_built"
    if status == "blocked_external":
        return "blocked_external"
    if load_state == "loaded":
        return "implemented_loaded"
    return "implemented_partial"


def source_format(access_mode: str) -> str:
    mapping = {
        "api": "api_json",
        "file": "file_batch",
        "bigquery": "bigquery_table",
        "web": "web_portal",
    }
    return mapping.get(access_mode.strip(), "unknown")


def signal_role(row: dict[str, str]) -> str:
    return (row.get("signal_promotion_state") or "promoted").strip() or "promoted"


def required_input(row: dict[str, str]) -> str:
    mode = (row.get("access_mode") or "").strip()
    pipeline_id = (row.get("pipeline_id") or row.get("source_id") or "").strip()

    if mode == "file":
        return f"data/{pipeline_id}/*"
    if mode == "api":
        return f"API payload from {row.get('primary_url', '').strip()}"
    if mode == "bigquery":
        return "BigQuery query/export result"
    if mode == "web":
        return f"Portal export/scrape output under data/{pipeline_id}/"
    return "source-specific contract required"


def known_blockers(row: dict[str, str]) -> str:
    status = (row.get("status") or "").strip()
    note = (row.get("notes") or "").strip()
    if status in {"loaded"}:
        return "-"
    return note or status or "-"


def escape_md(text: str) -> str:
    return text.replace("|", "\\|")


def fetch_runtime_sources(api_base: str) -> dict[str, dict[str, str]]:
    normalized_base = api_base.strip().rstrip("/")
    if not normalized_base:
        return {}
    url = f"{normalized_base}/api/v1/meta/sources"
    try:
        with urlopen(url, timeout=20) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError):
        return {}

    runtime_sources: dict[str, dict[str, str]] = {}
    for source in payload.get("sources") or []:
        source_id = str(source.get("id") or "").strip()
        if source_id:
            runtime_sources[source_id] = source
    return runtime_sources


def overlay_runtime_row(
    row: dict[str, str],
    runtime_sources: dict[str, dict[str, str]],
) -> dict[str, str]:
    source_id = (row.get("source_id") or "").strip()
    runtime = runtime_sources.get(source_id)
    if runtime is None:
        return row

    updated = dict(row)
    for field in (
        "status",
        "implementation_state",
        "load_state",
        "signal_promotion_state",
        "access_mode",
        "public_access_mode",
        "discovery_status",
        "last_seen_url",
        "cadence_expected",
        "cadence_observed",
        "quality_status",
        "notes",
    ):
        value = runtime.get(field)
        if value not in (None, ""):
            updated[field] = str(value)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate pipeline status markdown")
    parser.add_argument("--registry-path", default="docs/source_registry_co_v1.csv")
    parser.add_argument("--output", default="docs/pipeline_status.md")
    parser.add_argument("--api-base", default="")
    args = parser.parse_args()

    rows = list(csv.DictReader(Path(args.registry_path).open(encoding="utf-8", newline="")))
    runtime_sources = fetch_runtime_sources(args.api_base)
    if runtime_sources:
        rows = [overlay_runtime_row(row, runtime_sources) for row in rows]
    rows = [row for row in rows if parse_bool(row.get("in_universe_v1", ""))]
    rows.sort(key=lambda row: (row.get("source_id") or ""))

    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    source_descriptor = "`docs/source_registry_co_v1.csv`"
    if runtime_sources and args.api_base.strip():
        source_descriptor += (
            f" + live runtime overlay from `{args.api_base.rstrip('/')}/api/v1/meta/sources`"
        )

    lines = [
        "# Pipeline Status",
        "",
        f"Generated from {source_descriptor} (as-of UTC: {stamp}).",
        "",
        "Status buckets:",
        "- `implemented_loaded`: implemented and loaded in registry.",
        "- `implemented_partial`: implemented but partial/stale/not fully loaded.",
        "- `blocked_external`: implemented but externally blocked.",
        "- `not_built`: not implemented in public repo.",
        "- Signal roles: `promoted` drives user-facing signals, `enrichment_only` is supporting evidence, `quarantined` is excluded from signal generation.",
        "",
        "| Source ID | Pipeline ID | Status Bucket | Signal Role | Load State | Source Format | Required Input | Known Blockers |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for row in rows:
        src = (row.get("source_id") or "").strip()
        pipeline = (row.get("pipeline_id") or src).strip() or src
        bucket = status_bucket(row)
        role = signal_role(row)
        load_state = (row.get("load_state") or "").strip() or "-"
        fmt = source_format((row.get("access_mode") or "").strip())
        req = required_input(row)
        blockers = known_blockers(row)

        lines.append(
            "| {src} | {pipeline} | {bucket} | {role} | {load_state} | {fmt} | {req} | {blockers} |".format(
                src=escape_md(src),
                pipeline=escape_md(pipeline),
                bucket=escape_md(bucket),
                role=escape_md(role),
                load_state=escape_md(load_state),
                fmt=escape_md(fmt),
                req=escape_md(req),
                blockers=escape_md(blockers),
            )
        )

    Path(args.output).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("UPDATED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
