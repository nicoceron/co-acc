"""Input loaders for the qualification gate.

Sources of dataset entries that get fed into the Socrata probe:

- ``load_appendix`` — archived ``dataset_relevance_appendix.csv``
- ``load_audit_json`` — ``colombia_open_data_audit.json``
- ``load_source_registry`` — legacy ``source_registry_co_v1.csv``
- ``load_signal_source_ids`` — pulls source ids out of
  ``signal_source_deps.yml`` to mark coverage
- ``load_pipeline_env_sources`` — env-backed Socrata IDs that lived
  inside legacy Pipeline classes (kept here for forward-compat;
  Wave 4.B retired the Pipeline stack so this loader returns []
  unless an external operator stages compatible files)
- ``load_known_dataset_entries`` — orchestrates all of the above
"""
# ruff: noqa: E501
from __future__ import annotations

import ast
import csv
import json
import logging
import os
import re
from typing import TYPE_CHECKING

from coacc_etl.qualification.socrata_probe import DEFAULT_DOMAIN

if TYPE_CHECKING:
    from pathlib import Path

LOG = logging.getLogger("source_qualification")

SOCRATA_ID_RE = re.compile(r"(?<![a-z0-9])([a-z0-9]{4}-[a-z0-9]{4})(?![a-z0-9])")


def _load_env_file(path: Path, *, override: bool = False) -> None:
    """Load simple KEY=VALUE pairs from a local env file without extra deps."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if not override and key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def _extract_socrata_ids(*values: object) -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for value in values:
        if value is None:
            continue
        for match in SOCRATA_ID_RE.finditer(str(value).lower()):
            dataset_id = match.group(1)
            if dataset_id not in seen:
                seen.add(dataset_id)
                ids.append(dataset_id)
    return ids


def _split_refs(value: str) -> set[str]:
    return {part for part in value.split("|") if part}


def _join_refs(refs: set[str]) -> str:
    return "|".join(sorted(refs))


def _merge_entry(
    entries_by_id: dict[str, dict[str, str]],
    entry: dict[str, str],
    *,
    origin_ref: str,
    source_ref: str = "",
    signal_ref: str = "",
) -> None:
    dataset_id = entry.get("dataset_id", "").strip().lower()
    if not dataset_id:
        return
    existing = entries_by_id.setdefault(
        dataset_id,
        {
            "dataset_id": dataset_id,
            "name": "",
            "sector_or_category": "",
            "scope": "",
            "recommendation": "",
            "relevance": "",
            "audit_status": "",
            "source_refs": "",
            "origin_refs": "",
            "signal_refs": "",
            "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
        },
    )

    for key in ("name", "sector_or_category", "scope", "recommendation", "relevance", "url"):
        value = (entry.get(key) or "").strip()
        if value and not existing.get(key):
            existing[key] = value

    audit_status = (entry.get("audit_status") or "").strip()
    if audit_status and not existing.get("audit_status"):
        existing["audit_status"] = audit_status

    origins = _split_refs(existing.get("origin_refs", ""))
    origins.add(origin_ref)
    existing["origin_refs"] = _join_refs(origins)

    source_refs = _split_refs(existing.get("source_refs", ""))
    if source_ref:
        source_refs.add(source_ref)
    source_refs.update(_split_refs(entry.get("source_refs", "")))
    existing["source_refs"] = _join_refs(source_refs)

    signal_refs = _split_refs(existing.get("signal_refs", ""))
    if signal_ref:
        signal_refs.add(signal_ref)
    signal_refs.update(_split_refs(entry.get("signal_refs", "")))
    existing["signal_refs"] = _join_refs(signal_refs)

    if not existing.get("audit_status"):
        existing["audit_status"] = "valid"


def load_appendix(path: Path, *, include_current: bool = False) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not path.exists():
        LOG.warning("appendix CSV not found: %s", path)
        return rows
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            if not include_current and r.get("scope") == "current_registry":
                continue
            ds_id = r.get("dataset_id", "").strip()
            if not ds_id:
                continue
            audit = r.get("audit_status", "")
            if audit in ("dead_404", "forbidden_403"):
                continue
            r["origin_refs"] = _join_refs(_split_refs(r.get("origin_refs", "")) | {"appendix"})
            rows.append(r)
    return rows


def load_audit_json(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        LOG.warning("audit JSON not found: %s", path)
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("valid_datasets_flat", [])
    if not isinstance(entries, list):
        raise ValueError(f"audit JSON valid_datasets_flat is not a list: {path}")

    rows: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dataset_id = str(entry.get("id", "")).strip().lower()
        if not dataset_id:
            continue
        rows.append(
            {
                "dataset_id": dataset_id,
                "name": str(entry.get("name", "")).strip(),
                "sector_or_category": str(entry.get("sector", "")).strip(),
                "scope": "colombia_open_data_audit",
                "recommendation": "candidate",
                "relevance": "unknown",
                "audit_status": "valid",
                "origin_refs": "audit_json",
                "url": str(entry.get("url", "")).strip()
                or f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
            }
        )
    return rows


def load_source_registry(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        LOG.warning("source registry CSV not found: %s", path)
        return []

    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            source_id = (r.get("source_id") or "").strip()
            candidate_ids = _extract_socrata_ids(
                r.get("primary_url"),
                r.get("last_seen_url"),
                r.get("notes"),
            )
            for dataset_id in candidate_ids:
                rows.append(
                    {
                        "dataset_id": dataset_id,
                        "name": (r.get("name") or "").strip(),
                        "sector_or_category": (r.get("category") or "").strip(),
                        "scope": "source_registry",
                        "recommendation": "keep",
                        "relevance": (r.get("signal_promotion_state") or "").strip() or "registry",
                        "audit_status": "valid",
                        "source_refs": source_id,
                        "origin_refs": "source_registry",
                        "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
                    }
                )
    return rows


def load_signal_source_ids(path: Path) -> set[str]:
    if not path.exists():
        LOG.warning("signal dependency file not found: %s", path)
        return set()
    text = path.read_text(encoding="utf-8")
    source_ids: set[str] = set()
    for match in re.finditer(r"\b(?:sources|required|optional):\s*\[([^\]]*)\]", text):
        for value in match.group(1).split(","):
            source_id = value.strip().strip("'\"")
            if source_id:
                source_ids.add(source_id)
    return source_ids


def load_pipeline_env_sources(pipelines_dir: Path) -> list[dict[str, str]]:
    """Read env-backed Socrata dataset IDs from pipeline class declarations.

    Wave 4.B retired the bespoke Pipeline stack, so the directory may be
    missing entirely; that returns an empty list rather than raising.
    """
    if not pipelines_dir.exists():
        LOG.warning("pipeline directory not found: %s", pipelines_dir)
        return []

    rows: list[dict[str, str]] = []
    for path in sorted(pipelines_dir.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            LOG.warning("failed to parse pipeline file %s: %s", path, exc)
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            attrs: dict[str, str] = {}
            for stmt in node.body:
                if not isinstance(stmt, ast.Assign):
                    continue
                for target in stmt.targets:
                    if not isinstance(target, ast.Name):
                        continue
                    if target.id not in {"name", "source_id", "socrata_dataset_id_env"}:
                        continue
                    if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                        attrs[target.id] = stmt.value.value
            env_name = attrs.get("socrata_dataset_id_env", "")
            dataset_id = os.environ.get(env_name, "").strip().lower() if env_name else ""
            if not dataset_id or not SOCRATA_ID_RE.fullmatch(dataset_id):
                continue
            source_id = attrs.get("source_id") or attrs.get("name") or node.name
            rows.append(
                {
                    "dataset_id": dataset_id,
                    "name": source_id,
                    "sector_or_category": "pipeline_env",
                    "scope": "pipeline_env",
                    "recommendation": "candidate",
                    "relevance": "env_configured",
                    "audit_status": "valid",
                    "source_refs": source_id,
                    "origin_refs": f"pipeline_env:{path.stem}",
                    "url": f"https://{DEFAULT_DOMAIN}/d/{dataset_id}",
                }
            )
    return rows


def load_known_dataset_entries(
    *,
    appendix_path: Path,
    audit_json_path: Path,
    source_registry_path: Path,
    signal_deps_path: Path,
    pipelines_dir: Path,
    include_current: bool,
    include_appendix: bool,
    include_audit_json: bool,
    include_source_registry: bool,
    include_pipeline_env: bool,
) -> list[dict[str, str]]:
    entries_by_id: dict[str, dict[str, str]] = {}

    source_to_dataset_ids: dict[str, set[str]] = {}

    def add_entries(entries: list[dict[str, str]], fallback_origin: str) -> None:
        for entry in entries:
            source_refs = _split_refs(entry.get("source_refs", ""))
            origin_ref = entry.get("origin_refs", "") or fallback_origin
            _merge_entry(
                entries_by_id,
                entry,
                origin_ref=origin_ref,
                source_ref="",
            )
            dataset_id = entry.get("dataset_id", "").strip().lower()
            for source_id in source_refs:
                source_to_dataset_ids.setdefault(source_id, set()).add(dataset_id)

    if include_appendix:
        add_entries(load_appendix(appendix_path, include_current=include_current), "appendix")
    if include_audit_json:
        add_entries(load_audit_json(audit_json_path), "audit_json")
    if include_source_registry:
        source_registry_entries = load_source_registry(source_registry_path)
        add_entries(source_registry_entries, "source_registry")
    if include_pipeline_env:
        add_entries(load_pipeline_env_sources(pipelines_dir), "pipeline_env")

    signal_source_ids = load_signal_source_ids(signal_deps_path)
    for source_id in signal_source_ids:
        for dataset_id in source_to_dataset_ids.get(source_id, set()):
            entry = entries_by_id.get(dataset_id)
            if not entry:
                continue
            _merge_entry(
                entries_by_id,
                {"dataset_id": dataset_id},
                origin_ref="signal_deps",
                signal_ref=source_id,
            )

    return sorted(entries_by_id.values(), key=lambda row: row["dataset_id"])
