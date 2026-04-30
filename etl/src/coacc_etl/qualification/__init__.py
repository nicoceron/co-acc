"""Pre-lake source qualification gate.

Reads the current source inventories, probes Socrata metadata + column
schemas, then proves join keys against the curated key classes. The
Gemini LLM pass is a required second-review gate for government
datasets with badly named columns. Outputs:

  1. ``docs/datasets/catalog.signed.csv``  — full metadata/rowcount
  2. ``docs/datasets/catalog.report.md``    — human-readable summary
  3. ``docs/datasets/catalog.proven.csv``  — datasets with proven joins

This module is the public surface that re-exports every helper that
was previously top-level in ``coacc_etl.source_qualification``. The
legacy import path ``coacc_etl.source_qualification`` survives as a
thin shim for back-compat (see that module's docstring).
"""
from __future__ import annotations

from coacc_etl.qualification.cli import main
from coacc_etl.qualification.inputs import (
    SOCRATA_ID_RE,
    _extract_socrata_ids,
    _join_refs,
    _load_env_file,
    _merge_entry,
    _split_refs,
    load_appendix,
    load_audit_json,
    load_known_dataset_entries,
    load_pipeline_env_sources,
    load_signal_source_ids,
    load_source_registry,
)
from coacc_etl.qualification.llm_review import (
    DEFAULT_LLM_MODELS,
    LLM_JOIN_KEY_PROMPT,
    LLM_RELEVANCE_PROMPT,
    _apply_llm_findings,
    _call_anthropic,
    _call_gemini,
    _call_openai,
    _llm_review_anthropic,
    _llm_review_gemini,
    _llm_review_openai,
    _load_cache,
    _parse_json_from_response,
    _save_cache,
    _try_llm_review,
)
from coacc_etl.qualification.promotion import (
    CANDIDATE_RECOMMENDATIONS,
    CATALOG_FIELDS,
    JOIN_KEY_CLASSES,
    REVERSE_KEY_INDEX,
    TriageCatalogRow,
    _build_reverse_key_index,
    _find_join_keys,
    _find_secondary_key_class,
    _has_id_marker,
    _is_stable_identifier_column,
    _is_valid_llm_join_key,
    _normalize_col,
    classify_dataset,
)
from coacc_etl.qualification.report import (
    read_catalog,
    write_catalog,
    write_proven,
    write_report,
)
from coacc_etl.qualification.socrata_probe import (
    DEFAULT_DOMAIN,
    _build_client,
    _get_with_retry,
    _meaningful_column_count,
    _parse_socrata_ts,
    _probe_join_density,
    probe_dataset,
)

__all__ = [
    "CANDIDATE_RECOMMENDATIONS",
    "CATALOG_FIELDS",
    "DEFAULT_DOMAIN",
    "DEFAULT_LLM_MODELS",
    "JOIN_KEY_CLASSES",
    "LLM_JOIN_KEY_PROMPT",
    "LLM_RELEVANCE_PROMPT",
    "REVERSE_KEY_INDEX",
    "SOCRATA_ID_RE",
    "TriageCatalogRow",
    "classify_dataset",
    "load_appendix",
    "load_audit_json",
    "load_known_dataset_entries",
    "load_pipeline_env_sources",
    "load_signal_source_ids",
    "load_source_registry",
    "main",
    "probe_dataset",
    "read_catalog",
    "write_catalog",
    "write_proven",
    "write_report",
]
