"""Back-compat shim for the legacy ``coacc_etl.source_qualification`` module.

The real implementation lives under ``coacc_etl.qualification`` (split into
inputs / socrata_probe / llm_review / promotion / report / cli per Wave 5.1).
This shim re-exports the public surface so existing imports keep working.

It also re-imports a handful of stdlib + third-party modules at top level
because the legacy test suite monkey-patches them via this module
(``triage.httpx``, ``triage.os``, ``triage.time``).
"""
from __future__ import annotations

# Stdlib / third-party imports kept at module level for legacy monkeypatch
# targets used by ``etl/tests/test_source_qualification.py``.
import os  # noqa: F401  (re-export for legacy monkeypatch)
import time  # noqa: F401  (re-export for legacy monkeypatch)

import httpx  # noqa: F401  (re-export for legacy monkeypatch)

from coacc_etl.qualification import *  # noqa: F401, F403  (re-export public API)
from coacc_etl.qualification import main  # noqa: F401  (entry point alias)

# Re-export every name the legacy test suite reaches into. Importing-as
# preserves identity so monkeypatch.setattr against this module works.
from coacc_etl.qualification.inputs import (  # noqa: F401
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
from coacc_etl.qualification.llm_review import (  # noqa: F401
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
from coacc_etl.qualification.promotion import (  # noqa: F401
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
from coacc_etl.qualification.report import (  # noqa: F401
    read_catalog,
    write_catalog,
    write_proven,
    write_report,
)
from coacc_etl.qualification.socrata_probe import (  # noqa: F401
    DEFAULT_DOMAIN,
    _build_client,
    _get_with_retry,
    _meaningful_column_count,
    _parse_socrata_ts,
    _probe_join_density,
    probe_dataset,
)


if __name__ == "__main__":
    import sys

    sys.exit(main())
