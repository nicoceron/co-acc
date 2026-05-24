"""argparse entry point for ``coacc-source-qualification``.

Orchestrates the qualification gate: collect input entries, probe each
dataset against Socrata, optionally run the LLM second pass, write the
signed catalog + proven catalog + markdown report.
"""
# ruff: noqa: E501
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from coacc_etl.qualification.inputs import (
    _load_env_file,
    load_appendix,
    load_known_dataset_entries,
)
from coacc_etl.qualification.llm_review import (
    _apply_llm_findings,
    _try_llm_review,
)
from coacc_etl.qualification.promotion import (
    TriageCatalogRow,
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
    probe_dataset,
)

LOG = logging.getLogger("source_qualification")

REPO_ROOT = Path(__file__).resolve().parents[4]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--appendix",
        default=str(REPO_ROOT / "docs/datasets/archive/dataset_relevance_appendix.csv"),
        help="Path to the (archived) relevance appendix CSV",
    )
    parser.add_argument(
        "--audit-json",
        default=str(REPO_ROOT / "docs/datasets/colombia_open_data_audit.json"),
        help="Path to the Colombia open-data audit JSON",
    )
    parser.add_argument(
        "--source-registry",
        default=str(REPO_ROOT / "docs/source_registry_co_v1.csv"),
        help="Path to the current source registry CSV",
    )
    parser.add_argument(
        "--signal-deps",
        default=str(REPO_ROOT / "config/signal_source_deps.yml"),
        help="Path to signal dependency registry",
    )
    parser.add_argument(
        "--pipelines-dir",
        default=str(REPO_ROOT / "etl/src/coacc_etl/pipelines"),
        help="Path to ETL pipeline modules for env-backed Socrata IDs",
    )
    parser.add_argument(
        "--catalog-out",
        default=str(REPO_ROOT / "docs/datasets/catalog.signed.csv"),
        help="Output: full catalog CSV with all probed metadata (signed canonical)",
    )
    parser.add_argument(
        "--proven-out",
        default=str(REPO_ROOT / "docs/datasets/catalog.proven.csv"),
        help="Output: only datasets with proven join keys",
    )
    parser.add_argument(
        "--report-out",
        default=str(REPO_ROOT / "docs/datasets/catalog.report.md"),
        help="Output: human-readable markdown report",
    )
    parser.add_argument(
        "--domain",
        default=DEFAULT_DOMAIN,
        help="Socrata domain (default www.datos.gov.co)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only probe first N candidates")
    parser.add_argument("--sleep", type=float, default=0.25, help="Seconds between requests")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    parser.add_argument("--count-timeout", type=float, default=20.0, help="Timeout for count(*) queries")
    parser.add_argument("--skip-count", action="store_true", help="Skip count(*) queries entirely")
    parser.add_argument("--probe-sample", type=int, default=5, help="Rows to sample for join density")
    parser.add_argument("--include-current", action="store_true", help="Also probe current_registry sources")
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only use Socrata metadata/schema; skip count(*) and row sampling",
    )
    parser.add_argument(
        "--all-known",
        action="store_true",
        help="Use appendix + audit JSON + source registry + env-backed pipeline IDs",
    )
    parser.add_argument(
        "--no-appendix",
        action="store_true",
        help="With --all-known, skip archived dataset_relevance_appendix.csv",
    )
    parser.add_argument(
        "--no-audit-json",
        action="store_true",
        help="With --all-known, skip colombia_open_data_audit.json",
    )
    parser.add_argument(
        "--no-source-registry",
        action="store_true",
        help="With --all-known, skip docs/source_registry_co_v1.csv",
    )
    parser.add_argument(
        "--no-pipeline-env",
        action="store_true",
        help="With --all-known, skip env-backed pipeline dataset IDs",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--llm-review", action="store_true",
        help="Run LLM-powered review on datasets with ≤1 join key found",
    )
    parser.add_argument(
        "--llm-provider", default="gemini",
        help="LLM provider: gemini, anthropic, or openai (default: gemini)",
    )
    parser.add_argument(
        "--llm-model", default="",
        help="LLM model name (default depends on provider)",
    )
    parser.add_argument(
        "--llm-api-key", default="",
        help="LLM API key (or set provider env var such as GEMINI_API_KEY)",
    )
    parser.add_argument(
        "--llm-cache",
        default=str(REPO_ROOT / "docs/datasets/source_qualification_llm_cache.json"),
        help="Path to LLM review cache file",
    )
    parser.add_argument(
        "--llm-required",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail when --llm-review is requested but no LLM results are produced",
    )
    parser.add_argument(
        "--llm-sleep", type=float, default=0.5,
        help="Seconds between LLM API calls (default: 0.5)",
    )
    parser.add_argument(
        "--llm-min-confidence",
        type=float,
        default=0.75,
        help="Minimum confidence required to accept an LLM join-key finding",
    )
    parser.add_argument(
        "--llm-only", action="store_true",
        help="Only run LLM review (skip Socrata probing, use existing catalog)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    env_path = REPO_ROOT / ".env"
    _load_env_file(env_path, override=True)

    appendix_path = Path(args.appendix)
    audit_json_path = Path(args.audit_json)
    source_registry_path = Path(args.source_registry)
    signal_deps_path = Path(args.signal_deps)
    pipelines_dir = Path(args.pipelines_dir)

    if args.llm_only:
        entries = []
    elif args.all_known:
        entries = load_known_dataset_entries(
            appendix_path=appendix_path,
            audit_json_path=audit_json_path,
            source_registry_path=source_registry_path,
            signal_deps_path=signal_deps_path,
            pipelines_dir=pipelines_dir,
            include_current=args.include_current,
            include_appendix=not args.no_appendix,
            include_audit_json=not args.no_audit_json,
            include_source_registry=not args.no_source_registry,
            include_pipeline_env=not args.no_pipeline_env,
        )
    else:
        if not appendix_path.is_file():
            LOG.error("appendix CSV not found: %s", appendix_path)
            return 2
        entries = load_appendix(appendix_path, include_current=args.include_current)
    if args.limit and not args.llm_only:
        entries = entries[: args.limit]

    if args.metadata_only:
        args.skip_count = True
        args.probe_sample = 0

    if args.llm_only:
        LOG.info("loading existing catalog for LLM-only review: %s", args.catalog_out)
    else:
        LOG.info(
            "probing %d candidate datasets from %s",
            len(entries),
            "all known sources" if args.all_known else str(appendix_path),
        )

    rows: list[TriageCatalogRow] = []
    catalog_path = Path(args.catalog_out)
    proven_path = Path(args.proven_out)
    report_path = Path(args.report_out)

    def _flush() -> None:
        write_catalog(rows, catalog_path)
        write_proven(rows, proven_path)
        write_report(rows, report_path)

    if args.llm_only:
        rows = read_catalog(catalog_path)
    else:
        with _build_client(args.timeout) as meta_client:
            count_client = None
            if not args.skip_count and args.count_timeout > args.timeout:
                count_client = _build_client(args.count_timeout)

            try:
                for i, entry in enumerate(entries, start=1):
                    try:
                        row = probe_dataset(
                            meta_client,
                            entry,
                            domain=args.domain,
                            probe_sample=args.probe_sample,
                            skip_count=args.skip_count,
                            count_client=count_client,
                        )
                    except Exception as exc:
                        row = TriageCatalogRow(
                            dataset_id=entry.get("dataset_id", ""),
                            name=entry.get("name", ""),
                            sector=entry.get("sector_or_category", ""),
                            scope=entry.get("scope", ""),
                            recommendation=entry.get("recommendation", ""),
                            relevance=entry.get("relevance", ""),
                            audit_status=entry.get("audit_status", ""),
                            source_refs=entry.get("source_refs", ""),
                            origin_refs=entry.get("origin_refs", ""),
                            signal_refs=entry.get("signal_refs", ""),
                            url=entry.get("url", ""),
                            probe_notes=[f"probe failed: {exc}"],
                        )
                    rows.append(row)
                    LOG.info(
                        "[%d/%d] %s rows=%s join_keys=%s classes=%s cols=%s/%s",
                        i,
                        len(entries),
                        row.dataset_id,
                        row.rows,
                        row.join_keys_found,
                        row.join_key_classes,
                        row.n_meaningful_columns,
                        row.n_columns,
                    )
                    if i % 50 == 0:
                        _flush()
                        LOG.info("flushed partial output at %d/%d", i, len(entries))
                    if args.sleep > 0:
                        time.sleep(args.sleep)
            finally:
                if count_client is not None:
                    count_client.close()

        _flush()

    if args.llm_review or args.llm_only:
        review_targets = [r for r in rows if r.join_keys_found <= 1 and r.audit_status not in ("dead_404", "forbidden_403")]
        LOG.info(
            "running LLM review on %d datasets (with ≤1 join key)",
            len(review_targets),
        )
        if not review_targets:
            LOG.warning("no datasets to review with LLM")
        else:
            with _build_client(args.timeout) as review_client:
                llm_results = _try_llm_review(
                    review_targets,
                    provider=args.llm_provider,
                    model=args.llm_model,
                    api_key=args.llm_api_key or None,
                    sleep_between=args.llm_sleep,
                    cache_path=Path(args.llm_cache),
                )
                if args.llm_required and not llm_results:
                    LOG.error(
                        "LLM review is required but produced no results; "
                        "check provider, API key, model, network, and quota"
                    )
                    return 3
                rows = _apply_llm_findings(
                    rows, llm_results,
                    client=review_client,
                    domain=args.domain,
                    probe_sample=args.probe_sample,
                    min_confidence=args.llm_min_confidence,
                )
            _flush()
            llm_key_count = sum(1 for r in rows if "llm_added" in " ".join(r.probe_notes))
            llm_tier_count = sum(1 for r in rows if any("llm_tier" in n for n in r.probe_notes))
            LOG.info("LLM review: added keys to %d datasets, classified %d", llm_key_count, llm_tier_count)

    ingest_classes = {"ingest_priority", "ingest", "ingest_if_useful"}
    schema_classes = {"schema_core_join", "schema_context_join"}
    proven_count = sum(1 for r in rows if r.join_keys_found > 0)
    ingest_count = sum(1 for r in rows if classify_dataset(r) in ingest_classes)
    schema_count = sum(1 for r in rows if classify_dataset(r) in schema_classes)
    LOG.info("wrote %s, %s, %s", args.catalog_out, args.proven_out, args.report_out)
    LOG.info("datasets with join keys: %d / %d", proven_count, len(rows))
    LOG.info("datasets recommended for ingestion: %d", ingest_count)
    LOG.info("datasets schema-qualified pending row/freshness checks: %d", schema_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
