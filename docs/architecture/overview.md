# Architecture overview

One-page trace of how data moves through co/acc, from raw audit JSON to API responses. If you only read one architecture doc, read this one.

---

## The trip in one diagram

```
                      ┌────────────────────────────────────────────────────────┐
                      │  Sources of dataset truth                              │
                      │   - docs/datasets/colombia_open_data_audit.json        │
                      │   - docs/datasets/archive/dataset_relevance_appendix   │
                      │   - docs/datasets/catalog.signed.csv source_refs       │
                      │   - config/signal_source_deps.yml                      │
                      └────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  qualification gate     coacc_etl.qualification.cli:main             │
        │   1. inputs.py          collect candidate datasets                   │
        │   2. socrata_probe.py   metadata + sample probe                      │
        │   3. promotion.py       deterministic join-key classifier            │
        │   4. llm_review.py      Gemini second-review for ambiguous columns   │
        │   5. report.py          write signed catalog + proven + report       │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  Signed catalog                                                      │
        │   docs/datasets/catalog.signed.csv     148 rows, full metadata       │
        │   docs/datasets/catalog.proven.csv     148 rows, proven join keys    │
        │   docs/datasets/catalog.report.md      human-readable rollup         │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  YAML contracts          etl/datasets/<socrata-4x4-id>.yml           │
        │                                                                      │
        │  One per dataset, validated by Pydantic on load. Fields:             │
        │    id, name, sector, tier (core | context | backlog),                │
        │    join_keys, watermark_column, partition_column, columns_map,       │
        │    required_coverage, freq, url, notes,                              │
        │    full_refresh_only (snapshot mode toggle)                          │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  Generic ingester       coacc_etl.ingest.socrata.ingest(spec)        │
        │                                                                      │
        │  Two source classes:                                                 │
        │    INCREMENTAL  $where=watermark_col > 'last_seen_iso'               │
        │                 watermark advances to max(batch[watermark_col])      │
        │                 partitions: year=YYYY/month=MM/                      │
        │    SNAPSHOT     full pull, no $where, $order=:id ASC                 │
        │                 partitions: snapshot=YYYYMMDDTHHMMSSZ/               │
        │                 watermark untouched                                  │
        │                                                                      │
        │  Coverage gate (coverage.py): non-null ratio per declared column.    │
        │   - Pass → write parquet, persist coverage report                    │
        │   - Fail → write failure report, refuse to advance watermark         │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  Lake          $COACC_LAKE_ROOT (default /var/lib/coacc/lake)        │
        │                                                                      │
        │  raw/source=<id>/year=YYYY/month=MM/<ts>-<uuid>.parquet              │
        │  raw/source=<id>/snapshot=<iso>/<ts>-<uuid>.parquet                  │
        │  raw/source=<id>/year=0/month=00/...                ← unparseable    │
        │                                                       partition col  │
        │  meta/watermarks.parquet         current watermark per source        │
        │  meta/watermark_events.parquet   append-only history                 │
        │  meta/coverage/<id>/<ts>.json    pass reports                        │
        │  meta/failures/<id>/<ts>.json    fail reports (no watermark)         │
        │                                                                      │
        │  curated/  DuckDB-built signal features and entity dimensions.       │
        │            table=dim_subject_document/, table=fct_procurement_...    │
        │            and table=signal_feature_procurement_*                    │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                       ┌────────────────────┼────────────────────┐
                       ▼                    ▼                    ▼
        ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
        │  lakehouse.reality  │  │  signal engine      │  │  anomaly + narrator │
        │  freshness,         │  │  materialized       │  │  model train/score, │
        │  coverage, manifest │  │  signal_hits and    │  │  verified narrative │
        │  + evidence checks  │  │  evidence_bundles   │  │  Markdown           │
        │  scripts/lake_      │  │  from curated       │  │  from parquet       │
        │  reality.py         │  │  parquet            │  │                     │
        └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  api/ FastAPI service                                                │
        │   - lake-backed signals, cases, search, entity detail, patterns      │
        │   - promoted anomaly score parquet and precomputed narratives        │
        │   - citizen-agent endpoint reads promoted case data and citations    │
        │   - Neo4j remains optional for exploration/projection                │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │  frontend/                                                           │
        │   Existing UI and contract tests are green. Phase 15 still owns      │
        │   the new case browser, narrative reader, and citizen-agent pages.   │
        └──────────────────────────────────────────────────────────────────────┘
```

---

## Two invariants this architecture defends

1. **The lake is canonical.** Every consumer (API, signal materializer, future graph loader) reads parquet. There is no second source of truth. The `pipeline_registry`/`Pipeline` stack that previously held Neo4j-only state was retired in Wave 4.B precisely because it created a second source of truth that drifted from the source data.

2. **Watermarks derive from data, not wall-clock.** `wm.advance(last_seen_ts=max(batch[watermark_column]))`. A pipeline run with zero new rows leaves the watermark untouched. A pipeline run that hits a coverage gate failure leaves the watermark untouched. A `--full-refresh` advances the watermark to the new max with `force=True`. There is no other way to move a watermark.

These two invariants together give us reproducibility: rerun ingest from an empty lake against the same Socrata snapshot and you get byte-for-byte identical row content (file names differ — the writer stamps `<iso>-<uuid>` — but the data is the same; see `etl/tests/test_ingest/test_determinism.py`).

---

## What's missing (deliberately)

- **Full legacy graph and pattern parity** — the source of truth is now
  lake/curated parquet; signal lists, case dossiers, entity lookup/search,
  entity signal drilldowns, anomaly score drilldowns, lake-backed graph
  context, public company graph, and shipped signal-backed pattern routes can
  run without Neo4j. Full Neo4j relationship expansion and Cypher-only legacy
  pattern parity are still Phase 11/11.5 compatibility work.
- **Optional graph projection** — Neo4j is useful for exploration, but not
  required for corruption-pattern detection correctness.
- **More custom non-Socrata adapters** — PACO has landed; RUES,
  Registraduría, official_case_bulletins, etc. remain tracked in
  `_KNOWN_DEFERRED_SOURCES` (`etl/tests/test_signal_source_alignment.py`).
- **More curated tables and demo-specific signal coverage** — typed
  dimensions, signal features, the Isolation Forest baseline, and the
  supervised anomaly top-up exist. The API reads promoted score parquet; extra
  feature tables remain useful for broader pattern parity and demos.
- **Frontend completion** — the backend can extract contract case subgraphs,
  score contracts, precompute verified narratives, expose those narratives
  through case detail responses, and answer citizen-agent queries from promoted
  case data. Phase 15 still owns the actual `/casos`, case detail, and
  `/agente` frontend surfaces.

---

## Where to look for what

| If you want to… | Look at |
|---|---|
| Add a Socrata dataset | `etl/datasets/<id>.yml` (incremental) or `full_refresh_only: true` (snapshot) |
| Add a non-Socrata adapter | `coacc_etl.ingest.custom/` + `etl/datasets/<adapter>.yml` + remove from `_KNOWN_DEFERRED_SOURCES` |
| Re-probe Socrata | `make qualify QUALIFY_ARGS="--all-known --llm-review"` |
| Add a signal | `config/signal_registry.yml` + `config/signal_source_deps.yml` |
| Inspect a dataset's lake state | `lake/raw/source=<id>/`, `lake/meta/watermarks.parquet`, `lake/meta/coverage/<id>/` |
| See what's planned next | `docs/cleanup/refactor_plan.md` (Wave-by-wave history) |
