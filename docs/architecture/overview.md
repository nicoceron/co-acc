# Architecture overview

One-page trace of how data moves through co/acc, from raw audit JSON to API responses. If you only read one architecture doc, read this one.

---

## The trip in one diagram

```
                      ┌────────────────────────────────────────────────────────┐
                      │  Sources of dataset truth                              │
                      │   - docs/datasets/colombia_open_data_audit.json        │
                      │   - docs/datasets/archive/dataset_relevance_appendix   │
                      │   - docs/source_registry_co_v1.csv (legacy, retiring)  │
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
        │  curated/  reserved for future curation passes (signal feature       │
        │            engineering, entity resolution outputs).                  │
        └──────────────────────────────────────────────────────────────────────┘
                                            │
                       ┌────────────────────┼────────────────────┐
                       ▼                    ▼                    ▼
        ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
        │  lakehouse.reality  │  │  api/               │  │  signal materializer│
        │  freshness +        │  │  FastAPI service    │  │  (api workspace)    │
        │  coverage rollup    │  │  reads parquet via  │  │  reads parquet,     │
        │  per dataset        │  │  duckdb + Cypher    │  │  emits signal rows  │
        │  scripts/lake_      │  │  against Neo4j      │  │  per signal_        │
        │  reality.py         │  │  (downstream)       │  │  registry.yml       │
        └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

---

## Two invariants this architecture defends

1. **The lake is canonical.** Every consumer (API, signal materializer, future graph loader) reads parquet. There is no second source of truth. The `pipeline_registry`/`Pipeline` stack that previously held Neo4j-only state was retired in Wave 4.B precisely because it created a second source of truth that drifted from the source data.

2. **Watermarks derive from data, not wall-clock.** `wm.advance(last_seen_ts=max(batch[watermark_column]))`. A pipeline run with zero new rows leaves the watermark untouched. A pipeline run that hits a coverage gate failure leaves the watermark untouched. A `--full-refresh` advances the watermark to the new max with `force=True`. There is no other way to move a watermark.

These two invariants together give us reproducibility: rerun ingest from an empty lake against the same Socrata snapshot and you get byte-for-byte identical row content (file names differ — the writer stamps `<iso>-<uuid>` — but the data is the same; see `etl/tests/test_ingest/test_determinism.py`).

---

## What's missing (deliberately)

- **A graph loader** — Neo4j removal in Wave 4.B leaves no ETL→Neo4j path. The `api/` workspace still has Cypher queries; rebuilding the graph from parquet is a future downstream consumer, not part of this pipeline.
- **Custom non-Socrata adapters** — PACO, RUES, Registraduría, official_case_bulletins, etc. Tracked in `_KNOWN_DEFERRED_SOURCES` (`etl/tests/test_signal_source_alignment.py`). Will land under `coacc_etl.ingest.custom/` as they're built.
- **A curated layer** — `lake/curated/` exists as a directory but no curation runs yet. Signal feature engineering and entity resolution outputs will land there.

---

## Where to look for what

| If you want to… | Look at |
|---|---|
| Add a Socrata dataset | `etl/datasets/<id>.yml` (incremental) or `full_refresh_only: true` (snapshot) |
| Add a non-Socrata adapter | `coacc_etl.ingest.custom/` (TBD) + remove from `_KNOWN_DEFERRED_SOURCES` |
| Re-probe Socrata | `make qualify QUALIFY_ARGS="--all-known --llm-review"` |
| Add a signal | `config/signal_registry.yml` + `config/signal_source_deps.yml` |
| Inspect a dataset's lake state | `lake/raw/source=<id>/`, `lake/meta/watermarks.parquet`, `lake/meta/coverage/<id>/` |
| See what's planned next | `docs/cleanup/refactor_plan.md` (Wave-by-wave history) |
