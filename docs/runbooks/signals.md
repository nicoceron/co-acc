# Signal Materialization Runbook

The lake-backed signal materializer turns shipped curated signal feature
tables into run-scoped parquet outputs. It does not require Neo4j.

## Build

Rebuild every shipped signal run:

```bash
make materialize-all
# equivalent explicit form:
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl signals materialize --all
```

Materialize one signal:

```bash
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl signals materialize \
  --signal procurement_sanctioned_supplier_awarded
```

`--run-id <id>` is available for deterministic operator reruns and tests.
`--allow-empty` writes a run even when a selected signal currently emits zero
hits; without it, zero-hit selected signals fail the run.

## Inputs

The first shipped materializer slice reads:

- `lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded/`
- `lake/curated/table=signal_feature_procurement_supplier_concentration_across_entities/`
- `lake/curated/table=signal_feature_procurement_repeat_awards_same_supplier/`

Run `make curate` first when any of those feature tables are missing or stale.

## Outputs

Each run writes:

- `lake/curated/signal_hits/run_id=<run>/part-00000.parquet`
- `lake/curated/evidence_bundles/run_id=<run>/part-00000.parquet`
- `lake/meta/signal_runs/<run>.json`

`signal_hits` carries the public signal contract: run id, signal id/version,
entity uid, scope key/type, severity, score, title, description, public flags,
identity quality, timestamps, evidence bundle id, and evidence refs.

`evidence_bundles` expands each evidence ref into an auditable row with source
id, record/url, source parquet path, row selector, label, and identity metadata.
The materializer writes one row per deterministic `hit_id`; API readers also
deduplicate by `hit_id` and evidence `item_index` so repeated source feature
rows do not duplicate user-facing hits.

## Recovery

Runs are append-only by `run_id`. Reusing a `run_id` atomically replaces only
that run partition and manifest. If a run fails, inflight directories are
removed and no completed manifest is written.

Use:

```bash
make materialize-all RUN_ID=manual-rerun-20260601
```

The API reports the newest completed lake signal run from
`lake/meta/signal_runs/`, falling back to curated-table manifests when no
signal run exists.

## API Behavior

There is no feature flag required for lake signal runs. When a completed
signal-run manifest is present, `/api/v1/signals` reads counts, samples, and
evidence items from `signal_hits` and `evidence_bundles`. If no signal run is
present yet, it falls back to the shipped curated signal feature tables for
counts and samples.

When Neo4j is unavailable, `/api/v1/cases/` exposes a public-safe lake dossier
view with one case per materialized signal hit. `/api/v1/cases/{hit_id}` reads
the corresponding signal hit and evidence bundle directly from parquet.
Graph-backed case creation and refresh remain available only when Neo4j is
connected.

When Neo4j is unavailable, `/api/v1/search`, `/api/v1/entity/{identifier}`,
and `/api/v1/entity/by-element-id/{element_id}` read curated
`dim_company`, `dim_buyer`, and `dim_person` parquet through DuckDB.
`/api/v1/entity/{entity_id}/signals` reads the latest materialized signal run
and returns deduplicated signal hits plus evidence items for that entity key.
Public-mode person/entity guards still apply before reading lake dimensions.

## Reality Notes

On the local lake generated during the 2026-06-01 run:

- Raw materialized parquet: 30,397 `signal_hits` rows and 91,433
  `evidence_bundles` rows
- API-deduplicated `procurement_sanctioned_supplier_awarded`: 16,894 hits
- `procurement_supplier_concentration_across_entities`: 497 hits
- `procurement_repeat_awards_same_supplier`: 9,716 hits
- Total API-deduplicated hits: 27,107
- Total API-deduplicated evidence items: 84,853
