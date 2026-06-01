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

There is no feature flag required for lake signal runs. When Neo4j is
unavailable, `/api/v1/signals` still reads the lake-backed curated signal
feature tables for counts and samples. When a completed signal-run manifest is
present, the API exposes that `run_id` and timestamp as the latest materializer
run. Neo4j-backed materialization remains a compatibility path only when a
Neo4j session is available.

## Reality Notes

On the local lake generated during the 2026-06-01 run:

- `procurement_sanctioned_supplier_awarded`: 20,184 hits
- `procurement_supplier_concentration_across_entities`: 497 hits
- `procurement_repeat_awards_same_supplier`: 9,716 hits
- Total `signal_hits`: 30,397 rows
- Total `evidence_bundles`: 91,433 rows
