# Production Lake Ops Runbook

The production app should not depend on a developer laptop holding every
dataset. The production lake is a durable mounted path, and the `lake-ops`
Compose profile runs ingestion, curation, materialization, and lake checks
against that path.

## Storage Model

Set `COACC_HOST_LAKE_ROOT` to the host directory or mounted volume that owns
the lake. Production compose mounts it read/write into the ops container at
`/workspace/lake` and read-only into the API at `/app/lake`.

Expected layout:

- `lake/raw/` contains paged source extracts.
- `lake/curated/` contains DuckDB-built parquet tables.
- `lake/curated/signal_hits/` and `lake/curated/evidence_bundles/` contain
  run-scoped materialized pattern outputs.
- `lake/meta/` contains watermarks, manifests, run metadata, and reality
  snapshots.
- `lake/meta/operations/latest.json` contains the latest lake ops run status
  used by `/ready` and `/api/v1/meta/operations`.

## Manual Runs

From the repository root on the production host:

```bash
infra/scripts/run-lake-ops.sh refresh
```

Modes:

| Mode | Behavior |
|---|---|
| `check` | Validate curated contracts and write a curated-only lake reality snapshot. |
| `curate` | Rebuild all curated tables, then run checks. |
| `materialize` | Materialize all shipped lake-backed signals, then run checks. |
| `refresh` | Incrementally ingest all ingest-ready core datasets, curate, materialize, and check. |
| `smoke` | Run the bounded Phase 7 smoke ingest, curate, materialize, and check. |
| `full` | Run the guarded Phase 7 full ingest, curate, materialize, and check. Requires `COACC_LAKE_OPS_ALLOW_FULL=true`. |

`refresh` is the normal scheduled mode. `full` is intentionally guarded
because it can download and rewrite much larger source slices.

## Batch Controls

The ops runner forwards the existing ingestion knobs:

- `COACC_LAKE_OPS_PAGE_SIZE`
- `COACC_LAKE_OPS_MAX_PAGES`
- `COACC_LAKE_OPS_TIMEOUT_SECONDS`
- `COACC_LAKE_OPS_MIN_FREE_GB`
- `COACC_LAKE_OPS_CONTINUE_ON_ERROR`

The lower-level Socrata defaults remain:

- `COACC_SOCRATA_PAGE_SIZE`
- `COACC_SOCRATA_MAX_PAGES`
- `COACC_SOCRATA_TIMEOUT_SECONDS`
- `COACC_SOCRATA_MAX_TIMEOUT_SECONDS`

These controls page through source APIs and write parquet batches to the lake.
DuckDB then scans parquet directly for curated tables and signal outputs.

## Cron

Install or refresh a host cron entry:

```bash
COACC_LAKE_OPS_CRON_SCHEDULE="15 2 * * *" infra/scripts/lake-ops-cron.sh
```

The default schedule runs `refresh` daily at 02:15 host time and logs to
`/var/log/coacc-lake-ops.log`. Override the mode with `COACC_LAKE_OPS_MODE`.

`infra/scripts/run-lake-ops.sh` loads `.env` from the repository root when it
exists. Set `COACC_LAKE_OPS_ALERT_WEBHOOK_URL` to send a JSON webhook when the
ops container exits nonzero.

## Compose Profile

The underlying command is:

```bash
docker compose -f infra/docker/docker-compose.prod.yml --profile ops run --rm lake-ops refresh
```

The helper script supplies safe placeholder values for app-only required env
vars when the ops container is run by itself. Real production app deployment
still needs real `DOMAIN`, `JWT_SECRET_KEY`, and `NEO4J_PASSWORD` values.

## Status And Readiness

Every `lake-ops` run writes:

- `lake/meta/operations/<run_id>.json`
- `lake/meta/operations/latest.json`

Production compose defaults `COACC_READY_MAX_LAKE_OPS_AGE_HOURS=48`, so
`/ready` fails when the latest ops run is missing, failed, or older than 48
hours. Operators can inspect the same status through:

```bash
curl https://$DOMAIN/api/v1/meta/operations
```

## API/Frontend Contract

The API must read the lake read-only in production. The frontend must consume
API summaries, entity details, signal hits, evidence bundles, and chart data.
It must not fetch raw source datasets or depend on fixture fallback when
`VITE_ALLOW_FIXTURES=false`.

Keep `COACC_SIGNALS_REQUIRE_MATERIALIZED=true` for production until additional
signal materializers are implemented and verified.
