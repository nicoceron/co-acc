# Environment Runbook

Use `.env.example` as the tracked template. `make setup-env` creates a local
`.env` and replaces the placeholder `JWT_SECRET_KEY` and `NEO4J_PASSWORD`.
Never commit `.env`.

## Lake / ETL

| Variable | Default | Purpose |
|---|---:|---|
| `COACC_LAKE_ROOT` | repo `lake/` via Make targets | Lake root for raw, curated, and metadata parquet. |
| `COACC_SOCRATA_PAGE_SIZE` | `10000` | Rows requested per Socrata page when CLI flags do not override it. |
| `COACC_SOCRATA_MAX_PAGES` | `10000` | Hard page cap per Socrata dataset when CLI flags do not override it. |
| `COACC_SOCRATA_TIMEOUT_SECONDS` | `60` | Initial Socrata request timeout in seconds. The `ingest`, `ingest-all`, and `ingest-phase7` commands can override this with `--timeout-seconds`. |
| `COACC_SOCRATA_MAX_TIMEOUT_SECONDS` | `240` | Maximum Socrata request timeout in seconds during retry backoff. Retries double the initial timeout up to this ceiling. |
| `COACC_WATERMARK_FUTURE_GRACE_DAYS` | `1` | Maximum allowed timestamp lead before a row is treated as implausibly future-dated and routed to `year=0/month=00` without advancing the watermark. |
| `COACC_LAKE_OPS_MODE` | `refresh` | Default production lake ops mode for `infra/scripts/run-lake-ops.sh` and cron. |
| `COACC_LAKE_OPS_RUN_ID_PREFIX` | `prod` | Prefix used when generating materialization run ids. |
| `COACC_LAKE_OPS_CONTINUE_ON_ERROR` | `true` | Keep ingesting later datasets when one source fails. |
| `COACC_LAKE_OPS_ALLOW_FULL` | `false` | Required guard for `lake-ops full`. |
| `COACC_LAKE_OPS_PAGE_SIZE` / `COACC_LAKE_OPS_MAX_PAGES` | unset | Optional per-run overrides forwarded to ingest commands. |
| `COACC_LAKE_OPS_TIMEOUT_SECONDS` | unset | Optional per-run Socrata timeout override. |
| `COACC_LAKE_OPS_MIN_FREE_GB` | unset | Optional disk floor for Phase 7 smoke/full modes. |
| `COACC_LAKE_OPS_SKIP_REALITY` | `false` | Skip the curated-only lake reality snapshot after ops runs. |
| `COACC_LAKE_OPS_CRON_SCHEDULE` | `15 2 * * *` | Host cron schedule installed by `infra/scripts/lake-ops-cron.sh`. |
| `COACC_LAKE_OPS_ALERT_WEBHOOK_URL` | unset | Optional webhook called by `infra/scripts/run-lake-ops.sh` when the ops container exits nonzero. |
| `COACC_LAKE_OPS_ALERT_NAME` | `coacc-lake-ops` | Service name included in optional failure webhook payloads. |
| `COACC_BACKUP_DIR` | `./backups` for local scripts | Destination for `infra/scripts/backup-lake.sh` archives. |
| `COACC_BACKUP_RETENTION_DAYS` | `30` | Retention window for `coacc_lake_*.tar.gz` archives. |
| `COACC_BACKUP_VERIFY_CONTRACTS` | `true` | When true, backup verification and restore run the lake `check` flow against the extracted lake. |
| `COACC_LAKE_BACKUP_CRON_SCHEDULE` | `45 3 * * *` | Host cron schedule installed by `infra/scripts/lake-backup-cron.sh`. |
| `COACC_RESTORE_ROOT` | unset | Optional restore target used by `infra/scripts/restore-lake-backup.sh` when not passed as an argument. |
| `COACC_RESTORE_OVERWRITE` | `false` | Must be `true` before restore scripts replace lake/config/docs/etl subdirectories in a non-empty target. |
| `COACC_PROD_SMOKE_BASE_URL` | `https://$DOMAIN` | Optional target URL for `scripts/production_smoke.py` and deploy smoke. |
| `COACC_PROD_SMOKE_WAIT_TIMEOUT` | `120` | Seconds the production smoke waits for `/ready`. |
| `COACC_PROD_SMOKE_INSECURE` | `true` in deploy script | Skip TLS certificate verification for production smoke. |
| `COACC_PROD_SMOKE_SKIP` | `false` | Skip production smoke and fall back to `/health` only. |
| `COACC_PROD_SMOKE_SKIP_FRONTEND` | `false` | Skip frontend root HTML smoke. |
| `COACC_PROD_SMOKE_REQUIRE_OPS` | `true` | Require healthy `/api/v1/meta/operations` in production smoke. |
| `COACC_PROD_SMOKE_REQUIRE_MATERIALIZED_ONLY` | `true` | Require the signal list to expose only materialized signals in production smoke. |

## External APIs

| Variable | Purpose |
|---|---|
| `SOCRATA_APP_TOKEN` | Optional Socrata app token for higher-rate datos.gov.co requests. |
| `SOCRATA_KEY_ID` / `SOCRATA_KEY_SECRET` | Optional Socrata credentials for authenticated requests. |
| `GEMINI_API_KEY` | Optional Gemini key for source qualification LLM review. |
| `ANTHROPIC_API_KEY` | Optional Anthropic key for source qualification LLM review. |
| `OPENAI_API_KEY` | Optional OpenAI key for source qualification LLM review. |

## App Runtime

| Variable | Default | Purpose |
|---|---:|---|
| `NEO4J_REQUIRED` | `false` | When `true`, API startup fails if Neo4j is unavailable. When `false`, lake-backed health, meta, signal, search, entity, context, baseline, and public graph routes can still run; legacy Cypher-only routes remain graph-dependent. |
| `API_PORT` | `8000` | Host port for the API service in dev compose and the local Vite proxy target. |
| `FRONTEND_PORT` | `3000` | Host port for the frontend service in dev compose. Use `3100` when another local dev server already owns `3000`. |
| `NEO4J_HTTP_PORT` / `NEO4J_BOLT_PORT` | `7474` / `7687` | Host ports for the dev Neo4j browser and Bolt listener. |
| `VITE_API_URL` | `http://localhost:8000` in `.env.example` | Optional Vite dev/build-time API base override for `make frontend`. The compose frontend does not pass it at runtime and defaults to same-origin `/api`. |
| `VITE_ALLOW_FIXTURES` | `true` in `.env.example`, unset/false in production | Controls Atlas demo fixture fallback. Set to `false` or leave unset in production-safe builds so pages show live API data or empty states instead of demo records. |
| `COACC_CONFIG_DIR` | auto-discovered | Optional API override for mounted `config/` files such as `signal_registry.yml` and signal SQL. |
| `COACC_DATASET_CATALOG_DIR` | auto-discovered | Optional API override for mounted `docs/datasets/` catalog CSVs. |
| `COACC_DATASET_CONTRACT_DIR` | auto-discovered | Optional API override for mounted `etl/datasets/` YAML contracts. |
| `COACC_REQUIRE_LAKE_ASSETS` | `false` | When `true`, `/ready` fails if required lake, registry, catalog, contract, or materialized signal-run assets are missing. Production compose sets this to `true`. |
| `COACC_READY_MIN_LOADED_SOURCES` | `0` | Optional minimum loaded source count for `/ready`. Production compose defaults this to `1` so an empty mounted lake is not considered ready. |
| `COACC_READY_MAX_LAKE_OPS_AGE_HOURS` | `0` locally, `48` in production compose | When greater than zero, `/ready` requires `lake/meta/operations/latest.json` to show a successful lake ops run newer than this age. |
| `COACC_SIGNALS_REQUIRE_MATERIALIZED` | `false` locally, `true` in production compose | When `true`, public `/api/v1/signals/` responses hide registered-only signals and expose only signals backed by the latest lake/graph materialization. |

`/health` is a liveness endpoint. `/ready` is the production readiness
endpoint; it reports config, catalog, contract, lake directory, latest
materialized signal run, materialized parquet, recent lake ops status when
configured, and Neo4j availability checks.
In `dev`, `test`, and `local` environments missing lake assets are warnings
unless `COACC_REQUIRE_LAKE_ASSETS=true`. In production, readiness is strict.

Production compose mounts host paths into stable in-container paths:

| Host variable | Container path | Purpose |
|---|---|---|
| `COACC_HOST_LAKE_ROOT` | `/app/lake` | Durable raw, curated, and metadata lake storage mounted read-only into the API. |
| `COACC_HOST_CONFIG_DIR` | `/app/config` | Runtime config files. |
| `COACC_HOST_DATASET_CATALOG_DIR` | `/app/docs/datasets` | Signed and proven dataset catalogs. |
| `COACC_HOST_DATASET_CONTRACT_DIR` | `/app/etl/datasets` | Dataset YAML contracts. |

The production `lake-ops` service uses the same host variables, but mounts the
lake read/write at `/workspace/lake` so ingestion, curation, and signal
materialization can update durable parquet outputs. See
`docs/runbooks/lake_ops.md`.

Lake backup and restore procedures are documented in
`docs/runbooks/backup_restore.md`.

Production deployment smoke is documented in `docs/runbooks/deploy.md`.

Run the local backend readiness gate before demo work or after backend/runtime
changes:

```bash
make backend-ready
```

For a narrower API-only smoke:

```bash
make api-smoke
```

The smoke starts Uvicorn with `NEO4J_REQUIRED=false`, points it at the repo
lake, and verifies `/health`, `/api/v1/meta/health`, `/api/v1/meta/stats`,
`/api/v1/public/meta`, `/api/v1/signals/`, `/api/v1/search`,
`/api/v1/entity/{entity_id}`, `/api/v1/entity/{entity_id}/signals`,
`/api/v1/patterns/{entity_id}`,
`/api/v1/public/patterns/company/{entity_id}`,
`/api/v1/entity/{entity_id}/evidence-trail`,
`/api/v1/entity/{entity_id}/exposure`,
`/api/v1/entity/{entity_id}/timeline`, `/api/v1/graph/{entity_id}`,
`/api/v1/public/graph/company/{entity_id}`,
`/api/v1/baseline/{entity_id}`, `/api/v1/cases/`,
`/api/v1/cases/{case_id}`, and `/api/v1/agent/query`.

The remaining API, frontend, and Neo4j variables are listed in `.env.example`.
Keep production secrets out of this repository and rotate any value that is
ever printed or committed accidentally.
