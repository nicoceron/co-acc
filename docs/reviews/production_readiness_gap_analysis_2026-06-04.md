# Production Readiness Gap Analysis

Date: 2026-06-04

This review answers two questions:

- What is actually complete today?
- What is still missing before this project can be called production ready?

## Executive State

The project has a credible lake-first foundation, working local ETL/API/frontend checks, and a live frontend path that can display canonical lake-backed signals. It is not production complete yet.

The main gap is not that all source datasets must be loaded into a developer laptop. The main gap is that the production data pipeline, lake storage, signal materializers, and frontend live-data surfaces are only partially implemented.

Update later on 2026-06-04: the first production config gap has been partially addressed. The API now has a strict `/ready` endpoint, production compose mounts lake/config/catalog/contract paths into the API container, and production compose uses `/ready` for the API healthcheck. A strict API runtime check against the current lake returned `/ready` status `ready` with no failed checks.

Second update on 2026-06-04: production frontend fixture leakage is partially addressed. Atlas now has an explicit `VITE_ALLOW_FIXTURES` gate. In production-safe mode, overview/search/cases/signals/sectors/entity surfaces no longer silently fall back to demo fixture data; they use live API data, derived live aggregates, or empty states. The mobile signal table has also been converted to stacked card rows.

Third update on 2026-06-04: entity detail is now live-wired. The entity page loads detail, materialized signals, evidence trail, and timeline from API endpoints, with partial states when optional enrichment endpoints are absent. It handles both search IDs such as `company:...` and signal-derived prefixed IDs that need numeric fallback. This does not make the project production complete because durable lake storage, scheduled ETL/materialization, full signal coverage, true geo/sector APIs, and an end-to-end visual deployment smoke remain open.

Fourth update on 2026-06-04: launch signal scope is now explicit. Signal list items report `materialized` and `materialization_state`, and production compose sets `COACC_SIGNALS_REQUIRE_MATERIALIZED=true` so public signal catalog responses expose only signals backed by materialized lake/graph outputs. At that point this exposed 4 public materialized signals instead of all 33 public registry definitions. This is honest launch scoping, not full pattern completion.

Fifth update on 2026-06-04: production lake operations are now scaffolded. Production compose has an `ops` profile with a `lake-ops` service that mounts the durable lake read/write, while the API keeps the same lake mount read-only. `infra/scripts/run-lake-ops.sh` can run `check`, `curate`, `materialize`, `refresh`, `smoke`, or guarded `full` modes, and `infra/scripts/lake-ops-cron.sh` installs a host cron entry. ETL path resolution now honors mounted config, catalog, and dataset-contract env vars. Each ops run writes `lake/meta/operations/latest.json`; `/api/v1/meta/operations` exposes it, production `/ready` requires a recent successful ops run by default, and the host runner can send an optional failure webhook. This gives production a repeatable batch/materialization entrypoint and a freshness/failure gate, but does not yet prove backups or a real deploy smoke.

Sixth update on 2026-06-04: lake backup and restore are now scripted and documented. `infra/scripts/backup-lake.sh` archives the durable lake plus runtime config, dataset catalogs, and dataset contracts; `verify-lake-backup.sh` extracts and validates an archive; `restore-lake-backup.sh` restores into a clean target by default; and `lake-backup-cron.sh` installs a scheduled host backup. This closes the missing lake backup/restore procedure, but production still needs an actual scheduled backup run, off-host storage, and a restore drill using the production volume.

Seventh update on 2026-06-04: production deployment smoke is now scripted. `scripts/production_smoke.py` verifies `/ready`, `/health`, lake ops status, source stats, public meta, materialized signal listing, a lake-backed case/entity path, and the frontend root. `infra/scripts/deploy.sh` now runs this smoke after `docker compose up -d` by default instead of relying only on `/health`. A local production-mode API run with strict lake assets, recent lake-ops requirement, materialized-only signals, and Neo4j offline passed the smoke for API paths. This makes deploys fail on stale lake ops, registered-only signal leakage, missing cases, or broken frontend routing, but still needs a real host run through Caddy to prove frontend routing and TLS.

Eighth update on 2026-06-04: production-safe frontend visual QA passed locally through headless Chrome DevTools screenshots. Desktop and mobile captures were taken for `/`, `/app`, `/app/signals`, `/app/search`, and `/app/entity/company:8605246546` with `VITE_ALLOW_FIXTURES=false` against the strict local API. The capture metrics showed no page-level horizontal overflow, and the dashboard feed plus mobile source registry were adjusted to avoid clipped dense tables. This verifies local rendering against live API data, but still does not replace a real host smoke through Caddy/TLS.

Ninth update on 2026-06-04: one additional public materializer is now implemented. `procurement_single_bidder_high_value` builds a contract-only partial feature table from high-value direct/special procurement awards, downgrades materialized hit severity to `medium` while optional offers/process evidence is absent, and is included in the default signal materialization run. The local lake run `phase11-local-20260604-single-bidder` materialized 4 public signals and 86,864 total hits. The signal detail frontend route now fetches live API sample hits instead of showing empty fixture-backed detail in production-safe mode.

Tenth update on 2026-06-04: `procurement_short_bidding_window` is now implemented as a batch lake materializer from the loaded SECOP II process source. It flags awarded supplier processes with short open-response windows, low response counts, exact supplier NIT identity, and process evidence URLs. The rule emits `low` materialized severity while separate offer-event evidence is not yet incorporated. The local run `phase11-local-20260604-short-window` materialized 5 public signals, 144,663 total hits, and 202,409 evidence rows.

Eleventh update on 2026-06-04: `procurement_offers_competition_drop` is now implemented as a buyer-level batch materializer from the loaded SECOP II process source. It compares each buyer's most recent process response counts against that buyer's prior baseline and emits evidence links to recent low-response processes. The local run `phase11-local-20260604-competition-drop` materialized 6 public signals, 144,676 total hits, and 202,474 evidence rows.

Twelfth update on 2026-06-04: `procurement_politically_exposed_position_supplier_overlap` is now implemented as a reviewer-only batch materializer from RUES company registry legal representatives, SIGEP sensitive-position records, and SECOP II contract exposure. The local run `phase11-local-20260604-sensitive-position-overlap` materialized 7 signal IDs total, including 6 public signals and 1 reviewer-only signal, with 144,934 total hits and 203,578 evidence rows.

Thirteenth update on 2026-06-04: `procurement_related_companies_shared_officer` is now implemented as a reviewer-only batch materializer from RUES company registry legal representatives and SECOP II contract exposure. It deduplicates RUES company/representative pairs, requires exact company NIT identity, and flags exposed supplier clusters sharing a legal representative. The local run `phase11-local-20260604-shared-officer` materialized 8 signal IDs total, including 6 public signals and 2 reviewer-only signals, with 146,407 total hits and 211,128 evidence rows. Strict production-mode API smoke still exposed only the 6 public materialized signals and returned 404 for unauthenticated access to the new reviewer-only signal.

Fourteenth update on 2026-06-04: `procurement_contract_value_outlier_by_category` is now implemented as a reviewer-only batch materializer from SECOP II contracts. It compares contracts against department/year/sector/modality/type peers, requires at least 100 peers, caps output to the top 3 extreme high-value contracts per peer group, and uses exact contract-key identity. The local run `phase11-local-20260604-contract-outlier` materialized 9 signal IDs total, including 6 public signals and 3 reviewer-only signals, with 148,429 total hits and 213,150 evidence rows. Strict production-mode API smoke still exposed only the 6 public materialized signals and returned 404 for unauthenticated access to the new reviewer-only signal.

Fifteenth update on 2026-06-04: `procurement_buyer_supplier_network_density` is now implemented as a reviewer-only batch materializer from SECOP II contracts. It requires exact supplier NIT identity and flags suppliers with dense repeated buyer relationships across at least 10 buyers, 8 repeated buyer pairs, 55% repeated-contract share, and COP 2B total exposure. The current local run `phase11-local-20260604-network-density` materialized 10 signal IDs total, including 6 public signals and 4 reviewer-only signals, with 148,793 total hits and 214,970 evidence rows. Strict production-mode API smoke still exposed only the 6 public materialized signals and returned 404 for unauthenticated access to the new reviewer-only signal.

Sixteenth update on 2026-06-04: `procurement_cartel_risk_cobidding` is now implemented as a reviewer-only batch materializer from SECOP II offers and process metadata. It deduplicates to one valid non-placeholder company NIT per process/supplier, self-joins only processes with 2-8 valid company suppliers, requires at least 20 shared processes, 5 buyers, 10% overlap against the more active supplier, and 35% overlap against the less active supplier, and emits two company-level hits per flagged pair with SECOP process URL evidence. The current local run `phase11-local-20260604-cobidding` materialized 11 signal IDs total, including 6 public signals and 5 reviewer-only signals, with 149,659 total hits and 219,300 evidence rows.

Seventeenth update on 2026-06-05: `procurement_payment_plan_anomalies` is now implemented as a reviewer-only contract-payment materializer from SECOP II contract fields. It requires exact supplier NIT identity and flags high advance-payment share, paid value exceeding contract value, or invoiced value exceeding contract value, with SECOP process URL evidence. The current local run `phase11-local-20260605-payment-anomalies` materialized 12 signal IDs total, including 6 public signals and 6 reviewer-only signals, with 150,034 total hits and 219,675 evidence rows.

Eighteenth update on 2026-06-05: the dedicated SECOP II suspension-event source `u99c-7mfm` has been ingested through the paged Socrata lake ingester, writing 537,314 raw rows. `procurement_contract_suspensions` is now implemented as a public materializer from deduplicated suspension events joined to exact-NIT SECOP II contracts; it requires at least two distinct suspension dates, COP 100M+ value, and SECOP process URL evidence. The current local run `phase11-local-20260605-suspensions` materialized 13 signal IDs total, including 7 public signals and 6 reviewer-only signals, with 160,494 total hits and 260,784 evidence rows.

Nineteenth update on 2026-06-05: the SECOP II modification-event source `u8cx-r425` has been ingested through the Phase 7 smoke path for a recent 63,000-row slice with coverage pass and watermark `2026-06-03T00:00:00Z`. `procurement_large_modifications` is now implemented as a public materializer from SECOP II modification values joined to exact-NIT SECOP II contracts; it requires COP 100M+ contracts and either COP 100M+ total modification value or at least 50% modification value share, with SECOP process plus modification-event evidence. The local feature table produced 582 rows, and the run `phase11-local-20260605-large-modifications` materialized 14 signal IDs total, including 8 public signals and 6 reviewer-only signals, with 161,074 hits and 264,092 evidence rows.

Twentieth update on 2026-06-05: a local lake backup/verify/restore drill passed against the then-current 10 GB lake. `infra/scripts/backup-lake.sh` wrote `/tmp/coacc-lake-drill/coacc_lake_20260605T010250Z.tar.gz` (9.9 GB), `verify-lake-backup.sh` passed with `PASS 14 curated contract(s)`, and `restore-lake-backup.sh` restored to `/tmp/coacc-restore-drill` with the same contract check. The restored lake contains 14 signal feature tables and the `u8cx-r425` watermark plus 63 raw modification parquet files. This proves the scripts locally, but it is not yet an off-host production-volume restore drill.

Twenty-first update on 2026-06-05: two more required SECOP sources were loaded through the Phase 7 smoke path: `mfmm-jqmq` (`secop_contract_execution`) wrote 9,535 rows with coverage pass and watermark `2026-06-04T00:00:00Z`, and `qmzu-gj57` (`secop_suppliers`) wrote 924 rows with coverage pass and watermark `2026-06-03T00:00:00Z`. `procurement_contract_execution_delay` is now a public materializer from SECOP execution rows joined to exact-NIT SECOP II contract awards; it emits 48 current hits with contract plus execution-item evidence. `procurement_cross_source_identity_inconsistency` is now a reviewer-only materializer from RUES company registry and SECOP supplier registry exact-NIT joins; it emits 50 current hits with company-registry plus supplier-registry evidence. The run `phase11-local-20260605-execution-identity` materialized 16 signal IDs total, including 9 public signals and 7 reviewer-only signals, with 161,172 hits and 264,342 evidence rows.

Twenty-second update on 2026-06-05: the fixed 2019 Cuentas Claras income source `jgra-rz2t` was loaded through a bounded full refresh, writing 188,171 rows with coverage pass and watermark `2025-08-09T00:00:00Z`; 146 far-future voucher-date rows were routed to sentinel partitions without advancing the watermark. `cuentas_claras_donor_supplier_overlap` is now a public materializer from exact company-NIT joins between campaign-finance income records and post-2019 SECOP II supplier exposure. The feature table produced 533 rows, and the run `phase11-local-20260605-cuentas-claras` materialized 17 signal IDs total, including 10 public signals and 7 reviewer-only signals, with 161,705 hits and 266,455 evidence rows.

Twenty-third update on 2026-06-05: the public conflict-disclosure source `gbry-rnq4` was loaded through a full refresh, writing 328,799 rows with coverage pass and watermark `2022-12-13T14:57:31.146000Z`. `procurement_public_servant_conflict_disclosure_overlap` is now a public materializer from exact person-document joins between affirmative conflict disclosures and high aggregate SECOP II person-supplier exposure. The feature table produced 65 rows, and the run `phase11-local-20260605-conflict-disclosures` materialized 18 signal IDs total, including 11 public signals and 7 reviewer-only signals, with 161,770 hits and 266,715 evidence rows.

Twenty-fourth update on 2026-06-05: the SECOP II sanctions source `it5q-hg94` was loaded through a full refresh, writing 538 rows with coverage pass and watermark `2026-05-28T00:00:00Z`; five future-dated rows were routed to sentinel partitions and excluded from public feature output. `pida5_pida27_pida4_chain` is now a public territorial materializer from exact contract-id joins between SECOP Integrado and SECOP II sanctions. The feature table produced 38 territory rows, covering 424 sanction events, 375 contracts, 302 suppliers, and COP 3.87T in joined contract value. The run `phase11-local-20260605-secopsanctions-pida` materialized 19 signal IDs total, including 12 public signals and 7 reviewer-only signals, with 161,808 hits and 266,963 evidence rows.

Twenty-fifth update on 2026-06-05: the official SECOP process-to-BPIN source `d9na-abhe` was loaded through a full refresh, writing 2,450,545 rows with coverage pass. `project_bpin_procurement_overlap` is now a public project materializer from validated numeric BPIN links joined to exact SECOP II contract IDs. The feature table produced 654 project rows: 82 high severity and 572 medium severity, covering COP 473.89T in joined contract value. The run `phase11-local-20260605-bpin-projects` materialized 20 signal IDs total, including 13 public signals and 7 reviewer-only signals, with 162,462 hits and 273,287 evidence rows.

Twenty-sixth update on 2026-06-05: the TVEC item-level purchase source `3hdv-smhz` was promoted to an ingest-ready snapshot source and loaded through a full refresh, writing 1,400,581 rows with coverage pass in snapshot `20260605T024336Z`. `tvec_multi_entity_capture` is now a public materializer from exact supplier-NIT TVEC order aggregation joined to exact SECOP II supplier awards. The feature table produced 87 supplier rows: 53 high severity and 34 medium severity, covering COP 12.09T in TVEC item value and COP 17.21T in joined SECOP II contract value. The run `phase11-local-20260605-tvec-capture` materialized 21 signal IDs total, including 14 public signals and 7 reviewer-only signals, with 162,549 hits and 274,157 evidence rows.

Twenty-seventh update on 2026-06-05: the SGR sources `mzgh-shtp` (`sgr_projects`) and `qkv4-ek54` (`sgr_expense_execution`) were loaded through full refreshes, writing 35,006 project rows and 386,293 expense-execution rows with coverage pass. `project_regalias_execution_procurement_overlap` is now a public project materializer from SGR project metadata, SGR execution amounts, and validated SECOP process-to-BPIN links joined to SECOP II contracts. The feature table produced 41 project rows: 10 high severity and 31 medium severity, covering COP 3.73T in joined procurement value and COP 12.43T in SGR execution exposure. The run `phase11-local-20260605-sgr-projects` materialized 22 signal IDs total, including 15 public signals and 7 reviewer-only signals, with 162,590 hits and 274,689 evidence rows.

Twenty-eighth update on 2026-06-05: `pida_full30_meta` is now a public territory materializer from SECOP Integrado text-category rollups. No additional raw source was loaded for this slice; it uses the already loaded `rpmr-utcd` (`secop_integrado`) source. The feature table produced 93 territory rows: 7 high severity and 86 medium severity, covering 541,240 tagged contracts and COP 12,124,250,151,149,100 in total contract value. The run `phase11-local-20260605-pida-full30` materialized 23 signal IDs total, including 16 public signals and 7 reviewer-only signals, with 162,683 hits and 275,154 evidence rows.

## Dataset Loading Reality

You do not need to load the entire national datasets onto a personal computer for normal operation.

The intended operating model is:

1. Ingestion pulls source data in pages or bounded batches.
2. Raw and curated data are persisted in a lake as local or mounted storage, ideally object storage or a production volume.
3. DuckDB and ETL jobs query Parquet files directly instead of loading all rows into memory.
4. Signal materialization writes compact `signal_hits` and `evidence_bundles` outputs for the API.
5. A developer laptop should usually run smoke slices, fixture checks, or connect to a shared lake.

Current state:

- Socrata ingestion is paged with `$limit`, `$offset`, `$order`, `--page-size`, `--max-pages`, and watermark/full-refresh controls.
- Curated tables are stored as Parquet and queried through DuckDB.
- Some anomaly scoring paths support bounded rows and prediction batch size.
- The current lake materializer supports 23 signal patterns: 16 public and 7 reviewer-only.

So the lake does handle batch-oriented processing, but only for the parts that have actually been implemented. The architecture is right; the coverage is incomplete.

## Current Evidence

| Area | Current evidence | State |
| --- | --- | --- |
| Dataset catalog | `docs/datasets/catalog.signed.csv` has 311 rows; `docs/datasets/catalog.proven.csv` has 148 rows; `etl/datasets` has 149 YAML specs | Catalog counts are inconsistent with stale README claims |
| Ingest-ready datasets | 44 ingest-ready specs total, including 43 Socrata and 1 PACO sanctions custom adapter | Partial |
| Source registry | 45 source entries; 43 implemented; 20 loaded locally; 33 promoted; 11 enrichment-only; 2 discovered but uningested | Partial |
| Curated lake | Key curated tables exist, including procurement awards, companies, buyers, persons, and subject documents | Working for current slice |
| Signal registry | 43 registered signals validate successfully | Registry complete enough to describe intended patterns |
| Materialized signals | 23 signal IDs materialized from current lake; 16 are public and 7 are reviewer-only; production signal catalog can now expose only materialized public signals; remaining registry definitions are marked `registered_only` | Honest launch scope; full pattern coverage still incomplete |
| API smoke | Lake-backed smoke passes with strict readiness, materialized public signals, cases, citations, source stats, and run metadata | Working for current slice |
| Frontend desktop | Production-safe local screenshots show live landing, dashboard, signals, search, entity, and source registry views without page overflow | Working for current slice |
| Frontend mobile | Production-safe local screenshots show stacked signal rows, readable entity detail, dashboard feed cards, and source registry cards without page overflow | Working for current slice |
| Frontend data source | Overview/search/cases/signals/sectors/entity pages now gate demo fixtures behind `VITE_ALLOW_FIXTURES`; entity detail is live-wired to API detail/signals/evidence/timeline | Partially addressed; geo and richer sector APIs remain |
| Prod compose | Production compose now mounts `/app/lake`, `/app/config`, `/app/docs/datasets`, and `/app/etl/datasets`, sets strict lake readiness, healthchecks `/ready`, and deploy smoke is scripted; local production-mode API smoke passed | Partially addressed; needs real host smoke through Caddy/frontend |
| Production lake ops | `lake-ops` Compose profile and host scripts run incremental refresh, smoke, guarded full load, curation, materialization, and checks against a durable mounted lake; latest run status is exposed through lake metadata and `/ready`; optional failure webhook is supported | Scaffolded; needs scheduled run proof |
| Backup/restore | Lake/config/catalog/contract backup, verification, restore, and cron scripts now exist with a runbook; local 10 GB backup/verify/restore drill passed on 2026-06-05 | Scripted and locally drilled; needs off-host storage and production restore drill |

## Reality Check

### Assumption: A laptop must contain every dataset.

- Evidence: Ingestion is paged; lake tables are Parquet; DuckDB queries lake files; CLI exposes page and batch controls.
- Status: Invalid.
- Risk: Over-sizing local requirements slows development and hides the real production storage problem.
- Action: Treat laptops as smoke/test clients. Put full production lake data in shared mounted storage or object storage.

### Assumption: The lake already detects every registered pattern in batches.

- Evidence: `SUPPORTED_SIGNAL_IDS` currently covers only `procurement_single_bidder_high_value`, `procurement_large_modifications`, `procurement_sanctioned_supplier_awarded`, `procurement_supplier_concentration_across_entities`, `procurement_contract_value_outlier_by_category`, `procurement_repeat_awards_same_supplier`, `procurement_buyer_supplier_network_density`, `procurement_cartel_risk_cobidding`, `procurement_payment_plan_anomalies`, `procurement_contract_suspensions`, `procurement_contract_execution_delay`, `procurement_short_bidding_window`, `procurement_offers_competition_drop`, `procurement_public_servant_conflict_disclosure_overlap`, `cuentas_claras_donor_supplier_overlap`, `pida_full30_meta`, `pida5_pida27_pida4_chain`, `project_bpin_procurement_overlap`, `project_regalias_execution_procurement_overlap`, `tvec_multi_entity_capture`, `procurement_politically_exposed_position_supplier_overlap`, `procurement_related_companies_shared_officer`, and `procurement_cross_source_identity_inconsistency`. The API now labels registered-only signals and production compose hides them from public signal listing.
- Status: Invalid for full pattern completion; valid for scoped launch behavior.
- Risk: If operators disable the materialized-only scope without implementing more materializers, the UI/API can imply broader pattern coverage than the backend can currently produce.
- Action: Keep `COACC_SIGNALS_REQUIRE_MATERIALIZED=true` for production until additional materializers are implemented and verified.

### Assumption: If tests pass, the frontend is production live-data ready.

- Evidence: Frontend checks pass, production-safe build with `VITE_ALLOW_FIXTURES=false` passes, API contract smoke passes, entity detail API smoke passed for `company:...` search IDs plus numeric fallback, and local Chrome screenshots verified desktop/mobile rendering against the live API.
- Status: Valid for the current local launch slice; not valid as proof of the real production host.
- Risk: Production users may still see incomplete live surfaces where full entity/geographic/sector context is expected, and Caddy/TLS routing has not been verified on the host.
- Action: Add geo/sector APIs or remove those views from launch scope, then run the scripted deploy smoke through the real host and Caddy/TLS.

### Assumption: Production Docker compose can run the app as-is.

- Evidence: Production compose now sets lake/config/catalog/contract env vars, mounts the corresponding host paths, uses `/ready` to fail when strict lake assets are missing, and includes a separate `lake-ops` profile for server-side batch refresh/materialization.
- Status: Partially valid after the 2026-06-04 update.
- Risk: A production deployment can still fail without a populated durable lake, real secrets, a scheduled ops proof, Caddy/frontend routing proof, and a real-host smoke run.
- Action: Run `infra/scripts/deploy.sh` on the production host with real env, durable lake mount, and materialized signal outputs; record the smoke result.

## P0 Gaps

These block a credible production launch.

| Gap | Why it matters | Required fix |
| --- | --- | --- |
| Production architecture decision | Lake-first with optional Neo4j is now reflected in API readiness, prod compose, and deploy smoke | Run the smoke against the real production host with mounted lake assets |
| Production lake storage | Full datasets should not live on a laptop, but production needs durable shared data | Add object store or persistent volume strategy, permissions, and production restore drill against the backup scripts |
| Signal materialization coverage | 43 signals are registered, but only 23 are materialized from lake data; production now reduces public listing to public materialized signals | Implement remaining materializers before broadening public signal scope |
| Frontend fixture leakage | Production-safe frontend now disables silent fixture fallback via `VITE_ALLOW_FIXTURES=false`; entity detail is live-wired; remaining non-live geography/sector surfaces show empty or derived states | Wire geography/sector views to live APIs or remove them from launch scope |
| Mobile signal table | Signals table has stacked mobile rows and passed local Chrome visual QA against the live API | Re-run through the real production host after deployment |
| Production container config | Compose/env docs now mount required config, dataset contracts, catalogs, and lake paths; deploy smoke now verifies `/ready` and lake-backed routes | Run it against real `DOMAIN`, `JWT_SECRET_KEY`, and durable lake mount |
| Automated ingestion and materialization | A production ops runner now exists, but it has not been run under a real scheduler with alerts | Prove scheduled `lake-ops refresh`, add alerts, and monitor source freshness/materialization counts |

## P1 Gaps

These are needed for a stable public or reviewer-facing beta.

| Gap | Why it matters | Required fix |
| --- | --- | --- |
| Dataset readiness | Only 44 of 149 ETL specs are ingest-ready; only 20 sources are loaded locally | Decide the launch source set and finish contracts/adapters for that set |
| Registry/readme drift | README claims are stale relative to actual catalog and ETL counts | Update docs from generated counts or add a count-check CI gate |
| API readiness semantics | `/ready` now fails closed for missing registry, catalog, contract, lake, materialized signal outputs, and required Neo4j; strict runtime check passed against current lake | Add CI/deploy smoke that asserts `/ready` passes only with production assets mounted |
| CORS/local dev defaults | Vite ports are now included in default API CORS and `.env.example` | Verify browser smoke in local dev and production same-origin mode |
| Security config gates | Example envs include insecure placeholders and production values are not strongly enforced everywhere | Enforce production secrets, secure cookie settings, public/reviewer access rules, and Neo4j requirements if graph remains enabled |
| Observability | Ingest and signal failures need operational visibility | Add metrics/logging/alerts for source freshness, row counts, materialization counts, API errors, and job failures |
| API contract freshness | OpenAPI and frontend expectations need to stay synchronized | Add live contract smoke in CI against the API |

## P2 Gaps

These improve quality and maintainability after the launch blockers are handled.

| Gap | Why it matters | Required fix |
| --- | --- | --- |
| Anomaly model governance | Current model paths exist, but production evaluation and stale-model policy need hard gates | Add evaluation thresholds, model cards, drift checks, and reviewer workflow |
| Narrator operations | LLM-backed summaries need strict cost, privacy, and fallback controls | Add provider config checks, timeout/cost limits, and deterministic fallbacks |
| Backups and disaster recovery | Lake backup/verify/restore scripts now exist, but no production restore drill has been recorded | Run a restore drill from off-host storage and document recovery time/objective |
| Release workflow | Local checks pass, but release gates are not yet tied to image promotion | Add CI/CD pipeline for lint, tests, build, contract checks, image push, and deployment smoke |

## Immediate Implementation Order

1. Run `scripts/production_smoke.py` through `infra/scripts/deploy.sh` on the real host with mounted durable lake data and real secrets.
2. Run browser-based desktop/mobile QA through the real production host and Caddy/TLS.
3. Prove scheduled `lake-ops refresh` on the production host and add freshness/job-failure alerts.
4. Run a production lake backup to off-host storage and complete a restore drill.
5. Add live geography/sector APIs or remove those visual surfaces from launch scope.
6. Implement the next batch of signal materializers before broadening `COACC_SIGNALS_REQUIRE_MATERIALIZED`.

## Completion Definition

This project should be considered production ready only when:

- A clean production deploy can start from documented env vars and mounted durable lake storage.
- Ingestion and materialization run on a schedule without requiring a developer laptop.
- The public registry, API, and frontend only expose signals that are actually supported by materialized data.
- Production frontend pages do not silently mix fixture/demo data with live data.
- Mobile and desktop views are verified against the live API.
- Health/readiness checks fail on missing critical data/config instead of serving misleading partial output.
- Backup/restore, observability, security config, and release gates are documented and tested.
