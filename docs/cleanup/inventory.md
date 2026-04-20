# Phase 0 Inventory

**Generated:** 2026-04-20
**Scope:** read-only scan of `/Users/ceron/Developer/co-acc`
**Method:** `du -sh`, `git log -1`, `grep` for references in Makefile, `.github/workflows/*`, scripts, Python source.
**Paired file:** `docs/cleanup/cleanup_review.csv` (row-level verdict list)

---

## Top-level size map

| Path | Size | Verdict | Notes |
|---|---|---|---|
| `lake/` | **2.0G** | **kill** | All `source=secop_integrado/year=2026`. Bad data from broken normalizer. Rebuild Phase 3. |
| `etl/` | 540M | keep | Source package; `.venv`/`.mypy_cache` inflate size. |
| `api/` | 422M | keep | Source package; `.venv` inflates size. |
| `frontend/` | 364M | keep | `node_modules` + `dist/` inflate size. |
| `govt data roadmap/` | 34M | decide | 25 PDFs. Superseded by `colombia_open_data_audit.json`. Keep external backup, drop from repo. |
| `data/` | 19M | kill | Only `paco_sanctions` (19M, demo) + `official_case_bulletins` (12K). No active consumer once ingester rewrite lands. |
| `.playwright-cli/` | 6.4M | decide | Playwright cache — likely node-side. Check frontend dev workflow. |
| `scripts/` | 4.0M | partial | 3MB is `roadmap_links.*` + `materialize_real_results.py` (196K). See scripts table below. |
| `docs/` | 620K | partial | Dated investigation MDs = kill; competition + brand + legal + release = keep. |
| `audit-results/` | 312K | **kill** | Old investigation dumps. `materialize-results` target writes here; rewrite target in Phase 3. |
| `config/`, `infra/`, `.github/`, root dotfiles | <200K | keep | Load-bearing. |
| `logs/`, `output/`, `.conductor/` | 0B | kill | Empty. |

---

## Kill category 1 — `audit-results/` (old investigation dumps)

| Path | Notes |
|---|---|
| `audit-results/bootstrap-all/` | 6 dated snapshots + `latest/`; produced by `run_bootstrap_all.py`. |
| `audit-results/data-population/` | `latest/` only. |
| `audit-results/investigations/` | empty. |
| `audit-results/materialized-results/` | empty; `make materialize-results` writes here. |
| `audit-results/signal-source-compatibility/` | `latest/` only. |
| `audit-results/source-probes/` | `latest/` only. |

Verdict: kill entire `audit-results/`. Producers (`run_bootstrap_all.py`, `materialize_real_results.py`, `scan_real_pattern_coverage.py`) are themselves kill candidates (see Scripts table). If Phase 3 rebuild keeps any of these, recreate the dir at that point.

---

## Kill category 2 — dated investigation docs under `docs/`

All from March 2026, last touched 2026-03-15 → 2026-03-30. None referenced by live code (competition docs, code, Makefile). Content: one-off research reports superseded by current competition plan.

| File | Size | Last commit |
|---|---|---|
| `br_acc_colombia_followup_2026-03-30.md` | 8K | 2026-03-30 |
| `candidate_dataset_probe_2026-03-25.md` | 24K | 2026-03-28 |
| `candidate_dataset_shortlist_2026-03-25.md` | 4K | 2026-03-28 |
| `colombia_connection_opportunities_2026-03-14.md` | 16K | 2026-03-16 |
| `colombia_corruption_functionality_report_2026-03-13.md` | 12K | 2026-03-16 |
| `colombia_corruption_functionality_report_2026-03-14.md` | 4K | 2026-03-15 |
| `colombia_corruption_practices_research_2026-03-19.md` | 8K | 2026-03-28 |
| `current_case_coverage_review_2026-03-26.md` | 16K | 2026-03-28 |
| `datos_abiertos_research_2026-03-18.md` | 16K | 2026-03-28 |
| `education_credential_probe_2026-03-27.md` | 4K | 2026-03-28 |
| `live_pattern_findings_2026-03-30.md` | 8K | 2026-03-30 |
| `payroll_pensions_benchmarks_2026-03-21.md` | 12K | 2026-03-28 |
| `periodismo_datos_corrupcion_metodos_2026-03-30.md` | 20K | 2026-03-30 |
| `real_case_validation_2026-03-19.md` | 4K | 2026-03-28 |
| `real_pattern_coverage_2026-03-21.md` | 20K | 2026-03-28 |
| `repo_implementation_gap_audit_2026-03-27.md` | 8K | 2026-03-28 |
| `san_jose_network_research_2026-03-22.md` | 16K | 2026-03-28 |
| `secop_archive_probe_2026-03-27.md` | 4K | 2026-03-28 |
| `transmilenio_information_request_fields_2026-03-28.csv` | 8K | 2026-03-28 |
| `transmilenio_information_request_package_2026-03-28.md` | 8K | 2026-03-28 |
| `transmilenio_microdesfalco_research_2026-03-28.md` | 8K | 2026-03-28 |
| `ungrd_drilldown_2026-03-27.md` | 12K | 2026-03-28 |

Verdict: kill all. Archive via `git log` if needed later.

---

## Kill category 3 — discovery + one-off investigation scripts

None of these are on the competition critical path. `materialize_real_results.py` is referenced by `make materialize-results` but the target will be rewritten in Phase 3.

| Script | Size | Last commit | Referenced by | Notes |
|---|---|---|---|---|
| `deep_discovery.py` | 4K | 2026-03-16 | — | Dataset discovery spike. Dead. |
| `deep_discovery2.py` | 4K | 2026-03-16 | — | Dataset discovery spike v2. Dead. |
| `hidden_discovery.py` | 4K | 2026-03-16 | — | Dead. |
| `semantic_discovery.py` | 4K | 2026-03-16 | — | Dead. |
| `probe_colombia_candidate_datasets.py` | 4K | 2026-03-28 | `make probe-colombia-candidates` | Makefile target, but target is dead-end spike. Kill. |
| `collect_san_jose_public_osint.py` | 12K | 2026-03-28 | — | One-off research. Kill. |
| `collect_transmilenio_finance_public_evidence.py` | 12K | 2026-03-28 | — | One-off. Kill. |
| `collect_ungrd_public_evidence.py` | 12K | 2026-03-28 | — | One-off. Kill. |
| `parse_transmilenio_finance_public_evidence.py` | 28K | 2026-03-28 | — | One-off. Kill. |
| `parse_ungrd_public_evidence.py` | 12K | 2026-03-28 | — | One-off. Kill. |
| `generate_demo_dataset.py` | 8K | 2026-03-28 | — | Synthetic demo. Replaced by real-data plan. Kill. |
| `materialize_real_results.py` | **196K** | 2026-03-28 | `make materialize-results` | Huge hand-rolled materializer. Target will be rewritten Phase 3. Kill. |
| `scan_real_pattern_coverage.py` | 8K | 2026-03-28 | `make scan-real-pattern-coverage` | Pattern-coverage probe. Obsolete vs config-driven ingester. Kill. |
| `run_bootstrap_all.py` | 20K | 2026-03-15 | `bootstrap-all-audit.yml` | Duplicated by `bootstrap_public_demo.sh`. Bootstrap concept survives but this implementation is dead. Kill (+ drop workflow). |
| `bootstrap_all/` dir | 72K | 2026-03-15 | `run_bootstrap_all.py` | Adapters used by kill candidate above. Kill together. |
| `sync_colombia_portal_registry.py` | 16K | 2026-03-15 | `make sync-colombia-registry` | Writes `source_registry_co_v1.csv` — but canonical registry moves to `colombia_open_data_audit.json` + `docs/datasets/catalog.csv` in Phase 2. Kill. |
| `check_compliance_pack.py` | 4.7K | 2026-03-12 | unclear | Governance leftover. Decide after grep for CI refs. |
| `check_open_core_boundary.py` | 2.1K | 2026-03-15 | unclear | Open-core gate. Decide. |
| `check_pipeline_contracts.py` | 2.2K | 2026-03-12 | unclear | Decide. |
| `check_pipeline_inputs.py` | 3.2K | 2026-03-12 | unclear | Decide. |
| `check_public_claims.py` | 6.2K | 2026-03-15 | unclear | Decide. |
| `check_public_privacy.py` | 2.1K | 2026-03-19 | unclear | Decide. |
| `check_source_urls.py` | 8.4K | 2026-03-12 | `source-url-audit.yml` | Workflow exists; registry will change in Phase 2. Decide. |
| `claude_merge_gate.py` | 7.7K | 2026-03-12 | `claude-pr-governor.yml` | Workflow exists. Decide whether governance layer stays. |
| `prompt_injection_scan.py` | 5.9K | 2026-03-12 | `security.yml` | Workflow exists. Likely keep (security). |
| `generate_reference_metrics.py` | 2.5K | 2026-03-12 | — | Writes `docs/reference_metrics.md`. Decide. |
| `generate_pipeline_status.py` | 6.4K | 2026-04-17 | `make generate-pipeline-status` | Writes `docs/pipeline_status.md`. Obsolete in Phase 3. Kill. |
| `generate_data_sources_summary.py` | 14K | 2026-04-17 | `make generate-source-summary` | Writes `docs/data-sources.md`. Obsolete in Phase 3. Kill. |
| `bootstrap_all_public.sh` | 339B | 2026-03-12 | `make demo-national` | Wrapper; kill with bootstrap stack. |
| `bootstrap_public_demo.sh` | 3.2K | 2026-03-15 | — | Orphaned wrapper. Kill. |
| `ingest_colombia_all.sh` | 3.2K | 2026-03-13 | — | Orphaned. Kill. |

### Keepers in `scripts/`

| Script | Why |
|---|---|
| `init_env.sh` | `make setup-env`. Keep. |
| `capture_fixture.py` | Phase 0 reality tests depend on it. Keep. |
| `extract_roadmap_links.py` | Already ran; output feeds dataset triage. Keep but archive output. |
| `lake_reality.py` | Phase 4 baseline. Keep (will be rewritten). |
| `ci/python_quality.sh`, `ci/frontend_quality.sh` | Invoked by `ci.yml`. Keep. |

### Scripts output kept vs killed

| File | Size | Verdict |
|---|---|---|
| `scripts/roadmap_links.csv` | 1.2M | decide — move out of `scripts/` into `docs/datasets/` |
| `scripts/roadmap_links.json` | 1.8M | decide — same |
| `scripts/roadmap_socrata_datasets.csv` | 72K | decide — superseded by audit JSON; kill |

---

## Kill category 4 — demo/stub data

| Path | Size | Verdict | Notes |
|---|---|---|---|
| `data/paco_sanctions/_antecedentes_siri_sanciones_extract/` | part of 19M | kill | Fetched 2026-03-16; no live consumer after ingester rewrite. |
| `data/paco_sanctions/_multas_secop_extract/` | part of 19M | kill | Same. |
| `data/official_case_bulletins/official_case_bulletins.json` | 12K | kill | Unused. |
| `output/playwright/` | 0B | kill | Empty dir. |
| `logs/` | 0B | kill | Empty dir. |
| `.conductor/` | 0B | kill | Empty dir. |

---

## Kill category 5 — broken lake

| Path | Size | Verdict | Notes |
|---|---|---|---|
| `lake/raw/source=secop_integrado/year=2026/` | 2.0G | **kill** | Produced by pipeline with normalizer bug; data content is corrupt or schema-mismatched. |
| `lake/meta/reality_report.csv` | part of 20K | kill | Stale report. |
| `lake/meta/watermark_events.parquet` | part of 20K | kill | Watermark state for broken pipeline. |
| `lake/meta/watermarks.parquet` | part of 20K | kill | Same. |
| `lake/curated/` | 0B | kill | Empty. |

After Phase 1: `lake/` is empty, Phase 3 rebuilds.

---

## Keep but rewrite (do not delete, will change in Phase 3)

| Path | Why |
|---|---|
| `Makefile` | 474 lines, ~120 targets. Most targets die with kill list. Rewrite in Phase 3 around config-driven ingester. |
| `etl/src/coacc_etl/pipelines/secop_integrado.py` | Broken normalizer. Rewrite in Phase 3 as generic ingester. |
| `etl/src/coacc_etl/streaming.py` | Watermark + partition bugs. Rewrite Phase 3. |
| `docs/source_registry_co_v1.csv` | Load-bearing for API service `source_registry.py`. Migrate to Phase 2 catalog before killing. |
| `docs/pipeline_status.md` | Auto-generated; kill with its producer. |
| `docs/data-sources.md` | Auto-generated; kill with its producer. |
| `docs/reference_metrics.md` | Check producer. |

---

## Unambiguous keepers (not touched in Phase 1)

- `api/` source + tests (repoint to new lake in Phase 5)
- `etl/src/coacc_etl/` except pipelines — keep `lakehouse/`, `streaming.py` (rewrite), shared helpers
- `frontend/` (repoint in Phase 5)
- `infra/`, `config/`, `.github/workflows/{ci.yml,deploy.yml,docker-ci.yml,publish-release.yml,release-drafter.yml,release-label-policy.yml,security.yml,auto-label.yml}`
- Root legal/governance MDs: `LICENSE`, `PRIVACY.md`, `SECURITY.md`, `ETHICS.md`, `TERMS.md`, `DISCLAIMER.md`, `ABUSE_RESPONSE.md`, `LGPD.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `README.md`
- `docs/competition/`, `docs/legal/`, `docs/brand/`, `docs/ci/`, `docs/release/`, `docs/cleanup/`, `docs/reality/`
- `Dockerfile`, `docker-compose.yml`, `.env*`, `.gitignore`, `.gitleaksignore`

---

## Open items requiring user decision before Phase 1 commits

1. **Governance scripts** (`check_compliance_pack`, `check_open_core_boundary`, `check_pipeline_*`, `check_public_*`, `check_source_urls`, `claude_merge_gate`, `prompt_injection_scan`): keep layer as-is, or simplify alongside rebuild?
2. **`source_registry_co_v1.csv`**: migrate rows into `docs/datasets/catalog.csv` (Phase 2), then kill? Or keep as legacy compat?
3. **`govt data roadmap/` PDFs (34M)**: drop from repo (already superseded by audit JSON)? Keep external backup if user wants.
4. **`materialize_real_results.py` (196K)**: confirm kill — big file, one-shot investigation artifact, not load-bearing on competition path.
5. **CI workflows `bootstrap-all-audit.yml`, `source-url-audit.yml`, `claude-pr-governor.yml`**: disable + delete alongside their scripts?
6. **`roadmap_links.*` (3M in `scripts/`)**: move to `docs/datasets/` and then kill `roadmap_socrata_datasets.csv` (superseded)?
