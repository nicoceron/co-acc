# co/acc Cleanup + Rebuild Plan

**Status:** draft — awaiting Phase 0 kickoff
**Owner:** solo dev (user)
**Canonical dataset audit:** `/Users/ceron/Developer/Projects/co-acc/colombia_open_data_audit.json` (550 IDs audited → 285 valid, 208 dead, 57 forbidden)
**Related docs:**
- `docs/competition/datos_al_ecosistema_2026.md` — competition rules + rubric
- `docs/competition/program_plan.md` — 5-track execution plan
- `docs/cleanup/inventory.md` — produced by Phase 0 (does not yet exist)
- `docs/cleanup/cleanup_review.csv` — produced by Phase 0 (does not yet exist)
- `docs/datasets/catalog.csv` — produced by Phase 2 (does not yet exist)

Check items off as completed. Each phase has a **Gate** — do not proceed past it until user signs off.

---

## Guiding principles

1. **Nothing deletes without proof it is not load-bearing.** `git log --all --oneline -- <path>` + grep-for-references before kill.
2. **Sanity checks at every boundary.** Dataset selection → download → lake → normalize → curate → model → API. Any regression = red build, not a warning.
3. **Config-driven, not pipeline-driven.** One ingester reading per-dataset YAML, not one hand-rolled Python file per source.
4. **Reproducible from empty.** Monthly macro loop replays from zero. If that breaks, the project is not competition-ready.
5. **Competition deadline is hard.** Finals first week of August 2026. Any scope expansion must be weighed against that date.

---

## Phase 0 — Inventory + triage (read-only, no deletions)

**Goal:** enumerate everything in the repo; produce delete candidates with evidence.

- [x] **0.1** Walk repo. Produce `docs/cleanup/inventory.md` with every top-level dir and first-level subdir, size, last-modified timestamp, one-line purpose guess, verdict {keep / kill / decide}.
- [x] **0.2** For each file in `scripts/` (~35 files): one-line purpose, grep for references in `Makefile`, `pyproject.toml`, `.github/`, `docker-compose*.yml`, other scripts. Dead scripts → kill list.
- [x] **0.3** For each `docs/*.md` investigation report (~30 dated files): note whether referenced by live code, Makefile, or competition plan. Unreferenced → archive or delete.
- [x] **0.4** For each `audit-results/*` subdir: dated investigation dump; default verdict kill unless referenced.
- [x] **0.5** For each `data/*` subdir and `output/*` subdir: note producer + consumer; if demo-only and nothing reads it now, kill.
- [x] **0.6** Produce `docs/cleanup/cleanup_review.csv` with columns: `path, category, size, last_commit_sha, last_commit_date, referenced_by, verdict, notes`.

**Sanity check 0:** every row in `cleanup_review.csv` with `verdict=kill` must have `referenced_by` empty (no grep hits) AND `last_commit_date` older than 30 days (no active work).

**Gate 0:** user approves `cleanup_review.csv` row-by-row or bulk per category. Save signed-off CSV as `docs/cleanup/cleanup_review.signed.csv`.

**✅ Gate 0 signed 2026-04-20.** Decisions recorded:
- Q1 Governance: **simplify** — keep `check_compliance_pack.py`, `check_open_core_boundary.py`, `check_public_privacy.py` (all invoked by `security.yml`). Kill the rest.
- Q2 `source_registry_co_v1.csv`: **migrate-then-kill** in Phase 2 (rows → `docs/datasets/catalog.csv`).
- Q3 `govt data roadmap/` PDFs: **keep on disk, add to `.gitignore`**.
- Q4 `materialize_real_results.py`: **kill**.
- Q5 CI workflows `bootstrap-all-audit.yml`, `source-url-audit.yml`, `claude-pr-governor.yml`: **delete** (plus their scripts).
- Q6 `roadmap_links.*` + `roadmap_socrata_datasets.csv` + `extract_roadmap_links.py`: **kill** — canonical source is now `/Users/ceron/Developer/Projects/co-acc/colombia_open_data_audit.json`.

Signed list: `docs/cleanup/cleanup_review.signed.csv`.

---

## Phase 1 — Delete cruft (only per approved list)

**Goal:** remove approved rows from Phase 0. One commit per category for reversibility.

Source of truth for Phase 1: `docs/cleanup/cleanup_review.signed.csv`.

- [ ] **1.1** `audit-results/` — entire dir.
- [ ] **1.2** Dated investigation docs (22 files) — see signed CSV rows `doc-investigation`.
- [ ] **1.3** Discovery + one-off scripts: `deep_discovery*`, `hidden_discovery`, `semantic_discovery`, `probe_colombia_*`, `collect_*_public_*`, `parse_*_public_*`, `generate_demo_dataset`, `materialize_real_results`, `scan_real_pattern_coverage`, `run_bootstrap_all` + `bootstrap_all/`, `bootstrap_all_public.sh`, `bootstrap_public_demo.sh`, `ingest_colombia_all.sh`, `sync_colombia_portal_registry`, `generate_pipeline_status`, `generate_data_sources_summary`.
- [ ] **1.4** Governance simplification — kill `check_pipeline_contracts`, `check_pipeline_inputs`, `check_public_claims`, `check_source_urls`, `claude_merge_gate`, `prompt_injection_scan`, `generate_reference_metrics`. Keep `check_compliance_pack`, `check_open_core_boundary`, `check_public_privacy`.
- [ ] **1.5** CI workflows — delete `bootstrap-all-audit.yml`, `source-url-audit.yml`, `claude-pr-governor.yml`. Verify `security.yml` still runs on remaining 3 governance scripts.
- [ ] **1.6** Demo payloads: `data/paco_sanctions/`, `data/official_case_bulletins/`, `output/playwright/`, `output/`, `logs/`, `.conductor/`.
- [ ] **1.7** Nuke lake: `lake/raw/`, `lake/curated/`, `lake/meta/`. Rebuild empty skeleton (`lake/raw/`, `lake/curated/`, `lake/meta/`).
- [ ] **1.8** Roadmap artifacts: kill `scripts/extract_roadmap_links.py`, `scripts/roadmap_links.csv`, `scripts/roadmap_links.json`, `scripts/roadmap_socrata_datasets.csv`. Copy canonical audit JSON into repo at `docs/datasets/colombia_open_data_audit.json`.
- [ ] **1.9** `.gitignore`: add `govt data roadmap/` (keep PDFs on disk, stop tracking), add `lake/raw/`, `lake/curated/`, `lake/meta/*.parquet`, `lake/meta/*.csv` if not already.
- [ ] **1.10** Auto-gen docs: delete `docs/pipeline_status.md`, `docs/data-sources.md`, `docs/reference_metrics.md`, `docs/ai-pr-governor.md`.
- [ ] **1.11** Makefile surgery: delete `make` targets whose script is gone (`materialize-results`, `scan-real-pattern-coverage`, `sync-colombia-registry`, `generate-pipeline-status`, `generate-source-summary`, `probe-colombia-candidates`, `demo-national`, `demo-bogota`, `demo-synthetic`, `build-watchlist-snapshots` if not used). Keep `setup-env`, `dev`, `stop`, `api`, `etl`, `frontend`, `lake-*`, `test*`, `lint`, `type-check`, `clean-data`.
- [ ] **1.12** Decide rows from signed CSV: `skills-lock.json`, `.playwright-cli/`, `docs/public_scope.md`. Investigate origin; resolve in a follow-up micro-commit.
- [ ] **1.13** `docs/competition/datos_al_ecosistema_2026.md` §12 decision log: append Phase 1 commit SHAs.

### Phase 1 commit order (one per bullet)

1. `chore(cleanup): purge audit-results/` → 1.1
2. `chore(cleanup): remove dated investigation docs` → 1.2
3. `chore(cleanup): remove discovery + one-off scripts` → 1.3
4. `chore(cleanup): simplify governance scripts` → 1.4
5. `chore(cleanup): delete unused CI workflows` → 1.5
6. `chore(cleanup): drop demo data + empty dirs` → 1.6
7. `chore(cleanup): nuke broken lake, reset skeleton` → 1.7
8. `chore(cleanup): replace roadmap artifacts with canonical audit JSON` → 1.8 + copy audit JSON
9. `chore(cleanup): gitignore roadmap PDFs and lake output` → 1.9
10. `chore(cleanup): remove auto-generated docs` → 1.10
11. `chore(cleanup): trim Makefile to live targets` → 1.11
12. `docs(competition): log Phase 1 cleanup commits` → 1.13

Commits 1.12 (decide rows) happen inline as micro-commits after investigation.

**Sanity check 1 (per commit):** run `uv run --directory etl pytest` and `uv run --directory api pytest`. Green ⇒ proceed. Red ⇒ `git revert` and reopen the row in the signed CSV.

**Sanity check 1 (phase end):**
- `git status` clean.
- `ls audit-results data/paco_sanctions data/official_case_bulletins lake/raw output/playwright logs` → all should fail (dirs gone).
- `wc -l Makefile` → target count dropped (spot check).
- `make test` green across api + etl + frontend.
- `du -sh .` → expect ≈3.3G reduction (2G lake + 34M roadmap-gitignore-no-actual-delete + ~1.2G misc if caches touched; real floor is ≥2G lake).

**Gate 1:** all sanity checks pass. Commit message pattern: `chore(cleanup): purge <category>`. Proceed to Phase 2.

---

## Phase 2 — Dataset sanity BEFORE download

**Goal:** turn 285 "alive" IDs into a curated Tier A ingest set (~15–25 datasets) with evidence.

- [ ] **2.1** Write `scripts/dataset_triage.py`. Input: `colombia_open_data_audit.json`. For each of 285 valid IDs, hit Socrata metadata API (`/api/views/{id}.json`) + count query (`/resource/{id}.json?$select=count(*)`). Extract: `rowsUpdatedAt`, `viewCount`, `downloadCount`, `updateFrequency`, `category`, `owner`, `columns[*].{name,fieldName,dataTypeName}`, row count.
- [ ] **2.2** Classify into tiers:
  - **Tier A (ingest now):** anticorruption-relevant (contracts, sanctions, budget execution, procurement, public roles, beneficiaries, subsidies, SECOP, PAA, inhabilidades, disciplinario). Must pass: ≥10k rows, `rowsUpdatedAt` within 365 days, at least 5 non-metadata columns with readable field names.
  - **Tier B (backlog):** sector context (education, health, transport stats). Useful later, not ingested for MVP.
  - **Tier C (skip):** irrelevant to anticorruption (tourism, events, cultural agenda, etc).
- [ ] **2.3** Write `docs/datasets/catalog.csv`. Columns: `id, name, sector, tier, rows, last_update_days, freq, n_columns, columns_json, url, pida_match, notes`.
- [ ] **2.4** Cross-check Tier A against PIDA 30 anticorruption dataset list (see `project_completion_plan` memory, Tabla 4 in `govt data roadmap` output). Mark `pida_match=yes/no` per row. Missing PIDA entries → open question for user (maybe unpublished, maybe renamed).
- [ ] **2.5** For every Tier A dataset, draft `etl/src/coacc_etl/datasets/<id>.yml` (see Phase 3.1 template). Commit as `feat(etl): dataset configs for Tier A`.

**Sanity check 2a (heuristic):** triage rejects any dataset where column names are gibberish, purely numeric, or missing semantic meaning.
**Sanity check 2b (human eyeball):** user reviews Tier A list, rejects any that fail the smell test. Produces `docs/datasets/catalog.signed.csv`.
**Sanity check 2c (coverage vs competition):** Tier A must cover at minimum: 1 procurement source, 1 sanctions/disciplinary source, 1 budget source. If missing, escalate.

**Gate 2:** user signs off `docs/datasets/catalog.signed.csv`. Tier A row set becomes **canonical ingest set**. No other dataset gets ingested without updating the signed catalog.

---

## Phase 3 — Config-driven ETL rewrite

**Goal:** kill the hand-rolled `pipelines/*.py` approach. Single generic ingester reads per-dataset YAML, writes to partitioned parquet, enforces coverage.

- [ ] **3.1** Schema file template `etl/src/coacc_etl/datasets/<id>.yml`:
  ```yaml
  id: jbjy-vk9h
  name: SECOP II Contratos Electrónicos
  tier: A
  pida_match: yes
  watermark_column: ultima_actualizacion  # used for incremental pulls
  partition_column: fecha_de_firma        # used for Hive partitioning
  columns:
    buyer_name: nombre_entidad
    buyer_id: nit_entidad
    supplier_name: proveedor_adjudicado
    supplier_id: documento_proveedor
    contract_value: valor_del_contrato
    signed_date: fecha_de_firma
    updated_at: ultima_actualizacion
  required_coverage:
    buyer_name: 0.95
    contract_value: 0.80
    supplier_name: 0.95
    signed_date: 0.80
  ```
- [ ] **3.2** Rewrite `etl/src/coacc_etl/ingest.py` as generic runner: load YAML → `socrata_get` with `$where` on `watermark_column` → normalize columns to canonical names → assert coverage → write `lake/raw/<id>/year=YYYY/month=MM/part-<ts>.parquet`.
- [ ] **3.3** Delete `etl/src/coacc_etl/pipelines/secop_integrado.py` + any other `pipelines/*.py` bespoke files. Keep only shared helpers (`colombia_shared.py` if still referenced).
- [ ] **3.4** Update CLI entrypoint in `etl/pyproject.toml` to `coacc-etl ingest <id>` and `coacc-etl ingest-all`.
- [ ] **3.5** Tests: `etl/tests/test_ingest.py` with golden fixtures per Tier A dataset. One fixture per dataset from `scripts/capture_fixture.py`.

**Sanity check 3a (pre-download):** ingester refuses to run if schema YAML missing `watermark_column`, `partition_column`, or any `required_coverage` key. Fail fast.
**Sanity check 3b (post-download, per-batch):** assert `coverage[col] >= required_coverage[col]` for every required column. On fail → do NOT advance watermark, write failure report to `lake/meta/failures/<id>/<iso_ts>.json` with column-level stats, exit non-zero.
**Sanity check 3c (partition correctness):** all `partition_column` values in a written file must fall inside the declared `year=/month=` partition. Cross-partition writes rejected.
**Sanity check 3d (watermark correctness):** `last_seen_ts` stored in watermark == `max(watermark_column)` in the batch. `datetime.now()` is banned as a watermark source (enforced by test).
**Sanity check 3e (ID stability):** running the same batch twice writes an identical parquet (deterministic sort + no timestamp columns from wall-clock).

**Gate 3:** test suite green, one full end-to-end ingest of `jbjy-vk9h` succeeds on real API, `lake/meta/coverage.json` shows all required columns passing thresholds.

---

## Phase 4 — Lake reality (continuous)

**Goal:** per-dataset health dashboard; regressions = red build.

- [ ] **4.1** Rewrite `scripts/lake_reality.py`:
  - auto-discover `lake/raw/*` via `COACC_LAKE_ROOT` env or `./lake/raw` default; fail loud if neither exists (no silent "no data").
  - per dataset: row count, partition spread, per-column null rate, min/max of `watermark_column`, duplicate-key ratio on declared business key.
  - output `lake/meta/reality/<YYYY-MM-DD>.json` plus diff vs prior day in `lake/meta/reality/diff.md`.
  - exit non-zero on regression beyond thresholds (e.g. coverage drop >1pp, row count drop, watermark stale >expected_freq × 3).
- [ ] **4.2** `make lake-reality` target invokes it. Pre-commit hook blocks commits if latest report is red.
- [ ] **4.3** Daily GitHub Action (or local cron) runs `make lake-reality` and commits the JSON snapshot.

**Sanity check 4:** any regression must block the build. No silent degradation. No "warning".

**Gate 4:** green `lake-reality` on at least 3 Tier A datasets for 3 consecutive days.

---

## Phase 5 — Downstream (Neo4j + AI + API + frontend)

**Prerequisite:** Phase 4 green for ≥3 datasets. Do not build on top of a broken lake.

- [ ] **5.1 Neo4j loader.** Reads curated parquet, asserts referential integrity (no orphan contracts without buyer/supplier nodes). Sanity check: node/edge counts match parquet cardinality within 0.1%.
- [ ] **5.2 Anomaly model.** Trains on lake, scores into parquet at `lake/curated/anomaly_scores/`. Contract test vs `AnomalyScoreContract`. Sanity check: score distribution shape + AUC on held-out labeled anomalies (start with rule-based synthetic labels if no real labels yet).
- [ ] **5.3 Narrator (generative).** Takes a case subgraph, emits narrative markdown. Contract test vs `NarrativeContract` (schema + citation coverage + hallucination guard: every named entity in narrative must exist in the subgraph).
- [ ] **5.4 API + frontend.** Repoint existing `api/` + `frontend/` to new lake. Regression sanity: golden-path UI test unchanged.

Detailed breakdown deferred until Phase 4 green. This section gets expanded into its own plan doc at that point.

---

## Phase 6 — Competition submission

Follow `docs/competition/program_plan.md` tracks D/M/F/R/C. No change — that plan stands once Phases 0–5 land.

---

## Reality-check loop (cross-cutting)

| Scale | Cadence | Mechanism | Red-build trigger |
|---|---|---|---|
| Micro | per-commit | `make test` + `make lake-reality` | Coverage regression, schema drift, watermark staleness |
| Meso | weekly Friday | `scripts/dataset_triage.py` rerun, coverage delta report, rubric burn-up | New dead datasets, Tier A dropouts, coverage trend ↓ |
| Macro | monthly | full replay from empty lake → verify reproducibility | Replay fails, total row count differs >1% from prior macro |

---

## Open questions (resolve before moving past each gate)

1. **Phase 0:** approve inventory scope (read-only walk OK?).
2. **Phase 1:** confirm bulk-kill categories vs per-row review preference.
3. **Phase 2:** accept Tier A thresholds (≥10k rows, <365d update, ≥5 meaningful columns) or tweak?
4. **Phase 3:** OK to delete all `pipelines/*.py` bespoke code, or keep `secop_integrado.py` as transitional?
5. **Phase 5:** is Neo4j still load-bearing or can we ship without it? (Rubric only scores open-data usage + AI components; graph DB is implementation detail.)
6. **Competition:** cronograma dates, team composition (≥1 woman required), in-person finals logistics.

---

## Decision log (append-only)

- **2026-04-20** — Plan created. Canonical dataset source = `colombia_open_data_audit.json` (285 valid IDs). Tier A target size = 15–25 datasets.
- **2026-04-20** — Gate 0 signed. Decisions Q1–Q6 recorded in Phase 0 section. `cleanup_review.signed.csv` committed.

---

## Progress snapshot

- Phase 0: ✅ complete — `inventory.md` + `cleanup_review.csv` produced; Gate 0 signed 2026-04-20
- Phase 1: ready — 12-commit plan defined; awaiting "go phase 1"
- Phase 1: not started
- Phase 2: not started
- Phase 3: not started
- Phase 4: not started
- Phase 5: not started
- Phase 6: blocked on Phase 5
