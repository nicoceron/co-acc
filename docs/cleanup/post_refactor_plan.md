# co/acc Post-Refactor Plan (Phases 7–16)

**Status:** reviewed 2026-05-15; schedule + eligibility blockers explicit
**Author:** synthesized from `docs/cleanup/plan.md`, `docs/cleanup/refactor_plan.md`,
`docs/competition/program_plan.md`, `docs/competition/datos_al_ecosistema_2026.md`,
and the Wave 0–6 refactor outcome.
**Hard deadline:** Datos al Ecosistema 2026 finals, **first week of August 2026**.
At review date **2026-05-15**, that is **~11.5-12 weeks** of calendar runway
(depending on the exact finals day in the first week of August). The last 2
weeks must be reserved for dress-rehearsal + finals logistics. Real
engineering runway is therefore **~9.5-10 weeks**.

**Eligibility blocker:** MinTIC's public updates say the convocatoria closed
on **2026-04-30** and that **349 teams advanced to the technical stage on
2026-05-06**. Before spending more competition-track engineering time, verify
that co/acc has a registration or selection artifact. If it does not, treat
this as a 2027 readiness roadmap, not an active 2026 competition plan.

Read this doc front-to-back before touching code. Each phase has a falsifiable
**Definition of Done** — if you can't point at the artifact that satisfies
every bullet, the phase is not done. No "in progress for two weeks"; either
ship it or escalate.

---

## 0. Where we stand (reviewed 2026-05-15)

The Wave 0–6 refactor is **complete**. Concretely:

- **Catalog:** `docs/datasets/catalog.signed.csv` (311 signed rows) +
  `catalog.proven.csv` (148 proven join-key rows) + `catalog.report.md`.
  Signed by the qualification gate.
- **YAML contracts:** 148 files under `etl/datasets/<id>.yml`. **42 are ingest-ready**
  (29 incremental + 13 snapshot). 106 are contract shells with
  `columns_map: {}` and are not ingest-ready yet. YAML tiers are 118
  `core` + 30 `context`; tier alone does not mean ingest readiness.
- **Ingester:** `coacc_etl.ingest.socrata.ingest(spec)`. Two source classes:
  incremental (watermark via Socrata `$where`) and snapshot (`full_refresh_only: true`).
  Coverage gate writes pass/fail reports, watermark advances only on success.
- **Lake layout:** `lake/raw/source=<id>/year=YYYY/month=MM/...parquet` (incremental)
  or `snapshot=<iso>/...parquet` (snapshot). `lake/meta/{watermarks,coverage,
  failures}/` for ops metadata. `lake/curated/` exists as an empty directory.
- **CLI:** `coacc-etl ingest <id>` / `ingest-all` / `qualify`.
- **Tests:** ETL pytest coverage exists for catalog, ingest, transforms,
  lakehouse, and alignment. Treat Wave 6 as last-known-green; rerun
  `make test-etl` before Phase 7 operations.
- **Retired:** `Pipeline` base class, `pipeline_registry`, `Neo4jBatchLoader`,
  every `pipelines/*.py`, the `neo4j` Python dependency on the ETL side.
- **Untouched:** `api/` workspace still queries Neo4j via 100+ Cypher templates.
  Frontend still consumes that API. Both work today only because Neo4j still
  holds whatever was loaded pre-refactor; **nothing currently repopulates it.**

What this means in practice: the **plumbing is built, the pipes are dry**.
The phases below fill the lake, monitor it, rebuild the graph from it, ship
the AI components the competition rubric demands, and submit.

---

## 1. Phases at a glance

Effort numbers are **realistic solo-dev estimates** (high end of a range,
1.2× the optimistic figure). They include test-writing, debugging, doc
updates, and the inevitable coverage-gate or schema-drift surprise.

| # | Phase | Effort | Critical for finals? | Depends on | Parallelizable with |
|---|---|---|---|---|---|
| 7  | Operational ingest (6 large datasets, see §3) | 4 days eng + 2–4 days wall-clock | **Yes** | — | — |
| 8  | Lake reality continuous monitoring | 7–9 days | **Yes** | 7 | 9.0, 12 |
| 9.0 | `paco_sanctions` adapter (only) — labels for Phase 13 | 4–5 days | **Yes** (promoted from 9) | 7 | 8, 12 |
| 9   | Remaining custom adapters | 2–3 days each | **Deferred post-finals** | 7 | — |
| 10 | Curated layer (NIT canonicalization + dim tables + signal features) | 10–12 days | **Yes** | 7, 8, 9.0 | — |
| 11 | Graph loader (parquet → Neo4j) — **keystone** | 14–18 days | **Yes** | 10 | 13 (after week 1 of 11) |
| 11.5 | API service rewires (`signal_materializer`, `score_service`, `case_service`) | 3–4 days | **Yes** | 11 | — |
| 12 | API legacy CSV retirement (`source_registry_co_v1.csv`) | 3–4 days | Yes (surgical) | 7 | 8, 9.0 |
| 13 | Anomaly model (R5 ML component) | 12–16 days | **Yes — rubric** | 10, 9.0 | 11 |
| 14 | Generative narrator (R5 IA generativa, **DuckDB-backed**) | 7–9 days | **Yes — rubric** | 13 | 11.5, 15 (start) |
| 15 | Frontend (case browser + narrative reader + ciudadano-agent) | 12–14 days | **Yes** | 11.5, 13, 14 | — |
| 16 | Compliance, docs, competition submission | 5 days + finals trip | **Yes — gates entry** | 7–15 green | — |

**Original 2026-05-06 critical path with realistic parallelization**
(solo-dev context-switching):

```
W1:    Phase 7
W2:    Phase 8 + Phase 9.0 + Phase 12         (3 small phases in parallel)
W3-4:  Phase 10
W5-7:  Phase 11 (keystone)
W7-9:  Phase 13 (overlaps last week of 11; eats fresh time)
W9:    Phase 11.5 + start Phase 14
W10:   Phase 14 + start Phase 15
W11-12: Phase 15
W13:   Phase 16 + dress rehearsal + finals trip prep
```

That fit 13 weeks **with zero slack** on 2026-05-06. As of the 2026-05-15
review, the plan has already spent roughly 1.3 of those weeks. If Phase 7
is not already complete outside this repo, the default branch is now the
80% plan in §18.2. If eligibility artifacts are missing, stop the
competition path before starting Phase 7.

---

## 2. Cross-cutting conventions (read once, apply everywhere)

### 2.1 Branching

- One branch per phase: `phase-7/operator-ingest`, `phase-8/lake-reality`, …
- One PR per **deliverable inside a phase**, not per phase. A phase is the
  acceptance unit; a PR is the review unit. Aim for PRs ≤500 LOC of code-diff
  (excluding generated parquet, fixtures, golden files).
- Trunk is `main`. No long-lived feature branches; rebase at least daily.
- **Commit message convention** (already in use): `<type>(<scope>): <subject>`
  where `type ∈ {feat, fix, refactor, chore, docs, test}` and `scope` matches
  the workspace (`etl`, `api`, `frontend`, `infra`, `docs`).

### 2.2 CI gates (must stay green per PR)

A new workflow `.github/workflows/tests.yml` is created in Phase 7's first
PR. Until it lands, the gates below run locally via `make`.

1. `make test` — all three workspaces (etl, api, frontend).
2. `make lint type-check` — ruff + mypy.
3. `make lake-reality DATASETS=<changed-ids>` — green on the datasets
   whose YAML the PR touches (red-build threshold defined in Phase 8).
   The `DATASETS=` form is added in Phase 8 step 4. Skipped on PRs that
   don't touch ETL or YAML contracts.
4. **Catalog drift check** when `etl/datasets/*.yml` or
   `docs/datasets/catalog.signed.csv` changes: rerun
   `etl/tests/test_catalog.py` and `etl/tests/test_signal_source_alignment.py`.
   Both already exist; this is a CI-routing rule, not new code.
5. Pre-push hook: `scripts/check_compliance_pack.py` + `check_public_privacy.py`
   + `check_open_core_boundary.py` (already wired).

### 2.3 Definition of Done (universal)

Every phase has its own DoD bullets, but these apply to all of them:

- [ ] All new code has tests; no untested branches.
- [ ] `mypy --strict` clean on the package being touched.
- [ ] Public-facing changes documented in `README.md` or the relevant
      doc under `docs/`.
- [ ] `CHANGELOG.md` updated under the `## [Unreleased]` heading.
- [ ] Commit history is clean (no `WIP`, no `fixup` left).
- [ ] No commit message says "works", "validated end-to-end", "production
      ready" without a same-PR reality-report file backing the claim
      (echoes the macro hook in `program_plan.md` §6.3).

### 2.4 Test pyramid

- **Unit (pytest):** pure functions, no I/O, ms-scale. The 76 existing tests
  are the bar.
- **Contract (pytest + Pydantic + Pandera):** validate that a module produces
  data matching a frozen schema. New phases each add one.
- **Integration (pytest, marker `live`, opt-in):** hit real Socrata / real
  Neo4j on a developer machine. Not run in CI.
- **End-to-end (Playwright + pytest):** in `frontend/tests/` for UI flows,
  in `api/tests/integration/` for HTTP. Phase 15 expands these.
- **Reality (`make lake-reality`):** the per-dataset health probe — runs
  in CI and as a daily cron after Phase 8.

### 2.5 Documentation discipline

- One canonical doc per concept. If you find yourself writing the same
  thing twice, link instead.
- Architecture decisions go in `docs/cleanup/post_refactor_plan.md` §17
  (this file's decision log) **with a date and a rationale**. No silent
  pivots.
- Keep `docs/architecture/overview.md` in sync with reality at the end of
  each phase. Diagram diffs go in the same PR as the change.
- Operator runbooks live in `docs/runbooks/<topic>.md` (Phase 7 creates
  the directory).

### 2.6 Observability minimum bar

For every long-running command (ingest, qualify, graph-load, model-train),
each run must emit:

- A start-of-run line with `dataset_id` (or scope), `git_sha`, `wall_clock_iso`.
- An end-of-run line with rows in/out, error count, duration, exit code.
- One JSON event per failed row/batch with enough context to reproduce.
- All logs structured (`logging` with `extra=`), never `print()`.

Logs go to stderr; structured artifacts go to `lake/meta/<topic>/<date>.json`.

### 2.7 Secrets and config

- Never commit secrets. `.env.example` is the contract; `.env` is gitignored.
- New env vars must be added to `.env.example` and `docs/runbooks/env.md`
  in the same PR that introduces them.
- Anthropic / OpenAI / Gemini keys are read via `os.environ`, never passed
  on the command line or written to `lake/`.

### 2.8 Bounded-memory data contract

The operator machine is assumed to be a laptop/dev box, not a warehouse
cluster. No phase may require holding a full Phase 7 dataset, full join, or
full graph projection in Python RAM.

- **Download is still required**, but only as paged source pulls. Socrata
  pages are written to parquet-backed staging/partitions as they arrive;
  later phases reuse local parquet rather than re-querying Socrata.
- **No unbounded materialization:** no `list.extend(page)` across an entire
  source, no pandas `.concat()` across all partitions, and no DuckDB
  `.df()`/`.fetchall()` on unbounded queries. Use projected columns, filters,
  `fetchmany()`, Arrow record batches, or `COPY (SELECT ...) TO parquet`.
- **Join discovery happens in DuckDB, not Python.** Validate join coverage
  with aggregate SQL (`count`, `count where matched`, anti-join samples) and
  materialize only the resulting edge/features parquet.
- **Graph connectivity is batch-built:** load endpoint nodes first with
  constraints/indexes, then create relationships in 1k-10k-row batches by
  matching indexed keys (`entity_uid`, `contract_id`, `process_id`,
  `nit_canonical`). Python only holds the current batch.
- **Checkpoint every bounded unit:** page, source partition, curated builder,
  graph builder. Failed runs resume from the last durable checkpoint rather
  than replaying a multi-million-row in-memory object.
- **Demo graph pruning is allowed and expected.** The finals/demo graph only
  needs Phase 7 + `paco_sanctions` + the node/edge types required by the top
  3-5 demo patterns. Loading all 148 YAML contracts into Neo4j is post-finals
  unless a specific demo pattern needs them.

---

## 3. Phase 7 — Operational ingest of the core large datasets

**Owner profile:** data engineer, comfortable with overnight runs and DuckDB.
**Prereq:** none (the YAMLs and ingester are already shipped).
**Effort:** **4 days engineering** + 2–4 days wall-clock for the ingest itself.
The "1 day" estimate is unrealistic: history shows 1–2 of these datasets
will trip a coverage-gate failure or watermark edge case, each costing
0.5–1 day to diagnose.

### 3.1 Goal

Land actual rows for the highest-value Socrata datasets in
`etl/datasets/` so the lake stops being demo-empty and downstream phases
have data to consume.

| Dataset | YAML | ~rows | Why |
|---|---|---|---|
| `2jzx-383z` | SIGEP Conjunto servidores públicos | 265k | Donor → official → vendor loop |
| `jbjy-vk9h` | SECOP II Contratos Electrónicos | 5.6M | Core of every signal |
| `qddk-cgux` | SECOP I Procesos Histórico | 6.1M | Historical procurement processes |
| `p6dx-8zbt` | SECOP II Procesos de Contratación | 8.4M | Required by single-bidder, repeat-awards, supplier-concentration |
| `c82u-588k` | RUES Personas Naturales/Jurídicas/ESAL | 9.2M | Company identity backbone |
| `rpmr-utcd` | SECOP I Contratos | 21.7M | Historical procurement spine |
| `wi7w-2nvm` | SECOP II Ofertas Por Proceso | 41.9M | Required for `single_bidder` feature confirmation in Phase 13 |

**Feature-set decision:** the originally proposed `supplier_win_rate_buyer`
feature for Phase 13 requires offers data linked to processes. To keep
scope manageable, that feature is **dropped from the MVP** (see §10.3).
`wi7w-2nvm` still carries its weight via `single_bidder` confirmation
and stays in Phase 7 because re-running ingest later is more expensive
than including it now, but it runs last because the YAML now records it
as a 41.9M-row source.

### 3.2 Implementation steps

1. **Verify bounded ingest before bulk runs.** As of 2026-05-15,
   `coacc_etl.ingest.socrata.ingest` no longer accumulates a full
   source in a Python `collected` list: incremental and snapshot ingest
   process one Socrata page at a time, write page-sized parquet under
   `lake/meta/ingest_staging/<batch_id>/`, compute coverage with
   streaming counters, track `max(watermark_column)` incrementally, and
   only move staged parquet into `lake/raw/` after the run validates.
   Before starting overnight pulls:
   - start with the default `DEFAULT_PAGE_SIZE=10000` and
     `DEFAULT_MAX_PAGES=10000` (100M-row cap), or override with
     `COACC_SOCRATA_PAGE_SIZE`, `COACC_SOCRATA_MAX_PAGES`,
     `coacc-etl ingest --page-size`, and `--max-pages`;
   - keep `COACC_WATERMARK_FUTURE_GRACE_DAYS=1` unless a source-specific
     investigation justifies changing it; timestamps beyond that ceiling
     are preserved in the sentinel partition but do not advance watermarks;
   - raise page size toward `50k` only if Socrata accepts it reliably;
     lower it if rate limits, timeouts, or per-page memory pressure appear;
   - run the focused ingest tests and confirm failed coverage leaves no
     staged parquet and no final raw parquet.
2. **Disk budget.** Estimate compressed parquet size: SECOP II contracts
   averages ~150 bytes/row compressed → 5.6M ≈ 850 MB; SIGEP ~80 bytes/row
   → ~2.5 GB. Plan for **20–25 GB** of `lake/raw/` after Phase 7 lands,
   with headroom to 50 GB for Phase 9–10 datasets. Verify free disk:
   `df -h $(realpath lake/)` before starting.
3. **Sequence the runs.** Smallest first to flush bugs:
   `2jzx-383z → jbjy-vk9h → qddk-cgux → p6dx-8zbt → c82u-588k →
   rpmr-utcd → wi7w-2nvm`. Don't parallelize on the same Socrata
   domain — share the rate limiter, keep the operator log linear. Use
   `make ingest-phase7-smoke` for the bounded live verification pass
   and `make ingest-phase7-full` for the guarded full-refresh sequence.
4. **Run with full-refresh once per dataset** via the Phase 7 runner:
   ```bash
   make ingest-phase7-full PHASE7_ARGS="--min-free-gb 80"
   ```
   Full mode propagates `full_refresh=True` so the watermark moves to the
   new data max even if a prior smoke run advanced it. The runner appends
   every result to `docs/runbooks/ingest_log.md`.
5. **Inspect after each run:**
   - `lake/meta/coverage/<id>/<ts>.json` — verify every required column
     meets its threshold.
   - `lake/meta/failures/<id>/<ts>.json` — must not exist on success.
   - Partition spread: `find lake/raw/source=<id>/year=*/month=*/ -type d
     | wc -l` should match the dataset's date range (e.g. SECOP I goes
     back to 2011 → ~180 partitions).
   - `year=0/month=00/` partition (sentinel for unparseable or
     implausibly future timestamps): row count divided by total should be
     ≪1%. If it's >5%, escalate — either the YAML's watermark column is
     wrong or the upstream has a systemic NULL/future-date pattern worth
     investigating before proceeding.
6. **Snapshot of run results.** Append one row per ingest to
   `docs/runbooks/ingest_log.md` with: dataset, start, end, rows in,
   rows out, coverage pass/fail, sentinel-partition fraction, operator note.
7. **Commit the metadata, not the parquet.** Parquet stays gitignored;
   metadata under `lake/meta/runs/*.log` is also gitignored. Commit only
   `docs/runbooks/ingest_log.md`.

### 3.3 Acceptance criteria / DoD

- [ ] All Phase 7 datasets have at least one parquet file under
      `lake/raw/source=<id>/`.
- [ ] Socrata ingest has no unbounded all-dataset `collected` list or full
      DataFrame requirement; peak memory stays under the operator-configured
      budget on the largest dataset.
- [ ] All Phase 7 datasets have a fresh `lake/meta/coverage/<id>/<ts>.json` showing
      every required column ≥ its declared threshold.
- [ ] No `lake/meta/failures/<id>/` row exists for any of them.
- [ ] `lake/meta/watermarks.parquet` lists each one with a non-null
      `last_seen_ts` (verified 2026-05-15: all Phase 7 sources are incremental,
      none are `full_refresh_only`, so this applies uniformly).
- [ ] `docs/runbooks/ingest_log.md` exists with one row per dataset
      (one row per Phase 7 source).
- [ ] Sentinel partition fraction ≤ 5% for each (or documented escalation).
- [ ] `make lake-reality` runs without error against the ingested
      datasets (Phase 8 will sharpen what "green" means; Phase 7 just
      requires it to not crash).

### 3.4 Risks / mitigations

- **Socrata 429 rate limit:** the ingester already backs off with retries.
  If a run dies, restart with `FULL_REFRESH=0` (default) and the watermark
  picks up from the last advance. Never restart with `=1` mid-run unless
  you intend to reset.
- **Disk fills:** baseline, monitor `df -h`, abort and run
  `make lake-compact` if free space drops below 10 GB.
- **Schema drift mid-run:** Socrata occasionally adds columns. The
  ingester writes via `union_by_name`, so a new column lands as NULL in
  prior partitions. Note in `ingest_log.md` and proceed.
- **Bogus future dates:** upstream rows can carry dates like 2099. The
  ingester preserves those rows in `year=0/month=00/` and excludes them
  from watermark advancement, so a single bad date cannot poison future
  incremental runs.

### 3.5 Rollback

Per-dataset rollback: `rm -rf lake/raw/source=<id>/`,
`rm lake/meta/watermarks.parquet` (or selective edit), retry. The ingester
is deterministic and idempotent (see `etl/tests/test_ingest/test_determinism.py`),
so byte-for-byte the same data lands again on a re-run.

---

## 4. Phase 8 — Lake reality continuous monitoring

**Owner profile:** data engineer + light DevOps.
**Prereq:** Phase 7 done.
**Effort:** 7–9 days.

### 4.1 Goal

Turn the existing `scripts/lake_reality.py` (one-shot SECOP-II-only,
prints a CSV) into a per-dataset health probe that runs daily, produces
a versioned JSON snapshot, computes a diff against the prior day,
and **fails the build on regression**. This is the gate that unlocks
Phase 11 onward — `program_plan.md` §1 requires "≥3 datasets green for
3 consecutive days" before downstream work is allowed.

### 4.2 Required metrics per dataset

For every directory under `lake/raw/source=<id>/`:

| Metric | Definition | Default red threshold |
|---|---|---|
| `row_count` | `SELECT count(*)` | drop ≥1% vs prior day |
| `partition_count` | distinct `year=YYYY/month=MM/` (or `snapshot=<iso>/`) | drop > 0 |
| `null_rate[<col>]` | per `required_coverage` column from YAML; **note:** YAML `required_coverage` is *coverage* (1 − null_rate), e.g. `0.95` means "≥95% non-null" | red when `null_rate[col] > (1 − required_coverage[col]) + 0.01` (i.e., the implicit allowed null rate is exceeded by ≥1pp) |
| `watermark_min`, `watermark_max` | from data, not metadata | watermark stale > expected freq × 3 |
| `dup_ratio` | `1 - (count(distinct join_key) / count(*))` | rises ≥0.5pp |
| `sentinel_fraction` | rows under `year=0/month=00/` / total | rises ≥1pp |
| `partition_skew` | std-dev of rows-per-partition / mean | rises ≥20% |
| `schema_hash` | hash of `(col_name, duckdb_type)` tuple list | any change → warn (not fail) |
| `freshness_seconds` | now − latest `parquet.mtime` | exceeds `freq` × 3 |

Snapshot output: `lake/meta/reality/<YYYY-MM-DD>.json` with one object per
dataset. Diff: `lake/meta/reality/<YYYY-MM-DD>.diff.md` listing changes
vs the previous snapshot.

### 4.3 Implementation steps

1. **Move the dataset registry off hardcoded dicts.** Today
   `scripts/lake_reality.py` hardcodes `SOCRATA_IDS`, `CRITICAL_COLUMNS`,
   `PK_COLUMNS` for `secop_integrado` only. Replace with: walk
   `etl/datasets/*.yml`, read `id`, `join_keys`, `required_coverage`,
   `watermark_column`, `freq` from each. Datasets without YAML are
   skipped with an INFO log.
2. **Refactor `coacc_etl.lakehouse.reality`.** Today it's a 67-line
   wrapper around `socrata_live_count`. Promote it to the real probe:
   - `compute_dataset_health(spec: DatasetSpec) -> DatasetHealth`
   - `DatasetHealth` is a Pydantic model containing every metric in §4.2.
   - Pure DuckDB; no live Socrata calls. (Live counts move to a separate
     `--with-live` flag because they cost API quota.)
3. **Add diff-and-judge.** New module `coacc_etl.lakehouse.health_diff`:
   - `diff_health(prev: DatasetHealth, curr: DatasetHealth) -> list[Finding]`
   - `Finding(severity: Literal["info","warn","fail"], metric: str, message: str)`
   - Default thresholds in §4.2 are configurable via
     `config/reality_thresholds.yml`.
4. **Rewrite `scripts/lake_reality.py`** with `argparse` (today it has
   no flags). Required options:
   - `--all` (default): every YAML-declared dataset with a present
     `lake/raw/source=<id>/` directory.
   - `--dataset <id>` (repeatable): single dataset.
   - `--datasets <id1,id2,…>`: comma-separated list. Passed as
     `make lake-reality DATASETS=jbjy-vk9h,qddk-cgux` from CI.
   - `--changed-yamls-only`: read `git diff --name-only HEAD~1..HEAD`,
     filter to `etl/datasets/*.yml`, derive dataset IDs from filenames.
     Used by the pre-commit hook in step 6.
   - `--with-live`: also probe Socrata live counts (slow, costs quota).
   - `--baseline <date>`: compare against a specific prior snapshot
     (default: yesterday).
   - Exit 0 on all-green, 1 on any FAIL, 2 on operator error.
5. **`make lake-reality` target:** today it shells to the old script
   with no args. Update to forward `DATASETS=` env to `--datasets`,
   and to forward `WITH_LIVE=1` to `--with-live`.
6. **Pre-commit hook:** `scripts/ci/precommit_lake_reality.sh` invokes
   `python scripts/lake_reality.py --changed-yamls-only`. Skips
   silently when no YAML is staged. Wire via the `pre-commit`
   framework with a config in `.pre-commit-config.yaml`.
7. **Daily GitHub Action.** New workflow `.github/workflows/lake-reality.yml`:
   - Runs nightly at 04:00 America/Bogota.
   - Restores `lake/` from a S3-compatible artifact (or local volume —
     decide in the decision log).
   - Runs `make lake-reality --with-live`.
   - Uploads `lake/meta/reality/<date>.json` and `.diff.md` as workflow
     artifacts.
   - Posts to a Slack/Discord webhook on FAIL (env-gated).
   - Commits the JSON snapshot back to `main` via a bot account on a
     `lake-reality/<date>` branch + auto-PR if green.

### 4.4 Tests

- `etl/tests/test_lakehouse/test_reality.py`:
  - `test_compute_dataset_health_minimal_spec` — fixture with 100 rows,
    asserts every metric is present and a sane type.
  - `test_diff_health_no_change` — same snapshot twice → empty findings.
  - `test_diff_health_row_count_drop` — synthetic regression → one FAIL.
  - `test_diff_health_schema_change_warns_not_fails` — column added → WARN.
  - `test_thresholds_loaded_from_config` — override file flips a fail to a warn.
- `etl/tests/test_scripts/test_lake_reality_cli.py` — invokes the CLI
  via `subprocess`, asserts exit codes and snapshot file presence.

### 4.5 Acceptance criteria / DoD

- [ ] `lake/meta/reality/<YYYY-MM-DD>.json` produced for every Phase 7
      dataset on three consecutive days.
- [ ] `<date>.diff.md` exists each day after the first.
- [ ] CI runs `make lake-reality` on every PR touching `etl/datasets/*.yml`.
- [ ] Daily cron / Action runs and posts on failure.
- [ ] Pre-commit hook blocks a YAML edit that would cause a known regression.
- [ ] `config/reality_thresholds.yml` exists and is documented in
      `docs/runbooks/lake_reality.md` (new file).
- [ ] **Gate condition met:** on three consecutive days, the cron run
      ends green for ≥3 datasets. Recorded in
      `docs/cleanup/post_refactor_plan.md` §17 decision log.

### 4.6 Risks / mitigations

- **Threshold churn:** week one will produce noisy diffs. Tune
  `reality_thresholds.yml` rather than disabling the script. Every
  threshold change goes in the decision log with a rationale.
- **Cron environment differs from dev:** the lake on the cron host is
  authoritative. Don't develop against a stale local lake.
- **Slack webhook leaks:** store as a GitHub Action secret, never in
  the repo.

---

## 5. Phase 9.0 + Phase 9 — Custom non-Socrata adapters

This phase splits in two: **9.0 is critical-path**, **9 is deferred**.
The split reflects the dependency from Phase 13's supervised model on
sanctioned-supplier labels.

### 5.0 Phase 9.0 — `paco_sanctions` adapter (critical path)

**Owner profile:** data engineer with web-scraping comfort.
**Prereq:** Phase 7.
**Effort:** 4–5 days (was 3–4; adds 0.5–1d for `DatasetSpec` schema
extension + new YAML creation, which the original estimate elided).
**Why critical:** Phase 13's supervised top-up uses
`supplier ∈ paco_sanctions ± 12 months` as positive label. Without
this adapter, the model is iforest-only — defensible but weaker on
the R5 rubric cell. **Recommended: ship it.**

#### 5.0.1 Implementation

> **Source-of-truth note.** YAML contracts under `etl/datasets/<id>.yml`
> are the source of truth for what the lake ingests. `paco_sanctions.yml`
> does **not** currently exist (verified 2026-05-08). Creating it is
> step 0, not an afterthought. The schema (`DatasetSpec`) is
> `extra="forbid"`, so adding the new `adapter:` field is an explicit
> schema change, not a free YAML key.

1. **Extend `DatasetSpec`** in
   `etl/src/coacc_etl/catalog/models.py`:
   - Add `adapter: Literal["socrata", "paco_sanctions", …] = "socrata"`
     (start with the two values now; new adapters in Phase 9.1 add
     their own literal).
   - Relax `_validate_socrata_id`: only enforce `_SOCRATA_ID_RE` when
     `adapter == "socrata"`. For non-Socrata adapters, allow a
     lowercase-snake-case slug (e.g. `paco_sanctions`).
   - Update `is_ingest_ready()` if a custom adapter has different
     readiness rules (paco doesn't need `columns_map` if the adapter
     normalises columns itself; document the decision either way).
   - Update `etl/tests/test_catalog.py` and any YAML-shape tests for
     the new field.
2. **Create `etl/datasets/paco_sanctions.yml`** with at minimum:
   `id: paco_sanctions`, `name`, `sector`, `tier: core`,
   `adapter: paco_sanctions`, `full_refresh_only: true`,
   `url: <portal.paco.gov.co URL>`, `join_keys` (at least `nit`),
   `required_coverage` for the columns the coverage gate checks.
   This commit lands before any adapter code so the catalog refresh
   sees the new file.
3. Survey `portal.paco.gov.co` (or its current URL) for available
   download paths: bulk export CSV/Excel, scraped HTML table, or
   API. Document findings in `docs/runbooks/custom_adapters.md`
   §paco_sanctions.
4. Write `etl/src/coacc_etl/ingest/custom/paco_sanctions.py` exposing:

   ```python
   def ingest(spec: DatasetSpec, *, full_refresh: bool = False) -> IngestResult: ...
   ```

   The `IngestResult` type is `coacc_etl.ingest.socrata.IngestResult`
   (re-exported from `coacc_etl.ingest.__init__`). Custom adapters
   reuse the same return type rather than defining a parallel one.
5. Drive ingestion through a new dispatcher:
   `coacc_etl.ingest.dispatcher.ingest(spec, …)` reads
   `spec.adapter` (added in step 1) and routes to either
   `coacc_etl.ingest.socrata.ingest` or
   `coacc_etl.ingest.custom.<adapter>.ingest`. The Makefile
   `make ingest` target switches to call the dispatcher rather
   than `coacc-etl ingest <id>` calling `socrata.ingest` directly.
6. Remove `paco_sanctions` from `_KNOWN_DEFERRED_SOURCES` in
   `etl/tests/test_signal_source_alignment.py`.
7. Coverage gate: reuse `coacc_etl.ingest.coverage` (the existing
   module under `etl/src/coacc_etl/ingest/coverage.py`). Required
   columns: `nit`, `entity_name`, `sanction_type`, `sanction_date`.
8. Snapshot mode (`full_refresh_only: true`) — PACO republishes
   wholesale. Output partition: `snapshot=<iso>/`.
9. Fixture under `etl/tests/fixtures/custom/paco_sanctions/`
   capturing one fetched response. Test exercises adapter against
   fixture with `respx` (for HTTP) or filesystem replay.

#### 5.0.2 DoD

- [ ] `etl/datasets/paco_sanctions.yml` exists, validates against the
      extended `DatasetSpec`, and is signed by the qualification gate.
- [ ] `DatasetSpec` carries an `adapter` field; `_validate_socrata_id`
      is conditioned on `adapter == "socrata"`; existing 148 YAMLs
      still validate unchanged.
- [ ] `coacc_etl.ingest.dispatcher.ingest(spec, …)` exists and
      routes by `spec.adapter`.
- [ ] `lake/raw/source=paco_sanctions/snapshot=<iso>/` populated.
- [ ] Coverage gate green on a live fetch.
- [ ] `paco_sanctions` removed from `_KNOWN_DEFERRED_SOURCES`.
- [ ] `etl/tests/test_ingest/test_custom_paco_sanctions.py` green.
- [ ] `make lake-reality` green for the new dataset.
- [ ] `docs/runbooks/custom_adapters.md` §paco_sanctions written
      (legal posture: PACO is public; document terms-of-use review).

### 5.1 Phase 9 — Remaining custom adapters (deferred post-finals)

**Default decision:** **defer everything below to post-finals**, in
priority order if pulled forward later.

After Phase 9.0 lands, the 12 remaining sources in
`_KNOWN_DEFERRED_SOURCES` are: adverse_media, judicial_providencias,
actos_administrativos, gacetas_territoriales, control_politico,
environmental_files_corantioquia, rub_beneficial_owners,
tvec_orders_consolidated, anim_inmuebles, pnis_beneficiarios,
dnp_project_contract_links, official_case_bulletins.

**Source-of-truth note.** None of these 12 sources have a YAML at
`etl/datasets/<src>.yml` today (verified 2026-05-08). For each one
pulled forward, step 0 is **create the YAML** with `adapter: <name>`,
and step 0a may require widening the `adapter` Literal in
`DatasetSpec`. Treat YAML creation as work, not a one-line edit.

#### 5.1.1 Adapter contract (same shape as 9.0)

Every adapter under `coacc_etl/ingest/custom/<source>.py` exposes:

```python
def ingest(spec: DatasetSpec, *, full_refresh: bool = False) -> IngestResult: ...
```

returning `coacc_etl.ingest.socrata.IngestResult`. The dispatcher
introduced in Phase 9.0 routes to it via `spec.adapter`.

#### 5.1.2 Per-adapter steps (when activated)

1. **Create** `etl/datasets/<name>.yml` with `id: <name>`,
   `adapter: <name>`, `tier: core`, plus the usual `join_keys`,
   `required_coverage`, and partition fields. If the new adapter
   name isn't in `DatasetSpec.adapter`'s Literal, widen it first.
2. Write the adapter, honoring `coacc_etl.ingest.coverage` gates,
   writing to `lake/raw/source=<id>/snapshot=<iso>/<ts>-<uuid>.parquet`,
   persisting failures to `lake/meta/failures/<id>/`, and being
   deterministic against a captured fixture.
3. Fixture under `etl/tests/fixtures/custom/<name>/`.
4. `etl/tests/test_ingest/test_custom_<name>.py` exercising the
   adapter against the fixture.
5. Remove the source from `_KNOWN_DEFERRED_SOURCES`.

#### 5.1.3 Priority order if pulled forward

Based on signal coverage and demo value:

1. `official_case_bulletins` — gates `official_case_bulletin_overlap`
   Cypher pattern.
2. `control_politico` — gates `control_politico_contractual` pattern.
3. Everything else is deferrable.

Document any pull-forward decision in §17 decision log.

#### 5.1.4 DoD per adapter

- [ ] YAML promoted to `tier: core`.
- [ ] Coverage gate green on a real fetch.
- [ ] Source removed from `_KNOWN_DEFERRED_SOURCES`.
- [ ] Test suite green.
- [ ] `make lake-reality` green on the new dataset.
- [ ] One paragraph in `docs/runbooks/custom_adapters.md` describing
      legal/ethical posture of the source (especially scrapes).

---

## 6. Phase 10 — Curated layer

**Owner profile:** data engineer, ideally with entity-resolution exposure.
**Prereq:** Phase 7 + 8 green.
**Effort:** 10–12 days.

### 6.1 Goal

Build `lake/curated/` from `lake/raw/`. Three deliverables:

1. **NIT canonicalization.** A single mapping `nit_canonical → {raw_nit, source, first_seen, last_seen, name_variants[]}` so SECOP, SIGEP, Cuentas Claras, etc. join cleanly. Solves `00012345-1` vs `12345` vs `12.345`.
2. **Signal feature parquets.** One parquet per signal under
   `lake/curated/signals/<signal_id>/` containing the precomputed
   features the Cypher pattern would otherwise compute on the fly.
   Phase 11's graph loader and Phase 13's anomaly model both read these.
3. **Entity dimension tables.** `lake/curated/dim_company/`,
   `dim_person/`, `dim_buyer/` — typed, deduplicated, with stable
   `entity_uid`. These become Neo4j nodes in Phase 11.

### 6.2 Module layout

```
etl/src/coacc_etl/curate/
├── __init__.py
├── canonical_nit.py       # canonicalize() + tests
├── canonical_name.py      # accent-fold, lowercase, strip legal-form suffixes
├── dim_company.py         # build dim_company.parquet from raw sources
├── dim_person.py          # build dim_person.parquet
├── dim_buyer.py
├── signal_features.py     # dispatcher: signal_id → builder
└── builders/
    ├── procurement_sanctioned_supplier_awarded.py
    ├── procurement_supplier_concentration_across_entities.py
    └── …  (one per signal that has both required sources ingested)
```

### 6.3 Curated contracts

Pydantic + Pandera schemas under `etl/src/coacc_etl/contracts/curated.py`.
At minimum:

```python
class DimCompany(BaseModel):
    entity_uid: str           # stable UUID
    nit_canonical: str
    nit_variants: list[str]
    name_canonical: str
    name_variants: list[str]
    first_seen: date
    last_seen: date
    sources: list[str]

class SignalFeatureRow(BaseModel):
    scope_key: str            # signal-defined dedup key
    signal_id: str
    entity_uid: str | None
    score_inputs: dict[str, float]
    evidence_refs: list[EvidenceRef]
```

`EvidenceRef`: `{dataset_id, partition, parquet_file, row_index, url}`.

### 6.4 Implementation steps

1. **NIT canonicalization (smallest first).** `canonical_nit.py`:
   - **Scope:** entity NITs only. Person identifiers (cédulas) come
     from SIGEP servidores (`2jzx-383z`) and are handled in
     `canonical_cedula.py` under `dim_person` (step 3 below) — they
     have a different structure (variable length, no DV, sometimes
     pasaporte alphanumeric for foreign nationals). Don't conflate.
   - Colombian NITs are 9 or 10 digits with a verification digit
     (DV). Public records sometimes write `900.123.456-7`,
     `900123456`, `900123456-7`, or `900-123-456-7`.
   - **Canonical form: digits-only, 10 chars, verification digit
     preserved as the last digit.** When upstream omits the DV,
     compute it via the standard MOD-11 algorithm (DIAN's published
     formula) and append it. When upstream provides the DV, validate
     and reject mismatches as a coverage failure.
   - Strip non-digits (dots, dashes, spaces). Foreign suppliers tagged
     with non-numeric prefixes get a separate `foreign_id` column
     and `nit_canonical=NULL`.
   - Property tests via Hypothesis: round-trip every formatting
     variation through `canonicalize()` and assert idempotence.
2. **`dim_company`:** read SECOP II contracts, SECOP II processes,
   SIGEP, sanciones, RUES (when adapter ships). Group by
   `nit_canonical`, collect name variants, emit one row per canonical
   entity. Output: `lake/curated/dim_company/part-<ts>.parquet`.
   Idempotent (run twice → same output bytes modulo timestamp suffix).
   Implementation must be DuckDB-first (`COPY (SELECT ...) TO parquet`
   or Arrow record batches), not full pandas materialization of all
   contributing raw datasets.
3. **`dim_buyer` and `dim_person`:** mirror with appropriate sources.
   `dim_person` builder ships its own `canonical_cedula()` (variable
   length, no DV; pasaporte handled as `foreign_id`). `dim_person`
   sources include SIGEP servidores (`2jzx-383z`) — the only Phase 7
   source that's cédula-keyed. Add a contract test that no row in
   `dim_person` has a `nit_canonical` set (cross-pollination guard).
4. **Signal feature builders.** Start with two:
   - `procurement_sanctioned_supplier_awarded`: inner-join SECOP II
     contracts × paco_sanctions (or sanctions stand-in) on
     `nit_canonical`. One row per (contract, supplier) overlap.
   - `procurement_supplier_concentration_across_entities`: per supplier,
     aggregate award value across distinct buyers in a 12-month window,
     emit when concentration ratio > threshold.
   Add others incrementally; the demo only needs ~3–5. Every builder first
   emits a join-coverage report (`left_rows`, `matched_rows`,
   `unmatched_sample`) so the operator knows whether keys connect before
   graph loading.
5. **Driver script.** `coacc-etl curate --all` walks every builder,
   writes its parquet, updates `lake/meta/curated_runs.parquet`.
6. **Reality probe extension.** Phase 8's `make lake-reality` learns
   to also probe `lake/curated/` directories with curated-specific
   thresholds (NIT canonicalization should match ≥99% of input rows;
   signal feature parquets should be non-empty for every dataset
   intersection that's plausibly non-empty).

### 6.5 Tests

- Unit per canonicalizer, with property tests.
- Contract test per builder: input fixtures → output parquet
  round-trips through the Pydantic/Pandera schema.
- Bounded-memory smoke test: builder runs against many small parquet
  partitions without converting the full DuckDB result to a pandas frame.
- Idempotency: `coacc-etl curate --signal X` twice on a frozen lake
  produces byte-identical output (excluding the timestamp filename).
- Cardinality sanity: `dim_company` row count ≤ sum of distinct NITs
  across input sources, ≥ max(distinct NITs) of any single source.

### 6.6 DoD

- [ ] `lake/curated/dim_company/`, `dim_buyer/`, `dim_person/` populated.
- [ ] At least 3 signal feature parquets non-empty.
- [ ] All curated outputs validate against contracts.
- [ ] `coacc-etl curate --all` runs end-to-end in <10 minutes on the
      Phase 7 lake.
- [ ] `make lake-reality` covers curated/ without errors.

### 6.7 Risks

- **Entity resolution is a tar pit.** Ship the rule-based version
  first; ML-based fuzzy match goes on the post-finals backlog. Don't
  let perfect kill the demo.
- **Curated output divergence:** if two builders disagree on the same
  entity, the contract's `entity_uid` collision test catches it.

---

## 7. Phase 11 — Graph loader (parquet → Neo4j)

**Owner profile:** senior data engineer with Cypher experience.
**Prereq:** Phase 10 green.
**Effort:** **14–18 days.** This is the keystone and the single
phase most likely to slip. `signal_materializer.py` already mixes
DuckDB and Neo4j (1023 LOC, has `_duckdb_rows`, `_duckdb_entity_context`
helpers) — that's an asset, not a blocker, since the loader can lean
on existing parquet-reading code.

### 7.1 Goal

Repopulate Neo4j from `lake/curated/` so the existing 100+ Cypher
patterns and the API service have something to query. The loader is
**downstream of the lake**, not the other way around: lake is canonical,
graph is a derived projection.

### 7.2 Architecture

```
lake/curated/dim_company/*.parquet  ──► MERGE (:Company {entity_uid})
lake/curated/dim_person/*.parquet   ──► MERGE (:Person  {entity_uid})
lake/curated/dim_buyer/*.parquet    ──► MERGE (:Buyer   {entity_uid})

lake/raw/source=<id>/...parquet    ──► MERGE (:Contract {contract_id})
                                    ──► MERGE (:Process {process_id})
                                    ──► (Buyer)-[:AWARDED]->(Contract)
                                    ──► (Contract)-[:WON_BY]->(Company)
                                    ──► (Process)-[:RESULTED_IN]->(Contract)

lake/curated/signals/<id>/...parquet ──► MERGE (:SignalHit {scope_key})
                                      ──► (SignalHit)-[:ABOUT]->(Entity)
                                      ──► (SignalHit)-[:CITES]->(Evidence)
```

The schema is derived from existing Cypher patterns under
`api/src/coacc/queries/`. Inventory those first to lock the node and
edge label set.

### 7.2.1 Local-device loading strategy

The graph loader is a projection builder, not a data warehouse import job.
It must prove connectivity without loading every endpoint or relationship
into application memory.

1. **Project only needed columns from parquet.** DuckDB queries must select
   stable IDs and edge properties only; no `SELECT *` from Phase 7 sources
   on the graph path.
2. **Load endpoint nodes before edges.** Create `Company`, `Buyer`,
   `Contract`, `Process`, and `SignalHit` nodes in batches, with unique
   constraints/indexes in place before the first relationship batch.
3. **Create relationships by indexed keys.** Relationship builders emit
   bounded rows like `{contract_id, supplier_uid}`; Neo4j matches indexed
   endpoints and `MERGE`s the edge. Missing endpoints are counted and
   written to `lake/meta/graph_loader/missing_endpoints/<ts>.json`.
4. **Prune for the finals graph.** Default finals scope is the Phase 7
   datasets, `paco_sanctions`, and the top 3-5 active demo patterns.
   A `--scope finals` loader flag should skip unrelated source-specific
   builders while preserving all entities/edges required by those patterns.
5. **Keep expensive pattern precomputation in parquet.** Repeated
   aggregations such as supplier concentration, single-bidder, and anomaly
   features belong in `lake/curated/signals/`; Neo4j stores the resulting
   `SignalHit` evidence graph.

### 7.3 Module layout

```
etl/src/coacc_etl/graph/
├── __init__.py
├── schema.py           # node/edge label constants + index DDL
├── loader.py           # GraphLoader class
├── builders/
│   ├── companies.py
│   ├── people.py
│   ├── contracts.py
│   ├── processes.py
│   └── signal_hits.py
└── verify.py           # post-load parity checks vs lake
```

`GraphLoader` properties:
- Reads parquet via DuckDB using bounded `fetchmany()` / Arrow record-batch
  iteration.
- Writes Cypher in configurable 1k-10k-row batches with
  `UNWIND $rows AS r MERGE …`.
- Idempotent: same input → same graph (no duplicate edges).
- Resumable: per-builder watermark in `lake/meta/graph_loader.parquet`
  records last loaded `(source, partition, file)`.
- Transactional per batch; failure rolls back the batch only.
- Emits structured logs and a per-run report at
  `lake/meta/graph_runs/<ts>.json`.

### 7.4 Implementation steps

1. **Inventory Cypher patterns.** Walk `api/src/coacc/queries/*.cypher`,
   extract every `MATCH`/`MERGE` to derive the canonical label/property
   set. Output: `docs/architecture/graph_schema.md` listing every node
   label, every edge label, every property. This is the contract the
   loader must satisfy.
2. **Index DDL first.** `etl/src/coacc_etl/graph/schema.py` exports
   `INDEX_DDL: list[str]` — UNIQUE constraints on `(:Company {entity_uid})`,
   `(:Contract {contract_id})`, etc. plus performance indices on edge
   join keys. `coacc-etl graph init` runs them.
3. **Reintroduce `neo4j` as an ETL dep behind an extra.** In
   `etl/pyproject.toml`, add `[project.optional-dependencies] graph =
   ["neo4j>=5.0,<6"]`. Default install stays Neo4j-free; graph loader
   needs `uv sync --extra graph`.
4. **Builders, smallest first.**
   - `companies.py` → MERGE `:Company` from `dim_company.parquet`.
     Verify after: `MATCH (c:Company) RETURN count(c)` matches parquet
     row count.
   - `contracts.py` → MERGE `:Contract` + `:WON_BY`/`:AWARDED` edges
     from `secop_ii_contracts`. Verify edge count.
   - `processes.py` → MERGE `:Process` + `:RESULTED_IN` edges.
   - `signal_hits.py` → MERGE `:SignalHit` from `lake/curated/signals/`.
5. **`coacc-etl graph load --builder all` CLI.** Subcommand of the
   existing CLI. Flags: `--builder <name>`, `--from-scratch`
   (drop+recreate), `--resume` (default, uses watermark),
   `--scope finals|full`, `--batch-size <n>`.
6. **Verify pass.** `etl/src/coacc_etl/graph/verify.py` reads parquet
   and compares aggregates: company count, contract count, total
   contract value, supplier-buyer pair count. Output:
   `lake/meta/graph_parity/<ts>.json`. Phase 8 reality probe also reads
   this and treats >0.1% drift as red.
7. **Decommission cleanly.** Delete `infra/neo4j/` references from
   any retired CI workflow; keep the dev `docker-compose.yml` Neo4j
   service (the API still depends on it).

### 7.5 Tests

- `etl/tests/test_graph/test_loader_companies.py` — fixture parquet →
  in-memory Neo4j (testcontainers or `neo4j:5-community` via Docker)
  → asserts node count and properties.
- `test_loader_idempotent.py` — load twice → no duplicate nodes/edges.
- `test_loader_resumable.py` — kill mid-load → resume → final state
  matches end-to-end run.
- `test_loader_bounded_memory.py` — synthetic many-partition input proves
  the loader never calls unbounded `.df()`/`.fetchall()` and keeps batch
  size at or below config.
- `test_verify_parity.py` — synthetic divergence → reports drift.
- Integration test `tests/integration/test_graph_full_load.py` (marker
  `live`, opt-in) loads a real subset of `jbjy-vk9h` through the
  builder chain.

### 7.6 DoD

- [ ] `coacc-etl graph init` creates indexes/constraints.
- [ ] `coacc-etl graph load --from-scratch` reloads the entire graph
      from `lake/curated/` + `lake/raw/` in **<6 hours** on the dev
      box. Incremental reload (resume from watermark): **<30 min**.
      Sub-30-min full reload is a post-optimization target; do not
      block the phase on it.
- [ ] `coacc-etl graph load --scope finals --from-scratch` builds the
      pruned demo graph on the operator machine without exceeding the
      configured memory budget.
- [ ] `coacc-etl graph verify` ≤ 0.1% drift on every aggregate.
- [ ] At least 5 existing Cypher patterns under `api/src/coacc/queries/`
      return non-empty results when run against the loaded graph.
- [ ] `docs/architecture/graph_schema.md` exists and matches the loaded
      labels/properties exactly.
- [ ] `docs/runbooks/graph_loader.md` exists with reload + recovery
      instructions.

### 7.7 Risks / mitigations

- **Cypher pattern churn:** the API queries are the de facto schema.
  Don't "improve" them while building the loader; lock the schema doc
  first, refactor patterns in a later phase.
- **Performance:** Neo4j MERGE on hot paths is slow. Use APOC
  `apoc.periodic.iterate` for hundreds-of-thousands-of-rows batches;
  if APOC unavailable, fall back to UNWIND-MERGE in 10k chunks.
- **Schema mismatch with API queries:** mitigated by the parity test
  above. Add a CI step that runs every `.cypher` file with `EXPLAIN`
  to catch missing labels.

---

## 8. Phase 11.5 — API service rewires

**Owner profile:** backend engineer who knows the API surface.
**Prereq:** Phase 11 green (graph reloaded from lake).
**Effort:** 3–4 days.

### 8.1 Goal

After Phase 11 reloads the graph from parquet, three API services need
explicit rewiring or extension. None can be skipped without breaking
existing routes.

| Service | Current state | Required change |
|---|---|---|
| `signal_materializer.py` (1023 LOC) | Hybrid Neo4j + DuckDB; reads Cypher results, also has `_duckdb_rows` / `_duckdb_entity_context` helpers — already partially lake-aware | Verify every signal definition still resolves against the reloaded graph; switch any input that previously came from `source_registry_co_v1.csv` to `catalog.signed.csv` (or wait for Phase 12) |
| `score_service.py` | Computes percentile-based scoring from in-memory ranks (`_conn_percentile`, `_fin_percentile`) | Add a `read_anomaly_scores(entity_uid: str)` path that reads `lake/curated/anomaly_scores/<run_id>.parquet` via DuckDB and joins by `entity_uid` |
| `case_service.py` | Builds case responses from investigation entities | Extend to look up `lake/curated/narratives/<case_id>.md` (Phase 14 output) and attach to the response shape |

### 8.2 Implementation steps

1. **Cypher EXPLAIN sweep.** New script
   `scripts/ci/cypher_explain_sweep.py` runs every `.cypher` file under
   `api/src/coacc/queries/` through `EXPLAIN` against a Phase-11-loaded
   Neo4j. Any query that errors (missing label, missing index, missing
   property) → fix the loader or the query. This is the contract test
   between Phase 11 and Phase 11.5.
2. **`signal_materializer` smoke test.** Add
   `api/tests/integration/test_signal_materializer_post_phase11.py`
   marker `live` — runs every signal in `config/signal_registry.yml`
   that's `status: active` and asserts non-empty hits or a documented
   "expected zero" result.
3. **`score_service.read_anomaly_scores` path.** New function reads
   the latest `lake/models/anomaly/current.json` manifest (Phase 13
   produces this), loads `lake/curated/anomaly_scores/<run_id>.parquet`,
   exposes per-entity scores. Falls back to legacy percentile method
   when no scores parquet present.
4. **`case_service` narrative attachment.** New function
   `attach_narrative(case_response)` reads
   `lake/curated/narratives/<case_id>.md` and adds it to the response.
   Returns response unchanged when no narrative exists.
5. **API contract test.** `api/tests/test_api_contract.py` snapshots
   the OpenAPI schema. Any breaking change requires a same-PR snapshot
   update + a CHANGELOG entry.

### 8.3 DoD

- [ ] Cypher EXPLAIN sweep exits 0 on every `.cypher` file.
- [ ] All existing API tests green against the Phase-11-loaded graph.
- [ ] New signal materializer integration test green.
- [ ] `score_service` returns anomaly-model scores when present, falls
      back gracefully when absent.
- [ ] `case_service` attaches narratives when present.
- [ ] OpenAPI schema unchanged (no breaking changes to existing routes).

### 8.4 Risks / mitigations

- **Hidden Cypher pattern dependencies on data that the loader doesn't
  yet write.** The EXPLAIN sweep catches structural issues but not
  semantic emptiness. Mitigation: add a "live result" assertion to the
  signal materializer integration test for the 5 highest-priority
  patterns.
- **`signal_materializer.py` is 1023 LOC.** Don't rewrite it in this
  phase. The minimum-change rule: every edit must be justified by a
  failing test. Refactor goes to post-finals backlog.

---

## 9. Phase 12 — API legacy CSV retirement (`source_registry_co_v1.csv`)

**Owner profile:** backend engineer.
**Prereq:** Phase 7 green. **Runs in parallel with Phase 8 and Phase 9.0.**
**Effort:** 3–4 days.

### 9.1 Goal

The API still reads `docs/source_registry_co_v1.csv` via
`api/src/coacc/services/source_registry.py`. Move it to read
`docs/datasets/catalog.signed.csv` instead, then delete the legacy CSV.
This phase is independent of Phase 11; it can land any time after Phase 7.

### 9.2 Inventory of consumers

`grep -rn source_registry_co_v1` returns hits in (verified 2026-05-08):

- `api/src/coacc/services/source_registry.py` — production loader.
- `etl/src/coacc_etl/qualification/inputs.py` — `load_source_registry`
  helper used during qualification.
- `etl/src/coacc_etl/qualification/cli.py` — `--source-registry`
  argument default value.
- `etl/tests/test_source_registry_alignment.py` — alignment test
  against the legacy CSV.
- `etl/tests/test_signal_source_alignment.py` — signal-source mapping
  test that resolves through the legacy CSV.
- `docs/architecture/overview.md` — diagram caption.
- `docs/datasets/qualification_architecture.md` — narrative reference.
- **`docker-compose.yml` (lines 36, 43, 87, 97)** — two
  `COACC_SOURCE_REGISTRY_PATH` env vars and two `:ro` volume mounts.
  Runtime config, not docs. **Deleting the CSV without updating
  compose breaks `docker compose up` for the dev environment** —
  this is the highest-stakes consumer in the list.
- **`.gitignore` line 54** — `!docs/source_registry_co_v1.csv`
  exception that re-includes the file under the `docs/` ignore.
  Must be removed when the CSV is deleted, otherwise the file
  appears resurrected on a fresh clone.

Historical references in `docs/cleanup/plan.md`,
`docs/cleanup/refactor_plan.md`, `docs/cleanup/inventory.md`, and
`docs/datasets/archive/secop_manual_review.md` are append-only
historical records and may stay; do not rewrite history.

All nine current-state consumers above must be migrated or
deleted in this phase.

### 9.3 Implementation steps

1. **Add a catalog-backed loader** in
   `api/src/coacc/services/source_registry.py`:

   ```python
   def load_source_registry_from_catalog() -> list[SourceRegistryEntry]: ...
   ```

   Reads `docs/datasets/catalog.signed.csv` and returns the same
   `SourceRegistryEntry` shape the existing
   `load_source_registry() -> list[SourceRegistryEntry]` produces.
2. **Switch the default path.** Change `_default_registry_path()` to
   return the catalog path when present, fall back to the legacy CSV
   only if a `COACC_USE_LEGACY_REGISTRY=1` env var is set (one-release
   safety net).
3. **Update qualification helpers.** `qualification/inputs.py` and
   `qualification/cli.py` switch their default to the catalog path.
4. **Update tests.** `test_source_registry_alignment.py` and
   `test_signal_source_alignment.py` repoint to
   `catalog.signed.csv`. Both already validate alignment via
   `primary_url`, which lives on the YAML side, so the mechanical
   change is small.
5. **Update `docker-compose.yml`** (must land before step 6, or
   the deletion breaks dev): change both `COACC_SOURCE_REGISTRY_PATH`
   env vars to point at `/app/docs/datasets/catalog.signed.csv`
   and rewrite both volume mounts to mount that file instead.
   Test with `docker compose up --build` locally before committing.
6. **Delete the legacy CSV** + its compatibility loader + the env-var
   safety net, and remove the `!docs/source_registry_co_v1.csv`
   exception line from `.gitignore`, in one final PR titled
   `chore: retire source_registry_co_v1`.
7. **Update `docs/architecture/overview.md`** and
   `docs/datasets/qualification_architecture.md` to remove the legacy
   CSV references.

### 9.4 DoD

- [ ] `grep -rn source_registry_co_v1` returns 0 hits in
      current-state files (historical records under `docs/cleanup/`
      and `docs/datasets/archive/` may keep their references).
- [ ] All API and ETL tests green against the catalog.
- [ ] `docker-compose.yml` mounts and env vars point at
      `catalog.signed.csv`; `docker compose up` boots cleanly.
- [ ] `.gitignore` no longer contains the
      `!docs/source_registry_co_v1.csv` exception.
- [ ] `docs/source_registry_co_v1.csv` deleted.
- [ ] Architecture overview updated.

---

## 10. Phase 13 — Anomaly model (R5 ML component)

**Owner profile:** ML engineer.
**Prereq:** Phase 10 green (curated signal features available) **and
Phase 9.0 green** (paco_sanctions adapter shipped, which provides
positive labels for the supervised top-up).
**Effort:** **12–16 days.**
**Why required:** competition rule R5 demands a real AI/ML component;
heuristics alone disqualify (`datos_al_ecosistema_2026.md` §2 R5).

### 10.1 Goal

Train and serve a contract-level anomaly classifier that scores every
SECOP contract on a 0–1 risk scale, with feature attributions, and
emits one row per contract to `lake/curated/anomaly_scores/`.

### 10.2 Module layout

```
etl/src/coacc_etl/models/
├── __init__.py
├── anomaly/
│   ├── __init__.py
│   ├── features.py        # build feature matrix from curated/
│   ├── train.py           # train + serialize model
│   ├── predict.py         # batch inference
│   ├── evaluate.py        # held-out metrics
│   └── card.md            # model card (data, training, metrics, limits)
└── shared/
    ├── splits.py
    └── metrics.py
```

### 10.3 Feature spec

Minimum viable feature set (revised — `program_plan.md` §3.2 listed
`supplier_win_rate_buyer`, which requires offers data joined to
processes; deferred to v2 to keep Phase 7 scope tight):

- `log_value` (log of contract value)
- `log_value_z_buyer` (z-score within buyer)
- `log_value_z_modality` (z-score within procurement modality)
- `buyer_supplier_concentration` (Herfindahl on this buyer's awards
  to this supplier in last 12 months)
- `n_prior_contracts_12mo_buyer`
- `n_prior_contracts_12mo_supplier`
- `share_of_buyer_total_value_12mo` (this contract's value as a share
  of the buyer's total awarded value in the prior 12 months — proxy
  for supplier dominance, replaces `supplier_win_rate_buyer`)
- `timing_anomaly_score` (deviation from typical signing-date pattern
  of the buyer)
- `modality_value_mismatch` (boolean: contract value lies in the
  bottom/top 5% for its modality)
- `single_bidder` (boolean — set when SECOP II Procesos shows ≤1
  effective offer; verified against `wi7w-2nvm` offers)
- `prior_sanction_supplier` (boolean — `True` when supplier
  `nit_canonical` appears in `paco_sanctions` with sanction date
  ≤ contract `signed_date`; this is the **supervised positive label
  driver**, not just a feature)

Schemas in `etl/src/coacc_etl/contracts/features.py`. Pandera-validated
on every build.

### 10.4 Modeling approach

Two-stage to handle label scarcity:

1. **Unsupervised baseline:** Isolation Forest on the feature matrix
   to score every contract. Trained on all of `secop_ii_contracts`.
   Fast, interpretable, no labels needed.
2. **Supervised top-up (if labels permit):** XGBoost binary classifier
   trained on a labeled subset where positive = contract whose
   supplier appears in `paco_sanctions` within ±12 months of signing.
   Held-out test: random 20% split on supplier (not on row, to prevent
   leak across splits).

Final score = `0.5 * iforest_score + 0.5 * xgb_proba` when both
available, else iforest only.

### 10.5 Implementation steps

1. **`features.py`** reads `lake/curated/dim_company/`, `dim_buyer/`,
   `secop_ii_contracts`, `secop_ii_processes`, `wi7w-2nvm` offers,
   `paco_sanctions`, joins on canonical keys, emits
   `lake/curated/anomaly_features/<run_id>.parquet`.
2. **`train.py`** trains the iforest and (if labels permit) xgb on
   a supplier-stratified split, serializes via `joblib` to
   `lake/models/anomaly/<run_id>/iforest.joblib` and `xgb.joblib`,
   emits `metrics.json`.
3. **Model versioning manifest.** `train.py` also writes
   `lake/models/anomaly/current.json` with the shape:

   ```json
   {
     "run_id": "20260601T120000Z",
     "model_kind": "iforest+xgb",
     "trained_at": "2026-06-01T12:00:00Z",
     "metrics": { "precision_at_100": 0.42, "auc": 0.71 },
     "feature_schema_hash": "sha256:..."
   }
   ```

   `predict.py` and `score_service.py` (Phase 11.5) read this manifest
   to find the active model. Promoting a new model = atomically
   updating `current.json`. Old runs are kept for rollback.
4. **`predict.py`** loads `current.json` → loads the named model →
   scores every row in `anomaly_features`, writes
   `lake/curated/anomaly_scores/<run_id>.parquet` with columns
   `(contract_id, entity_uid, score, top_features[])`.
5. **`evaluate.py`** computes precision@k for k ∈ {10, 100, 1000},
   calibration plot, AUC. Writes `docs/ai/anomaly_metrics.md`.
6. **`card.md`** = model card per CRISP-ML phase 4: problem framing,
   data sources, features, training, metrics, limitations, ethical
   considerations. Reference `ETHICS.md` for handling of named
   entities.
7. **CLI:** `coacc-etl model train anomaly` / `predict anomaly` /
   `evaluate anomaly` / `promote anomaly --run-id <id>` (atomic
   `current.json` swap).

### 10.6 Tests

- Unit tests on every feature: synthetic input → known output.
- Contract test on the feature matrix.
- Determinism: train twice with same seed → identical model bytes.
- `evaluate.py` reproducibility: same model + same data → same metrics
  to 6 decimal places.

### 10.7 DoD

- [ ] `coacc-etl model train anomaly` runs end-to-end in <30 min.
- [ ] `lake/models/anomaly/<run_id>/` contains `iforest.joblib` and
      `metrics.json` (and `xgb.joblib` if labels available).
- [ ] `lake/curated/anomaly_scores/<run_id>.parquet` non-empty,
      contract-keyed.
- [ ] `docs/ai/anomaly_model.md` (the card) committed.
- [ ] Precision@100 ≥ 0.4 on the held-out sanctioned-supplier set
      (per `program_plan.md` M2 reality check).
- [ ] `make test` green; new tests cover all feature builders.

### 10.8 Risks / mitigations

- **Label scarcity:** if `paco_sanctions` adapter (Phase 9) doesn't
  ship, fall back to iforest only and document explicitly in the
  model card. Don't fabricate labels.
- **Feature leakage:** `supplier_win_rate_buyer` must be computed
  using only data from before the contract's signed_date. Split by
  supplier, not by row, to prevent leakage across train/test.
- **Cold-start suppliers:** suppliers with <5 contracts get a
  `score_confidence` field flagged `low`; the API surfaces this
  rather than dropping the row.

---

## 11. Phase 14 — Generative narrator

**Owner profile:** ML engineer / prompt engineer.
**Prereq:** Phase 13 (scores) green. **Phase 11 is NOT a prereq** —
subgraph extraction reads from parquet via DuckDB, not from Neo4j.
This decouples the narrator from the keystone, lets Phase 14 start
as soon as Phase 13 produces scored cases.
**Effort:** **7–9 days.**
**Why required:** competition R5 IA generativa component.

### 11.1 Goal

Given a `case_id` (a graph subgraph + an anomaly score), emit a
≤400-word Spanish narrative with required sections **# Lead**,
**## Evidencia**, **## Señales**, **## Fuentes**, every named entity
backed by a citation that resolves to a real `datos.gov.co` row.

### 11.2 Module layout

```
etl/src/coacc_etl/models/narrator/
├── __init__.py
├── subgraph.py        # extract subgraph for a case_id
├── prompt.py          # deterministic prompt scaffold
├── generate.py        # LLM call + retry
├── verify.py          # citation guard, word count, section presence
└── card.md            # narrator model card
```

### 11.3 Pipeline

1. `subgraph.extract(case_id)` reads from `lake/raw/` and
   `lake/curated/` **via DuckDB** (decided here, not punted): a
   case = a contract or signal hit; the subgraph spans that contract
   plus its buyer, supplier, related processes, related signal hits,
   and any sanction overlap. Returns a `Subgraph` (per
   `program_plan.md` §3.4 contract). Rationale: DuckDB-on-parquet is
   cheaper, parallelizable, doesn't add Neo4j load on the demo path,
   and the lake is already canonical.
2. `prompt.build(subgraph, score)` produces a deterministic prompt
   with the subgraph rendered as YAML and the score as context.
3. `generate.call_llm(prompt)` calls Anthropic (or fallback to OpenAI/
   Gemini, mirroring `coacc_etl.qualification.llm_review.py`'s
   provider chain). Temperature ≤ 0.3 for reproducibility.
4. `verify.check(narrative, subgraph)` enforces:
   - All four sections present.
   - Word count 250–400.
   - Every entity name in the narrative appears in `subgraph.nodes`.
   - Every citation `(dataset_id, row_key)` resolves to a real
     `evidence_refs[]` entry.
   - No fabricated dataset IDs (every citation must be in the signed
     catalog).
   - **Ethics guard:** narrative must not assert criminality verbs
     (Spanish: `corrupto`, `criminal`, `delincuente`, `defraudó`,
     `cometió fraude`, `culpable`, etc.) attached to a named entity.
     Pattern-detection only language is allowed (`presenta el patrón`,
     `concentra el N% de`, `coincide con la sanción`). The narrator
     model card cites `ETHICS.md` for the full list.
   On failure → retry up to 2× with the verifier feedback in the prompt.
   On final failure → fall back to the templated narrative (§11.6).

### 11.4 Tests

- `test_prompt_deterministic`: same subgraph → same prompt bytes.
- `test_verify_rejects_hallucinated_entity`: narrative names a
  company not in subgraph → fail.
- `test_verify_rejects_fake_dataset_id`: citation to a non-existent
  ID → fail.
- `test_verify_word_count`: 100-word narrative → fail.
- `test_full_pipeline_against_fixture`: 10 fixture subgraphs → all
  produce valid narratives. Uses a recorded LLM response (no live
  call in CI).

### 11.5 DoD

- [ ] `coacc-etl narrator generate <case_id>` produces a valid
      narrative.
- [ ] 10 fixture subgraphs in `etl/tests/fixtures/subgraphs/` —
      all generate-and-verify green.
- [ ] `docs/ai/generative_narrator.md` model card committed.
- [ ] Verifier rejects every form of hallucination listed in §11.3.4.

### 11.6 Risks

- **LLM cost:** at ~1k input + 400 output tokens per narrative,
  budget caps must be in code (`max_tokens=600` hard cap).
- **Hallucination:** the verifier is the safety net. Failing-narrative
  fallback writes a templated narrative with placeholder language
  rather than crashing the API.
- **Latency:** narration is precomputed on a schedule, not on the
  request path. Cache to `lake/curated/narratives/<case_id>.md`.

---

## 12. Phase 15 — Frontend repoint + ciudadano-agent UI

**Owner profile:** frontend engineer.
**Prereq:** Phase 11.5 (API services rewired) + Phase 13 (scores) +
Phase 14 (narratives) green.
**Effort:** **12–14 days.**

### 12.1 Goal

The case browser, narrative reader, and ciudadano-agent chat are
**net-new pages**, not a "repoint" — the existing `frontend/` has
no equivalent surfaces. This phase delivers (1) live anomaly scores
from Phase 13, (2) generated narratives from Phase 14, (3) a
citizen-facing chat agent backed by `POST /agent/query` (per
`program_plan.md` §3.6).

### 12.2 Implementation steps

1. **API contract refresh.** Generate `docs/contracts/api.openapi.yaml`
   from FastAPI's `/openapi.json`. Run `npm run api:contract-test`
   (Schemathesis) — must pass.
2. **Routes added in `api/src/coacc/routers/`:**
   - `GET /cases?limit&offset&min_score` → paginated list (joins
     anomaly_scores + narratives).
   - `GET /cases/{case_id}` → detail (subgraph + narrative + evidence).
   - `POST /agent/query` → `{question}` → `{answer, citations[],
     subgraphs[]}`.
3. **Frontend pages:**
   - `/casos` — list view with score, badge, last-update.
   - `/casos/<id>` — detail with subgraph viz (reuse existing graph
     component if present), narrative rendered from markdown, evidence
     table with clickable `datos.gov.co` links.
   - `/agente` — chat UI with citation chips that link back to
     `/casos/<id>`.
4. **MSW handlers for the new endpoints** under
   `frontend/src/mocks/handlers.ts` so dev works offline.
5. **Playwright e2e** under `frontend/tests/e2e/`:
   - `casos-list.spec.ts` opens `/casos`, asserts ≥1 card.
   - `caso-detail.spec.ts` opens a fixture case, asserts narrative
     sections, clicks an evidence link, asserts 200.
   - `agente.spec.ts` posts a fixture question, asserts citation
     chips, clicks one, asserts navigation.
6. **Polish pass:** empty states, error boundaries, loading skeletons,
   `axe` a11y clean on every route.

### 12.3 DoD

- [ ] All three new routes accessible from the navigation.
- [ ] Playwright e2e green.
- [ ] `axe-core` zero P0/P1 violations across all routes.
- [ ] Vite build succeeds with no console errors on production mode.
- [ ] Screenshots committed under `docs/screenshots/`.

### 12.4 Risks

- **Subgraph viz performance:** subgraphs over ~200 nodes lag in the
  current graph component. Cap at 200 with a "show more" affordance;
  precompute the cap server-side.
- **Agent latency:** if `POST /agent/query` round-trips an LLM, set a
  client-side 30s timeout and stream tokens via SSE. Defer streaming
  to post-finals if it slips the deadline; spinner is acceptable.

---

## 13. Phase 16 — Compliance, documentation, competition submission

**Owner profile:** PM / comms lead (the user, in this solo-dev case).
**Prereq:** Phases 7–15 green; rubric self-score ≥85.
**Effort:** 5 days + finals trip.

This phase is the conversion of the technical work into a competition
entry. It mirrors `program_plan.md` Track C, but specialized to today's
reality.

### 13.1 Steps

1. **Inscripción / eligibility.** The official MinTIC updates list
   **2026-04-30** as the registration cutoff and say that **349 teams
   advanced on 2026-05-06**. Archive the existing registration and/or
   selection confirmation under
   `docs/competition/artifacts/inscripcion_<date>.eml`. If no artifact
   exists, withdraw the 2026 competition track and preserve this plan for
   a 2027 or non-competition launch.
2. **Team yaml.** `docs/competition/team.yml` populated; `rules_check.py`
   green on R1, R2, R3, R10.
3. **Repo public-readiness audit.** `python scripts/repo_publish_audit.py`
   (build it as part of this phase if absent): scans for `CLAUDE.md`,
   `AGENTS.md`, `.env*`, `lake/`, `govt data roadmap/`, secret regex,
   files >10MB. Exits 1 on any finding. Wire as pre-push hook.
4. **`herramientas.datos.gov.co/usos` registration.** Submit the entry
   pointing to the public repo + live demo URL (or a recorded video
   if no hosted demo). Save the resulting URL in
   `docs/competition/decision_log.md`.
5. **CRISP-ML doc.** `docs/crisp_ml.md` mapping phases 1–6 to actual
   artifacts in this repo. Every phase links to a real file path.
   Run `scripts/check_doc_links.py` (build if absent) — must exit 0.
6. **Architecture diagram.** Update `docs/architecture/overview.md`
   to show the post-Phase-15 reality: lake → curated → graph + scores
   + narratives → API → frontend.
7. **Dress rehearsal.** Live demo on the real stack with someone
   outside the team scoring against the rubric. Recorded.
8. **Rubric self-score ≥85.** `python scripts/rubric_score.py` (build
   if absent) per `program_plan.md` §6.2 — emits one row per Friday
   with the six-cell rubric breakdown.
9. **Backup video** of the demo on a USB stick, tested offline the
   night before finals.
10. **Travel.** ≥1 in-person attendee in Bogotá first week of August
    2026. Hotel + flight booked at least 3 weeks ahead.

### 13.2 DoD

- [ ] Inscripción confirmed, archived.
- [ ] Repo public-readiness audit clean.
- [ ] `usos` URL live and pointing to the repo.
- [ ] `docs/crisp_ml.md` link-check clean.
- [ ] Architecture diagram current.
- [ ] Dress rehearsal recorded; rubric ≥85.
- [ ] Travel booked, in-person attendee confirmed.
- [ ] Backup video on USB.

---

## 14. Operations runbooks (created during phases, indexed here)

| Runbook | Created in | Purpose |
|---|---|---|
| `docs/runbooks/ingest_log.md` | Phase 7 | Append-only log of bulk ingest runs |
| `docs/runbooks/lake_reality.md` | Phase 8 | How to read reality reports + tune thresholds |
| `docs/runbooks/custom_adapters.md` | Phase 9 | Per-adapter operational + ethical notes |
| `docs/runbooks/curate.md` | Phase 10 | How to rebuild `lake/curated/` from scratch |
| `docs/runbooks/graph_loader.md` | Phase 11 | Reload, recovery, parity check ops |
| `docs/runbooks/anomaly_model.md` | Phase 13 | Retrain, monitor, recalibrate |
| `docs/runbooks/narrator.md` | Phase 14 | Provider failover, cost cap, cache invalidation |
| `docs/runbooks/env.md` | Phase 7 (created) | Every env var the system reads |

---

## 15. Risk register (cross-cutting)

| # | Risk | Severity | Mitigation | Owner |
|---|---|---|---|---|
| R-A | Phase 11 graph schema drifts from API queries | Critical | Lock `graph_schema.md` first, parity test in CI | Data eng |
| R-B | Anomaly model fails to beat baseline | High | Two-stage model, document iforest-only fallback | ML eng |
| R-C | Narrator hallucinates dataset IDs | High | Verifier guards every citation; reject non-catalog IDs | ML eng |
| R-D | Disk fills mid-Phase-7 ingest | Medium | Pre-flight `df -h`, `make lake-compact` available | Data eng |
| R-E | Inscripción deadline missed | Disqualifying | Official cutoff was 2026-04-30; by 2026-05-15 either archive proof of registration/selection or stop 2026 competition work | Comms |
| R-F | Repo leaks `CLAUDE.md` / secrets | Disqualifying | `repo_publish_audit.py` pre-push hook | Comms |
| R-G | Solo-dev burnout from a 9.5-10-week engineering runway | High | Start from the 80% branch unless Phase 7 is already green; protect rest days | User |
| R-H | Wave 6 placeholder YAMLs (106) tempt scope creep | Medium | Tier-by-tier: only promote a placeholder to core when a downstream signal needs it | Data eng |
| R-I | Competition rubric reinterpreted by jury | Medium | Header-text rubric is canonical until MinTIC publishes the instrument; weekly `rubric_score.py` | Comms |
| R-J | Live demo wifi fails at finals | Medium | Backup video + local-stack laptop tested offline | Comms |
| R-K | 2026-05-15 schedule has negative slack versus the full plan | High | Friday velocity review per §18 descope tree; cut to 80% branch immediately if Phase 7 is not already green, 60% on the next miss | User |
| R-L | `signal_materializer.py` (1023 LOC) hides dependencies on legacy CSV not surfaced by tests | Medium | Phase 11.5 EXPLAIN sweep + integration test catches structural breaks; semantic regressions caught by Phase 8 reality probe | Backend eng |
| R-M | Anomaly model "scores public officials as risky" is reputation-sensitive | High | Narrator ethics guard rejects criminality verbs; model card cites `ETHICS.md`; UI labels scores as "patrón" not "fraude" | ML eng |
| R-N | NIT canonicalization gets the verification digit wrong → bad joins everywhere | High | Hypothesis property tests + DIAN MOD-11 algorithm validated against 100 known-good NITs before Phase 10 lands | Data eng |
| R-O | Operator laptop cannot hold Phase 7 datasets or graph projection in RAM | High | §2.8 bounded-memory contract; Phase 7 streaming ingest blocker; Phase 11 `--scope finals` pruned graph | Data eng |

---

## 16. Definition of "competition-ready"

The single test: a non-team observer, given only the public repo URL,
can in 15 minutes:

1. Read `README.md` and understand what co/acc does.
2. Click through to a live demo (or watch a recorded one) showing a
   flagged case with a generated narrative and clickable evidence.
3. Find `docs/crisp_ml.md` and trace any one phase to a real artifact.
4. Find `docs/ai/anomaly_model.md` and `docs/ai/generative_narrator.md`
   and read the model cards.
5. Find at least one `lake/meta/reality/<date>.json` snapshot.
6. Read `docs/competition/datos_al_ecosistema_2026.md` and see how
   every R-rule (R1–R12) is satisfied.

If any of those breaks, the entry is not competition-ready.

---

## 17. Decision log (append-only)

Format: `YYYY-MM-DD — decision — rationale — links`.

- **2026-05-06** — Adopt this plan as the canonical post-refactor
  roadmap. Supersedes the "next phases" stubs in `docs/cleanup/plan.md`
  Phase 4 § for sequencing purposes; that doc remains the historical
  record of Phases 0–3.
- **2026-05-06** — Phase 9 (custom adapters) deferred to post-finals
  by default. Pull forward at most one adapter (priority order:
  paco_sanctions → official_case_bulletins → control_politico) if
  the flagship demo signal demands it.
- **2026-05-06** — Phase 11 (graph loader) confirmed as the keystone
  on the critical path. Alternate "rewrite all queries to DuckDB"
  rejected as too risky for the August 2026 deadline given 100+
  Cypher templates already exist.
- **2026-05-06** — `paco_sanctions` adapter promoted from deferred
  Phase 9 to critical-path Phase 9.0 (3–4 days). Rationale: Phase 13
  supervised top-up needs sanctioned-supplier labels; without them
  the model is iforest-only and the R5 rubric cell is weaker.
- **2026-05-06** — Phase 12 (legacy CSV retirement) moved from
  post-Phase-11 to post-Phase-7. Rationale: it's independent of the
  graph loader; running it in parallel with Phase 8 frees critical-
  path time. The original sequencing was a mistake.
- **2026-05-06** — New Phase 11.5 (API service rewires) inserted
  between Phase 11 and Phase 12. Rationale: original plan ignored
  `signal_materializer`, `score_service`, `case_service` rewires
  required after Phase 11 — surfacing them as their own phase makes
  the work visible and testable.
- **2026-05-06** — Phase 14 narrator commits to **DuckDB on parquet**
  for subgraph extraction, not Neo4j. Rationale: cheaper,
  parallelizable, doesn't add Neo4j load on the demo path; lake is
  already canonical. Side benefit: drops Phase 11 prereq, lets
  narrator start as soon as Phase 13 produces scores.
- **2026-05-06** — Effort estimates recalibrated to high-end +1.2×
  for solo-dev. New realistic critical-path = 13 weeks (zero slack).
- **2026-05-06** — §18 descope tree added. Three branches at 95%/80%/
  60% confidence; cut Phase 15 ciudadano-agent first, narrator second.
  Phase 13 anomaly model is the floor (R5 compliance).
- **2026-05-06** — `supplier_win_rate_buyer` feature dropped from
  Phase 13 MVP. Rationale: requires offers-to-process linkage that
  scope-creeps Phase 7. Replaced with `share_of_buyer_total_value_12mo`
  which uses contracts only.
- **2026-05-06** — `wi7w-2nvm` (SECOP II Ofertas) added to Phase 7
  ingest list. Rationale: needed for `single_bidder` feature
  confirmation in Phase 13; cheaper to ingest now than backfill later.
- **2026-05-08** — Phase 9.0 effort bumped 3–4d → 4–5d. Rationale:
  the original estimate elided two real subtasks: creating
  `etl/datasets/paco_sanctions.yml` (the YAML didn't exist; YAMLs
  are source of truth) and extending `DatasetSpec` (adding the
  `adapter` field + relaxing `_validate_socrata_id` for
  non-Socrata IDs). Both are now explicit steps 1–2 in §5.0.1.
- **2026-05-08** — `DatasetSpec` schema change codified: a new
  `adapter` field with a Literal-typed value, default `"socrata"`.
  Validator `_validate_socrata_id` becomes conditional on
  `adapter == "socrata"`. All 12 deferred sources in Phase 9.1
  must add their adapter name to the Literal when activated.
- **2026-05-08** — Phase 12 inventory expanded: `docker-compose.yml`
  (4 references) and `.gitignore` exception line added to §9.2.
  These were missed in the first pass and are runtime-critical
  (compose) / deletion-blocking (gitignore) respectively.
- **2026-05-08** — Phase 7 sequence reordered: `wi7w-2nvm` now
  runs first (smallest, ~3M rows) to validate end-to-end before
  multi-million-row datasets commit hours of wall-clock time.
- **2026-05-08** — Phase 10 NIT canonicalization scope clarified:
  `canonical_nit` is for entity NITs only; `canonical_cedula` lives
  under `dim_person` and handles SIGEP `7y2j-43cv` person IDs.
  Cross-pollination guard added to the contract tests.
- **2026-05-08** — Phase 8 `null_rate` red-threshold formula made
  explicit: `null_rate[col] > (1 − required_coverage[col]) + 0.01`.
  Prior wording ("rises ≥1pp above declared threshold") inverted
  the semantic given that YAML stores coverage, not null rate.
- **2026-05-15** — Review corrected current-state counts and schedule
  risk: `catalog.signed.csv` has 311 signed rows, `catalog.proven.csv`
  has 148 proven join-key rows, and `etl/datasets/` has 42 ingest-ready
  contracts (29 incremental + 13 snapshot). The calendar runway is now
  ~11.5-12 weeks, leaving only ~9.5-10 engineering weeks after rehearsal
  reserve, so the full 13-week plan no longer fits unless Phase 7 is
  already complete.
- **2026-05-15** — Eligibility blocker added. MinTIC's public updates
  put registration close at 2026-04-30 and the technical-stage advance
  at 2026-05-06. Phase 16 now starts by archiving proof of registration
  or selection; without it, the 2026 competition track stops. Links:
  [2026-04-28 registration cutoff](https://www.mintic.gov.co/portal/inicio/Sala-de-prensa/Noticias/437417:Ultimos-dias-para-activar-soluciones-de-impacto-social-con-Datos-al-Ecosistema-2026),
  [2026-05-06 technical stage + finals](https://www.mintic.gov.co/portal/inicio/Sala-de-prensa/Noticias/437759:Mas-de-1-000-participantes-de-todo-el-pais-avanzan-en-el-reto-de-convertir-datos-publicos-en-soluciones-reales).
- **2026-05-15** — Bounded-memory contract added. Rationale: Phase 7
  sources reach tens of millions of rows and the operator machine may not
  fit full datasets or graph projections in RAM. Graph loading gets a
  `--scope finals` pruned path so patterns can be connected from indexed
  IDs without loading all endpoints at once.
- **2026-05-15** — Socrata ingest bounded-memory blocker resolved in
  `coacc_etl.ingest.socrata`: pages now land in lake-local staging,
  coverage is counted incrementally, watermarks derive from incremental
  max timestamp tracking, and final raw parquet is published only after
  validation passes. Focused ingest/lakehouse tests cover multi-page
  staging and cleanup on coverage failure.
- **2026-05-15** — Socrata pagination defaults raised/configured for
  Phase 7 volumes. Default page size is now 10,000 rows and max pages
  remains 10,000 (100M-row cap); operators can override with
  `COACC_SOCRATA_PAGE_SIZE`, `COACC_SOCRATA_MAX_PAGES`, `--page-size`,
  and `--max-pages`.
- **2026-05-15** — Phase 7 runner added. `make ingest-phase7-smoke`
  and `make ingest-phase7-full` now wrap the Phase 7 sequence with
  disk preflight, smoke watermark seeding, mode-specific pagination,
  stop/continue-on-error behavior, and append-only
  `docs/runbooks/ingest_log.md` logging.
- **2026-05-15** — Phase 7 source list corrected against YAML source of
  truth. Earlier draft mislabeled `7y2j-43cv` as SIGEP servidores
  (it is IGAC property transactions), mislabeled `c82u-588k` as SECOP I
  processes (it is RUES identity), omitted the actual SECOP II processes
  source `p6dx-8zbt`, and underestimated `wi7w-2nvm` at ~3M rows (YAML
  says 41.9M). Runner order now follows corrected source semantics and
  runs the largest source last, superseding the 2026-05-08 `wi7w-2nvm`
  first decision.
- **2026-05-15** — Phase 7 live smoke exposed a future-date watermark
  hazard in `rpmr-utcd`: 109 of 14,865 smoke rows had
  `fecha_de_firma_del_contrato` beyond the plausible future window,
  including far-future years. The Socrata ingester now preserves those
  rows in `year=0/month=00/`, excludes them from watermark advancement,
  and documents the configurable `COACC_WATERMARK_FUTURE_GRACE_DAYS`
  ceiling. Corrected smoke for `rpmr-utcd` advanced to
  `2026-05-15T00:00:00+00:00` instead of the corrupt 2099 value.

(Append new decisions as they're made. One line per decision.)

---

## 18. Descope tree (the safety valve)

The original 13-week schedule had zero slack and was anchored on
2026-05-06. At the 2026-05-15 review, the full plan has negative slack
unless Phase 7 is already green. **Velocity must be reviewed every
Friday.** If the gates below are not met by the dates listed, drop to
the next confidence branch. Don't negotiate; cut.

### 18.1 95% confidence branch — full plan

**Gate (2026-06-12):** Phases 7, 8, 9.0, 12 all green. Phase 10
underway with at least `dim_company` parquet committed.

**Ship:** everything in §1. Anomaly model with supervised top-up,
generative narrator, ciudadano-agent UI.

### 18.2 80% confidence branch — drop the agent UI

**Trigger:** 2026-06-12 missed Phase 10 start, or 2026-07-03 still has
Phase 11 in flight. Also the default from 2026-05-15 if Phase 7 is not
already green.

**Cuts:**
- **Drop Phase 15.3 ciudadano-agent.** Frontend ships only the case
  browser and narrative reader. The agent route, MSW handler, and
  Playwright spec are deleted.
- **Phase 14 narrator runs in batch only**, not on-demand. All cases
  pre-generated in a single nightly run; UI reads cached
  `lake/curated/narratives/`.
- **Phase 13 stays full-featured** (it's the R5 ML core).
- **Phase 9 (remaining adapters) stays deferred** — no change.

**Saves:** ~5 working days from Phase 15. Frees one extra week of
buffer for Phase 11 or Phase 13.

**Rubric impact:** loses ~3 points on "Diseño / usabilidad" cell
(no agent surface). Net rubric still ≥80 if other cells hold.

### 18.3 60% confidence branch — drop the narrator too

**Trigger:** 2026-07-10 has Phase 13 still in flight, or Phase 11
still not green.

**Cuts:**
- **Drop Phase 14 generative narrator entirely.** Case detail page
  shows the score, the feature attributions (top contributing
  features in plain Spanish from a static template), and the
  evidence table — but no LLM-generated narrative.
- **Drop Phase 15 ciudadano-agent** (already cut in 80% branch).
- **Phase 13 anomaly model stays** — without it the entry doesn't
  meet R5 and is disqualified. This is the floor.
- **Phase 11 graph loader stays** — without it the API has no data
  to surface.

**Saves:** ~9 working days from Phases 14+15. Buys two weeks for
Phase 11 + 13 stabilization.

**Rubric impact:** loses the "IA generativa" half of the §11.3 R5
component. Compensate by leaning hard on the anomaly model in the
demo: feature attributions, calibration plot, precision@k table all
become primary demo content. Net rubric ~70–75. **Below the §16
target of 85.** Risky but submission-viable.

### 18.4 What is NOT cuttable

- **Phase 7** — without the lake, nothing.
- **Phase 8** — without monitoring, you don't know what's broken.
- **Phase 11** — without the graph reload, the API serves nothing.
- **Phase 13** — without the model, R5 is not satisfied.
- **Phase 16** — without the inscripción, no entry.

If you're at **2026-07-24** and any of those are still red,
withdraw the entry and prepare for 2027.

---

## 19. What this plan deliberately does **not** cover

- **Streaming ingestion.** Daily batch ingest is sufficient for
  competition. Real-time is post-finals.
- **Multi-tenant or hosted SaaS.** The deliverable is a public repo
  + a single demo deployment.
- **Mobile UI.** Frontend is desktop-first; responsive on tablets is
  best-effort, mobile post-finals.
- **Internationalization beyond Spanish.** UI strings stay Spanish;
  English README is acceptable.
- **Heavy MLOps.** Models are retrained on operator command, not on
  a schedule, until post-finals.
- **External authentication.** API uses the existing
  `coacc.services.auth_service`; no SSO until post-finals.

If a request lands that touches one of these, push back to the decision
log first.

---

**Read also:**
- `docs/cleanup/plan.md` — historical Phases 0–3 record.
- `docs/cleanup/refactor_plan.md` — Wave 0–6 history.
- `docs/architecture/overview.md` — current architecture diagram.
- `docs/competition/datos_al_ecosistema_2026.md` — competition rules.
- `docs/competition/program_plan.md` — track-based program plan
  (D/M/F/R/C). This file's phase numbering aligns with that doc's
  WP groupings (D/R covered by Phases 7–12; M by 13–14; F by 15;
  C by 16).
- MinTIC official updates:
  [2026-04-28 registration cutoff notice](https://www.mintic.gov.co/portal/inicio/Sala-de-prensa/Noticias/437417:Ultimos-dias-para-activar-soluciones-de-impacto-social-con-Datos-al-Ecosistema-2026)
  and
  [2026-05-06 technical-stage/finals notice](https://www.mintic.gov.co/portal/inicio/Sala-de-prensa/Noticias/437759:Mas-de-1-000-participantes-de-todo-el-pais-avanzan-en-el-reto-de-convertir-datos-publicos-en-soluciones-reales).
