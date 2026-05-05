# co/acc Refactor Plan — post-qualification

**Status:** draft, awaiting Wave 0 kickoff
**Owner:** solo dev (user)
**Source of truth:** `etl/src/coacc_etl/source_qualification.py` outputs —
  `docs/datasets/source_qualification_catalog.csv` (311 rows),
  `docs/datasets/source_qualification_proven.csv` (148 rows),
  `docs/datasets/source_qualification_report.md`.
**Predecessor:** `docs/cleanup/plan.md` (Phases 0–1 done, Phase 2+ superseded by this doc).
**Companion:** `docs/competition/program_plan.md` (competition tracks — unchanged).

---

## Decisions locked with user

| # | Question | Decision |
|---|---|---|
| 1 | Non-Socrata bespoke pipelines (PACO, official_case_bulletins, adverse_media, judicial_cases, etc.) | **Backlog.** Park. Not migrated during this refactor. Keep their source files untouched until post-MVP, or delete along with Neo4j path if trivially dead. |
| 2 | Neo4j loader path (`base.py`, `loader.py`, `linking_hooks.py`, all pipeline bodies) | **Full removal.** Re-introduce a graph loader in Phase 5 as a lake consumer, not as ETL. |
| 3 | `config/signal_registry_co_v1.yml` + `config/bootstrap_all_contract.yml` alignment | **Wave 6.** Defer until catalog is canonical. |
| 4 | Wave 1 (dead code) and Wave 2 (YAML catalog) | **Parallel commits** allowed. Independent surfaces. |
| 5 | Core (118) vs context (30) datasets | **Core first.** Core keys — `nit`, `contract`, `process`, `entity` — are the anticorruption join keys (procurement, contracts, beneficiaries, sanctions). Context keys — `bpin`, `divipola` — are enrichment only. Ingest core 118 in this refactor. Park context 30 as `tier: context` YAMLs but do not wire into ingest-all yet. |

---

## Guiding principles

Carry forward from `docs/cleanup/plan.md`:

1. **Qualification is the gate.** Nothing enters the lake unless the signed catalog lists it. Nothing in the signed catalog lacks proven join keys.
2. **Config-driven, not pipeline-driven.** One generic Socrata ingester reads per-dataset YAML. Bespoke Python bodies only where the source genuinely cannot be expressed as a Socrata pull.
3. **Reproducible from empty.** Ingest-all replays from zero.
4. **Sanity checks at every boundary.** Coverage, watermark, partition, determinism — tested, not assumed.
5. **Nothing deletes without proof.** `git log --all --oneline -- <path>` + grep for references before kill.

New principles for this refactor:

6. **Signal value dictates scope.** Core join keys ingested first — they produce anticorruption signals. Context joins park as enrichment YAMLs, unwired.
7. **Neo4j is downstream, not ETL.** ETL writes parquet. Period. Graph loader reads parquet. No pipeline writes Neo4j directly.
8. **Tests parameterise over the catalog.** One generic contract test replaces ~60 per-pipeline test files.

---

## Target folder layout

Proposed reorg. All moves are in Waves 1–6; this is the end state.

```
co-acc/
├── etl/
│   ├── src/coacc_etl/
│   │   ├── __init__.py
│   │   ├── cli.py                       # was runner.py; simplified, ingest-only
│   │   ├── catalog/                     # NEW — runtime authority over signed catalog
│   │   │   ├── __init__.py
│   │   │   ├── models.py                # Pydantic DatasetSpec
│   │   │   ├── loader.py                # reads etl/datasets/*.yml, validates
│   │   │   └── signed.py                # reads docs/datasets/catalog.signed.csv
│   │   ├── qualification/               # SPLIT from 2035-LOC source_qualification.py
│   │   │   ├── __init__.py
│   │   │   ├── inputs.py                # audit json + signal deps + env-backed IDs
│   │   │   ├── socrata_probe.py         # metadata/schema pulls
│   │   │   ├── llm_review.py            # Gemini review (gemini-2.5-flash-lite)
│   │   │   ├── promotion.py             # join-key classifier + promotion policy
│   │   │   └── report.py                # writes catalog/proven/report/cache
│   │   ├── ingest/                      # NEW
│   │   │   ├── __init__.py
│   │   │   ├── socrata.py               # generic YAML-driven Socrata ingester
│   │   │   ├── coverage.py              # coverage gate
│   │   │   ├── watermark.py             # watermark gate (uses lakehouse.watermark)
│   │   │   └── custom/                  # non-Socrata adapters (backlog park)
│   │   │       └── __init__.py
│   │   ├── lakehouse/                   # EXISTING — keep
│   │   │   ├── writer.py
│   │   │   ├── reader.py
│   │   │   ├── reality.py
│   │   │   ├── watermark.py
│   │   │   ├── paths.py
│   │   │   └── compactor.py
│   │   ├── transforms/                  # EXISTING — keep
│   │   └── schemas/                     # EXISTING — keep
│   ├── datasets/                        # NEW — 118 core YAML contracts (+ 30 context parked)
│   │   ├── _schema.yml                  # JSON-schema for DatasetSpec (docs-only)
│   │   ├── jbjy-vk9h.yml                # SECOP II Contratos (example)
│   │   └── ...                          # one per row in catalog.signed.csv
│   ├── tests/
│   │   ├── test_cli.py
│   │   ├── test_catalog.py
│   │   ├── test_qualification/
│   │   │   ├── test_socrata_probe.py
│   │   │   ├── test_llm_review.py
│   │   │   └── test_promotion.py
│   │   ├── test_ingest/
│   │   │   ├── test_socrata_contract.py      # parameterised over all core YAMLs
│   │   │   └── test_coverage.py
│   │   ├── test_lakehouse.py
│   │   ├── test_transforms/
│   │   └── fixtures/                    # scoped to signed catalog only; prune ruthlessly
│   └── pyproject.toml
├── api/                                 # UNCHANGED — separate workspace
├── frontend/                            # UNCHANGED — separate workspace
├── infra/
│   ├── docker/                          # MOVE Dockerfile + docker-compose*.yml here
│   ├── neo4j/                           # existing
│   └── caddy/                           # existing Caddyfile grouped here
├── config/
│   ├── signals/                         # existing
│   ├── signal_registry.yml              # rename: drop "_co_v1"
│   ├── signal_source_deps.yml           # keep
│   └── source_url_exceptions.yml        # keep
├── docs/
│   ├── competition/                     # UNCHANGED
│   ├── cleanup/
│   │   ├── plan.md                      # Phases 0–1 (frozen)
│   │   ├── refactor_plan.md             # THIS DOC (Waves 0–6)
│   │   ├── inventory.md                 # historical
│   │   └── cleanup_review.signed.csv    # historical
│   ├── datasets/
│   │   ├── catalog.signed.csv           # PROMOTED from source_qualification_catalog.csv
│   │   ├── catalog.proven.csv           # PROMOTED from source_qualification_proven.csv
│   │   ├── catalog.report.md            # PROMOTED from source_qualification_report.md
│   │   ├── colombia_open_data_audit.json
│   │   ├── qualification_architecture.md
│   │   └── archive/                     # retired inputs kept for history
│   │       ├── dataset_relevance_appendix.csv
│   │       ├── source_registry_co_v1.csv
│   │       ├── source_qualification_llm_cache.json
│   │       ├── current_dataset_relevance_review.md
│   │       └── secop_manual_review.md
│   └── architecture/                    # NEW — one-page overview (optional, Wave 6)
├── scripts/
│   ├── capture_fixture.py
│   ├── check_compliance_pack.py
│   ├── check_open_core_boundary.py
│   ├── check_public_privacy.py
│   ├── ci/
│   └── lake_reality.py                  # thin wrapper; real code in coacc_etl.lakehouse.reality
├── lake/                                # gitignored runtime
├── Makefile                             # trimmed — ingest, ingest-all, qualify, lake-reality, test
└── [LICENSE, README.md, SECURITY.md, ETHICS.md, PRIVACY.md, TERMS.md, DISCLAIMER.md, LGPD.md, ABUSE_RESPONSE.md, CONTRIBUTING.md, CHANGELOG.md]
```

Key deletions vs current:
- `etl/src/coacc_etl/pipelines/` entire directory (~95 files, ~23k LOC) — 35 stubs gone in Wave 1, ~60 bespoke converted to YAML in Wave 4.
- `etl/src/coacc_etl/base.py`, `loader.py`, `linking_hooks.py`, `bogota_secop.py`, `rues.py` — Neo4j path, removed across Waves 4–5.
- `etl/tests/pipelines/`, `etl/tests/integration/`, `etl/tests/fixtures/` (selective) — replaced by parameterised contract test.
- ~70 `etl/tests/test_*_pipeline.py` files — replaced.
- `docs/source_registry_co_v1.csv`, `docs/datasets/dataset_relevance_appendix.csv` — archived under `docs/datasets/archive/`.

---

## Waves

Each wave = one logical PR's worth of work. Green tests gate the next wave.

### Wave 0 — Source-of-truth promotion (doc + catalog rename, no code move)

**Goal:** declare the signed catalog canonical. Retire input artifacts.

- [x] **0.1** Rename qualification outputs in `docs/datasets/`:
  - `source_qualification_catalog.csv` → `catalog.signed.csv`
  - `source_qualification_proven.csv`   → `catalog.proven.csv`
  - `source_qualification_report.md`    → `catalog.report.md`
  - Keep `source_qualification_llm_cache.json` as is (runtime cache, not a signed artifact) — later moved to `docs/datasets/archive/` when the next qualification run writes a new one.
- [x] **0.2** Move retired inputs under `docs/datasets/archive/`:
  - `docs/datasets/dataset_relevance_appendix.csv`
  - `docs/datasets/current_dataset_relevance_review.md`
  - `docs/datasets/secop_manual_review.md`
  - **Deferred to Wave 6:** `docs/source_registry_co_v1.csv` is load-bearing for `api/src/coacc/services/source_registry.py`, `docker-compose.yml` mounts, and `etl/tests/test_source_registry_alignment.py`. Move only after API is repointed at the signed catalog.
- [x] **0.3** Update `etl/src/coacc_etl/source_qualification.py` paths to read/write the new filenames. `--appendix` default now points at `docs/datasets/archive/`.
- [x] **0.4** Update `docs/datasets/source_qualification_architecture.md` → renamed to `qualification_architecture.md`, rewritten to reference new canonical filenames and note retirement of appendix; registry retirement deferred to Wave 6.
- [x] **0.5** Update this plan + `docs/cleanup/plan.md` references to new filenames.

**Sanity check 0:** `coacc-etl qualify --all-known --metadata-only` still produces the same catalog under new filenames (diff == 0). Re-running after rename is idempotent.

**Gate 0:** user confirms `catalog.signed.csv` is the canonical ingest set. 118 core + 30 context.

---

### Wave 1 — Kill known-dead code (runs in parallel with Wave 2)

**Goal:** remove files with zero references. Registry auto-excludes them; deletion is mechanical.

- [x] **1.1** Deleted 39 LakeCsvPipeline stub pipelines (no `COACC_DATASET_*` env vars configured, no tests). Enumerated via size filter then verified against import graph.
- [x] **1.2** Deleted `lake_template.py` (all 39 importers gone); dropped from `_EXCLUDED_MODULES`. `colombia_procurement`, `colombia_shared`, `disclosure_mining`, `project_graph` **kept** — 64/37/1/6 live importers, they follow their dependents in Wave 4.
- [x] **1.3** Grepped every deleted path. Only matches were `config/signals/sql/*.sql` + `api/tests/unit/test_wave_b_signals.py` (aspirational signal references; never worked against live system because stubs were no-ops) — Wave 6 aligns those.
- [x] **1.4** No stub-specific test files existed. Also deleted 5 thin-alias subclasses (`actos_administrativos`, `tvec_orders_consolidated`, `environmental_files_corantioquia`, `gacetas_territoriales`, `judicial_providencias`) that only renamed a functional pipeline. Updated `test_pipeline_registry.py` to stop asserting on a deleted stub.
- [x] **1.5** No empty fixture dirs to clean (none existed for stubs).

**Sanity check 1:**
- `pytest etl/tests/` green after deletion.
- `python -c "from coacc_etl.pipeline_registry import list_pipeline_names; print(len(list_pipeline_names()))"` returns the expected non-stub count.
- `git grep -F <deleted_class_name>` empty for every removed class.

**Gate 1:** test suite green, `pipelines/` file count drops by ~35–40, no regressions in `coacc-etl sources`.

---

### Wave 2 — Emit YAML contracts from the signed catalog (runs in parallel with Wave 1)

**Goal:** one YAML per dataset in `etl/datasets/`. Generated from `catalog.signed.csv` + `catalog.proven.csv`.

- [x] **2.1** Defined `etl/src/coacc_etl/catalog/models.py` with a `DatasetSpec` Pydantic model:
  ```yaml
  id: jbjy-vk9h
  name: SECOP II - Contratos Electrónicos
  sector: Procurement
  tier: core            # core | context | backlog
  join_keys:
    contract: numero_del_contrato
    entity:   nit_entidad
    nit:      documento_proveedor
  watermark_column: ultima_actualizacion
  partition_column: fecha_de_firma
  columns_map:
    buyer_name:     nombre_entidad
    buyer_id:       nit_entidad
    supplier_name:  proveedor_adjudicado
    supplier_id:    documento_proveedor
    contract_value: valor_del_contrato
    signed_date:    fecha_de_firma
    updated_at:     ultima_actualizacion
  required_coverage:
    buyer_name:     0.95
    supplier_name:  0.95
    contract_value: 0.80
    signed_date:    0.80
  freq: weekly
  url: https://www.datos.gov.co/d/jbjy-vk9h
  notes: ""
  ```
- [x] **2.2** Wrote `scripts/bootstrap_dataset_yamls.py` (one-shot): read `catalog.proven.csv` → emitted 148 YAMLs (118 core + 30 context) under `etl/datasets/`. `watermark_column`, `partition_column`, `columns_map`, `required_coverage`, `freq` left as placeholders; Wave 4 fills them per dataset during migration. No `tier: backlog` rows needed at this stage.
- [x] **2.3** Implemented `coacc_etl.catalog.loader.load_catalog() -> dict[str, DatasetSpec]`. Validates every YAML against the Pydantic model on load; fails loud on id/filename drift or extra fields.
- [x] **2.4** `tests/test_catalog.py` asserts: (a) every `catalog.proven.csv` id has a matching YAML and vice versa, (b) every YAML validates, (c) tier distribution = 118 core + 30 context, (d) every core spec has at least one core join key, (e) context specs only hold context/entity classes, (f) no YAML is yet ingest-ready (Wave 2 placeholders only).
- [x] **2.5** Deleted `scripts/bootstrap_dataset_yamls.py` after use (YAMLs are now the source of truth; re-bootstrap would overwrite Wave-4 hand-edits).

**Sanity check 2:**
- Catalog loader returns exactly `len(catalog.signed.csv) == len(etl/datasets/*.yml)`.
- Every `tier: core` YAML has non-empty `watermark_column`, `partition_column`, `join_keys`, and at least one `required_coverage` threshold.
- Placeholder `tier: backlog` count is explicit and bounded — user acknowledges the number before Wave 3.

**Gate 2:** user reviews the count of core vs context vs backlog. Only `tier: core` datasets are migrated in Wave 4.

---

### Wave 3 — Generic Socrata ingester (lake-only, zero Neo4j)

**Goal:** one ingester module. Reads YAML → writes parquet. Passes sanity gates from `docs/cleanup/plan.md` Phase 3.

- [x] **3.1** Implemented `coacc_etl.ingest.socrata.ingest(spec: DatasetSpec)` — loads watermark from `lakehouse.watermark`, queries Socrata with `$where` + deterministic `$order` on `spec.watermark_column`, normalizes via `spec.columns_map`, enforces `spec.required_coverage`, writes parquet via `lakehouse.writer.append_parquet` under `lake/raw/source=<id>/year=YYYY/month=MM/`, advances watermark to `max(batch[spec.watermark_column])` — **never** `datetime.now()`. On coverage failure, writes report to `lake/meta/failures/<id>/` and does not advance.
- [x] **3.2** Added `coacc_etl.ingest.coverage.assert_coverage` + `write_failure_report` + `write_coverage_report`. Added `force=True` escape-hatch on `wm.set`/`wm.advance` used only by `ingest(full_refresh=True)`.
- [x] **3.3** Added subcommands to `coacc_etl.runner` (module rename to `cli.py` stays in Wave 5 per plan): `coacc-etl ingest <id>`, `coacc-etl ingest-all [--continue-on-error]`, `coacc-etl qualify <args>`. Dropped the `run --to-lake` branch entirely (pipelines no longer self-write parquet; everything flows through `ingest`).
- [x] **3.4** `tests/test_ingest/test_socrata_contract.py` + `conftest.py` covers: parquet written, columns renamed per `columns_map`, watermark = max parsed ts (never wall-clock), incremental skips when no new rows, coverage failure blocks watermark + writes failure report, partition gate rejects unparseable rows, non-ingest-ready spec refused, full-refresh bypasses watermark. New `test_yaml_is_either_placeholder_or_fully_ingest_ready` catches half-filled YAMLs.
- [x] **3.5** `tests/test_ingest/test_determinism.py` — two fresh-lake ingests of the same fixture produce byte-identical *content* (sorted frame comparison, since parquet file names and zstd metadata carry timestamps). Watermark ts matches exactly across runs because it derives from data, not wall-clock.
- [x] **3.6** First live end-to-end ingest against real Socrata: `8qxx-ubmq` (Hallazgos Fiscales, 73 rows). Selected over the plan's original `jbjy-vk9h` (SECOP II Contratos) because it is 6 orders of magnitude smaller, giving a sub-second feedback loop. Filled in its YAML with `fecha_recibo_traslado` as watermark + partition. Result: 73 rows written across 15 year/month partitions, coverage = 100% on all 4 tracked cols, watermark advanced to `2024-12-26T00:00:00+00:00`. Incremental re-run correctly issued `$where=fecha_recibo_traslado > '2024-12-26…'` and no-op'd. Coverage report at `lake/meta/coverage/8qxx-ubmq/`.

**Sanity check 3 (per dataset):**
- Coverage gate blocks on < threshold.
- Partition gate rejects any `partition_column` value outside the declared `year=/month=` boundary.
- Watermark gate rejects wall-clock-sourced watermarks (test via fake `time.time`).
- Determinism gate: two runs against same fixture = identical bytes.

**Gate 3:** all sanity checks green. At least 1 live ingest against real Socrata succeeds end-to-end.

---

### Wave 4 — Migrate bespoke pipelines to YAML + shed Neo4j

**Goal:** convert every Socrata-backed bespoke pipeline to a YAML + generic ingester. Neo4j disappears as a side effect.

Sequence: for each `tier: core` dataset currently implemented as a bespoke `pipelines/<name>.py`:

1. Capture a Socrata fixture via `scripts/capture_fixture.py` into `etl/tests/fixtures/<id>/sample.json` (if not already present).
2. Fill in the dataset's YAML (Wave 2 placeholder) with real `watermark_column`, `partition_column`, `columns_map`, `required_coverage` — source these from the bespoke pipeline's existing normalization code.
3. Parameterised contract test (Wave 3) automatically picks up the new YAML and fixture.
4. Delete the bespoke `etl/src/coacc_etl/pipelines/<name>.py`.
5. Delete `etl/tests/test_<name>_pipeline.py`, `etl/tests/test_download_connected_<name>.py` if present.
6. Run `pytest etl/tests/` — must stay green.
7. Commit.

After all core Socrata-backed pipelines migrated:

- [ ] **4.N-1** Delete `etl/src/coacc_etl/base.py`, `loader.py`, `linking_hooks.py` (Neo4j loader path).
- [ ] **4.N-2** Delete `etl/src/coacc_etl/pipeline_registry.py` — replaced by catalog loader.
- [ ] **4.N-3** Delete `etl/src/coacc_etl/bogota_secop.py`, `rues.py` if no remaining consumers (else park under `ingest/custom/`).
- [ ] **4.N-4** Drop `neo4j` from `etl/pyproject.toml` runtime deps.
- [ ] **4.N-5** Leave `etl/src/coacc_etl/pipelines/` non-Socrata backlog entries (PACO, official_case_bulletins, adverse_media, judicial_cases, etc.) under `etl/src/coacc_etl/ingest/custom/` as shells that raise `NotImplementedError("backlog — post-MVP")`. Their tests go with them or delete.

**Sanity check 4 (after each pipeline migrated):**
- `pytest etl/tests/` green.
- `git grep -F <deleted_class_name>` empty.
- Parameterised contract test covers the migrated dataset.

**Sanity check 4 (end of wave):**
- `from neo4j import GraphDatabase` appears nowhere in `etl/src/coacc_etl/` except possibly `ingest/custom/` backlog shells.
- `etl/src/coacc_etl/pipelines/` directory is gone (or contains only `__init__.py` if kept for import back-compat — prefer gone).
- `coacc-etl ingest-all` runs the full `tier: core` set against fixtures in under 30s.
- LOC in `etl/src/coacc_etl/` drops to ≤ ~3000 (from ~23000).

**Gate 4:** user signs off — core datasets ingest end-to-end without Neo4j. Backlog parked explicitly.

---

### Wave 5 — Folder reorg + qualification split

**Goal:** enact the target layout. Split the 2035-LOC qualification file.

- [x] **5.1** Split `etl/src/coacc_etl/source_qualification.py` into `etl/src/coacc_etl/qualification/` package — six submodules (promotion 466 LOC, socrata_probe 277, llm_review 542, inputs 342, report 167, cli 341) plus `__init__.py` (105) re-exports. Legacy import path preserved as a 90-line back-compat shim so the existing 7 qualification tests pass unchanged.
- [x] **5.2** `runner.py` → `cli.py`, entry point `coacc-etl = "coacc_etl.cli:cli"`. Trimmed Makefile from ~80 stale targets to ~12 working ones in the same commit (Wave 6.6 was overdue).
- [x] **5.3** `coacc_etl.catalog` package — already done in Wave 2.
- [x] **5.4** `coacc_etl.ingest` package — already done in Wave 3 + extended in Wave 4.C with snapshot mode.
- [x] **5.5** Folder reorg: orphan root `Dockerfile` deleted (no consumers; per-workspace Dockerfiles handled CI + compose); `infra/docker-compose.prod.yml` + `…prod.images.yml` → `infra/docker/`. Dev `docker-compose.yml` stays at root by deliberate deviation — moving it would force every operator to `cd infra/docker` or use `-f`. Updated `infra/scripts/{deploy,backup-neo4j}.sh` paths.
- [x] **5.6** `infra/Caddyfile` → `infra/caddy/Caddyfile`. Mount path in prod compose updated.
- [x] **5.7** `config/signal_registry_co_v1.yml` → `config/signal_registry.yml`. Single internal consumer (`api/src/coacc/services/signal_registry.py`) repointed; 196 api unit tests + 73 etl tests green.
- [x] **5.8** Imports / tests / Makefile updated per move; every step kept the suite green.

**Sanity check 5:**
- Every move preserves behavior: `pytest` green, `docker compose build` works, CI green.
- `find . -name '*.py' | xargs grep -l 'source_qualification\b' | grep -v archive` returns only files using the new `qualification` import path.

**Gate 5:** `tree etl` matches the target layout. All tests green. Docker builds.

---

### Wave 6 — Tests / docs / config alignment / Makefile trim

**Goal:** prune test surface, align configs, refresh docs.

- [x] **6.1** Per-pipeline test files: all gone (deleted across Wave 4.A + 4.B).
- [x] **6.2** `etl/tests/fixtures/`: pruned to nothing in Wave 4.B (every fixture's pipeline was retired).
- [x] **6.3** Signal registry/deps alignment shipped as 3 invariant tests in `etl/tests/test_signal_source_alignment.py`. One signal (`adverse_media_supplier_review_only`) marked `status: parked` because its required source has no ingest path. Audit found 24 aspirational sources never wired and 10 custom adapters known-deferred; both sets are explicit allow-lists in the test file.
- [x] **6.4** Status-update banner added to `docs/competition/program_plan.md` + `datos_al_ecosistema_2026.md` pointing at the YAML catalog + `coacc_etl.ingest.socrata.ingest` instead of the retired Pipeline stack.
- [x] **6.5** README rewritten — covers the YAML-contract flow, the two source classes (incremental + snapshot), the 3-command CLI, repo layout, where the Neo4j path went.
- [x] **6.6** Makefile trim done in Wave 5.2 (~80 → ~12 working targets).
- [x] **6.7** `docs/architecture/overview.md` — one-page trace from audit JSON → signed catalog → YAML → lake → API/signals, plus the two invariants (lake is canonical, watermarks derive from data) and "where to look for what" cheat-sheet.

**Sanity check 6:**
- Test file count drops dramatically (target: `etl/tests/` has < 15 `.py` test files vs current ~70).
- Every signal in `signal_registry.yml` maps to a dataset in `catalog.signed.csv` OR is explicitly marked `parked`.
- `make test` green. `make lake-reality` green. `make qualify` runs end-to-end.
- `du -sh etl/` drops materially.

**Gate 6:** `CHANGELOG.md` entry summarizes the refactor. Ready for Wave 7 (Phase 4/5 per `plan.md`: lake reality → downstream).

---

## What this refactor does **not** do

- **Phase 4 lake reality dashboard.** Covered by existing `scripts/lake_reality.py` + `coacc_etl.lakehouse.reality`; hardening is a later phase.
- **Phase 5 downstream** (Neo4j graph loader, anomaly model, narrator, API repoint, frontend). Prerequisite is 3 datasets green in lake reality for 3 consecutive days — gate from `plan.md`.
- **Competition submission tracks** — unchanged; `docs/competition/program_plan.md` stands.
- **Re-qualification.** The 148-row catalog is accepted as-is. Re-running qualification is a separate action, not part of this refactor.

---

## Risk log

| Risk | Mitigation |
|---|---|
| YAML bootstrap misses business logic in bespoke pipelines (custom normalization, value sanitization) | Migrate one dataset per commit; contract test must pass on golden fixture before deleting the `.py`. If normalization is too custom for generic Socrata path, park under `ingest/custom/`. |
| 118 core datasets have placeholder `watermark_column` / `partition_column` after Wave 2 | Wave 4 fills them in per dataset during migration. Datasets that can't be resolved → `tier: backlog`, do not ingest. |
| Neo4j removal breaks `api/` or `frontend/` | Grep confirmed: `api/` does not import `coacc_etl`. Frontend is unaffected. Only internal ETL code touches Neo4j. |
| `signal_registry_co_v1.yml` references sources no longer ingested | Wave 6 aligns registry to signed catalog. Mismatches = explicit `parked`. |
| Folder reorg churns imports across hundreds of call sites | Do moves one at a time; each move = one commit with `pytest` green. Target layout in this doc is non-negotiable; path is. |
| Running Wave 1 (deletions) and Wave 2 (YAML gen) in parallel → merge conflicts | Waves touch disjoint paths: Wave 1 touches `pipelines/` + stub tests; Wave 2 touches `etl/datasets/` (new) + `catalog/` (new). No overlap. |
