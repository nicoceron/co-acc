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

- [ ] **1.1** Delete 35 stub pipelines (12-line files with no class body). Enumerate via `find etl/src/coacc_etl/pipelines -name '*.py' -size -700c`.
- [ ] **1.2** Delete registry-excluded modules: `colombia_procurement.py`, `disclosure_mining.py`, `project_graph.py`, `lake_template.py`. Remove from `_EXCLUDED_MODULES` set.
- [ ] **1.3** Grep every deleted path against the rest of the repo. If any reference exists, stop and audit.
- [ ] **1.4** Delete the corresponding test files (should be few — stubs mostly lack tests).
- [ ] **1.5** Delete empty fixture subdirs.

**Sanity check 1:**
- `pytest etl/tests/` green after deletion.
- `python -c "from coacc_etl.pipeline_registry import list_pipeline_names; print(len(list_pipeline_names()))"` returns the expected non-stub count.
- `git grep -F <deleted_class_name>` empty for every removed class.

**Gate 1:** test suite green, `pipelines/` file count drops by ~35–40, no regressions in `coacc-etl sources`.

---

### Wave 2 — Emit YAML contracts from the signed catalog (runs in parallel with Wave 1)

**Goal:** one YAML per dataset in `etl/datasets/`. Generated from `catalog.signed.csv` + `catalog.proven.csv`.

- [ ] **2.1** Define `etl/src/coacc_etl/catalog/models.py` with a `DatasetSpec` Pydantic model:
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
- [ ] **2.2** Write `scripts/bootstrap_dataset_yamls.py` (one-shot): reads `catalog.signed.csv` + `catalog.proven.csv` → emits one `etl/datasets/<id>.yml` per row. 118 core + 30 context = 148 files. `watermark_column` / `partition_column` / `required_coverage` / `columns_map` are left as placeholders where unknown and flagged `tier: backlog` if unresolvable.
- [ ] **2.3** Implement `coacc_etl.catalog.loader.load_catalog() -> dict[str, DatasetSpec]`. Validates every YAML against the Pydantic model on load; fails loud.
- [ ] **2.4** Unit test: `test_catalog.py` loads all 148 YAMLs, asserts each dataset_id in `catalog.signed.csv` has a matching YAML (no drift).
- [ ] **2.5** Delete `scripts/bootstrap_dataset_yamls.py` after use (one-shot; the YAMLs become the source).

**Sanity check 2:**
- Catalog loader returns exactly `len(catalog.signed.csv) == len(etl/datasets/*.yml)`.
- Every `tier: core` YAML has non-empty `watermark_column`, `partition_column`, `join_keys`, and at least one `required_coverage` threshold.
- Placeholder `tier: backlog` count is explicit and bounded — user acknowledges the number before Wave 3.

**Gate 2:** user reviews the count of core vs context vs backlog. Only `tier: core` datasets are migrated in Wave 4.

---

### Wave 3 — Generic Socrata ingester (lake-only, zero Neo4j)

**Goal:** one ingester module. Reads YAML → writes parquet. Passes sanity gates from `docs/cleanup/plan.md` Phase 3.

- [ ] **3.1** Implement `coacc_etl.ingest.socrata.ingest(spec: DatasetSpec)`:
  - Load watermark from `lakehouse.watermark`.
  - Query Socrata with `$where` on `spec.watermark_column`.
  - Normalize columns via `spec.columns_map`.
  - Enforce `spec.required_coverage` via `coacc_etl.ingest.coverage.assert_coverage` — on fail, write failure report to `lake/meta/failures/<id>/<iso_ts>.json` and do not advance watermark.
  - Write to `lake/raw/<id>/year=YYYY/month=MM/part-<ts>.parquet` via `lakehouse.writer`.
  - Advance watermark to `max(batch[spec.watermark_column])` — **never** `datetime.now()`.
- [ ] **3.2** New CLI in `coacc_etl.cli`:
  - `coacc-etl ingest <id>` — run one dataset.
  - `coacc-etl ingest-all` — run every `tier: core` in dependency-safe order.
  - `coacc-etl qualify ...` — thin wrapper over `coacc_etl.qualification`.
  - Remove every Neo4j flag from the CLI.
- [ ] **3.3** Delete the `--to-lake` branch — all ingestion is lake-only now.
- [ ] **3.4** Parameterised test `test_ingest/test_socrata_contract.py`:
  - Iterates every `tier: core` YAML.
  - For each: loads a golden HTTP fixture, runs the ingester against it, asserts parquet written, coverage passes, watermark advanced correctly, partitions correct.
- [ ] **3.5** Determinism test: ingest same fixture twice → byte-identical parquet.
- [ ] **3.6** Run one live end-to-end ingest against real Socrata for `jbjy-vk9h`. Confirm parquet file appears; coverage report under `lake/meta/coverage/`.

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

- [ ] **5.1** Split `etl/src/coacc_etl/source_qualification.py` into `etl/src/coacc_etl/qualification/` package (`inputs.py`, `socrata_probe.py`, `llm_review.py`, `promotion.py`, `report.py`). Keep public API stable via `qualification/__init__.py` re-exports.
- [ ] **5.2** Rename `etl/src/coacc_etl/runner.py` → `etl/src/coacc_etl/cli.py`. Update entry points in `etl/pyproject.toml`.
- [ ] **5.3** Create `coacc_etl.catalog` package (models, loader, signed) per target layout.
- [ ] **5.4** Create `coacc_etl.ingest` package (socrata, coverage, watermark, custom/) per target layout.
- [ ] **5.5** Move `Dockerfile`, `docker-compose.yml`, `docker-compose.prod.yml`, `docker-compose.prod.images.yml` → `infra/docker/`. Update CI + `Makefile` paths.
- [ ] **5.6** Move `infra/Caddyfile` → `infra/caddy/Caddyfile`.
- [ ] **5.7** Rename `config/signal_registry_co_v1.yml` → `config/signal_registry.yml`. Grep + update every reference.
- [ ] **5.8** Update imports + tests + Makefile targets after every move. Each move = one commit.

**Sanity check 5:**
- Every move preserves behavior: `pytest` green, `docker compose build` works, CI green.
- `find . -name '*.py' | xargs grep -l 'source_qualification\b' | grep -v archive` returns only files using the new `qualification` import path.

**Gate 5:** `tree etl` matches the target layout. All tests green. Docker builds.

---

### Wave 6 — Tests / docs / config alignment / Makefile trim

**Goal:** prune test surface, align configs, refresh docs.

- [ ] **6.1** Delete remaining per-pipeline test files replaced by the parameterised contract test.
- [ ] **6.2** Prune `etl/tests/fixtures/` — keep only fixtures for datasets present in `catalog.signed.csv`.
- [ ] **6.3** Align `config/signal_registry.yml` + `config/bootstrap_all_contract.yml` with `catalog.signed.csv`:
  - Any `sources:` entry in `signal_source_deps.yml` that is not in the signed catalog → flag + escalate.
  - Any signal that depends only on `tier: backlog` sources → mark `status: parked`.
- [ ] **6.4** Update `docs/competition/program_plan.md` + `docs/competition/datos_al_ecosistema_2026.md` to reference new paths (quick grep + patch).
- [ ] **6.5** Rewrite `README.md` — new ingest flow, YAML contract example, where the signed catalog lives.
- [ ] **6.6** Trim `Makefile`: keep `qualify`, `ingest`, `ingest-all`, `lake-reality`, `test`, `lint`, `format`. Delete stale targets.
- [ ] **6.7** (Optional) Write `docs/architecture/overview.md` — one-page "how data flows from audit JSON → signed catalog → YAML → lake → signals → API".

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
