# co/acc Program Plan — Datos al Ecosistema 2026

> **Status update (Wave 6, 2026-05):** the architecture sections below were
> written before the Wave 4–5 refactor. The "generic Socrata pipeline" (R2)
> shipped as `coacc_etl.ingest.socrata.ingest(spec)`, driven by per-dataset
> YAML contracts under `etl/datasets/`. The bespoke `etl/src/coacc_etl/pipelines/`
> directory and its per-pipeline tests under `etl/tests/pipelines/` are gone
> — the equivalent contract tests now live in `etl/tests/test_ingest/`,
> `etl/tests/test_catalog.py`, and `etl/tests/test_signal_source_alignment.py`.
> Reality checks still flow through `coacc_etl.lakehouse.reality` +
> `scripts/lake_reality.py`. Treat the WP contracts below as the original
> intent; the runtime artifacts are the YAML catalog + ingest module.

**Purpose:** the complete execution plan for the competition entry. Every work package is designed so another teammate can own it in parallel via a published interface contract and a committed stub; no work package blocks another on anything other than a schema.

**How to read this:** this is the *program* plan. The *entry* plan (`datos_al_ecosistema_2026.md`) answers "what are we entering and why?". This document answers "who builds what, in what order, and how do we catch drift?"

**How reality checks work here — the three scales:**

- **Micro (per WP)**: each work package has a single command that prints green or red. No subjective "done".
- **Meso (weekly)**: integration rehearsal + rubric self-score + disqualification scan. Happens Fridays regardless of anyone's "feel".
- **Macro (pre-submission)**: full dress rehearsal against live Socrata + jury-style walkthrough.

---

## 1. Program shape

### 1.1 Tracks
| Track | What it owns | Can be staffed by |
|---|---|---|
| **D — Data Lakehouse** | Socrata ingest, normalization, partition, watermark, dedup, lake health | Data engineer |
| **M — Models / AI** | Feature engineering, anomaly classifier, generative narrator, model cards, CRISP-ML docs | ML engineer |
| **F — Frontend** | Public web app, case detail, agent UI, design polish | Frontend engineer / designer |
| **R — Registry / Roadmap coverage** | Sector-roadmap dataset inventory, generic Socrata pipeline, disk guard | Data engineer (can be same as D) |
| **C — Compliance + Comms** | Public repo audit, LICENSE, README, CRISP-ML doc, `herramientas.datos.gov.co/usos` registration, team/inscripción, finals logistics | Comms lead / PM |

### 1.2 Program principles
1. **Contracts, not schedules, couple the tracks.** Every track publishes input/output schemas first; implementation follows.
2. **Every downstream track has a working stub.** Nobody waits. Anomaly model ships against `FakeLake` fixtures; frontend ships against `FakeAPI`. Real plug-in is a swap, not a rewrite.
3. **No claim without a command.** Any commit that asserts "X works" must reference a script whose output was committed within the last 24h (§6.3 hook).
4. **Disqualification rules are tripwires, not checklists.** Every Friday we scan the 12 R-rules; any ambiguous one triggers a decision-log entry.

---

## 2. Dependency DAG

Work packages on the critical path are **bold**. Everything else runs in parallel behind a stub.

```
C4 (team + inscripción)           ──► runs first and in parallel with D0
C1 (repo hygiene audit)           ──► any time
C2 (LICENSE + README skeleton)    ──► any time

D0 (reality instrumentation)
 └─► **D1 (normalizer fix)**
       ├─► D2 (repartition)
       ├─► D3 (watermark fix)
       └─► **D4 (graph load unblock)**
             └─► D5 (SECOP family)  ─┐
R1 (registry table)                  │
 └─► R2 (generic Socrata pipeline)   │
       └─► R3 (disk guard + priority ingest)
                                     │
(contracts from D1 + D4) ──► M1 (feature spec)
                              └─► **M2 (anomaly classifier)**
                                    └─► M4 (eval + model card)
                              └─► **M3 (generative narrator)**
                                    └─► M5 (CRISP-ML doc)

(contract from D4 + M2 + M3 + API stub) ──► F1 (scaffold + FakeAPI)
                                              └─► F2 (case detail + narrator)
                                              └─► F3 (ciudadano-agent)
                                              └─► F4 (polish pass)

(M4 + M5 + F4 + D5 + R3) ──► C3 (usos registration)
                         ──► C5 (CRISP-ML + model cards published)
                         ──► C6 (finals dress rehearsal)
                         ──► C7 (finals presencial)
```

**Critical path**: C4 → D0 → D1 → D4 → (M2 ∥ M3) → M4 → M5 → C5 → C6 → C7. Every other WP has slack and can proceed behind a stub.

---

## 3. Interface contracts (published first, implemented second)

Contracts live as committed schema files under `docs/contracts/` and, where applicable, as Pydantic/Pandera/JSON Schema definitions under `etl/src/coacc_etl/contracts/`. **Changes to a contract require a migration note in `docs/contracts/CHANGELOG.md`.**

### 3.1 `LakeContract` — normalized SECOP row
Location: `docs/contracts/lake_secop_integrado.md` + `etl/src/coacc_etl/contracts/lake.py`

Fields (non-exhaustive):
- `contract_id: str` (non-empty, unique)
- `buyer_document_id: str` (non-empty)
- `buyer_name: str` (non-empty, ≥95% coverage)
- `supplier_document_id: str` (non-empty, ≥95% coverage)
- `supplier_name: str` (non-empty, ≥95% coverage)
- `contract_value: float | null` (≥80% non-null)
- `signed_date: date | null` (≥80% non-null — drives partitioning)
- `ultima_actualizacion: timestamp` (drives watermark)
- `department, municipality, modality, status, origin: str`
- Partition keys: `year=YYYY/month=MM` derived from `signed_date`; `unknown/unknown` when null.

Stub: `etl/tests/fixtures/secop_integrado/lake_sample.parquet` — 200 rows meeting the contract, generated from the live sample once and committed.

Reality check: `python scripts/lake_reality.py` — prints coverage % per field, delta vs Socrata, dup count. Thresholds defined in `docs/contracts/lake_secop_integrado.md`.

### 3.2 `FeatureContract` — one row per contract, model-ready
Location: `docs/contracts/features_v1.md`

Fields:
- `contract_id, buyer_document_id, supplier_document_id`
- `log_value, log_value_z_buyer, log_value_z_modality`
- `buyer_supplier_concentration: float ∈ [0, 1]`
- `supplier_win_rate_buyer: float`
- `timing_anomaly_score: float`
- `modality_value_mismatch: bool`
- `n_prior_contracts_12mo_buyer: int`

Stub: `scripts/emit_fake_features.py` — generates deterministic synthetic features from `lake_sample.parquet`. M2 trains against this until D4 is done.

Reality check: `python -m coacc_etl.features.audit` — prints per-feature null%, distribution summary, correlation matrix. Fails if any feature is ≥50% null.

### 3.3 `AnomalyScoreContract`
- Input: `FeatureContract` rows
- Output: `contract_id → score ∈ [0, 1]` + `reason: list[str]` (top contributing features, human-readable)

Stub: `FakeAnomalyModel` returns `random.uniform(0, 1)` seeded by `hash(contract_id)`. Frontend and narrator work against it immediately.

Reality check: `python -m coacc_etl.models.anomaly.evaluate` — prints precision@k for k ∈ {10, 100, 1000} against the held-out sanctioned-cases set, plus calibration plot path.

### 3.4 `SubgraphContract`
Given `case_id`, return JSON:
```json
{
  "case_id": "...",
  "nodes": [{"id":"...", "type":"Company|Contract|Sector|Pattern", "props":{}}],
  "edges": [{"src":"...","dst":"...","type":"CONTRATOU|BELONGS_TO|...","props":{}}],
  "evidence_refs": [{"dataset":"jbjy-vk9h","row_key":"...","url":"..."}],
  "score": 0.87,
  "features": {"log_value_z_modality": 3.2, "...": "..."}
}
```

Stub: `etl/tests/fixtures/subgraphs/*.json` — 10 handwritten cases covering the pattern taxonomy.

Reality check: `python -m coacc_etl.graph.subgraph_probe <case_id>` — validates shape against Pydantic schema, asserts every `evidence_refs[].url` returns HTTP 200.

### 3.5 `NarrativeContract`
Input: Subgraph. Output: markdown with required sections `# Lead`, `## Evidencia`, `## Señales`, `## Fuentes`. Spanish. ≤400 words.

Stub: `FakeNarrator` returns a deterministic templated narrative.

Reality check: `python -m coacc_etl.models.narrator.audit` — runs on the 10 fixture subgraphs, asserts all four sections present, no fabricated dataset IDs (every citation must resolve in the roadmap registry), word count in range.

### 3.6 `APIContract` — frontend ↔ backend
OpenAPI 3.1 at `docs/contracts/api.openapi.yaml`. Endpoints:
- `GET /cases?limit&offset&min_score` → paginated case list
- `GET /cases/{id}` → case detail including subgraph + narrative + raw evidence
- `POST /agent/query` → `{ question } → { answer, citations, subgraphs }`
- `GET /health` → readiness signal

Stub: MSW handlers under `frontend/src/mocks/` returning fixture JSON. F1 onward runs against MSW; switching to live backend is a base-URL env flip.

Reality check: `npm run api:contract-test` in `frontend/` — Schemathesis against the live backend's OpenAPI. Fails on any shape divergence.

### 3.7 `RegistryContract` — dataset inventory
Location: `lake/meta/roadmap_registry.parquet`. Columns:
- `dataset_id, sector_pdf, priority (1-3), live_rows, live_probed_at, lake_rows, lake_last_refresh, status (live|stale|missing|blocked), est_bytes`

Stub: seeded from `scripts/roadmap_socrata_datasets.csv` with null live counts. R1 fills in live counts.

Reality check: `python scripts/registry_health.py` — prints count by status, flags any `priority=1` dataset in `missing`.

---

## 4. Work packages

Each WP: **Owner role · Depends on · Stub · Deliverable · Reality check · Gate · Time-box**. Time-boxes are aggressive — if a WP blows the box, raise it Friday rather than extend quietly.

### Track D — Data Lakehouse

**D0. Reality instrumentation**
- Owner: Data eng
- Depends on: —
- Stub: —
- Deliverable: `etl/src/coacc_etl/lakehouse/reality.py`, `scripts/lake_reality.py`, `scripts/capture_fixture.py`, 5 red tests in `etl/tests/pipelines/test_secop_integrado.py`, baseline `docs/reality/<date>_secop_pre_fix.csv`
- Reality check: `python scripts/lake_reality.py` prints `buyer_name_null_pct=100`, five tests fail loudly, baseline committed.
- Gate: all of the above in a single commit; `!docs/reality/*.csv` whitelisted in `.gitignore`; `make lake-reality` target runs it.
- Time-box: 1 day. **Note: prior attempt rejected — see prior review; fix the nine items before marking D0 green.**

**D1. Normalizer fix (jbjy-vk9h schema)**
- Owner: Data eng
- Depends on: D0
- Stub: —
- Deliverable: rewritten `normalize_frame_for_lake` and `_transform_frame` in `secop_integrado.py`, both using `jbjy-vk9h` column names.
- Reality check: `pytest etl/tests/pipelines/test_secop_integrado.py` — all tests green; `python scripts/lake_reality.py` over the fixture lake shows buyer/supplier/value coverage thresholds met.
- Gate: tests green on fixture; no change to raw lake yet.
- Time-box: 1 day.

**D2. Repartition + dedup**
- Owner: Data eng
- Depends on: D1
- Stub: —
- Deliverable: `scripts/repartition_secop.py` rewrites `lake/raw/source=secop_integrado/` using `signed_date` partitioning + dedup on `id_contrato` keeping max `ultima_actualizacion`.
- Reality check: pre-swap — `lake_rows == lake_raw_rows − dupes`, `MIN(signed_date) < 2015`, partition count > 50, all D1 thresholds still met.
- Gate: backup of old lake kept for 7 days; `docs/reality/<date>_secop_post_repartition.csv` committed showing the diff.
- Time-box: 1 day.

**D3. Watermark + incremental fix**
- Owner: Data eng
- Depends on: D1
- Stub: —
- Deliverable: watermark stores `max(ultima_actualizacion)`; incremental `$where` uses `ultima_actualizacion > '<iso>'`; `scripts/incremental_smoke.py` proves zero-row second run.
- Reality check: run incremental twice in a row, second run writes 0 rows; forced delta shows only new rows.
- Gate: smoke script green; committed log of both runs.
- Time-box: 1 day.

**D4. Graph load unblock**
- Owner: Data eng
- Depends on: D1
- Stub: —
- Deliverable: `run_streaming(limit=20_000)` against throwaway Neo4j; DuckDB rollup-then-UPSERT pattern replaces per-chunk Cypher aggregates.
- Reality check: `sum(r.total_value)` in Neo4j == `sum(contract_value)` in lake for the same `(buyer, supplier)` pairs. Zero rows where `r.buyer_name=''`.
- Gate: `docs/reality/<date>_graph_parity.csv` committed.
- Time-box: 1.5 days.

**D5. SECOP family expansion**
- Owner: Data eng
- Depends on: D1 pattern (not D4)
- Stub: each dataset has its own fixture
- Deliverable: pipelines for `aimg-uskh`, `qmzu-gj57`, `cb9c-h8sn`, `wi7w-2nvm`, `ibyt-yi2f`, SECOP-I family — each with its own D0→D4 mini-loop.
- Reality check: per-pipeline `lake_reality.py` row meets thresholds; `delta_pct < 0.5%`.
- Gate: one row per pipeline in `lake/meta/reality_report.csv`.
- Time-box: 2 days per pipeline, parallelizable.

### Track R — Registry / Roadmap coverage

**R1. Registry table**
- Owner: Data eng
- Depends on: D0 (uses same probe)
- Stub: seeded CSV
- Deliverable: `lake/meta/roadmap_registry.parquet` + `scripts/registry_health.py`.
- Reality check: every priority-1 dataset has a non-null `live_rows`.
- Gate: health script green; committed snapshot.
- Time-box: 1 day.

**R2. Generic Socrata pipeline**
- Owner: Data eng
- Depends on: D0 pattern (not D1)
- Stub: —
- Deliverable: `coacc_etl.pipelines.generic_socrata.GenericSocrataPipeline(dataset_id)` that lands raw parquet under `lake/raw/source=<dataset_id>/year=.../month=...` — partitioned by the dataset's declared data timestamp where one exists, else by ingest time with an explicit `partition_by_ingest=True` flag and a WARNING.
- Reality check: one ingest of a small registry dataset produces a `reality_report.csv` row with `delta_pct < 1%`.
- Gate: reality row committed.
- Time-box: 2 days.

**R3. Disk guard + priority-first ingest**
- Owner: Data eng
- Depends on: R1, R2
- Stub: —
- Deliverable: pre-ingest disk check + cap on concurrent ingestions; CLI `coacc-etl ingest --priority 1` loops over registry priority-1 datasets.
- Reality check: attempting to ingest a dataset that would exceed 80% disk refuses with clear error; priority-1 loop completes with all rows ≥99% of live counts.
- Gate: priority-1 coverage table in `docs/reality/<date>_registry_priority1.csv`.
- Time-box: 2 days (plus ingest wall-clock).

### Track M — Models / AI

**M1. Feature spec + emit pipeline**
- Owner: ML eng
- Depends on: `FeatureContract` published (can start before D1 lands as long as the contract is frozen)
- Stub: `emit_fake_features.py`
- Deliverable: feature extraction Python module + `features.parquet` output + schema doc in `docs/contracts/features_v1.md`.
- Reality check: `python -m coacc_etl.features.audit` — null% under thresholds, distributions plotted.
- Gate: audit green on fixture lake; re-run green on real lake after D1.
- Time-box: 2 days.

**M2. Anomaly classifier**
- Owner: ML eng
- Depends on: M1
- Stub: `FakeAnomalyModel`
- Deliverable: trained model (isolation forest or XGBoost) + `predict.py` + `docs/ai/anomaly_model.md` (problem, data, features, training, metrics, limitations, ethical note).
- Reality check: precision@100 ≥ 0.4 against held-out sanctioned-cases fixture; calibration plot committed.
- Gate: metrics table in the model card committed; model artifact hash recorded.
- Time-box: 3 days (doesn't count wall-clock for re-training over full lake).

**M3. Generative narrator**
- Owner: ML eng
- Depends on: `SubgraphContract` and `NarrativeContract` published
- Stub: `FakeNarrator`
- Deliverable: narrator module that consumes Subgraph and emits the required markdown sections in Spanish, with citations.
- Reality check: audit script on 10 fixture subgraphs — all sections present, all citations resolve, word count in range, zero hallucinated dataset IDs.
- Gate: audit green; `docs/ai/generative_narrator.md` committed.
- Time-box: 2 days.

**M4. Model evaluation + held-out set**
- Owner: ML eng
- Depends on: M2
- Stub: —
- Deliverable: held-out sanctioned-cases fixture + `scripts/evaluate_anomaly.py` producing precision/recall, calibration, top-error analysis.
- Reality check: evaluation reproducible from committed fixture + model artifact; same numbers across two runs.
- Gate: evaluation report committed; metrics referenced in the model card.
- Time-box: 1 day.

**M5. CRISP-ML + model cards published**
- Owner: ML eng (+ Comms review)
- Depends on: M2, M3, M4
- Stub: —
- Deliverable: `docs/crisp_ml.md` mapping phases 1-6 to co/acc artifacts; two model cards finalized; architecture diagram committed under `docs/architecture/`.
- Reality check: every CRISP-ML phase links to a real artifact URL within this repo; broken-link scan passes (`scripts/check_doc_links.py`).
- Gate: link-check green; peer review sign-off.
- Time-box: 1 day.

### Track F — Frontend

**F1. Scaffold + FakeAPI**
- Owner: FE eng
- Depends on: `APIContract` published
- Stub: MSW handlers + fixture JSON
- Deliverable: `frontend-redesign` branch builds against MSW; all pages render against fixtures.
- Reality check: `npm run test` green, Playwright smoke visits every route successfully.
- Gate: CI green on the branch; screenshots committed under `docs/screenshots/`.
- Time-box: 2 days.

**F2. Case detail + narrator surface**
- Owner: FE eng
- Depends on: F1, `NarrativeContract` frozen
- Stub: FakeNarrator output
- Deliverable: case detail page rendering subgraph visualization + narrative + evidence table with working dataset links.
- Reality check: Playwright test opens a fixture case, asserts narrative sections, clicks an evidence link, confirms 200.
- Gate: test green; screenshots committed.
- Time-box: 2 days.

**F3. Ciudadano-agent UI**
- Owner: FE eng
- Depends on: F1, `APIContract` agent endpoint
- Stub: FakeAgent returns canned answers with citations
- Deliverable: chat UI with message history, citation chips, subgraph drill-down on citation click.
- Reality check: Playwright test sends a fixture query, asserts citation chips render, asserts a citation click navigates to the right case.
- Gate: test green.
- Time-box: 2 days.

**F4. Polish pass (dogfood)**
- Owner: FE eng (+ designer if available)
- Depends on: F2, F3
- Stub: —
- Deliverable: visual polish, empty states, error boundaries, accessibility pass (`axe` clean on all routes).
- Reality check: `dogfood` skill run (`/dogfood`) produces a triage report; P0/P1 bugs fixed.
- Gate: dogfood report green.
- Time-box: 2 days.

### Track C — Compliance + Comms

**C1. Public repo audit**
- Owner: Comms / PM
- Depends on: —
- Stub: —
- Deliverable: audit script `scripts/repo_publish_audit.py` that scans for: `CLAUDE.md`/`AGENTS.md`/`.env*`/`.claude/`/`lake/`/`govt data roadmap/` present in git index; secrets (regex for common token shapes); large files (>10MB).
- Reality check: script exits 1 on any finding.
- Gate: clean run committed; run as a pre-push hook (configured via the `update-config` skill).
- Time-box: 0.5 day.

**C2. LICENSE + README + architecture diagram**
- Owner: Comms / PM
- Depends on: —
- Stub: —
- Deliverable: `LICENSE` (proposed MIT or Apache-2.0 — pick one in decision log), top-level `README.md` with: problem, datasets, architecture diagram embed, install, run, screenshots, team, link to `docs/competition/` and `docs/ai/`.
- Reality check: link-check green; a fresh clone can build and run the frontend against MSW in under 10 minutes from the README alone.
- Gate: fresh-clone test recorded (screen recording committed or a log).
- Time-box: 1 day.

**C3. `herramientas.datos.gov.co/usos` registration**
- Owner: Comms / PM
- Depends on: F4 + M5 (demo-ready solution)
- Stub: —
- Deliverable: "Uso" registered on the portal pointing to the public repo + live demo URL (or video URL if no hosted demo).
- Reality check: public URL of the Uso entry committed in `docs/competition/decision_log.md`; the entry is independently visitable.
- Gate: URL resolves; page shows repo link.
- Time-box: 0.5 day (administrative).

**C4. Team + inscripción**
- Owner: Comms / PM
- Depends on: —
- Stub: —
- Deliverable: team of 2-4 with ≥1 woman; roles mapped (analyst/scientist, dev, viz, comms); Microsoft Forms inscripción submitted.
- Reality check: inscripción confirmation email archived under `docs/competition/artifacts/`.
- Gate: confirmation email archived; team list in decision log.
- Time-box: blocking — before Fase 1 close.

**C5. CRISP-ML + model cards published externally**
- Owner: Comms / PM
- Depends on: M5
- Stub: —
- Deliverable: docs rendered on the public repo, linked from README.
- Reality check: each jury-facing doc viewable on `github.com/<org>/<repo>` without auth.
- Gate: walkthrough by a non-team reader who can find every doc from the README in under 2 minutes.
- Time-box: 0.5 day.

**C6. Dress rehearsal**
- Owner: All
- Depends on: everything above
- Stub: —
- Deliverable: 45-minute live demo on the real stack, real data, real model, real narrator, real agent. Jury-style Q&A by someone outside the team.
- Reality check: every rubric cell §3 in the entry plan scored live; weak cells trigger a 48h patch cycle.
- Gate: recorded rehearsal + scorecard committed.
- Time-box: 1 day.

**C7. Finals presencial**
- Owner: ≥1 team member (in-person)
- Depends on: inscripción confirmation, dress rehearsal
- Stub: —
- Deliverable: on-site presentation.
- Reality check: demo runs without requiring live wifi (offline video backup present + laptop with local stack).
- Gate: backup video tested offline the night before.
- Time-box: event-bound.

---

## 5. Stubs and fakes — how parallelism actually works

Each fake is a tiny module that implements the contract with trivial data and lives under a dedicated path so real/fake is a one-line swap.

| Contract | Fake location | Swap mechanism |
|---|---|---|
| `LakeContract` | `etl/tests/fixtures/secop_integrado/lake_sample.parquet` | `COACC_LAKE_ROOT` env flip |
| `FeatureContract` | `scripts/emit_fake_features.py` → `lake/features/*.parquet` | env flag `COACC_FEATURES_SOURCE=fake\|real` |
| `AnomalyScoreContract` | `etl/src/coacc_etl/models/anomaly/fake.py` | dependency-injection in `etl/src/coacc_etl/api/` |
| `SubgraphContract` | `etl/tests/fixtures/subgraphs/*.json` | service flag |
| `NarrativeContract` | `etl/src/coacc_etl/models/narrator/fake.py` | service flag |
| `APIContract` | `frontend/src/mocks/` (MSW) | Vite env `VITE_API_MODE=mock\|live` |
| `RegistryContract` | seeded `scripts/roadmap_socrata_datasets.csv` | reads real parquet once R1 lands |

**Rule:** any track that depends on another must run its CI green against the fake before claiming the dependency is the blocker. "Waiting on X" is not valid until the fake is proven to work for the asking track.

---

## 6. Reality check loops — three scales

### 6.1 Micro — per-WP gates (already enumerated above)
One command per WP. Output goes to `docs/reality/` or a known path. No manual sign-off.

### 6.2 Meso — weekly loops

Every Friday, automatically:

1. **Integration rehearsal** — `make demo-smoke`:
   - Boots the API against the real lake
   - Loads the frontend against the real API (not MSW)
   - Playwright visits every route
   - Hits `/cases/<highest-score>`, asserts narrative + citations render
   - Posts a fixture question to the agent, asserts citations in the response
   - Output: `docs/reality/<date>_demo_smoke.{log,mp4}`

2. **Rubric burn-up** — `python scripts/rubric_score.py`:
   - Emits a self-score for each of the 6 rubric criteria (0-max) with a one-line justification
   - Trend plotted in `docs/reality/rubric_trend.svg`
   - **Red flag rule:** any cell whose score hasn't moved in 2 consecutive weeks triggers an explicit escalation in decision log

3. **Disqualification scan** — `python scripts/rules_check.py`:
   - For each rule R1-R12 in the entry plan, prints PASS/WARN/FAIL + rationale
   - Any FAIL blocks the week from closing

4. **Data freshness** — `python scripts/lake_reality.py`:
   - Run against all sources
   - Any source with `delta_pct > 1%` flagged
   - Stale-by-watermark sources (`last_seen_ts` > 7 days old) flagged

Friday output lives under `docs/reality/weekly/<iso-week>/` with a top-level `status.md`.

### 6.3 Macro — commit-time honesty hooks

Two pre-commit hooks (wire via `update-config` skill):

- **reality-hook**: if commit message contains `matches exactly`, `validated end-to-end`, `no data loss`, `production ready`, or similar — require at least one file under `docs/reality/` staged in the same commit with mtime within 1h. No fresh reality CSV → hook rejects the commit.
- **rubric-hook**: if commit touches any of the rubric-driving surfaces (`frontend/`, `etl/src/coacc_etl/models/`, `docs/ai/`, `docs/crisp_ml.md`), require `rubric_score.py` to have been run within 24h (mtime of `docs/reality/rubric_trend.svg`). Stale → hook warns and asks for `--no-verify=false` reasoning.

Neither hook uses `--no-verify`. If they misbehave, open an issue; don't bypass them.

### 6.4 Macro — pre-submission dress rehearsal (C6)
See WP C6. Run 2 weeks before finals. If any rubric cell scores below target, that week is a patch cycle.

---

## 7. Rubric burn-up — what "heading in the right direction" looks like

Each Friday `rubric_score.py` emits a row like:
```
date,innovacion,datos_abiertos,rigor_tecnico,ia_emergente,impacto,diseno,total
2026-04-24,9,12,8,4,12,5,50
2026-05-01,9,14,10,6,13,6,58
...
```
Target trajectory:
- End of Fase 1 (post-inscripción): ≥ 50
- End of Fase 2 (development close): ≥ 75
- Dress rehearsal: ≥ 85

If a week drops or flatlines without explanation, escalate.

---

## 8. Disqualification tripwire (R-rules from entry plan §2)

Runs weekly (§6.2) and pre-commit on any file under `docs/competition/`:

| Rule | Tripwire | Automation |
|---|---|---|
| R1 team 2-4 | `scripts/rules_check.py` reads `docs/competition/team.yml` | auto |
| R2 ≥1 woman | same | auto |
| R3 multidisciplinary | roles in `team.yml` | auto |
| R4 ≥1 datos.gov.co dataset | reads `lake/meta/reality_report.csv` | auto |
| R5 ≥1 AI component | asserts two model-card files exist + are non-stub | auto |
| R6 CRISP-ML docs | asserts `docs/crisp_ml.md` exists + link-check green | auto |
| R7 public repo | asserts repo visibility via `gh repo view --json visibility` | auto |
| R8 usos registration | reads a URL field in `docs/competition/decision_log.md` + HTTP 200 probe | auto once populated |
| R9 originality / IP | secrets/license scan via C1 | auto |
| R10 finals presencial | manual checkbox in `team.yml` | human |
| R11 inscripción | archived email or confirmation number | manual + auto |
| R12 estratégicos | asserts registry coverage against priority-1 list | auto |

Any `FAIL` blocks the week.

---

## 9. Milestones + decision log

Integration milestones (for the program, not individual WPs):

- **M-1 Hello Data** — D0 + D1 + R1 done. Any team member can read a compliant `LakeContract` row. Target: end of week 1.
- **M-2 Hello Graph** — D4 done. Neo4j parity against lake. Frontend can hit a live case endpoint. Target: end of week 2.
- **M-3 Signal Live** — M2 + M3 integrated end-to-end. One real case renders with a real narrative and a real anomaly score. Target: end of week 3.
- **M-4 Coverage** — D5 + R3 done. SECOP family loaded, priority-1 registry covered. Target: end of week 4.
- **M-5 Jury-ready** — C5 + C6. Dress rehearsal ≥85. Target: end of week 5.
- **M-6 Submitted** — C3 done, repo public, usos registered. Target: Fase 2 close.
- **M-7 Finals** — C7.

Decision log — append-only section in `docs/competition/datos_al_ecosistema_2026.md` §12. Every merge to main that affects the program plan adds a row.

---

## 10. Owner board — fill in

```yaml
# docs/competition/team.yml
team:
  - name: TBD
    role: data-engineer
    tracks: [D, R]
    gender: TBD
    in_person_finals_available: TBD
  - name: TBD
    role: ml-engineer
    tracks: [M]
    gender: TBD
    in_person_finals_available: TBD
  - name: TBD
    role: frontend-engineer
    tracks: [F]
    gender: TBD
    in_person_finals_available: TBD
  - name: TBD
    role: comms-pm
    tracks: [C]
    gender: TBD
    in_person_finals_available: TBD
```
R2 (≥1 woman) and R10 (in-person attendance) are verified from this file by `rules_check.py`.

---

## 11. What happens when reality disagrees with the plan

The plan is wrong, not reality. Observed order of operations on divergence:

1. The weekly Friday loop surfaces the drift.
2. Owner of the affected WP opens an issue within 24h describing the drift.
3. If the drift is about a contract, we follow the `docs/contracts/CHANGELOG.md` migration flow: bump version, provide migration note, update both sides' CI.
4. If the drift is about scope or schedule, we update the decision log + this doc in the same PR.
5. No silent fixes. No commit-and-forget. If a rubric cell moves, the decision log says why.

The point of three-scale reality checks is that drift is caught in the same week it happens. The cost of delay is exponential this close to finals; the cost of a Friday hour is linear.

---

**Canonical references:** this doc, `docs/competition/datos_al_ecosistema_2026.md`, `docs/contracts/*`, `docs/reality/*`. If a contradiction exists between them, the entry plan wins on rules/scoring; this plan wins on execution; contracts win on interfaces.
