# Datos al Ecosistema 2026 — Entry Plan (co/acc)

> **Status update (Wave 6, 2026-05):** sections that reference
> `etl/src/coacc_etl/pipelines/` describe the pre-refactor architecture.
> The Wave 4 refactor retired the bespoke Pipeline stack; pipeline-per-dataset
> bodies were replaced by per-dataset YAML contracts (`etl/datasets/<id>.yml`)
> driving a single generic Socrata ingester (`coacc_etl.ingest.socrata.ingest`).
> Custom non-Socrata adapters (PACO, RUES, Registraduría, etc.) are tracked
> in `_KNOWN_DEFERRED_SOURCES` inside
> `etl/tests/test_signal_source_alignment.py` and will land under
> `coacc_etl.ingest.custom/` as they are built.

**Canonical reference for our entry into MinTIC's "Datos al Ecosistema 2026: IA para Colombia" competition.** Every product, technical, and positioning decision on the competition-facing surface of co/acc checks against this document. If a change conflicts with anything here, either the change is wrong or this doc needs an explicit update commit — not both silently.

**Source (operator paste, 2026-04-20):** MinTIC competition page — the "TÉRMINOS DE REFERENCIA DEL CONCURSO DATOS AL ECOSISTEMA 2026". Raw text archived in git history of this doc; treat that commit as the frozen reference.

---

## 1. TL;DR — what we're building for the competition

- **Entry**: co/acc's Colombian Open Graph + anomaly-detection pipeline over SECOP + related roadmap datasets, presented as a public-corruption signals platform.
- **Category**: primary *IA aplicada a datos abiertos* (detección de anomalías / IA generativa para reportes), secondary *Gobernanza y transparencia*.
- **Complexity level**: **Advanced** (>20 variables, >10k rows — SECOP alone is 5.6M; we integrate multiple datos.gov.co datasets + unstructured sources).
- **Reto fit**: closest official reto is #2 *Seguridad ciudadana y justicia* framed around anti-corruption pattern detection; we can also submit under the "temática libre" track if the jury prefers a transparency framing.
- **Hard deadline**: the jury reviews the public repo + the "Usos" page on `herramientas.datos.gov.co/usos` — if either is missing on the published cutoff, we don't advance. Finals presencial first week of August 2026.

## 2. Hard constraints — violation = disqualification

From §4 (Reglas) and §12 (Ética). Non-negotiable.

| # | Rule | Binding on us |
|---|---|---|
| R1 | Team size 2–4 | — |
| R2 | ≥1 woman on the team | **Open question — team not formed** |
| R3 | Multidisciplinary: ≥1 data analyst/scientist + programming + viz + comms roles | Map existing contributors to these roles |
| R4 | ≥1 dataset from `datos.gov.co` | Trivially met — SECOP alone is `jbjy-vk9h`. Must **document** which ones. |
| R5 | ≥1 AI component (prediction / classification / insight generation) | Anomaly-scoring model on procurement patterns. **Must be an actual trained or rules+ML hybrid model, not heuristics only.** |
| R6 | CRISP-ML documentation | Phase-map our engineering work to CRISP-ML stages (§6 below) |
| R7 | Public repo (GitHub/GitLab) with code + docs + flow diagrams + architecture | The current repo is public-ready but has `CLAUDE.md`, `AGENTS.md`, and other assistant-instruction files gitignored — verify none leak into the submission repo |
| R8 | Register the solution in `herramientas.datos.gov.co/usos` | Separate action item; blocks advancement |
| R9 | Originality / IP-clean | No vendored copyrighted corpora; sector roadmap PDFs are public docs but store only derivative extracts, not the PDFs themselves (already gitignored) |
| R10 | Final presencial attendance (Bogotá / GovCamp 2026, first week of August) | Logistics: nominate one in-person attendee now |
| R11 | Inscripción on time via Microsoft Forms link | **Cronograma absent from the paste** — open question |
| R12 | "Uso de datos abiertos estratégicos" — preference for datasets from the 25 Sector Roadmaps or the Hoja de Ruta Nacional 2025-2026 | Our `scripts/roadmap_socrata_datasets.csv` (209 dataset IDs from the 25 PDFs) is the natural mapping — **we can literally cite it** |

## 3. Scoring rubric — where the 100 points live

The rubric text in §7 of the brief has an internal inconsistency (the worked example uses *Innovación 20 pts / IA 10 pts* while the header text says *Innovación 15 pts / IA 20 pts*). Treat the **header text** as canonical unless MinTIC publishes the rubric instrument; assume the worked example is illustrative math.

| Criterion | Max | What the jury looks for | Our current strength | Work needed |
|---|---|---|---|---|
| Innovación y creatividad | 15 | Original angle, disruptive framing | **High** — evidence-first open graph + anti-corruption signals is uncommon in LATAM open-data competitions | Sharpen the "why this hasn't been built before" narrative |
| Uso de datos abiertos | 20 | Integration depth, strategic roadmap alignment | **High** — we map 209 datasets from the 25 sector PDFs; lakehouse already loaded SECOP | Explicit table of which datasets feed which signal + roadmap citation |
| Análisis y rigor técnico | 15 | Methodology quality, build coherence | **Medium** — ETL + Neo4j spine works, but Phase 0 reality check is still uncommitted and normalizer is broken for SECOP II | **Blocker**: finish the fix plan before submission demo |
| Uso de tecnologías emergentes (IA) | 20 | Actual ML/IA component, not just SQL | **Low-medium** — we have heuristics and graph patterns; no trained model yet | Build at least one anomaly-detection model + one generative summarizer for case reports |
| Impacto y escalabilidad | 20 | Social/economic impact, replicability, sustainability | **High** — public corruption surveillance has direct fiscal impact; architecture scales to other LATAM open-data portals (Socrata is multi-country) | Quantified impact claim (pesos at risk surfaced per run) + scaling story |
| Diseño, comunicación y usabilidad | 10 | Presentation, clarity, ease of use | **Medium** — frontend-redesign branch exists but unpolished | Dogfood pass on the UI before demo |

**Target**: ≥85/100. That means ≥90% on Uso de datos abiertos and Impacto, ≥75% on everything else. The one cell that scares me is IA/tech — see §7.

## 4. Category and reto selection

### Primary category: IA aplicada a datos abiertos
The brief lists six sub-categories. Our entry hits three:
- **Detección de anomalías** — identification of possible fraud in subsidies or irregularities in public contracting. *This is literally co/acc's thesis.*
- **IA generativa** — automated public-report generation for each flagged case.
- **Agentes de IA para servicios públicos** — a ciudadano-facing agent that answers "who got which contracts?" and "is this pattern unusual?" with open-data citations.

### Reto mapping

| Official reto | Fit | Our angle |
|---|---|---|
| #1 Salud | Low | Skip |
| **#2 Seguridad ciudadana y justicia** | **Med-High** | Reframe anti-corruption as justice; SECOP + oversight bodies data |
| #3 Transporte | Low | Skip |
| #4 Agricultura | Low | Skip |
| **#5 Economía y empleo** | Med | Contract concentration → market competition signals |
| #6 Desarrollo sostenible | Low | Skip unless we add environmental oversight angle |
| **#7 Innovación y tecnología** (asistentes virtuales para acceder a datos abiertos) | **High** | We can ship a ciudadano-agent as one of the deliverables |
| #8 Cultura/turismo | Low | Skip |
| #9 Educación | Low | Skip |
| **Temática libre** | **High** | Our actual thesis — submit here if jury allows |

**Decision**: submit under *temática libre* framed as "Transparencia en contratación pública con IA", with fallback mapping to #2 + #7 if the form demands one of the listed retos.

## 5. What we actually ship

### 5.1 The solution (functional)
A public-facing web app (existing `frontend-redesign` branch) that:
1. Lists flagged procurement patterns with anomaly scores.
2. For each case, shows a generated summary (IA generativa component — trained/fine-tuned or via a prompted LLM with deterministic structure).
3. Ciudadano-agent chatbox: natural-language queries over the open graph, with dataset citations on every answer.
4. Links every claim to the originating `datos.gov.co` row (evidence spine — already a co/acc architectural primitive).

### 5.2 The platform (under the hood)
- Lakehouse: DuckDB + Parquet on `lake/raw/**`, 5.6M SECOP rows today, expanding per the §10 dataset plan below.
- Graph: Neo4j as evidence spine connecting Company ↔ Contract ↔ Sector ↔ Pattern.
- Signal engine: rules + the new ML anomaly model + graph patterns (dossier trail, procurement concentration, timing anomalies).

### 5.3 The IA component (R5 compliance)
Must be a real model, not just SQL. Minimum viable:
- **Anomaly-scoring classifier** on contract records. Features: `contract_value`, supplier concentration ratio, award-to-modality mismatch, entity-level historical deviation. Trained on the SECOP lake using an isolation forest or an XGBoost classifier where label comes from documented sanctioned cases (we have `sanciones` + `procuraduría` data paths).
- **Generative case narrator**: prompt-templated LLM that reads the graph subgraph + raw evidence and outputs a 300-word case summary in Spanish, with citations. Deterministic scaffolding + LLM fill-in. Counts as both "IA generativa" and "Agentes de IA para servicios públicos".

Document both in the repo under `docs/ai/` with: problem framing, features, training data provenance, metrics (precision/recall on a held-out sanctioned-cases set), and a model card. Jury in §7 scores "Análisis y rigor técnico" heavily on methodology — CRISP-ML is the lingua franca they expect.

## 6. CRISP-ML alignment (R6)

Map our existing work + the §7 modeling task to CRISP-ML's six phases. The submission repo must have a document that spells this out. Mapping:

| CRISP-ML phase | co/acc artifact |
|---|---|
| 1. Business/Problem understanding | `docs/competition/datos_al_ecosistema_2026.md` §4-5 (this doc) + the project's existing corruption-signals thesis |
| 2. Data understanding | Sector roadmap mapping (`scripts/roadmap_socrata_datasets.csv`), schema docs per pipeline, lake reality reports (`docs/reality/*.csv`) |
| 3. Data preparation | The ETL lakehouse (`etl/src/coacc_etl/**`), normalizers, watermarking, dedup |
| 4. Modeling | `docs/ai/anomaly_model.md` + `docs/ai/generative_narrator.md` (to be written) + model training code under `etl/src/coacc_etl/models/` (new) |
| 5. Evaluation | Held-out sanctioned-cases set, precision/recall, confusion matrix, jury-grade error analysis |
| 6. Deployment/Monitoring | Frontend-redesign + `make lake-health` continuous reality check (Phase 7 of the fix plan) |

## 7. Risk register — what kills the entry

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| K1 | SECOP normalizer ships broken (Phase 1 of fix plan not done) → demo shows null buyer/supplier fields | Critical | Phase 1 MUST land before inscripción deadline |
| K2 | No real ML model — only heuristics | Critical | Build anomaly classifier first, polish second |
| K3 | Repo leaks assistant-instruction files (`CLAUDE.md`, `AGENTS.md`) | High | `.gitignore` already covers these; audit before push |
| K4 | Team doesn't include ≥1 woman | Disqualifying | Confirm before inscripción |
| K5 | Missing registration in `herramientas.datos.gov.co/usos` | Disqualifying | Do this the day the solution is demo-ready, not the day of final |
| K6 | Finals presencial conflict | Disqualifying | Confirm availability first week of August 2026 |
| K7 | Originality challenge (someone claims we used their code) | High | All third-party deps must be MIT/Apache/BSD with attribution file |
| K8 | Jury asks for a live dataset refresh and the watermark is broken | High | Phase 3 of fix plan lands before demo |
| K9 | Commit message "works end-to-end" on untested claim (history shows we've done this before) | Medium | Phase 7 pre-commit gate keeps us honest |
| K10 | Lake uses stale data the day of the finals | Medium | Cron the ingest for the Thursday before finals, not same-day |

## 8. Open questions (must resolve)

1. **Cronograma**: the paste had "8. CRONOGRAMA" with no dates below it. Get the actual Fase 1 inscripción deadline from `datos.gov.co` or the MinTIC page. Everything else cascades.
2. **Team composition**: names, roles (analyst/scientist, engineer, designer, comms), and the ≥1 woman requirement satisfied.
3. **Temática libre admisibility**: the §13 reto list doesn't explicitly mention anti-corruption. Confirm whether "temática libre" lets us propose our own reto or whether we must fit inside §13.
4. **Inscription form access**: the form link (`forms.cloud.microsoft/...`) should be tested once the team is formed.
5. **Incentive / prize mechanics**: "26 premios (tablets)" — per category? Per team member? Affects nothing technical but worth knowing.
6. **Complexity classification**: we self-classify as *Advanced*; verify the jury doesn't auto-route by reto.

## 9. Dataset plan (maps to §12/R12 — "datos abiertos estratégicos")

Source of truth: `scripts/roadmap_socrata_datasets.csv` (209 dataset IDs extracted from the 25 sector roadmap PDFs).

**Wave 1 — SECOP family (already in the repo or planned):**
- `jbjy-vk9h` SECOP II Contratos Electrónicos *(current, being fixed in the active fix plan)*
- `aimg-uskh` SECOP II Procesos
- `gnxj-bape` SECOP II Contratos (legacy/alternate)
- `qmzu-gj57` SECOP II Proveedores Registrados
- `cb9c-h8sn` SECOP II Adiciones *(key for over-run signals)*
- `wi7w-2nvm` SECOP II Ofertas Por Proceso
- `ibyt-yi2f` SECOP II Facturas
- SECOP I: `nuxh-53y2`, `f789-7hwg`, `7fix-nd37`, `tauh-5jvn`

**Wave 2 — oversight / sanctions:**
- Procuraduría sanciones, Contraloría responsabilidades fiscales, DIAN registro mercantil — confirm IDs against the CSV and the existing pipelines in `etl/src/coacc_etl/pipelines/`.

**Wave 3 — sector-specific enrichment:**
- Top cross-cutting datasets (`ustw-qgb4`, `uzcf-b9dh`, `xbc7-65j4`, `efv4-ibbr` — each referenced in 18+ sector PDFs) land as raw parquet via the generic Socrata pipeline in Phase 6 of the fix plan.

Jury-visible output: a README table in the submission repo listing dataset ID → purpose → number of rows ingested → last refresh, pulled from `lake/meta/reality_report.csv`.

## 10. Schedule alignment — phases vs. competition timeline

Until the cronograma is known, treat these as **relative** phases to the MinTIC-defined Fases:

| MinTIC fase | What we need done by then |
|---|---|
| Fase 1 — Inscripción y capacitación | Fix plan Phases 0–3 landed; team formed; repo public; inscripción submitted |
| Fase 2 — Selección y desarrollo | Fix plan Phases 4–5 landed; anomaly model trained; generative narrator prototype; frontend-redesign polished; data published in `herramientas.datos.gov.co/usos` |
| Fase 3 — Presentación finalistas | Full stack deployable and demo-stable; model card + CRISP-ML doc complete; architecture diagrams; metrics dashboard |
| Fase 4 — Premiación (first week of August 2026) | Team (at least one person) in Bogotá; live demo backup plan if wifi fails; prepared responses to the standard jury questions |

## 11. Publication checklist (R7, R8, R9)

Before inscripción:
- [ ] Submission repo visibility: public
- [ ] `CLAUDE.md`, `AGENTS.md`, `.claude/` not in git
- [ ] `.env`, `.env.local` not in git
- [ ] `lake/` not in git (already `.gitignore`d)
- [ ] `govt data roadmap/` not in git (added to `.gitignore`)
- [ ] LICENSE file (propose MIT or Apache-2.0)
- [ ] README with: problem, dataset list, architecture diagram, install steps, run steps, screenshots, team
- [ ] Link to live demo (or a video if live demo risky)
- [ ] Third-party attributions

Before Fase 2 close:
- [ ] `herramientas.datos.gov.co/usos` entry created, linking to the repo
- [ ] `docs/ai/` has the two model docs + model cards
- [ ] `docs/crisp_ml.md` traces phases 1-6 with links

## 12. Decision log

Keep a running log in this section. Each decision references back to the rubric or the rules.

| Date | Decision | Rationale | Ref |
|---|---|---|---|
| 2026-04-20 | Adopt this doc as canonical competition reference | User request: "always have reference to" | — |
| 2026-04-20 | Primary category = IA aplicada a datos abiertos; reto track = temática libre (fallback #2 + #7) | Best fit to co/acc thesis per §4 analysis | §4 |
| 2026-04-20 | Must build at least one trained model (anomaly) + one generative component (narrator) | R5 compliance; scoring §3 IA cell | §5.3 |

## 13. Pointers to other canonical docs

- Fix plan (SECOP normalizer + lakehouse correctness): conversation 2026-04-20, will land as `docs/reality/` snapshots + test commits.
- Roadmap link extraction: `scripts/extract_roadmap_links.py` → `scripts/roadmap_socrata_datasets.csv`.
- Project completion plan (memory): `memory/project_completion_plan.md`.
- Sector roadmap PDFs: `govt data roadmap/` (local only, gitignored).

---

**Instruction for future sessions:** if a change to co/acc is motivated by the competition, update §12 (Decision log) of this doc in the same commit. If a rule in §2 or a rubric cell in §3 appears to change based on new MinTIC guidance, open a new commit that updates the doc *and* archives the new guidance — never overwrite silently.
