# co/acc Minimum Viable Contest Product Requirements

Status: implementation contract  
Updated: 2026-07-13  
Target branch: `main`  
Contest: [Datos al Ecosistema 2026: IA para Colombia](https://www.datos.gov.co/stories/s/Concurso-Datos-al-Ecosistema-2026-IA-para-Colombia/ddau-8cy9/)

## 1. MVP decision

The MVP is a public, read-only Spanish web application that lets a user:

1. see every registered corruption-risk pattern;
2. see which patterns have materialized hits;
3. compare their evidence-quality confidence;
4. open a company or contract hit and inspect its source evidence; and
5. view a small AI-prioritized contract list with plain-language explanations.

The MVP uses the lake and signal inventory already present in the repository. It
does **not** require another dataset, another detector, Neo4j, a second model, or
new infrastructure.

```text
Public datasets
  -> dataset contracts and bounded ETL
  -> Parquet lake
  -> DuckDB features
  -> materialized signal hits and evidence bundles
  -> one batch Isolation Forest
  -> FastAPI
  -> React Atlas UI
```

Neo4j compatibility code may remain, but the complete launch path must work
with Neo4j unavailable.

### Product claim

co/acc organizes public records into evidence-backed risk signals and
prioritizes unusual procurement activity. It does not determine that corruption
occurred, estimate guilt, or replace legal and journalistic investigation.

## 2. Frozen launch inventory

The verified local baseline on 2026-07-13 is:

| Asset | Baseline |
| --- | ---: |
| Available source datasets reported by the lake-backed API | 47 |
| Registered signal definitions | 68 |
| Materialized signals with hits | 49 |
| Materialized signals with a confidence index | 49 |
| Registered definitions without a materialized hit set | 19 |
| Latest verified signal run | `phase-missing-patterns-all-signals-shared-representative-20260606` |

These values are a release baseline, not hard-coded UI content. A final refresh
may change counts. The release requirement is structural:

- every registered definition is visible;
- every materialized hit has a confidence index and evidence state;
- definitions without hits remain visible as `registered_only` with no invented
  confidence value; and
- the UI never substitutes fixtures when the API is unavailable.

Only sources that are already ingested, contracted, and consumed by the current
lake outputs are in scope. Catalog entries that are unimplemented, quarantined,
parked, or not loaded are not MVP inputs.

### Data freeze rule

Do not add datasets before launch. New data is allowed only when it is a refresh
of an already working source or is necessary to repair a broken evidence URL,
identity field, date, or source attribution in an existing materialized signal.

## 3. Primary user and job

Primary user: a journalist, civic researcher, watchdog, public servant, juror,
or citizen reviewing Colombian public procurement.

Primary job:

> Find a risk pattern, understand why a hit exists and how reliable its joins
> are, then verify the underlying public records.

The minimum successful journey is:

```text
Landing
  -> Patterns or Signals
  -> materialized pattern
  -> company/contract hit
  -> confidence components
  -> official evidence
```

The minimum AI journey is:

```text
Landing
  -> Prioritized contracts
  -> anomaly score and top feature deviations
  -> contract detail
  -> official SECOP evidence
```

## 4. Confidence-index contract

Confidence measures support for a documentary match. It is not the probability
that corruption occurred.

```text
confidence_index = 100 * (
  0.55 * identity_match
  + 0.25 * evidence_traceability
  + 0.20 * source_corroboration
)
```

Required components:

- identity match: quality of the entity join;
- evidence traceability: quantity of linked evidence items; and
- source corroboration: number of distinct source inputs supporting the signal.

Requirements:

1. The API returns the 0-100 index and all three components for every hit.
2. Signal and pattern catalog rows show average confidence across materialized
   hits.
3. The detector-specific `score` remains separate and is labelled as risk or
   anomaly strength.
4. A high confidence value must never be described as proof or likelihood of
   corruption.
5. Missing materialization produces no confidence value, not zero and not an
   estimated placeholder.
6. The calculation and thresholds remain documented in
   [confidence_index.md](confidence_index.md).

## 5. Functional requirements

### FR-1: Landing

The landing page must:

- state the public problem and solution in Spanish;
- link directly to Patterns, Signals, Search, Prioritized contracts, and
  Methodology;
- display the latest data, signal, and model refresh dates;
- state that the product presents documentary context, not accusations; and
- show no fixture-derived production metric.

### FR-2: Complete pattern and signal catalogs

The Signals catalog must show every registered definition, including entries
with no hits. The Patterns catalog may group multiple signal definitions under
one public pattern only when it exposes every member `signal_id` and aggregates
their hit count and confidence transparently.

Each catalog row must show:

- clean display identifier;
- title and description;
- category and severity;
- hit count;
- `materialized` or `registered_only` state;
- confidence index when materialized; and
- source dependencies.

No confidence threshold, access tier, category, source, or severity may hide a
registered pattern. Filters may help navigation but must default to the complete
catalog and must always allow returning to it.

Acceptance:

- the current Signals baseline displays 68 registered definitions;
- the current baseline identifies 49 as materialized;
- internal compatibility suffixes are not displayed or required in URLs; and
- a registry-only item is visibly different from a zero-hit materialized item.

### FR-3: Signal and pattern detail

A materialized detail page must show:

- description, severity, category, sources, and observation date;
- hit count and sample hits;
- risk/anomaly score;
- confidence index;
- identity, traceability, and corroboration components;
- entity and scope identifiers in human-readable form;
- evidence count and evidence items; and
- the public disclaimer.

An unmaterialized detail page must show its definition and required sources with
an honest “not materialized” state. It must not fabricate sample hits.

### FR-4: Search and entity detail

Search must accept company name or Colombian NIT and return lake-backed entities
with source attribution.

Entity detail must show:

- company name and normalized NIT;
- known procurement context;
- every materialized signal attached to the entity;
- confidence and risk score as separate fields; and
- linked evidence with source labels and observation dates.

The normal entity response must not silently remove a signal because of its
pattern family or confidence value.

### FR-5: Evidence

Every materialized hit must retain its evidence state even when a source lacks a
direct public URL.

Evidence items must show, when available:

- source name;
- source record identifier;
- official URL;
- observed date;
- identity-match type; and
- row or file selector for reproducibility.

Broken URLs, missing evidence bundles, and unattributed source records are
release defects. They lower confidence but do not make the underlying pattern
definition disappear.

### FR-6: Minimum AI surface

The contest AI component is one existing batch Isolation Forest that ranks
unusual procurement contracts. Do not add a supervised top-up, ensemble, LLM
classifier, or a second anomaly model.

The release may use the smallest already working and reproducible feature schema.
Expanding from the existing contract feature set to an aspirational 15- or
18-variable model is not an MVP dependency.

Each displayed AI result must include:

- normalized 0-1 anomaly score;
- score-confidence label already produced by the model pipeline;
- three strongest feature deviations in plain Spanish;
- model run ID and scoring date;
- contract, supplier, buyer, amount, and signing date; and
- direct SECOP evidence when available.

The model must be deterministic from a recorded random seed, feature schema,
data snapshot, and code commit.

Minimum evaluation:

- report evaluated rows and weak-label positives;
- report base rate, precision at 100 and 1,000, average precision, and ROC AUC;
- compare with random ranking;
- hold out complete suppliers when canonical NIT is available; and
- describe PACO or sanction matches as weak evaluation labels, not ground truth.

Evaluation quality affects claims, not product availability. If the model misses
a target threshold, the product may still show unsupervised anomaly distance but
must not claim validated corruption detection.

### FR-7: Methodology

The public methodology must explain:

- lake and contract architecture;
- current source inventory and provenance;
- exact and probabilistic identity joins;
- deterministic pattern materialization;
- confidence formula and its limitations;
- Isolation Forest inputs, evaluation, and limitations;
- refresh dates and known source gaps; and
- the boundary between a signal and a legal finding.

### FR-8: Honest states

The UI distinguishes loading, live, empty, partial, stale, registered-only, and
unavailable states. With `VITE_ALLOW_FIXTURES=false`, API failure must never
produce plausible demo data.

## 6. API requirements

The MVP requires these lake-backed read paths:

- health and readiness;
- source/meta summary;
- signal list and detail;
- pattern list and entity patterns;
- company search and entity detail;
- entity signals and evidence trail;
- anomaly/prioritized-contract list and detail; and
- public company pattern lookup.

The catalog, detail, entity, public-company, context, and case readers must all
use the same visibility rule: return the complete materialized result set for an
otherwise accessible entity.

API identifiers shown to clients must use clean display IDs. Legacy lake IDs may
remain internally so existing Parquet runs can be read without a destructive
rebuild.

## 7. Operator requirements

### OP-1: Lake-only deployment

Production contains the frontend, API, reverse proxy, lake files, and scheduled
or on-demand lake operations. Neo4j is neither deployed nor required for health,
readiness, search, signals, patterns, evidence, anomaly scores, or the demo.

### OP-2: Release artifacts

The deployed release requires:

- a completed source/lake manifest;
- materialized `signal_hits` and `evidence_bundles` Parquet;
- a completed signal-run manifest;
- materialized anomaly-score Parquet;
- a model/evaluation manifest;
- source registry and data dates; and
- the code commit used to produce the release.

The full public-source lake need not be rebuilt during deployment. The MVP may
deploy a verified release snapshot, provided the ingestion and materialization
commands remain documented and reproducible.

### OP-3: Final refresh

Before submission:

1. refresh already working sources that can complete within the release window;
2. materialize the supported signal set once;
3. write confidence columns and evidence bundles;
4. score the AI contract cohort;
5. record one final successful run ID; and
6. freeze that run for the demo.

A failed optional-source refresh must not destroy the last completed release
artifacts.

### OP-4: Readiness

`/ready` must fail when required release artifacts are missing or unreadable.
Neo4j absence is not a failure.

Readiness must report:

- lake availability;
- source and signal run IDs;
- materialized signal count;
- evidence-bundle availability;
- anomaly/model run ID; and
- artifact age.

### OP-5: Resource limit

The MVP runs on one host with at most 16 GB RAM and 50 GB persistent disk.
Ingestion, DuckDB materialization, training, and scoring must remain bounded in
memory. No Kubernetes, warehouse, GPU, or distributed graph service is required.

## 8. Privacy, safety, and security

- Anonymous access is read-only and rate limited.
- Public identifiers and evidence come from documented public sources.
- Raw secret values, tokens, private annotations, and filesystem paths are never
  returned.
- Personal document numbers are masked where public display is not necessary to
  verify an entity match.
- HTTPS, same-origin serving, and security headers are enabled.
- Every main surface repeats or links to the “signals, not proof” disclaimer.
- Confidence never changes a person's legal status or produces an automated
  accusation.

## 9. Explicitly out of scope

Do not build or require:

- new datasets or new source adapters;
- new pattern definitions beyond the current registry;
- Neo4j deployment or graph projection;
- interactive graph visualization;
- a second model, model ensemble, deep learning, or LLM classification;
- AI agents, chat, automated narration, or generated investigation reports;
- accounts, invitations, saved investigations, sharing, or annotations;
- write actions against source systems;
- real-time scoring or streaming ingestion;
- automatic legal conclusions or a “corruption probability”;
- a data warehouse, Kubernetes, multi-region deployment, or GPU infrastructure;
- generalized dashboard-building, PDF export, or editorial case production; or
- a perfect historical backfill of every catalog source.

Existing code for an excluded feature may remain if it does not appear in the
minimum navigation, add infrastructure, or break launch tests.

## 10. Contest deliverables

The product is contest-complete only when these exist:

- verified team admission and eligibility artifact;
- public GitHub repository at the submitted commit;
- working public HTTPS deployment;
- CRISP-ML documentation;
- model card with fresh metrics and limitations;
- confidence-index methodology;
- architecture and data-flow diagram;
- source catalog and provenance;
- reproducible setup and refresh commands;
- social-impact and scalability statement;
- short live-data demonstration script;
- datos.gov.co use registration; and
- final presentation assets and presenter plan.

## 11. Release acceptance gate

The MVP is finished only when all of these pass:

1. The team is eligible for the contest.
2. The public signal and pattern catalogs expose every registered definition.
3. Every materialized hit has a confidence index and three visible components.
4. Registered-only definitions show no fabricated confidence or evidence.
5. Risk score and confidence are visibly different concepts.
6. Search reaches a real company with at least one materialized signal.
7. A juror can open source evidence from a real hit.
8. The AI list is generated from a recorded Isolation Forest run, not fixtures.
9. The frontend contains no hidden-pattern or restricted-pattern presentation.
10. API and ETL tests pass, frontend tests and production build pass, and
    `git diff --check` is clean.
11. The no-Neo4j lake-backed smoke test passes against the frozen release lake.
12. `/health` and `/ready` pass on the deployed host.
13. Desktop and 360 px browser checks complete both minimum user journeys.
14. No production page displays fixture data or predictive-corruption wording.
15. Methodology, model card, architecture, setup, impact, demo, and repository
    links are ready for juror validation.

Passing unit tests alone is not acceptance.

## 12. Current implementation state

Verified on 2026-07-13:

| Area | State | Remaining MVP work |
| --- | --- | --- |
| Lake/API | No-Neo4j smoke passes against the real lake | Freeze a final refreshed run |
| Sources | 47 sources reported by the smoke run | No new sources |
| Signals | 68 registered; 49 materialized and confidence-scored | Evidence-link QA and final materialization |
| Visibility | Catalog and materialized results are returned without hidden pattern tiers | Browser QA |
| Confidence | API, Parquet contract, compatibility fallback, and UI implemented | Validate final-run distributions |
| ETL tests | 164 pass | Keep green |
| API tests | 261 pass when excluding the time-sensitive lake-operations fixture file | Repair the two stale-date test fixtures |
| Frontend | Tests and production build pass | Complete minimum navigation and browser QA |
| AI | Isolation Forest pipeline and lake-backed scores exist | Freeze one schema/run, evaluate, and expose explanations |
| Deployment | API works without Neo4j | Remove Neo4j from the production launch path and deploy |
| Documentation | Architecture, CRISP-ML, model, and confidence documentation exist | Align model card and demo with final run |

## 13. Implementation order

Work only in this order:

1. Repair the two time-sensitive API tests and freeze the release acceptance
   commands.
2. Complete the visible catalog/detail/search/evidence journeys using real API
   data and no fixtures.
3. Freeze the smallest working Isolation Forest schema; rerun scoring and the
   supplier-disjoint evaluation.
4. Refresh already working sources, materialize signals once, validate
   confidence distributions, and freeze the final run.
5. Add the public Methodology/model-card surface and final disclaimers.
6. Create the no-Neo4j production Compose path and make readiness validate the
   frozen artifacts.
7. Run API, ETL, frontend, local smoke, evidence-link, desktop, and mobile QA.
8. Deploy behind HTTPS and repeat the smoke tests on the real host.
9. Finalize contest eligibility, repository, use registration, presentation,
   and the short live-data demo.

After step 9 passes, stop. Everything in the out-of-scope list is post-MVP.
