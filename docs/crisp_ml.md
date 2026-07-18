# CRISP-ML Mapping

This document maps the current CO-ACC implementation to the six CRISP-ML(Q)
phases for competition and public-release review. It links only to committed
repo artifacts so `scripts/check_doc_links.py` can verify the evidence trail.

## 1. Business And Data Understanding

CO-ACC frames Colombian public procurement oversight as an evidence-prioritizing
workflow, not an accusation engine. The project scope, user-facing value, and
lake-first architecture are summarized in the [README](../README.md) and the
[architecture overview](architecture/overview.md). Competition assumptions and
R5 AI/ML requirements are tracked in the [program plan](competition/program_plan.md).

Ethical and legal constraints are explicit inputs to the problem definition:
see [ETHICS](../ETHICS.md), [PRIVACY](../PRIVACY.md), and the
[legal index](legal/legal-index.md).

## 2. Data Engineering

The canonical source registry is the signed catalog plus YAML dataset contracts
under [etl/datasets](../etl/datasets). Raw data lands in a parquet lake with
watermarks, coverage metadata, and reality probes described in the
[lake reality runbook](runbooks/lake_reality.md). Curated dimensions and signal
feature tables are documented in the [curation runbook](runbooks/curate.md) and
[signals runbook](runbooks/signals.md).

The contest MVP uses four launch datasets: SECOP II contracts, SECOP II
procurement processes, SECOP II contract suspensions, and PACO sanctions. The
selection and local join/coverage checks are preserved in the
[dataset-selection notebook](analysis/dataset_selection_analysis.ipynb).

PACO sanctions are handled as a custom non-Socrata adapter with operational
notes in the [custom adapters runbook](runbooks/custom_adapters.md).

## 3. Model Engineering

The MVP model is documented in the [anomaly model card](ai/anomaly_model.md)
and operated through the [anomaly model runbook](runbooks/anomaly_model.md). It
builds contract-level features from curated parquet, trains one deterministic
Isolation Forest, and writes promoted score parquet for the API. Generative
narration and supervised top-ups are outside the MVP launch path.

## 4. Model Evaluation

Anomaly evaluation is written to `lake/models/anomaly/<run>/metrics.json` by
`coacc-etl model train anomaly` and `coacc-etl model evaluate anomaly`. The
current metrics record evaluated rows, weak-label positives, base rate,
precision at 100 and 1,000, average precision, ROC AUC, random-ranking
expectation, and a supplier-disjoint holdout based on canonical entity identity.
PACO matches are weak evaluation labels rather than ground truth.

## 5. Deployment

The API and frontend run against the lake-backed services described in the
[architecture overview](architecture/overview.md). Runtime environment
variables, including the optional Neo4j setting, are documented in the
[environment runbook](runbooks/env.md). The public API contract is generated at
[docs/contracts/api.openapi.yaml](contracts/api.openapi.yaml) and checked by
`frontend/scripts/api-contract-test.mjs`.

Signal truth and anomaly scores read from curated parquet and promoted lake
artifacts. Neo4j is disabled in the production MVP Compose path.

## 6. Monitoring And Maintenance

Operational checks are captured in the [post-refactor plan](cleanup/post_refactor_plan.md),
[lake reality runbook](runbooks/lake_reality.md), and the repository scripts:

- `make check` for lint, typing, API tests, ETL tests, and frontend tests.
- `make backend-ready` for the local non-frontend runtime gate.
- `make api-smoke` for local Uvicorn startup, frontend meta/search/entity/
  signal/pattern/evidence/exposure/timeline/graph/baseline routes, public
  meta/pattern/graph routes, and lake-backed API route smoke.
- `make lake-reality` for local lake health and curated-table drift.
- `make curated-contracts` for shipped curated parquet schema/non-null contracts.
- `scripts/check_compliance_pack.py` for public legal/ethics baseline.
- `scripts/check_public_privacy.py` for public-surface privacy rules.
- `scripts/check_open_core_boundary.py` for open-core boundary regressions.
- `scripts/repo_publish_audit.py` for tracked/unignored public-readiness checks.
- `scripts/check_doc_links.py` for this CRISP-ML evidence trail.

Known remaining non-code launch tasks, including competition registration
artifacts, `herramientas.datos.gov.co/usos`, dress rehearsal, and travel, stay
tracked in [Phase 16](cleanup/post_refactor_plan.md).
