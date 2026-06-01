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

PACO sanctions are handled as a custom non-Socrata adapter with operational
notes in the [custom adapters runbook](runbooks/custom_adapters.md).

## 3. Model Engineering

The Phase 13 model is documented in the [anomaly model card](ai/anomaly_model.md)
and operated through the [anomaly model runbook](runbooks/anomaly_model.md). It
builds contract-level features from curated parquet, trains an Isolation Forest
baseline, trains a supervised histogram-gradient-boosting top-up when weak
labels permit, and writes promoted score parquet for the API.

The Phase 14 generative narrator is documented in the
[narrator model card](ai/generative_narrator.md) and
[narrator runbook](runbooks/narrator.md). It precomputes citation-bound
Markdown narratives from lake-backed case subgraphs and verifier checks.

## 4. Model Evaluation

Anomaly evaluation is written to `lake/models/anomaly/<run>/metrics.json` by
`coacc-etl model train anomaly` and `coacc-etl model evaluate anomaly`. The
current local smoke cited in the [anomaly model card](ai/anomaly_model.md)
records both overall `precision_at_100` and deterministic holdout
`holdout_precision_at_100` against PACO-backed weak labels.

Narrator quality gates are deterministic verifier checks plus recorded
provider-response fixtures, summarized in the
[narrator runbook](runbooks/narrator.md).

## 5. Deployment

The API and frontend run against the lake-backed services described in the
[architecture overview](architecture/overview.md). Runtime environment
variables, including the optional Neo4j setting, are documented in the
[environment runbook](runbooks/env.md). The public API contract is generated at
[docs/contracts/api.openapi.yaml](contracts/api.openapi.yaml) and checked by
`frontend/scripts/api-contract-test.mjs`.

Neo4j is optional and treated as a derived exploration cache. Signal truth,
anomaly scores, narratives, and agent answers read from curated parquet and
promoted lake artifacts.

## 6. Monitoring And Maintenance

Operational checks are captured in the [post-refactor plan](cleanup/post_refactor_plan.md),
[lake reality runbook](runbooks/lake_reality.md), and the repository scripts:

- `make check` for lint, typing, API tests, ETL tests, and frontend tests.
- `make backend-ready` for the local non-frontend runtime gate.
- `make api-smoke` for local Uvicorn startup and lake-backed API route smoke.
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
