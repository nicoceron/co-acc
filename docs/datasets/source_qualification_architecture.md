# Source Qualification Gate

Generated: 2026-04-23
Status: implemented as pre-lake metadata gate

## Purpose

This gate prevents useless public datasets from entering the lake.

It does not download dataset rows. It reads only source inventories and Socrata metadata/schema, then classifies whether a dataset has usable join keys for the corruption-detection graph.

## Placement

```text
source inventories
  -> source qualification gate
  -> signed catalog
  -> source contracts
  -> raw lake
  -> standardized lake
  -> curated models
  -> graph/signals/API
```

The gate belongs before the lake. The raw lake should contain contracted datasets, not every plausible open-data candidate.

## Inputs

The all-known cleanup pass combines:

- `docs/datasets/colombia_open_data_audit.json`
- `docs/datasets/dataset_relevance_appendix.csv`
- `docs/source_registry_co_v1.csv`
- `config/signal_source_deps.yml`
- env-backed Socrata dataset IDs declared by ETL pipeline classes

## Outputs

Recommended metadata-only deterministic pass:

```bash
cd etl
.venv/bin/python -m coacc_etl.source_qualification \
  --all-known \
  --metadata-only
```

Required Gemini review pass after deterministic metadata triage:

```bash
.venv/bin/python -m coacc_etl.source_qualification \
  --llm-only \
  --llm-review \
  --llm-provider gemini
```

The Gemini pass requires `GEMINI_API_KEY`, `GOOGLE_API_KEY`, or `GOOGLE_GENERATIVE_AI_API_KEY`.

## Promotion Policy

A dataset can be promoted only if one condition is true:

- It has a deterministic or LLM-confirmed core join key: `nit`, `contract`, `process`, or `entity`.
- It has a context join key: `bpin` or `divipola`, and an explicit signal or enrichment use.
- It is a non-Socrata source already covered by a manually reviewed source contract.

Everything else stays out of source contracts and therefore out of the lake.

No code or dataset is deleted by this gate. It produces evidence for promotion, backlog, quarantine, or retirement.
