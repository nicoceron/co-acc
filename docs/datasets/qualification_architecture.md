# Source Qualification Gate

Status: implemented as pre-lake metadata gate. Outputs are the canonical ingest authority.

## Purpose

This gate prevents useless public datasets from entering the lake.

It does not download dataset rows. It reads only source inventories and Socrata metadata/schema, then classifies whether a dataset has usable join keys for the corruption-detection graph.

## Placement

```text
source inventories
  -> source qualification gate
  -> signed catalog (docs/datasets/catalog.signed.csv)
  -> per-dataset YAML contracts (etl/datasets/*.yml)        [Wave 2]
  -> generic Socrata ingester                               [Wave 3]
  -> raw lake
  -> standardized lake
  -> curated models
  -> graph / signals / API
```

The gate belongs before the lake. The raw lake should contain contracted datasets, not every plausible open-data candidate.

## Canonical outputs

Produced by `etl/src/coacc_etl/source_qualification.py`:

- `docs/datasets/catalog.signed.csv` — every probed dataset with metadata, schema, join-key classes, and LLM review verdict. This is the signed source of truth.
- `docs/datasets/catalog.proven.csv` — subset with at least one proven join key. Feeds Wave 2 YAML bootstrap.
- `docs/datasets/catalog.report.md` — human-readable summary grouped by promotion class.
- `docs/datasets/source_qualification_llm_cache.json` — Gemini review cache (runtime artifact; not signed).

## Inputs

The `--all-known` pass combines:

- `docs/datasets/colombia_open_data_audit.json` — audit of 550 portal IDs (285 valid).
- `docs/datasets/archive/dataset_relevance_appendix.csv` — retired relevance appendix. Still consumed as a hint source; will be dropped once `catalog.signed.csv` fully supplants it.
- `docs/source_registry_co_v1.csv` — current operational registry. **Scheduled for retirement in Wave 6** (still load-bearing for `api/src/coacc/services/source_registry.py` and `docker-compose.yml` mounts).
- `config/signal_source_deps.yml` — signal dependency declarations.
- env-backed Socrata dataset IDs declared by ETL pipeline classes.

## How to run

Deterministic metadata pass:

```bash
cd etl
.venv/bin/python -m coacc_etl.source_qualification \
  --all-known \
  --metadata-only
```

Required Gemini review pass:

```bash
.venv/bin/python -m coacc_etl.source_qualification \
  --llm-only \
  --llm-review \
  --llm-provider gemini
```

The Gemini pass requires one of `GEMINI_API_KEY`, `GOOGLE_API_KEY`, or `GOOGLE_GENERATIVE_AI_API_KEY`. Model: `gemini-2.5-flash-lite`.

## Promotion policy

A dataset can be promoted only if one condition is true:

- It has a deterministic or LLM-confirmed core join key: `nit`, `contract`, `process`, or `entity`.
- It has a context join key: `bpin` or `divipola`, and an explicit signal or enrichment use.
- It is a non-Socrata source already covered by a manually reviewed source contract.

Everything else stays out of source contracts and therefore out of the lake.

No code or dataset is deleted by this gate. It produces evidence for promotion, backlog, quarantine, or retirement.

## Current result (2026-04-23 run)

- 311 datasets probed.
- 148 schema-qualified with proven join keys after Gemini review.
- 118 have core keys (`nit`, `contract`, `process`, `entity`).
- 30 are context-only (`bpin`, `divipola`).
- Gemini added accepted join-key columns for 15 datasets.
- 0 ingestion-ready — this is metadata/schema only; row + freshness validation happens in Wave 3 via the generic ingester.

## Next steps (see `docs/cleanup/refactor_plan.md`)

- Wave 2: emit one YAML contract per row in `catalog.signed.csv` under `etl/datasets/<id>.yml`.
- Wave 3: generic Socrata ingester reads those YAMLs and writes parquet to the lake.
- Wave 4: migrate existing bespoke pipelines onto the generic ingester; delete legacy Neo4j path.
