# Repo Implementation Gap Audit

As of `2026-03-27`, the live runtime graph is materially ahead of the static source-status docs.

## Live Runtime Reality

- `52` source entries are exposed by `GET /api/v1/meta/sources`
- `51` are `loaded`
- `1` is `blocked_external`
- `0` promoted sources are currently not loaded at runtime

The only truly unresolved source at runtime is:

- `registraduria_death_status_checks`
  - status: `blocked_external`
  - load_state: `not_loaded`
  - access mode: `manual_import`
  - blocker: external/manual Registraduría checks

## What Was Missing And Is Now Fixed

- Static documentation drift
  - `docs/pipeline_status.md` and `docs/data-sources.md` were being generated from `docs/source_registry_co_v1.csv` only.
  - That made the docs claim dozens of sources were still `not_loaded` even though the live API already showed them as loaded.
  - Fix applied:
    - `scripts/generate_pipeline_status.py` now accepts `--api-base` and overlays runtime source status from `/api/v1/meta/sources`
    - `scripts/generate_data_sources_summary.py` now accepts `--api-base` and uses the same live overlay for counts
    - `Makefile` now runs both generators with `--api-base http://localhost:8000`

- Archive document-type evidence
  - `dmgg-8hin` is now not just raw archive metadata.
  - The repo now models typed archive evidence on `CONTRATOU`:
    - `archive_supervision_document_count`
    - `archive_payment_document_count`
    - `archive_assignment_document_count`
    - `archive_start_record_document_count`
    - `archive_resume_document_count`
    - `archive_report_document_count`
  - This is already wired through:
    - ETL: `etl/src/coacc_etl/pipelines/secop_document_archives.py`
    - graph API: `api/src/coacc/routers/graph.py`
    - materialized investigations: `scripts/materialize_real_results.py`

## Real Remaining Gaps

### 1. Archive Evidence Is Now In Main API Watchlist Ranking

Current state:

- archive document-type evidence is used in materialized investigation generation
- archive document-type evidence is exposed in graph expansion
- archive document-type evidence is now also used in:
  - `api/src/coacc/queries/meta_prioritized_people.cypher`
  - `api/src/coacc/queries/meta_prioritized_companies.cypher`
  - `api/src/coacc/models/dashboard.py`
  - `api/src/coacc/routers/meta.py` watchlist responses and alert text

Why this matters:

- the live API watchlists and the materialized investigations are now aligned on archive-backed risk
- archive-backed supervision/payment/designation evidence is no longer hidden in only one ranking path

Remaining limitation:

- buyers and territories still do not expose archive-type evidence as first-class ranking fields

### 2. Frontend Has No Archive-Type Facets

Current state:

- archive evidence is present in the graph payload and dossier JSON
- there are no public UI facets/chips/filters for:
  - supervision docs
  - payment docs
  - designation/delegation docs
  - acta de inicio docs
  - hoja de vida docs
  - report docs

Why this matters:

- users can inspect the evidence if they open a dossier or raw graph
- they cannot yet sort or filter watchlist/investigation views by document-backed evidence type

Recommended next implementation:

- add archive-type chips in the investigations/results UI
- add one-click filters for `supervision`, `payment`, and `designation`

### 3. Registraduría Still Depends On Manual/External Access

Current state:

- the pipeline is implemented conceptually in the registry
- runtime status remains `blocked_external`

Why this matters:

- this is still the only live source gap in the Colombia source universe
- it would be useful for death-status / identity-vigency screening, but it is not currently automatable in the same way as the open datasets

Recommended next implementation:

- keep it explicitly manual
- treat it as a controlled investigator import, not an automated public bootstrap step

## Things That Should Stay Out

These ideas were tested and should remain out unless a better identifier appears:

- person-level `SECOP contractor ↔ graduate by cédula` from public SNIES/ICFES slices
  - current official public datasets checked in this repo do not expose student document numbers
- weak name-only local roster joins
  - they create noise, not defensible corruption signals
- dataset-title-driven ingestion
  - the correct rule remains schema-first + connected-batch overlap testing

## Bottom Line

The repo is no longer missing broad source implementation. The main remaining gaps are now narrower:

1. archive document-type evidence should be filterable in the frontend
2. buyer/territory watchlists still do not surface archive-backed ranking fields
3. Registraduría remains the only truly blocked external/manual source
