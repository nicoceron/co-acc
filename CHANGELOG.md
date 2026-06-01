# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), versioning follows [SemVer](https://semver.org/).

---

## [Unreleased]

### Added
- Added a lake-backed `/api/v1/signals` fallback so curated signal counts and
  samples are available even when Neo4j is not running.
- Added the first DuckDB-curated parquet builder and CLI (`coacc-etl curate` /
  `make curate`) for subject documents, SECOP II contract awards, and the
  PACO-backed `procurement_sanctioned_supplier_awarded` signal feature table.
- Added the SECOP-only
  `procurement_supplier_concentration_across_entities` curated signal feature
  table for supplier exposure across many public buyers.
- Added the SECOP-only `procurement_repeat_awards_same_supplier` curated
  signal feature table for high-repeat buyer/supplier award exposure.
- Added typed curated `dim_company`, `dim_buyer`, and `dim_person` tables with
  Colombian NIT and cedula canonicalization.
- Added `coacc-etl signals materialize --all` to write lake-backed
  `signal_hits`, `evidence_bundles`, and signal run manifests from the shipped
  curated signal feature tables.
- Added API readers for materialized lake signal runs, including offline
  `/api/v1/signals` samples and public-safe case evidence dossiers when Neo4j
  is unavailable.
- Added lake-backed entity lookup, entity search, and entity signal drilldown
  APIs from curated dimensions and materialized signal runs for Neo4j-offline
  local runtime.
- Added lake-backed pattern API fallbacks for materialized signal-backed
  public patterns when Neo4j is unavailable.
- Added the first Phase 13 anomaly-model batch workflow: DuckDB feature
  materialization, Isolation Forest training, batched score parquet output,
  model promotion metadata, tests, and runbook/model-card docs.
- Added API readers for promoted anomaly score parquet so lake-backed cases
  and entity anomaly-score drilldowns work without Neo4j.
- Added the first Phase 14 narrator foundation: lake-backed case subgraph
  extraction, deterministic prompts, optional LLM provider calls, verifier,
  templated fallback, CLI, and docs.
- Added lake-backed case API fields for precomputed narrator Markdown.
- Added `coacc-etl narrator generate-batch` to precompute narratives for top
  promoted anomaly cases.
- Added recorded no-network narrator fixture coverage for 10 subgraphs and
  expanded verifier guard tests.
- Added curated-table coverage to `make lake-reality`, including row counts,
  freshness, manifest reconciliation, schema hashes, and evidence-ref checks
  for signal feature tables.
- Added the `paco_sanctions` custom ETL adapter, dataset contract, and tests
  so PACO public sanction feeds can land in the parquet lake without Neo4j.
- Added Phase 8 lake reality snapshots: per-dataset local parquet health metrics,
  JSON/Markdown diff artifacts, threshold config, runbook, and a pre-commit helper.

### Changed
- Made Neo4j optional at API startup via `NEO4J_REQUIRED=false`; graph-backed
  routes still require Neo4j, while lake-backed signal routes can run without it.
- Made materialized signal readers deduplicate repeated `hit_id` and evidence
  rows defensively, and made the ETL materializer emit one row per deterministic
  signal hit.
- Updated `make api` to start through `python -m uvicorn` with the repo-level
  `COACC_LAKE_ROOT`, so local API runs see the populated parquet lake by default.
- Added the legal index required by the compliance gate and aligned public
  pattern queries with the public privacy checker.
- Resolved semantic signal source ids such as `secop_ii_contracts` to raw lake
  dataset ids such as `jbjy-vk9h` through the signed catalog when registering
  DuckDB source views.
- Reframed the post-refactor plan around DuckDB/lake-first signal detection,
  with Neo4j demoted to an optional visualization/cache projection.
- Made Phase 7 full ingest more resilient for large Socrata sources with
  adaptive request timeouts, retryable malformed JSON responses, sentinel
  partitioning for missing date columns, and keyset pagination for the
  multi-million-row core datasets.

## [v0.4.0] - 2026-03-02

### Added
- **Dual auth**: Bearer token + httpOnly cookie support, with transparent fallback between both
- `GET /api/v1/auth/me` returns `restored: true` when session is restored via cookie
- `POST /api/v1/investigations/{id}/share` — share investigations with optional `expires_at`
- `DELETE /api/v1/investigations/{id}/share` — revoke shared investigations
- ETL schema validation framework (`etl/src/coacc_etl/schemas/`) with Pandera validators for CNPJ, TSE, DOU, PGFN, and Transparência
- Community bootstrap framework for contributor scripts, CI templates, and onboarding

### Changed
- Rate limiter now keys by authenticated user, not just IP
- CORS configured with `credentials: true` to support cookie auth
- Improved search with Lucene escaping and server-side result count
- Improved download scripts for CNPJ, DOU, STF, renúncias, TSE bens/filiados
- mypy strict mode enabled; inline `type-ignore` added for libs without stubs (weasyprint, splink, pyarrow, defusedxml)
- ruff lint hardening across codebase

### Compatibility
- No breaking changes to the public API
- Clients using Bearer tokens continue working without modification
- Cookie auth is opt-in

---

## [v0.3.0] - 2026-03-01

### Added
- 8 factual public-safe community patterns:
  - `sanctioned_still_receiving` (P02): active sanction and contract date overlap
  - `amendment_beneficiary_contracts` (P09): amendment/grant beneficiary with recorded contracts
  - `split_contracts_below_threshold` (P19): recurring contracts below configured threshold
  - `contract_concentration` (P24): supplier spend concentration above threshold in an agency
  - `embargoed_receiving` (P36): environmental embargo coexisting with public contract/loan flow
  - `debtor_contracts` (P37): high active debt with recurring public contract receipts
  - `srp_multi_org_hitchhiking` (P56): same SRP/ARP bid linked to multiple agencies
  - `inexigibility_recurrence` (P57): recurring inexigibility for supplier+agency+object
- 8 dedicated `public_pattern_*.cypher` query files
- Milestone-based SemVer release system with PT+EN release notes
- ComprasNet ETL now creates deterministic `(:Contract)-[:REFERENTE_A]->(:Bid)` linkage

### Changed
- `GET /api/v1/patterns/` (community) expanded from 4 to 8 patterns
- Public payload standardized with `risk_signal`, `evidence_refs`, `evidence_count`
- Public boundary hardening: `CLAUDE.md` and `AGENTS*.md` blocked from tracked/public scope

### Compatibility
- No breaking changes
- No migration required

---

## [v0.2.0] - 2026-03-01

### Added
- Full code scope for the World Transparency Graph public edition
- Public language and scope gates
- Snapshot, privacy, compliance, and security checks

### Changed
- Pattern engine endpoints disabled with explicit `503` responses pending validation

---

## [v0.1.0-public-alpha] - 2026-03-01

### Added
- Initial public-safe open-core snapshot
- Public-mode API guards
- Privacy gate tooling
- Synthetic demo dataset (`data/demo/`)
- Baseline CI and security workflows

---

> **Public integrity notice**: Signals and patterns in this project reflect co-occurrences in public records and do not constitute legal proof.
