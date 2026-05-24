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
- Added the `paco_sanctions` custom ETL adapter, dataset contract, and tests
  so PACO public sanction feeds can land in the parquet lake without Neo4j.
- Added Phase 8 lake reality snapshots: per-dataset local parquet health metrics,
  JSON/Markdown diff artifacts, threshold config, runbook, and a pre-commit helper.

### Changed
- Made Neo4j optional at API startup via `NEO4J_REQUIRED=false`; graph-backed
  routes still require Neo4j, while lake-backed signal routes can run without it.
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
