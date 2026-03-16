# Colombia Corruption Functionality Report

Date: March 13, 2026

This report describes what the Colombia fork successfully does today in the running workspace, what is partially implemented, and what is still missing.

## Verified live environment

- Frontend: `http://localhost:3100`
- API: `http://localhost:8000`
- Neo4j Browser: `http://localhost:7474`
- `GET /health` -> `{"status":"ok"}`
- `GET /api/v1/meta/health` -> `{"neo4j":"connected"}`

## Current live graph state

Verified from `GET /api/v1/meta/stats` on March 13, 2026 after PACO and PTE ingestion:

| Metric | Value |
| --- | ---: |
| Total nodes | 4,682,958 |
| Total relationships | 8,344,772 |
| People | 410,274 |
| Companies | 3,136,982 |
| Contracts | 21,988,788 |
| Bids / procurement records | 592,114 |
| Sanctions | 55,575 |
| Elections | 121,321 |
| Declared assets | 328,799 |
| Finance records | 495,600 |
| Implemented sources | 25 |
| Loaded sources | 20 |
| Healthy sources | 20 |

## What works successfully right now

### 1. Unified Colombia graph foundation

Status: live

The repo already runs a single Colombia-focused Neo4j graph that links:

- people
- companies
- contracts
- procurement processes
- sanctions
- public-sector roles
- campaign-finance records
- Ley 2013 disclosures
- SGR records

This is not yet the full public-sector graph originally proposed, but it is a working unified graph rather than disconnected source silos.

### 2. Suspicious people watchlist

Status: live

The dashboard currently ranks people from cross-source overlap. The live score uses evidence already present in the graph:

- public-role / payroll links
- campaign donations
- candidacies
- asset disclosures
- conflict / finance disclosures

The endpoint is live at `/api/v1/meta/watchlist/people`.

### 3. Suspicious companies watchlist

Status: live

The dashboard currently ranks companies from public corruption-risk signals. The live score currently works best on:

- sanctions
- contract exposure
- company presence across public procurement records

The endpoint is live at `/api/v1/meta/watchlist/companies`.

Important limitation:
the current loaded graph is strongest on sanctions and contract exposure. The richer procurement-chain and competition signals are implemented in code, but their new SECOP auxiliary datasets are not fully backfilled yet.

### 4. PACO sanction and red-flag enrichment

Status: live

The graph now ingests PACO public-risk feeds directly. Live load on March 13, 2026 added:

- `54,031` PACO sanction nodes
- `678` PACO company sanction edges
- `53,356` PACO person sanction edges

This materially improves sanction-history coverage beyond the original SECOP sanction surface.

### 5. PTE budget trigger layer

Status: live partial

The graph now ingests two live PTE exports:

- `pte_sector_commitments`: `32` sector commitment finance nodes
- `pte_top_contracts`: `100` top-contract finance nodes plus `300` context relationships

This gives the product an upstream PGN budget context layer for current large-contract surveillance, even though the full procurement-chain detector still depends on the remaining SECOP auxiliary backfill.

### 6. Sanction-based supplier detection

Status: live

This is the strongest proven company-side detector in the current live graph.

Verified live:

- `GET /api/v1/patterns/4:99064bd9-b887-44b4-9e83-39bb644c694d:126927?lang=en`
- result currently returns `sanctioned_supplier_record`

This means the product can already flag suppliers that appear in sanctions history and surface them in company analysis.

### 7. Entity analysis workspace

Status: live

For both people and companies, the analysis view now works as a real investigation surface:

- readable case summary
- graph visualization
- connection list
- pattern panel
- baseline panel
- timeline panel
- source attribution

This was previously broken for some company pages because the graph expansion query stalled. That is fixed.

### 8. Bounded graph traversal for company analysis

Status: live

Company analysis pages no longer hang on graph load. The graph endpoint was rewritten to use bounded traversal, so the product can render usable company graphs again.

### 9. Public-official / supplier overlap pattern

Status: implemented

This detector exists in the API pattern layer and is available for company analysis. It is intended to catch cases where a supplier is linked to a person who also appears in public-office records.

Current limitation:
it is implemented, but results depend on overlap density in the loaded graph. It is not yet the dominant live signal.

### 10. Low-competition bidding pattern

Status: implemented

This detector exists in the API pattern layer. It is intended to catch low-competition procurement behavior.

Current limitation:
it needs the newer SECOP auxiliary bidding data to be fully loaded before it becomes broadly useful in the live graph.

### 11. Budget-chain gap patterns

Status: implemented, partially live

Two company-side budget-chain detectors are already wired into the API:

- invoice vs execution gap
- invoice vs commitment gap

Current limitation:
these detectors require the new auxiliary SECOP datasets to be loaded broadly before they can produce strong live coverage.

### 12. Colombia-specific identifier model

Status: live

The repo now resolves and displays Colombia-style identifiers through:

- `document_id`
- `nit`
- `cedula`

This replaced Brazil-only assumptions around `cpf` and `cnpj` as the primary public identifier model.

## Six high-value detector status

| Detector | Status today | Notes | Access class |
| --- | --- | --- | --- |
| Budget leakage: allocation -> commitment -> contract -> invoice -> execution | Partial | PTE trigger data is now live, and the SECOP commitments, CDP, invoices, and execution pipelines already exist. Full coverage still depends on backfilling the remaining SECOP auxiliary datasets. | Build now |
| Bid-rigging from offers + shared metadata | Partial | `secop_offers` exists in code. Shared contact / address / legal-rep enrichment is not complete. | Build now |
| Officeholder / contractor overlap | Live partial | Current graph and pattern layer already support public-role vs supplier overlap logic. | Build now |
| Company-capacity mismatch | Not implemented | Needs Supersociedades financial and legal company data. | Build now |
| Relative / conflict enrichment | Partial | Ley 2013 disclosures are in. RUES / RUP is not. | Needs registered / paid access for stronger version |
| Land-benefit around public works | Not implemented | Needs IGAC transaction ingestion and stronger property-owner resolution. | Build now for anomaly signals; stronger proof needs extra access |

## Source-layer status by priority

### Already live in the graph

- `paco_sanctions`
- `pte_sector_commitments`
- `pte_top_contracts`
- `secop_integrado`
- `secop_sanctions`
- `asset_disclosures`
- `conflict_disclosures`
- `cuentas_claras_income_2019`
- parts of `SIGEP`
- parts of `SGR`

### Implemented in code, but not fully live yet

These pipelines are already built in the repo:

- `secop_offers`
- `secop_budget_commitments`
- `secop_cdp_requests`
- `secop_invoices`
- `secop_execution_locations`

These are the main building blocks for:

- budget leakage detection
- bid-rigging
- payment-chain reconciliation
- execution anomaly checks

### Still missing from the repo

These high-value layers are not yet implemented:

- `RUES / RUP`
- `MapaInversiones`
- `Registraduría defunciones / vigency checks`
- `Supersociedades`
- `IGAC property transactions`
- payroll / HR ingestion

## Functionality matrix: build now vs access-constrained

### Build now from open or public Colombia sources

- SECOP auxiliary tables
- PACO
- PTE trigger layer
- SIGEP overlap rules
- Supersociedades capacity checks
- IGAC anomaly signals
- multi-object contractor classification

### Needs registered or paid workflows for the stronger version

- RUES / RUP company-control enrichment
- stronger relative / company-control matching
- stronger land-benefit proof through property-owner services

### Needs institutional or privileged access

- real ghost-payroll engine from employer payroll / HR files
- nationwide payroll-vs-contractor anti-joins
- bulk payment-account duplication checks
- tax-record intelligence
- RUB beneficial-ownership intelligence

## Ghost employee pipeline status

Status: not implemented

The repo does not yet do the real ghost-payroll workflow described in the roadmap.

What is still missing:

- payroll or HR roster ingestion
- death-status checks by identifier
- duplicate cédula detection in payroll
- duplicate bank-account payment detection
- payroll vs contractor payment overlap

Honest assessment:
without employer payroll and payment records, the product can flag public-record inconsistencies, but it cannot honestly claim a full ghost-employee detection engine.

## Product/UI cleanup completed on March 13, 2026

The frontend display layer was cleaned up so the user-facing analysis pages stop leaking Portuguese or raw graph-internal labels for:

- source IDs such as `secop_ii_contracts`
- schema keys such as `razon_social`
- raw edge labels where relationship translations already exist

This does not rename the stored graph schema. It only normalizes what the user sees.

## Bottom line

The Colombia fork is no longer just a generic search UI. It now has a working corruption-analysis surface with:

- live suspicious-people ranking
- live suspicious-company ranking
- live sanction-driven supplier detection
- live entity case analysis
- working graph exploration
- implemented overlap and budget-gap pattern infrastructure

But it is still not the full end-state platform originally described.

What is already true:

- the Colombia graph exists
- the dashboard surfaces suspicious entities
- the product can explain and explore live suspicious cases
- the source model is Colombia-specific

What is not yet true:

- full budget-trigger intelligence from PTE / MapaInversiones
- full bid-rigging intelligence from all SECOP auxiliary data
- company-control enrichment through RUES / RUP
- company-capacity scoring through Supersociedades
- land-benefit detection through IGAC
- true ghost-payroll detection through payroll / HR data
