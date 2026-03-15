# co/acc open graph

**Open-source graph infrastructure for Colombia's public datasets.**

This fork adapts the `World-Open-Graph/br-acc` stack to a Colombia-only scope. It keeps the same architecture pattern, but the public source registry, ETL entrypoints, landing copy, and demo data are now centered on official Colombia datasets from [datos.gov.co](https://www.datos.gov.co/).

## What This Repo Does

- Maps official Colombia datasets into a Neo4j graph.
- Exposes a FastAPI API and a React explorer for search and graph inspection.
- Tracks the Colombia source universe in [`docs/source_registry_co_v1.csv`](docs/source_registry_co_v1.csv).
- Includes reproducible discovery tooling for the official Socrata catalog.
- Ships the full 30-source curated Colombia ETL surface today, including:
  - `mapa_inversiones_projects`
  - `rues_chambers`
  - `registraduria_death_status_checks`
  - `supersoc_top_companies`
  - `igac_property_transactions`
  - `paco_sanctions`
  - `pte_sector_commitments`
  - `pte_top_contracts`
  - `secop_integrado`
  - `secop_sanctions`
  - `secop_offers`
  - `secop_budget_commitments`
  - `secop_cdp_requests`
  - `secop_invoices`
  - `secop_contract_execution`
  - `secop_execution_locations`
  - `secop_contract_additions`
  - `secop_contract_modifications`
  - `secop_suppliers`
  - `secop_ii_processes`
  - `secop_ii_contracts`
  - `sigep_public_servants`
  - `sigep_sensitive_positions`
  - `asset_disclosures`
  - `conflict_disclosures`
  - `sgr_projects`
  - `sgr_expense_execution`
  - `health_providers`
  - `higher_ed_enrollment`
  - `cuentas_claras_income_2019`

## Current Colombia Scope

As of **March 14, 2026**, the curated registry covers and implements 30 official datasets, including:

- MapaInversiones project-basics reports
- RUES public chamber-of-commerce directory
- Registraduria death-status and vigency checks via normalized manual imports
- Supersociedades top-company financials
- IGAC property-market activity
- PACO sanctions and red-flag feeds
- PTE sector commitments and top-contract exports
- SECOP Integrado
- SECOP II Procesos de Contratación
- SECOP II Ofertas por Proceso
- SECOP II Contratos Electrónicos
- SECOP II Compromisos Presupuestales
- SECOP II Solicitudes CDP
- SECOP II Facturas
- SECOP II Ubicaciones de ejecución
- SECOP II Proveedores Registrados
- SECOP I / II multas y sanciones
- SIGEP public servants
- Ley 2013 asset and conflict disclosures
- Sistema General de Regalías execution and project datasets
- REPS health providers
- MEN higher-education enrollment
- Cuentas Claras campaign income

The source list is generated from official portal metadata with:

```bash
python3 scripts/sync_colombia_portal_registry.py
```

## Quick Start

```bash
cp .env.example .env
docker compose up -d --build
bash infra/scripts/seed-dev.sh
```

Verify:

- API: `http://localhost:8000/health`
- API docs: `http://localhost:8000/docs`
- Frontend: `http://localhost:3000`
- Neo4j Browser: `http://localhost:7474`

## Colombia ETL Workflow

Download official datasets with the matching `make download-*` target for each source. For the full curated Colombia set:

```bash
make download-secop-integrado
make download-secop-sanciones
make download-secop-ejecucion
make download-secop-adiciones
make download-secop-modificaciones
make download-secop-proveedores
make download-secop-procesos
make download-secop-ofertas
make download-secop-contratos
make download-secop-compromisos
make download-secop-solicitudes-cdp
make download-secop-facturas
make download-secop-ubicaciones-ejecucion
make download-sigep-servidores
make download-sigep-cargos-sensibles
make download-ley2013-activos
make download-ley2013-conflictos
make download-sgr-proyectos
make download-sgr-gastos
make download-reps-salud
make download-men-matricula
make download-cuentas-claras-2019
make download-paco-sanciones
make download-pte-compromisos-sector
make download-pte-contratos-grandes
make download-mapa-proyectos
make download-rues-camaras
make download-supersoc-1000
make download-igac-transacciones
```

For Registraduria checks, normalize a manually collected CSV into the ETL schema before loading:

```bash
cd etl
uv run python scripts/prepare_registraduria_death_status_checks.py \
  --input ../data/registraduria_death_status_checks/raw_checks.csv \
  --output ../data/registraduria_death_status_checks/registraduria_death_status_checks.csv
```

Run the full Colombia pipeline set:

```bash
make etl-all
```

Refresh docs from the registry:

```bash
make sync-colombia-registry
make generate-pipeline-status
make generate-source-summary
```

## Architecture

| Layer | Technology |
|---|---|
| Graph DB | Neo4j 5 Community |
| Backend | FastAPI |
| Frontend | Vite + React + TypeScript |
| ETL | Python + pandas + httpx |
| Source Discovery | Socrata catalog API |

## Source Model

The Colombia fork uses a more generic identifier model than the Brazil original:

- `Company.document_id` / `Company.nit`
- `Person.document_id` / `Person.cedula`

This matters because Colombia datasets do not expose a single CNPJ-style canonical registry in the same way the Brazil fork did. For procurement-first graph loading, SECOP contractor identifiers are the current anchor.

## Demo Data

The local seed remains synthetic and public-safe. It exists only to make the stack usable immediately after boot; it is not a production snapshot.

## Important Limits

- This repo does **not** include a preloaded production graph.
- Some official datasets expose personal records; public mode keeps person access disabled by default.
- Pattern queries are inherited from the original stack but are not yet re-tuned for Colombia-specific semantics.

## Key Files

- [`docs/source_registry_co_v1.csv`](docs/source_registry_co_v1.csv)
- [`docs/data-sources.md`](docs/data-sources.md)
- [`docs/pipeline_status.md`](docs/pipeline_status.md)
- [`scripts/sync_colombia_portal_registry.py`](scripts/sync_colombia_portal_registry.py)
- [`etl/src/coacc_etl/pipelines/paco_sanctions.py`](etl/src/coacc_etl/pipelines/paco_sanctions.py)
- [`etl/src/coacc_etl/pipelines/pte_sector_commitments.py`](etl/src/coacc_etl/pipelines/pte_sector_commitments.py)
- [`etl/src/coacc_etl/pipelines/pte_top_contracts.py`](etl/src/coacc_etl/pipelines/pte_top_contracts.py)
- [`etl/src/coacc_etl/pipelines/mapa_inversiones_projects.py`](etl/src/coacc_etl/pipelines/mapa_inversiones_projects.py)
- [`etl/src/coacc_etl/pipelines/rues_chambers.py`](etl/src/coacc_etl/pipelines/rues_chambers.py)
- [`etl/src/coacc_etl/pipelines/registraduria_death_status_checks.py`](etl/src/coacc_etl/pipelines/registraduria_death_status_checks.py)
- [`etl/src/coacc_etl/pipelines/supersoc_top_companies.py`](etl/src/coacc_etl/pipelines/supersoc_top_companies.py)
- [`etl/src/coacc_etl/pipelines/igac_property_transactions.py`](etl/src/coacc_etl/pipelines/igac_property_transactions.py)
- [`etl/src/coacc_etl/pipelines/secop_integrado.py`](etl/src/coacc_etl/pipelines/secop_integrado.py)
- [`etl/src/coacc_etl/pipelines/secop_sanctions.py`](etl/src/coacc_etl/pipelines/secop_sanctions.py)
- [`etl/src/coacc_etl/pipelines/secop_ii_processes.py`](etl/src/coacc_etl/pipelines/secop_ii_processes.py)
- [`etl/src/coacc_etl/pipelines/secop_offers.py`](etl/src/coacc_etl/pipelines/secop_offers.py)
- [`etl/src/coacc_etl/pipelines/secop_ii_contracts.py`](etl/src/coacc_etl/pipelines/secop_ii_contracts.py)
- [`etl/src/coacc_etl/pipelines/secop_budget_commitments.py`](etl/src/coacc_etl/pipelines/secop_budget_commitments.py)
- [`etl/src/coacc_etl/pipelines/secop_cdp_requests.py`](etl/src/coacc_etl/pipelines/secop_cdp_requests.py)
- [`etl/src/coacc_etl/pipelines/secop_invoices.py`](etl/src/coacc_etl/pipelines/secop_invoices.py)
- [`etl/src/coacc_etl/pipelines/secop_suppliers.py`](etl/src/coacc_etl/pipelines/secop_suppliers.py)
- [`etl/src/coacc_etl/pipelines/secop_contract_execution.py`](etl/src/coacc_etl/pipelines/secop_contract_execution.py)
- [`etl/src/coacc_etl/pipelines/secop_execution_locations.py`](etl/src/coacc_etl/pipelines/secop_execution_locations.py)
- [`etl/src/coacc_etl/pipelines/secop_contract_additions.py`](etl/src/coacc_etl/pipelines/secop_contract_additions.py)
- [`etl/src/coacc_etl/pipelines/secop_contract_modifications.py`](etl/src/coacc_etl/pipelines/secop_contract_modifications.py)
- [`etl/src/coacc_etl/pipelines/sigep_public_servants.py`](etl/src/coacc_etl/pipelines/sigep_public_servants.py)
- [`etl/src/coacc_etl/pipelines/sigep_sensitive_positions.py`](etl/src/coacc_etl/pipelines/sigep_sensitive_positions.py)
- [`etl/src/coacc_etl/pipelines/asset_disclosures.py`](etl/src/coacc_etl/pipelines/asset_disclosures.py)
- [`etl/src/coacc_etl/pipelines/conflict_disclosures.py`](etl/src/coacc_etl/pipelines/conflict_disclosures.py)
- [`etl/src/coacc_etl/pipelines/sgr_projects.py`](etl/src/coacc_etl/pipelines/sgr_projects.py)
- [`etl/src/coacc_etl/pipelines/sgr_expense_execution.py`](etl/src/coacc_etl/pipelines/sgr_expense_execution.py)
- [`etl/src/coacc_etl/pipelines/health_providers.py`](etl/src/coacc_etl/pipelines/health_providers.py)
- [`etl/src/coacc_etl/pipelines/higher_ed_enrollment.py`](etl/src/coacc_etl/pipelines/higher_ed_enrollment.py)
- [`etl/src/coacc_etl/pipelines/cuentas_claras_income_2019.py`](etl/src/coacc_etl/pipelines/cuentas_claras_income_2019.py)

## Legal & Ethics

This project is governed by strict ethical and legal standards for public data processing:

- [ETHICS.md](ETHICS.md): Ethical guidelines for data-driven investigations.
- [LGPD.md](LGPD.md): Compliance with data protection principles.
- [PRIVACY.md](PRIVACY.md): Public-surface privacy rules and redaction policies.
- [SECURITY.md](SECURITY.md): Security policy and vulnerability reporting.
- [ABUSE_RESPONSE.md](ABUSE_RESPONSE.md): Procedures for reporting and responding to data abuse.
- [TERMS.md](TERMS.md): Terms of use for the open-graph infrastructure.
- [DISCLAIMER.md](DISCLAIMER.md): Legal disclaimers regarding official data sources.

## License

AGPL-3.0-or-later.
