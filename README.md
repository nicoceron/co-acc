# co/acc open graph

**Open-source graph infrastructure for Colombia's public datasets.**

This fork adapts the `World-Open-Graph/br-acc` stack to a Colombia-only scope. It keeps the same architecture pattern, but the public source registry, ETL entrypoints, landing copy, and demo data are now centered on official Colombia datasets from [datos.gov.co](https://www.datos.gov.co/).

## What Is Included In This Public Repo

- **Graph Infrastructure:** Full Neo4j schema and indexing configuration for Colombia.
- **API Surface:** FastAPI backend with prioritized watchlist and search endpoints.
- **Frontend Explorer:** React-based graph and entity analysis interface.
- **ETL Pipelines:** 30+ curated pipelines for SECOP, SIGEP, SGR, and other official sources.
- **Source Registry:** Auditable tracking of Colombia's public source universe in [`docs/source_registry_co_v1.csv`](docs/source_registry_co_v1.csv).

## Modifications from Original

This project was modified from the original `World-Open-Graph/br-acc` (March 2026):
- **Localization:** All Brazilian entities (CPF/CNPJ) and terminology were replaced with Colombian identifiers (Cédula/NIT).
- **Pipelines:** 40+ Brazilian data pipelines were removed and replaced with 30+ Colombian-specific pipelines (SECOP, SIGEP, SGR).
- **UI:** The interface was translated into Spanish (es-CO) and adapted for Colombian public procurement transparency goals.
- **Rules:** Automated risk patterns were tuned for Colombian thresholds and legal frameworks.

## What Is Not Included By Default

- **Production Graph:** This repo does **not** include a preloaded production graph or a reference production snapshot.
- **PII Data:** Some official datasets expose personal records; public mode keeps person access disabled by default.
- **API Keys:** You must provide your own Socrata App Token for high-rate data downloads.

## What Is Reproducible Locally Today

You can build the full Colombia graph on your own hardware:

1.  **Download:** Use the `make download-secop-integrado` target (or similar) to fetch datasets from [datos.gov.co](https://www.datos.gov.co/).
2.  **Ingest:** Run `make etl-all` to process and link the data into Neo4j.
3.  **Analyze:** Use the built-in pattern engine to detect execution gaps and prioritized entities.

## Quick Start

```bash
cp .env.example .env
docker compose up -d --build
bash infra/scripts/seed-dev.sh
```

Verify:

- API: `http://localhost:8000/health`
- API docs: `http://localhost:8000/docs`
- Frontend: `http://localhost:3100` (mapped to port 3100 by default)
- Neo4j Browser: `http://localhost:7474`

## Colombia ETL Workflow

Download official datasets with the matching `make download-secop-integrado` target (and others) for each source.
 For the full curated Colombia set:

```bash
make download-secop-integrado
make download-secop-sanciones
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

Run the full Colombia pipeline set:

```bash
make etl-all
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

## Legal & Ethics

This project is governed by strict ethical and legal standards for public data processing:

- [ETHICS.md](ETHICS.md): Ethical guidelines for data-driven investigations.
- [LGPD.md](LGPD.md): Compliance with data protection principles (Ley 1581 / LGPD).
- [PRIVACY.md](PRIVACY.md): Public-surface privacy rules and redaction policies.
- [SECURITY.md](SECURITY.md): Security policy and vulnerability reporting.
- [ABUSE_RESPONSE.md](ABUSE_RESPONSE.md): Procedures for reporting and responding to data abuse.
- [TERMS.md](TERMS.md): Terms of use for the open-graph infrastructure.
- [DISCLAIMER.md](DISCLAIMER.md): Legal disclaimers regarding official data sources.

## License

AGPL-3.0-or-later.
