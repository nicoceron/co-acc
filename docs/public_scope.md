# Public Scope

This page defines the public boundary of `co/acc`.

## Included In Public Repo

- FastAPI API (`api/`) and public-safe routers.
- ETL framework and pipeline modules (`etl/`).
- Frontend explorer (`frontend/`).
- Dockerized local infrastructure (`infra/`).
- Compliance and governance documents.
- Synthetic demo data under `data/demo/`.

## Not Included By Default

- A pre-populated production Neo4j database dump.
- Private/institutional operational modules.
- Guarantees that every external government portal is reachable at all times.

## Reproducibility Modes

| Mode | What you get | Command |
|---|---|---|
| `demo_local` | Deterministic local stack + seeded demo graph | `bash infra/scripts/seed-dev.sh` |
| `byo_ingestion` | SECOP downloads plus Colombia ETL runs | `make download-secop-integrado && make download-secop-sanciones && make etl-all` |
| `registry_snapshot` | Timestamped official source registry snapshot | `docs/source_registry_co_v1.csv` |

## Transparency Notes

- Source availability and load status are tracked in `docs/source_registry_co_v1.csv`.
- Registry-backed summary is generated into `docs/data-sources.md`.
- Registry metadata is refreshed from the Socrata catalog by `scripts/sync_colombia_portal_registry.py`.
