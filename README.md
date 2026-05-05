# co/acc — Colombian Open Anti-Corruption Graph

**Open-source data infrastructure that turns fragmented Colombian government datasets into a single auditable lake of corruption-relevant signals.** Solo-dev, AGPL, designed to run on a memory-constrained server.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)
[![Datasets in catalog: 148](https://img.shields.io/badge/catalog-148_datasets-green.svg)](docs/datasets/catalog.signed.csv)
[![Ingest-ready: 42](https://img.shields.io/badge/ingest_ready-42-brightgreen.svg)](etl/datasets/)
[![Ingester source classes: 2](https://img.shields.io/badge/ingester-incremental_+_snapshot-blue.svg)](etl/src/coacc_etl/ingest/socrata.py)

---

## What it does

Colombian public data is transparent but **fragmented**. A contractor might appear in SECOP II, their campaign donations in Cuentas Claras, their public-office history in SIGEP, and their fiscal sanctions in PACO. `co/acc` pulls all of those into a single parquet lake under one canonical catalog so investigators (and downstream signal queries) can join them.

Sample patterns the lake makes joinable:

- **Donor → official → vendor loops** — campaign donor in `cuentas_claras_income_2019` appears as a public servant in `sigep_public_servants` and wins contracts in `secop_ii_contracts`.
- **Sanctioned-supplier-still-winning** — supplier in `paco_sanctions` / `secop_sanctions` continues to receive new awards in `secop_ii_contracts`.
- **Self-dealing** — public servant in `sigep_public_servants` declares a conflict in `conflict_disclosures`, then their NIT shows up as a contractor in `secop_ii_contracts` with the same buyer entity.

---

## Architecture

```
docs/datasets/catalog.signed.csv     ← signed catalog of every probed dataset (148 rows)
docs/datasets/catalog.proven.csv     ← those with proven join keys (148 rows)
                          │
                          ▼
etl/datasets/<id>.yml                ← one YAML contract per dataset
                          │
                          ▼
coacc_etl.ingest.socrata.ingest()    ← generic Socrata ingester
                          │
                          ▼
lake/raw/source=<id>/year=YYYY/month=MM/   ← incremental partition
lake/raw/source=<id>/snapshot=<iso>/        ← snapshot partition (full_refresh_only)
lake/meta/{watermarks,coverage,failures}/  ← operational metadata
                          │
                          ▼
api/  +  frontend/  +  signals/             ← downstream consumers
```

**Two source classes today** (Wave 4.C):

- **Incremental** — dataset has a row-level timestamp (`fecha_*`, `periodo`, etc.). Each run pulls rows newer than the last watermark via Socrata `$where`. Watermark advances to `max(batch[watermark_column])`, never wall-clock.
- **Snapshot** (`full_refresh_only: true`) — dataset is republished wholesale by the upstream system (no row-level timestamp). Each run pulls every row and writes to a fresh `snapshot=<iso>/` partition.

**Custom non-Socrata adapters** (PACO, RUES, Registraduría, official_case_bulletins, etc.) live as backlog under `_KNOWN_DEFERRED_SOURCES` in `etl/tests/test_signal_source_alignment.py`; they'll land under `coacc_etl.ingest.custom/` as they are built.

---

## Quick start

### Prerequisites
- Python ≥3.12 with [`uv`](https://docs.astral.sh/uv/) (fast Python dependency manager)
- ~50 GB free disk for the full lake (smaller datasets work on much less)

### Install + smoke test
```bash
git clone https://github.com/nicoceron/co-acc.git
cd co-acc/etl
uv sync                              # installs the etl workspace
uv run pytest                        # 76 tests, no live HTTP required

# Optional: copy any LLM keys for the qualification gate
cp ../.env.example ../.env           # then edit to taste
```

### Ingest one dataset
```bash
# Hallazgos Fiscales — 73 rows, ideal for a sub-second smoke test
uv run coacc-etl ingest 8qxx-ubmq

# Inspect the result
ls lake/raw/source=8qxx-ubmq/year=*/month=*/
cat lake/meta/coverage/8qxx-ubmq/*.json
```

### Ingest everything that's YAML-ready
```bash
make ingest-all                       # walks tier=core, deps-safe order
```

### Run the qualification gate (probe Socrata + LLM second-pass)
```bash
make qualify QUALIFY_ARGS="--all-known --llm-review"
# Writes:
#   docs/datasets/catalog.signed.csv
#   docs/datasets/catalog.proven.csv
#   docs/datasets/catalog.report.md
```

### Make targets (the working set)
```bash
make ingest DATASET=<id> [FULL_REFRESH=1]
make ingest-all [FULL_REFRESH=1] [CONTINUE_ON_ERROR=1]
make qualify QUALIFY_ARGS="..."
make lake-init lake-reality lake-compact
make test  test-etl  test-api  test-frontend
make lint  type-check  format
```

---

## Adding a dataset

1. Put a row in `docs/datasets/catalog.signed.csv` (qualification will do this for you, or hand-edit) and `catalog.proven.csv` if it has a join key.
2. Drop a YAML contract in `etl/datasets/<socrata-4x4-id>.yml`. Minimal incremental shape:

```yaml
id: 8qxx-ubmq
name: Hallazgos Fiscales
sector: sanctions
tier: core
join_keys:
  nit: [nit]
watermark_column: fecha_recibo_traslado
partition_column: fecha_recibo_traslado
columns_map:
  nit: nit
  entity_name: nombre_sujeto
  received_date: fecha_recibo_traslado
  amount: cuant_a
required_coverage:
  nit: 0.95
  fecha_recibo_traslado: 0.80
freq: monthly
url: https://www.datos.gov.co/d/8qxx-ubmq
```

For a snapshot dataset (no row-level timestamp), set `full_refresh_only: true` and leave `watermark_column` / `partition_column` as `null`.

3. `uv run coacc-etl ingest <id>` to validate end-to-end.

---

## Repo layout

```
co-acc/
├── etl/                     # this workspace
│   ├── src/coacc_etl/
│   │   ├── catalog/         # YAML loader + Pydantic DatasetSpec
│   │   ├── ingest/          # generic Socrata ingester
│   │   ├── lakehouse/       # parquet writer/reader/watermark
│   │   ├── qualification/   # 6 submodules: probe, llm, promotion, …
│   │   ├── schemas/  transforms/  entity_resolution/
│   │   ├── cli.py           # 3 commands: ingest, ingest-all, qualify
│   │   └── source_qualification.py  # back-compat shim
│   ├── datasets/            # 148 YAML contracts (42 ingest-ready)
│   └── tests/               # 76 tests, all pass on a clean clone
├── api/                     # FastAPI service (separate uv workspace)
├── frontend/                # React/Vite UI
├── config/
│   ├── signal_registry.yml
│   ├── signal_source_deps.yml
│   └── bootstrap_all_contract.yml
├── docs/
│   ├── datasets/            # signed catalog, report, architecture
│   ├── competition/         # MinTIC competition planning
│   └── cleanup/             # refactor plan + history
├── infra/
│   ├── caddy/               # Caddyfile for prod
│   ├── docker/              # docker-compose.prod{,images}.yml
│   └── neo4j/               # init.cypher, import dir
├── docker-compose.yml       # dev compose (root)
├── lake/                    # gitignored runtime
└── Makefile
```

---

## Where the Neo4j path went

Pre-Wave-4, this project ran ~60 hand-written `Pipeline` subclasses that read CSVs and loaded a Neo4j graph. That stack was retired in Wave 4.B (commit `c4a9e201`):

- The `Pipeline` base class, `Neo4jBatchLoader`, `pipeline_registry`, every `pipelines/*.py`, and the `neo4j` Python dependency are gone.
- The lake is the canonical source. A future graph loader can read parquet into Neo4j (or DuckDB, or whatever) as a downstream consumer rather than as ETL.
- The `api/` workspace still holds Cypher queries as forward-looking artifacts; signal materialization will rebuild graph state from the lake when downstream lands.

---

## Legal & ethics

- **[ETHICS.md](ETHICS.md)** — guidelines for data-driven investigations
- **[LGPD.md](LGPD.md)** — Ley 1581 / LGPD compliance
- **[PRIVACY.md](PRIVACY.md)** — public-surface privacy + redaction rules
- **[SECURITY.md](SECURITY.md)** — security policy + vulnerability reporting
- **[ABUSE_RESPONSE.md](ABUSE_RESPONSE.md)** — abuse reporting + response
- **[TERMS.md](TERMS.md)** — terms of use
- **[DISCLAIMER.md](DISCLAIMER.md)** — official-source disclaimers

---

## License

AGPL-3.0. See [LICENSE](LICENSE).

*Part of the World Open Graph initiative.*
