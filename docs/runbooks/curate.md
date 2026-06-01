# Curated Lake Runbook

The curated layer is built from local parquet under `lake/raw/` with DuckDB.
It is the first downstream source of truth for signal features and API data
projection; Neo4j remains optional.

## Command

Build every shipped curated table:

```bash
make curate
# equivalent explicit form:
cd etl && COACC_LAKE_ROOT=../lake uv run coacc-etl curate --all
```

Build one table:

```bash
make curate TABLE=signal_feature_procurement_sanctioned_supplier_awarded
make curate TABLE=signal_feature_procurement_supplier_concentration_across_entities
make curate TABLE=signal_feature_procurement_repeat_awards_same_supplier
make curate TABLE=dim_company
make curate TABLE=dim_buyer
make curate TABLE=dim_person
```

`LAKE_ROOT` defaults to `./lake`. Override it when running against another
lake:

```bash
make curate LAKE_ROOT=/path/to/lake
```

## Current Outputs

- `lake/curated/table=dim_subject_document/`
- `lake/curated/table=dim_company/`
- `lake/curated/table=dim_buyer/`
- `lake/curated/table=dim_person/`
- `lake/curated/table=fct_procurement_contract_awards/`
- `lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded/`
- `lake/curated/table=signal_feature_procurement_supplier_concentration_across_entities/`
- `lake/curated/table=signal_feature_procurement_repeat_awards_same_supplier/`
- `lake/meta/curated/<timestamp>.json`

The full default builder requires:

- `secop_ii_contracts`, resolved from raw source `jbjy-vk9h`
- `paco_sanctions`
- `5u9e-g5w9` (SIGEP corruption-sensitive posts)
- `8tz7-h3eu` (asset declarations)

Table-specific builds only require their declared source inputs. For example,
`signal_feature_procurement_supplier_concentration_across_entities` and
`signal_feature_procurement_repeat_awards_same_supplier` require only
`secop_ii_contracts`.

`dim_company` and `dim_buyer` canonicalize Colombian NITs with the DIAN
MOD-11 verification-digit algorithm. `dim_person` uses cedula-style document
canonicalization and explicitly keeps `nit_canonical` empty as a guard against
mixing person and company identifiers.

Semantic source names are resolved through `docs/datasets/catalog.proven.csv`.

## API Exposure

The API signal list/detail endpoints read curated signal feature tables when
Neo4j has no matching materialized hits or is unavailable:

- `GET /api/v1/signals/`
- `GET /api/v1/signals/procurement_sanctioned_supplier_awarded`
- `GET /api/v1/signals/procurement_supplier_concentration_across_entities`
- `GET /api/v1/signals/procurement_repeat_awards_same_supplier`

Set `NEO4J_REQUIRED=false` to allow API startup without a graph. In that mode,
graph-backed routes still return 503, but lake-backed signal routes and
`/health` remain available.

## Memory Posture

The builder uses DuckDB `read_parquet` views and `COPY (SELECT ...) TO parquet`.
It must not materialize full joins in Python. Sanction joins are performed via
small normalized match-key views so DuckDB can hash join on one key instead of
evaluating broad multi-condition joins across the full SECOP contract table.
Supplier concentration is a grouped DuckDB aggregation over SECOP contracts and
uses `COPY (SELECT ...) TO parquet`; Python only receives table row counts.
Repeat awards is likewise grouped in DuckDB by supplier document key and buyer
document key, then written directly to parquet.

## Reality Notes

On the local lake generated during the 2026-06-01 run:

- `dim_subject_document`: 1,215,832 rows
- `dim_company`: 101,916 rows
- `dim_buyer`: 4,914 rows
- `dim_person`: 259,990 rows
- `fct_procurement_contract_awards`: 5,442,058 rows
- `signal_feature_procurement_sanctioned_supplier_awarded`: 20,184 rows
- `signal_feature_procurement_supplier_concentration_across_entities`: 497 rows
- `signal_feature_procurement_repeat_awards_same_supplier`: 9,716 rows

The first signal feature table joined 13,423 distinct SECOP contracts, 664
distinct supplier document keys, and PACO evidence from `multas_secop`,
`antecedentes_siri_sanciones`, `colusiones_en_contratacion`, and
`responsabilidades_fiscales`.

The supplier concentration feature emits supplier-level rows when a supplier
has at least 25 contracts, 50 distinct public buyers, and COP 1B total awarded
value in SECOP II contracts. Evidence refs are the top SECOP process URLs by
contract value.

The repeat-awards feature emits buyer/supplier pair rows when the same buyer
awards at least 10 contracts and COP 1B total value to the same supplier.
Evidence refs are the top SECOP process URLs by contract value.
