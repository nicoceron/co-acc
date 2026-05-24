# Curated Lake Runbook

The curated layer is built from local parquet under `lake/raw/` with DuckDB.
It is the first downstream source of truth for signal features and API data
projection; Neo4j remains optional.

## Command

Build every shipped curated table:

```bash
make curate
```

Build one table:

```bash
make curate TABLE=signal_feature_procurement_sanctioned_supplier_awarded
```

`LAKE_ROOT` defaults to `./lake`. Override it when running against another
lake:

```bash
make curate LAKE_ROOT=/path/to/lake
```

## Current Outputs

- `lake/curated/table=dim_subject_document/`
- `lake/curated/table=fct_procurement_contract_awards/`
- `lake/curated/table=signal_feature_procurement_sanctioned_supplier_awarded/`
- `lake/meta/curated/<timestamp>.json`

The current builder requires:

- `secop_ii_contracts`, resolved from raw source `jbjy-vk9h`
- `paco_sanctions`

Semantic source names are resolved through `docs/datasets/catalog.proven.csv`.

## Memory Posture

The builder uses DuckDB `read_parquet` views and `COPY (SELECT ...) TO parquet`.
It must not materialize full joins in Python. Sanction joins are performed via
small normalized match-key views so DuckDB can hash join on one key instead of
evaluating broad multi-condition joins across the full SECOP contract table.

## Reality Notes

On the local lake generated during the 2026-05-24 run:

- `dim_subject_document`: 1,215,832 rows
- `fct_procurement_contract_awards`: 5,442,058 rows
- `signal_feature_procurement_sanctioned_supplier_awarded`: 20,184 rows

The first signal feature table joined 13,423 distinct SECOP contracts, 664
distinct supplier document keys, and PACO evidence from `multas_secop`,
`antecedentes_siri_sanciones`, `colusiones_en_contratacion`, and
`responsabilidades_fiscales`.
