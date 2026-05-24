# Custom Adapters

Custom adapters cover public sources that are not Socrata datasets but still
belong in the parquet lake. They must use the same `DatasetSpec` contract and
return the same `IngestResult` shape as the default Socrata adapter.

## PACO Sanctions

Dataset id: `paco_sanctions`

Adapter: `paco_sanctions`

Command:

```bash
make ingest DATASET=paco_sanctions FULL_REFRESH=1
```

The adapter downloads four public PACO feeds:

- `antecedentes_siri_sanciones`
- `colusiones_en_contratacion`
- `multas_secop`
- `responsabilidades_fiscales`

Outputs land under:

```text
lake/raw/source=paco_sanctions/snapshot=<iso>/
```

The raw snapshot includes canonical join/evidence columns:

- `subject_document_id`
- `subject_name`
- `subject_type`
- `sanction_type`
- `sanction_date`
- `reference`
- `contract_id`
- `amount`
- `affected_entity`
- `raw_record_json`

All original source fields are retained in `raw_record_json`; feed-specific
`raw_*` columns may also be present in individual parquet parts. Later
curation must use DuckDB joins against `subject_document_id`; Neo4j is not
required.

Legal posture: PACO publishes these files as public data. Treat the adapter
as evidence indexing, not accusation generation: every downstream signal must
show the source feed and evidence row behind the hit.
