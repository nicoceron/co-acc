# Lake Reality Runbook

The lake reality probe verifies local parquet health without downloading the
source data into memory. DuckDB scans parquet files in place and writes two
artifacts per run:

- `lake/meta/reality/YYYY-MM-DD.json`
- `lake/meta/reality/YYYY-MM-DD.diff.md`

## Source Of Truth

Dataset membership and column expectations come from `etl/datasets/*.yml`.
Those YAML contracts are the source of truth for:

- dataset IDs and names
- join-key columns
- source-to-canonical `columns_map`
- required coverage thresholds
- watermark and partition columns

The probe resolves YAML source column names to the canonical parquet column
names before checking null rates. This matters because ingest validates coverage
against Socrata field names, then writes renamed canonical columns to the lake.

## Run Commands

Scan every catalog-backed source and curated table present in the local lake:

```bash
make lake-reality
```

Scan one or more datasets:

```bash
make lake-reality DATASET=8qxx-ubmq
make lake-reality DATASETS=8qxx-ubmq,rpmr-utcd
```

Explicit dataset scans stay narrow. Scan one or more curated tables directly:

```bash
make lake-reality CURATED_TABLE=signal_feature_procurement_sanctioned_supplier_awarded
make lake-reality CURATED_TABLES=signal_feature_procurement_sanctioned_supplier_awarded,signal_feature_procurement_supplier_concentration_across_entities
```

Skip curated-table checks when you only want raw-source reality:

```bash
make lake-reality SKIP_CURATED=1
```

Scan only curated tables:

```bash
make lake-reality CURATED_ONLY=1
```

Use Socrata live counts as an extra check:

```bash
make lake-reality WITH_LIVE=1
```

Check only changed catalog YAMLs:

```bash
make lake-reality CHANGED_YAMLS_ONLY=1
```

## Daily Monitor

`.github/workflows/lake-reality.yml` runs at `09:00 UTC`, which is `04:00`
in America/Bogota. It expects a self-hosted GitHub Actions runner labeled
`coacc-lake` with the authoritative lake mounted locally.

Repository settings:

- `COACC_LAKE_ROOT` variable: optional lake path; defaults to
  `/var/lib/coacc/lake`
- `LAKE_REALITY_WEBHOOK_URL` secret: optional Slack or Discord webhook for
  failure notifications

The workflow runs `make lake-reality WITH_LIVE=1` by default and uploads
the JSON/Markdown artifacts from `lake/meta/reality/`.

Pull requests that touch `etl/datasets/*.yml` run the same workflow with
`CHANGED_YAMLS_ONLY=1` and without live Socrata counts. The self-hosted runner
must therefore have the lake mounted before PR checks can pass. Changed datasets
without local parquet are skipped because there is no lake state to compare yet.

## Exit Codes

- `0`: probe completed with no failure-level findings
- `1`: probe completed and found a regression or coverage failure
- `2`: operator error, such as missing lake root, unknown dataset, missing
  baseline file, or explicit dataset with no local parquet files

## Metrics

Each raw dataset snapshot includes:

- `row_count`
- `parquet_file_count`
- `partition_count`
- `null_rate`
- `watermark_min` / `watermark_max`
- `dup_ratio`
- `sentinel_fraction`
- `partition_skew`
- `schema_hash`
- `freshness_seconds`
- optional `live_count`

Each curated-table snapshot includes:

- `row_count`
- `parquet_file_count`
- `schema_hash`
- `freshness_seconds`
- manifest file/entry counts
- latest manifest path, timestamp, and row count
- whether manifest rows match parquet rows
- evidence-ref row count, evidence-ref count, and evidence-ref coverage when
  an `evidence_refs` column exists

## Thresholds

Default thresholds live in `config/reality_thresholds.yml`. Per-dataset
overrides can be added under `datasets.<dataset_id>`. Curated-table overrides
use the synthetic ID `curated:<table_name>`.

Common overrides:

```yaml
datasets:
  rpmr-utcd:
    sentinel_fraction_rise: 0.02
    row_count_drop_ratio: 0.005
  "curated:signal_feature_procurement_supplier_concentration_across_entities":
    max_freshness_seconds: 86400
```

## Operational Notes

The command scans local parquet in batches controlled by DuckDB and does not
materialize complete datasets into Python memory. It does still need local
parquet files. For a remote lake, restore or mount the needed parquet partitions
first, then run the probe over that local path with `COACC_LAKE_ROOT`.

For signal feature tables, `lake-reality` fails if `evidence_refs` is missing
or any row lacks at least one evidence ref. That keeps public API samples
auditable back to the source rows instead of becoming anonymous counts.
