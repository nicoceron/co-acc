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

Scan every catalog-backed source present in the local lake:

```bash
make lake-reality
```

Scan one or more datasets:

```bash
make lake-reality DATASET=8qxx-ubmq
make lake-reality DATASETS=8qxx-ubmq,rpmr-utcd
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

## Exit Codes

- `0`: probe completed with no failure-level findings
- `1`: probe completed and found a regression or coverage failure
- `2`: operator error, such as missing lake root, unknown dataset, missing
  baseline file, or explicit dataset with no local parquet files

## Metrics

Each dataset snapshot includes:

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

## Thresholds

Default thresholds live in `config/reality_thresholds.yml`. Per-dataset
overrides can be added under `datasets.<dataset_id>`.

Common overrides:

```yaml
datasets:
  rpmr-utcd:
    sentinel_fraction_rise: 0.02
    row_count_drop_ratio: 0.005
```

## Operational Notes

The command scans local parquet in batches controlled by DuckDB and does not
materialize complete datasets into Python memory. It does still need local
parquet files. For a remote lake, restore or mount the needed parquet partitions
first, then run the probe over that local path with `COACC_LAKE_ROOT`.
