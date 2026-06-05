# Backup And Restore Runbook

Production data durability has two parts:

- The lake/config backup, which is the source of truth for lake-first runtime.
- Optional Neo4j dumps or volume snapshots, only needed when graph-backed
  reviewer workflows are enabled.

## Lake Backup

Run from the repository root on the production host:

```bash
infra/scripts/backup-lake.sh
```

The archive includes:

- `lake/`
- `config/`
- `docs/datasets/`
- `etl/datasets/`
- `coacc-backup-manifest.json`

The script reads these host path variables:

- `COACC_HOST_LAKE_ROOT`
- `COACC_HOST_CONFIG_DIR`
- `COACC_HOST_DATASET_CATALOG_DIR`
- `COACC_HOST_DATASET_CONTRACT_DIR`

Backup output defaults to `COACC_BACKUP_DIR` or `./backups`.
Retention defaults to `COACC_BACKUP_RETENTION_DAYS=30`.

## Verify A Backup

```bash
infra/scripts/verify-lake-backup.sh /path/to/coacc_lake_YYYYMMDDTHHMMSSZ.tar.gz
```

Verification extracts the archive into a temporary directory and checks the
archive layout, required config/catalog/contract files, and dataset contracts.
By default it also runs the lake `check` flow against the restored temporary
lake:

```bash
COACC_BACKUP_VERIFY_CONTRACTS=true infra/scripts/verify-lake-backup.sh /path/to/archive.tar.gz
```

Set `COACC_BACKUP_VERIFY_CONTRACTS=false` only for tiny fixture archives or
when validating archive shape before curated parquet exists.

## Restore Drill

Restore into a clean target path:

```bash
infra/scripts/restore-lake-backup.sh /path/to/coacc_lake_YYYYMMDDTHHMMSSZ.tar.gz /tmp/coacc-restore-drill
```

Point a production-like smoke at the restored path:

```bash
COACC_HOST_LAKE_ROOT=/tmp/coacc-restore-drill/lake \
COACC_HOST_CONFIG_DIR=/tmp/coacc-restore-drill/config \
COACC_HOST_DATASET_CATALOG_DIR=/tmp/coacc-restore-drill/docs/datasets \
COACC_HOST_DATASET_CONTRACT_DIR=/tmp/coacc-restore-drill/etl/datasets \
DOMAIN=coacc.example.test \
JWT_SECRET_KEY=replace-with-prod-secret \
NEO4J_PASSWORD=replace-with-prod-secret \
docker compose -f infra/docker/docker-compose.prod.yml config >/dev/null
```

For live recovery, stop the app, restore into the durable volume mount, run
`infra/scripts/run-lake-ops.sh check`, then start the app and verify `/ready`.
`restore-lake-backup.sh` refuses non-empty targets unless
`COACC_RESTORE_OVERWRITE=true`.

## Drill Evidence

Local drill on 2026-06-05 against the then-current 14-signal lake:

- Backup command:
  `COACC_BACKUP_DIR=/tmp/coacc-lake-drill COACC_HOST_LAKE_ROOT=/Users/ceron/Developer/co-acc/lake infra/scripts/backup-lake.sh`
- Archive:
  `/tmp/coacc-lake-drill/coacc_lake_20260605T010250Z.tar.gz` (9.9 GB)
- Verification:
  `COACC_BACKUP_VERIFY_CONTRACTS=true infra/scripts/verify-lake-backup.sh /tmp/coacc-lake-drill/coacc_lake_20260605T010250Z.tar.gz`
- Restore:
  `COACC_BACKUP_VERIFY_CONTRACTS=true infra/scripts/restore-lake-backup.sh /tmp/coacc-lake-drill/coacc_lake_20260605T010250Z.tar.gz /tmp/coacc-restore-drill`

Both verification and restore reported `PASS 14 curated contract(s)`. The
restored lake had 14 signal feature tables and preserved the `u8cx-r425`
watermark at `2026-06-03T00:00:00Z`. This proves the scripts locally. The
later 21-signal lake should be included in the next scheduled/off-host drill; a
production-ready drill still needs the production volume and off-host archive
storage.

## Schedule

Install the lake backup cron:

```bash
COACC_LAKE_BACKUP_CRON_SCHEDULE="45 3 * * *" infra/scripts/lake-backup-cron.sh
```

The default schedule runs after the default `lake-ops refresh` window.

## Neo4j Backup

If Neo4j is enabled for reviewer workflows, keep the existing graph dump or
volume snapshot jobs:

```bash
infra/scripts/backup-neo4j.sh
infra/scripts/backup-cron.sh
infra/scripts/snapshot-volume.sh
```

Neo4j backup does not replace lake backup. The API can run lake-first with
Neo4j unavailable, but the lake cannot be reconstructed from Neo4j alone.
