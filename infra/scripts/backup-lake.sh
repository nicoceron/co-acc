#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

TIMESTAMP="${COACC_BACKUP_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
BACKUP_DIR="${COACC_BACKUP_DIR:-${BACKUP_DIR:-${REPO_ROOT}/backups}}"
RETENTION_DAYS="${COACC_BACKUP_RETENTION_DAYS:-${RETENTION_DAYS:-30}}"
LAKE_ROOT="${COACC_HOST_LAKE_ROOT:-${REPO_ROOT}/lake}"
CONFIG_DIR="${COACC_HOST_CONFIG_DIR:-${REPO_ROOT}/config}"
CATALOG_DIR="${COACC_HOST_DATASET_CATALOG_DIR:-${REPO_ROOT}/docs/datasets}"
CONTRACT_DIR="${COACC_HOST_DATASET_CONTRACT_DIR:-${REPO_ROOT}/etl/datasets}"
ARCHIVE="${COACC_BACKUP_ARCHIVE:-${BACKUP_DIR}/coacc_lake_${TIMESTAMP}.tar.gz}"

require_dir() {
  local name="$1"
  local path="$2"
  if [[ ! -d "${path}" ]]; then
    echo "Missing ${name}: ${path}" >&2
    exit 66
  fi
}

require_dir "lake root" "${LAKE_ROOT}"
require_dir "config dir" "${CONFIG_DIR}"
require_dir "dataset catalog dir" "${CATALOG_DIR}"
require_dir "dataset contract dir" "${CONTRACT_DIR}"

mkdir -p "${BACKUP_DIR}"

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
stage="${tmp}/stage"
mkdir -p "${stage}/docs" "${stage}/etl"

ln -s "${LAKE_ROOT}" "${stage}/lake"
ln -s "${CONFIG_DIR}" "${stage}/config"
ln -s "${CATALOG_DIR}" "${stage}/docs/datasets"
ln -s "${CONTRACT_DIR}" "${stage}/etl/datasets"

COACC_BACKUP_TIMESTAMP="${TIMESTAMP}" \
COACC_BACKUP_LAKE_ROOT="${LAKE_ROOT}" \
COACC_BACKUP_CONFIG_DIR="${CONFIG_DIR}" \
COACC_BACKUP_CATALOG_DIR="${CATALOG_DIR}" \
COACC_BACKUP_CONTRACT_DIR="${CONTRACT_DIR}" \
python3 - <<'PY' > "${stage}/coacc-backup-manifest.json"
from __future__ import annotations

import json
import os
from datetime import UTC, datetime

payload = {
    "schema_version": 1,
    "created_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
    "timestamp": os.environ["COACC_BACKUP_TIMESTAMP"],
    "layout": {
        "lake": "lake/",
        "config": "config/",
        "dataset_catalog": "docs/datasets/",
        "dataset_contracts": "etl/datasets/",
    },
    "source_paths": {
        "lake": os.environ["COACC_BACKUP_LAKE_ROOT"],
        "config": os.environ["COACC_BACKUP_CONFIG_DIR"],
        "dataset_catalog": os.environ["COACC_BACKUP_CATALOG_DIR"],
        "dataset_contracts": os.environ["COACC_BACKUP_CONTRACT_DIR"],
    },
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY

tar -czhf "${ARCHIVE}" \
  --exclude "./lake/meta/ingest_staging" \
  -C "${stage}" .

find "${BACKUP_DIR}" -name "coacc_lake_*.tar.gz" -mtime "+${RETENTION_DAYS}" -delete

echo "Lake backup complete: ${ARCHIVE}"
