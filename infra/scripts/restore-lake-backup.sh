#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARCHIVE="${1:-}"
RESTORE_ROOT="${2:-${COACC_RESTORE_ROOT:-}}"
VERIFY_CONTRACTS="${COACC_BACKUP_VERIFY_CONTRACTS:-true}"

if [[ -z "${ARCHIVE}" || ! -f "${ARCHIVE}" || -z "${RESTORE_ROOT}" ]]; then
  echo "Usage: $0 /path/to/coacc_lake_<timestamp>.tar.gz /restore/root" >&2
  exit 64
fi

if [[ -e "${RESTORE_ROOT}" && -n "$(find "${RESTORE_ROOT}" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
  if [[ "${COACC_RESTORE_OVERWRITE:-false}" != "true" ]]; then
    echo "Restore target is not empty: ${RESTORE_ROOT}" >&2
    echo "Set COACC_RESTORE_OVERWRITE=true to replace lake/config/docs/etl subdirectories." >&2
    exit 65
  fi
  rm -rf \
    "${RESTORE_ROOT}/lake" \
    "${RESTORE_ROOT}/config" \
    "${RESTORE_ROOT}/docs" \
    "${RESTORE_ROOT}/etl" \
    "${RESTORE_ROOT}/coacc-backup-manifest.json"
fi

mkdir -p "${RESTORE_ROOT}"
tar -xzf "${ARCHIVE}" -C "${RESTORE_ROOT}"

require_path() {
  local path="$1"
  local kind="$2"
  if [[ "${kind}" == "dir" && ! -d "${path}" ]]; then
    echo "Restore verification failed: missing directory ${path}" >&2
    exit 66
  fi
  if [[ "${kind}" == "file" && ! -f "${path}" ]]; then
    echo "Restore verification failed: missing file ${path}" >&2
    exit 66
  fi
}

require_path "${RESTORE_ROOT}/coacc-backup-manifest.json" file
require_path "${RESTORE_ROOT}/lake/raw" dir
require_path "${RESTORE_ROOT}/lake/curated" dir
require_path "${RESTORE_ROOT}/lake/meta" dir
require_path "${RESTORE_ROOT}/config/signal_registry.yml" file
require_path "${RESTORE_ROOT}/docs/datasets/catalog.signed.csv" file
require_path "${RESTORE_ROOT}/etl/datasets" dir

if [[ "${VERIFY_CONTRACTS}" == "true" ]]; then
  COACC_WORKSPACE="${REPO_ROOT}" \
  COACC_ETL_DIR="${REPO_ROOT}/etl" \
  COACC_LAKE_ROOT="${RESTORE_ROOT}/lake" \
  COACC_CONFIG_DIR="${RESTORE_ROOT}/config" \
  COACC_DATASET_CATALOG_DIR="${RESTORE_ROOT}/docs/datasets" \
  COACC_DATASET_CONTRACT_DIR="${RESTORE_ROOT}/etl/datasets" \
  COACC_LAKE_OPS_SKIP_REALITY=true \
    bash "${REPO_ROOT}/infra/scripts/lake-ops-entrypoint.sh" check
fi

echo "Lake backup restored to: ${RESTORE_ROOT}"
