#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ARCHIVE="${1:-}"
VERIFY_CONTRACTS="${COACC_BACKUP_VERIFY_CONTRACTS:-true}"

if [[ -z "${ARCHIVE}" || ! -f "${ARCHIVE}" ]]; then
  echo "Usage: $0 /path/to/coacc_lake_<timestamp>.tar.gz" >&2
  exit 64
fi

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
tar -xzf "${ARCHIVE}" -C "${tmp}"

require_path() {
  local path="$1"
  local kind="$2"
  if [[ "${kind}" == "dir" && ! -d "${path}" ]]; then
    echo "Backup verification failed: missing directory ${path}" >&2
    exit 65
  fi
  if [[ "${kind}" == "file" && ! -f "${path}" ]]; then
    echo "Backup verification failed: missing file ${path}" >&2
    exit 65
  fi
}

require_path "${tmp}/coacc-backup-manifest.json" file
require_path "${tmp}/lake" dir
require_path "${tmp}/lake/raw" dir
require_path "${tmp}/lake/curated" dir
require_path "${tmp}/lake/meta" dir
require_path "${tmp}/config" dir
require_path "${tmp}/docs/datasets" dir
require_path "${tmp}/etl/datasets" dir
require_path "${tmp}/config/signal_registry.yml" file
require_path "${tmp}/docs/datasets/catalog.signed.csv" file

contract_count="$(find "${tmp}/etl/datasets" -maxdepth 1 -name "*.yml" | wc -l | tr -d ' ')"
if [[ "${contract_count}" -lt 1 ]]; then
  echo "Backup verification failed: no dataset contracts found" >&2
  exit 65
fi

if [[ "${VERIFY_CONTRACTS}" == "true" ]]; then
  COACC_WORKSPACE="${REPO_ROOT}" \
  COACC_ETL_DIR="${REPO_ROOT}/etl" \
  COACC_LAKE_ROOT="${tmp}/lake" \
  COACC_CONFIG_DIR="${tmp}/config" \
  COACC_DATASET_CATALOG_DIR="${tmp}/docs/datasets" \
  COACC_DATASET_CONTRACT_DIR="${tmp}/etl/datasets" \
  COACC_LAKE_OPS_SKIP_REALITY=true \
    bash "${REPO_ROOT}/infra/scripts/lake-ops-entrypoint.sh" check
fi

echo "Lake backup verification passed: ${ARCHIVE}"
