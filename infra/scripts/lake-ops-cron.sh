#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCHEDULE="${COACC_LAKE_OPS_CRON_SCHEDULE:-15 2 * * *}"
LOG_PATH="${COACC_LAKE_OPS_CRON_LOG:-/var/log/coacc-lake-ops.log}"
RUNNER="${REPO_ROOT}/infra/scripts/run-lake-ops.sh"
MODE="${COACC_LAKE_OPS_MODE:-refresh}"
MARKER="# coacc-lake-ops"

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT

crontab -l 2>/dev/null | grep -vF "${MARKER}" > "${tmp}" || true
printf '%s cd %q && %q %q >> %q 2>&1 %s\n' \
  "${SCHEDULE}" \
  "${REPO_ROOT}" \
  "${RUNNER}" \
  "${MODE}" \
  "${LOG_PATH}" \
  "${MARKER}" >> "${tmp}"
crontab "${tmp}"

echo "Installed ${MODE} lake ops cron: ${SCHEDULE}"
