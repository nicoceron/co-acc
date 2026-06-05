#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCHEDULE="${COACC_LAKE_BACKUP_CRON_SCHEDULE:-45 3 * * *}"
LOG_PATH="${COACC_LAKE_BACKUP_CRON_LOG:-/var/log/coacc-lake-backup.log}"
RUNNER="${REPO_ROOT}/infra/scripts/backup-lake.sh"
MARKER="# coacc-lake-backup"

tmp="$(mktemp)"
trap 'rm -f "${tmp}"' EXIT

crontab -l 2>/dev/null | grep -vF "${MARKER}" > "${tmp}" || true
printf '%s cd %q && %q >> %q 2>&1 %s\n' \
  "${SCHEDULE}" \
  "${REPO_ROOT}" \
  "${RUNNER}" \
  "${LOG_PATH}" \
  "${MARKER}" >> "${tmp}"
crontab "${tmp}"

echo "Installed lake backup cron: ${SCHEDULE}"
