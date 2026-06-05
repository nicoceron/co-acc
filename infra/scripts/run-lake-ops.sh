#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-${COACC_LAKE_OPS_MODE:-refresh}}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.env"
  set +a
fi

COMPOSE_FILE="${COACC_PROD_COMPOSE_FILE:-${REPO_ROOT}/infra/docker/docker-compose.prod.yml}"

export DOMAIN="${DOMAIN:-coacc.local}"
export JWT_SECRET_KEY="${JWT_SECRET_KEY:-ops-only-not-used-change-me}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-ops-only-not-used-change-me}"
export COACC_HOST_LAKE_ROOT="${COACC_HOST_LAKE_ROOT:-${REPO_ROOT}/lake}"
export COACC_HOST_CONFIG_DIR="${COACC_HOST_CONFIG_DIR:-${REPO_ROOT}/config}"
export COACC_HOST_DATASET_CATALOG_DIR="${COACC_HOST_DATASET_CATALOG_DIR:-${REPO_ROOT}/docs/datasets}"
export COACC_HOST_DATASET_CONTRACT_DIR="${COACC_HOST_DATASET_CONTRACT_DIR:-${REPO_ROOT}/etl/datasets}"

send_failure_alert() {
  local exit_code="$1"
  local webhook="${COACC_LAKE_OPS_ALERT_WEBHOOK_URL:-}"
  if [[ -z "${webhook}" ]]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
    echo "lake ops failed, but curl/python3 is unavailable for webhook alert" >&2
    return 0
  fi

  local payload
  payload="$(
    COACC_ALERT_MODE="${MODE}" \
    COACC_ALERT_EXIT_CODE="${exit_code}" \
    COACC_ALERT_NAME="${COACC_LAKE_OPS_ALERT_NAME:-coacc-lake-ops}" \
    COACC_ALERT_DOMAIN="${DOMAIN}" \
    python3 -c 'import json, os; print(json.dumps({
      "text": "%s failed in %s mode with exit code %s" % (
        os.environ["COACC_ALERT_NAME"],
        os.environ["COACC_ALERT_MODE"],
        os.environ["COACC_ALERT_EXIT_CODE"],
      ),
      "service": os.environ["COACC_ALERT_NAME"],
      "mode": os.environ["COACC_ALERT_MODE"],
      "exit_code": int(os.environ["COACC_ALERT_EXIT_CODE"]),
      "domain": os.environ["COACC_ALERT_DOMAIN"],
    }))'
  )"
  curl -fsS -m 10 \
    -H "Content-Type: application/json" \
    -d "${payload}" \
    "${webhook}" >/dev/null || true
}

set +e
docker compose -f "${COMPOSE_FILE}" --profile ops run --rm lake-ops "${MODE}" "$@"
status="$?"
set -e
if [[ "${status}" -ne 0 ]]; then
  send_failure_alert "${status}"
  exit "${status}"
fi
