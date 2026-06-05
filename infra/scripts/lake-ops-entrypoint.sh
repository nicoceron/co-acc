#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-${COACC_LAKE_OPS_MODE:-refresh}}"
shift || true

WORKSPACE="${COACC_WORKSPACE:-/workspace}"
ETL_DIR="${COACC_ETL_DIR:-${WORKSPACE}/etl}"

export COACC_LAKE_ROOT="${COACC_LAKE_ROOT:-${WORKSPACE}/lake}"
export COACC_CONFIG_DIR="${COACC_CONFIG_DIR:-${WORKSPACE}/config}"
export COACC_DATASET_CATALOG_DIR="${COACC_DATASET_CATALOG_DIR:-${WORKSPACE}/docs/datasets}"
export COACC_DATASET_CONTRACT_DIR="${COACC_DATASET_CONTRACT_DIR:-${WORKSPACE}/etl/datasets}"
export COACC_RUNBOOK_DIR="${COACC_RUNBOOK_DIR:-${COACC_LAKE_ROOT}/meta/operations}"

RUN_ID="${COACC_LAKE_OPS_RUN_ID:-${COACC_LAKE_OPS_RUN_ID_PREFIX:-prod}-$(date -u +%Y%m%dT%H%M%SZ)}"
CONTINUE_ON_ERROR="${COACC_LAKE_OPS_CONTINUE_ON_ERROR:-true}"
SKIP_REALITY="${COACC_LAKE_OPS_SKIP_REALITY:-false}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
completed=false

mkdir -p \
  "${COACC_LAKE_ROOT}/raw" \
  "${COACC_LAKE_ROOT}/curated" \
  "${COACC_LAKE_ROOT}/meta" \
  "${COACC_RUNBOOK_DIR}"

run_etl() {
  (cd "${ETL_DIR}" && uv run coacc-etl "$@")
}

run_repo_python() {
  local script="$1"
  shift
  (cd "${ETL_DIR}" && uv run python "${WORKSPACE}/${script}" "$@")
}

write_status() {
  local status="$1"
  shift
  run_repo_python scripts/write_lake_ops_status.py \
    --lake-root "${COACC_LAKE_ROOT}" \
    --run-id "${RUN_ID}" \
    --mode "${MODE}" \
    --status "${status}" \
    --started-at "${STARTED_AT}" \
    "$@"
}

write_failed_status() {
  local exit_code="$?"
  if [[ "${completed}" != "true" ]]; then
    write_status failed \
      --exit-code "${exit_code}" \
      --error "lake ops exited with status ${exit_code}" || true
  fi
}

trap write_failed_status EXIT
write_status running

pagination_args=()
if [[ -n "${COACC_LAKE_OPS_PAGE_SIZE:-}" ]]; then
  pagination_args+=(--page-size "${COACC_LAKE_OPS_PAGE_SIZE}")
fi
if [[ -n "${COACC_LAKE_OPS_MAX_PAGES:-}" ]]; then
  pagination_args+=(--max-pages "${COACC_LAKE_OPS_MAX_PAGES}")
fi
if [[ -n "${COACC_LAKE_OPS_TIMEOUT_SECONDS:-}" ]]; then
  pagination_args+=(--timeout-seconds "${COACC_LAKE_OPS_TIMEOUT_SECONDS}")
fi

phase7_args=()
if [[ "${CONTINUE_ON_ERROR}" == "true" ]]; then
  phase7_args+=(--continue-on-error)
else
  phase7_args+=(--stop-on-error)
fi
if [[ -n "${COACC_LAKE_OPS_MIN_FREE_GB:-}" ]]; then
  phase7_args+=(--min-free-gb "${COACC_LAKE_OPS_MIN_FREE_GB}")
fi

ingest_all_args=(--incremental)
if [[ "${CONTINUE_ON_ERROR}" == "true" ]]; then
  ingest_all_args+=(--continue-on-error)
else
  ingest_all_args+=(--stop-on-error)
fi

run_checks() {
  run_repo_python scripts/check_curated_contracts.py --lake-root "${COACC_LAKE_ROOT}"
  if [[ "${SKIP_REALITY}" != "true" ]]; then
    local snapshot_date
    snapshot_date="${COACC_LAKE_OPS_REALITY_DATE:-$(date -u +%F)}"
    run_repo_python scripts/lake_reality.py --curated-only --date "${snapshot_date}"
  fi
}

case "${MODE}" in
  check)
    run_checks
    ;;
  curate)
    run_etl curate --all
    run_checks
    ;;
  materialize)
    run_etl signals materialize --all --run-id "${RUN_ID}"
    run_checks
    ;;
  refresh)
    run_etl ingest-all "${ingest_all_args[@]}" "${pagination_args[@]}"
    run_etl curate --all
    run_etl signals materialize --all --run-id "${RUN_ID}"
    run_checks
    ;;
  smoke)
    run_etl ingest-phase7 --mode smoke "${phase7_args[@]}" "${pagination_args[@]}"
    run_etl curate --all
    run_etl signals materialize --all --run-id "${RUN_ID}"
    run_checks
    ;;
  full)
    if [[ "${COACC_LAKE_OPS_ALLOW_FULL:-false}" != "true" ]]; then
      echo "Refusing full lake load without COACC_LAKE_OPS_ALLOW_FULL=true" >&2
      exit 64
    fi
    run_etl ingest-phase7 --mode full "${phase7_args[@]}" "${pagination_args[@]}"
    run_etl curate --all
    run_etl signals materialize --all --run-id "${RUN_ID}"
    run_checks
    ;;
  *)
    echo "Usage: $0 {check|curate|materialize|refresh|smoke|full}" >&2
    exit 64
    ;;
esac

success_args=(--exit-code 0)
case "${MODE}" in
  materialize|refresh|smoke|full)
    success_args+=(--signal-run-id "${RUN_ID}")
    ;;
esac
write_status succeeded "${success_args[@]}"
completed=true
