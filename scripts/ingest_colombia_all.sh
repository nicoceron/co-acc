#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

: "${NEO4J_PASSWORD:?NEO4J_PASSWORD must be set in the environment or .env}"

START_FROM="${START_FROM:-secop_integrado}"
KEEP_RAW="${KEEP_RAW:-0}"
STARTED=0

delete_raw_files() {
  if [[ "$KEEP_RAW" == "1" || "$#" -eq 0 ]]; then
    return
  fi

  python3 - "$@" <<'PY'
from pathlib import Path
import sys

for raw_path in sys.argv[1:]:
    Path(raw_path).unlink(missing_ok=True)
PY
}

run_source() {
  local source_name="$1"
  local download_target="$2"
  local etl_target="$3"
  shift 3

  if [[ "$STARTED" -eq 0 ]]; then
    if [[ "$source_name" != "$START_FROM" ]]; then
      return
    fi
    STARTED=1
  fi

  printf '\n==> %s: download\n' "$source_name"
  make "$download_target"

  printf '==> %s: ingest\n' "$source_name"
  make "$etl_target"

  delete_raw_files "$@"
}

run_source secop_integrado download-secop-integrado etl-secop-integrado \
  data/secop_integrado/secop_integrado.csv
run_source secop_sanctions download-secop-sanciones etl-secop-sanciones \
  data/secop_sanctions/secop_i_sanctions.csv \
  data/secop_sanctions/secop_ii_sanctions.csv
run_source secop_suppliers download-secop-proveedores etl-secop-proveedores \
  data/secop_suppliers/secop_suppliers.csv
run_source secop_ii_contracts download-secop-contratos etl-secop-contratos \
  data/secop_ii_contracts/secop_ii_contracts.csv
run_source secop_ii_processes download-secop-procesos etl-secop-procesos \
  data/secop_ii_processes/secop_ii_processes.csv
run_source secop_contract_execution download-secop-ejecucion etl-secop-ejecucion \
  data/secop_contract_execution/secop_contract_execution.csv
run_source secop_contract_additions download-secop-adiciones etl-secop-adiciones \
  data/secop_contract_additions/secop_contract_additions.csv
run_source secop_contract_modifications download-secop-modificaciones etl-secop-modificaciones \
  data/secop_contract_modifications/secop_contract_modifications.csv
run_source sigep_public_servants download-sigep-servidores etl-sigep-servidores \
  data/sigep_public_servants/sigep_public_servants.csv
run_source sigep_sensitive_positions download-sigep-cargos-sensibles etl-sigep-cargos-sensibles \
  data/sigep_sensitive_positions/sigep_sensitive_positions.csv
run_source asset_disclosures download-ley2013-activos etl-ley2013-activos \
  data/asset_disclosures/asset_disclosures.csv
run_source conflict_disclosures download-ley2013-conflictos etl-ley2013-conflictos \
  data/conflict_disclosures/conflict_disclosures.csv
run_source health_providers download-reps-salud etl-reps-salud \
  data/health_providers/health_providers.csv
run_source higher_ed_enrollment download-men-matricula etl-men-matricula \
  data/higher_ed_enrollment/higher_ed_enrollment.csv
run_source sgr_projects download-sgr-proyectos etl-sgr-proyectos \
  data/sgr_projects/sgr_projects.csv
run_source sgr_expense_execution download-sgr-gastos etl-sgr-gastos \
  data/sgr_expense_execution/sgr_expense_execution.csv
run_source cuentas_claras_income_2019 download-cuentas-claras-2019 etl-cuentas-claras-2019 \
  data/cuentas_claras_income_2019/cuentas_claras_income_2019.csv
