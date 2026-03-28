from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from coacc_etl.pipelines.colombia_shared import clean_text, read_csv_normalized

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class BogotaSecopScope:
    contract_ids: tuple[str, ...]
    process_request_ids: tuple[str, ...]
    process_portfolio_ids: tuple[str, ...]
    offer_process_portfolio_ids: tuple[str, ...]
    supplier_codes: tuple[str, ...]
    supplier_nits: tuple[str, ...]


def _unique_text(values: Iterable[object]) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = clean_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return tuple(ordered)


def _positive_numeric_text(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values.replace("", "0"), errors="coerce").fillna(0) > 0


def extract_bogota_secop_scope(
    *,
    processes: pd.DataFrame | None,
    contracts: pd.DataFrame,
) -> BogotaSecopScope:
    normalized_processes = processes if processes is not None else pd.DataFrame()
    process_request_ids = _unique_text(
        normalized_processes.get("id_del_proceso", pd.Series(dtype=str)).tolist()
    )
    process_portfolio_ids = _unique_text(
        list(normalized_processes.get("id_del_portafolio", pd.Series(dtype=str)).tolist())
        + list(contracts.get("proceso_de_compra", pd.Series(dtype=str)).tolist())
    )
    contract_ids = _unique_text(contracts.get("id_contrato", pd.Series(dtype=str)).tolist())
    if "respuestas_al_procedimiento" in normalized_processes.columns:
        eligible_processes = normalized_processes.loc[
            _positive_numeric_text(normalized_processes["respuestas_al_procedimiento"]),
            "id_del_portafolio",
        ].tolist()
        offer_process_portfolio_ids = _unique_text(eligible_processes) or process_portfolio_ids
    else:
        offer_process_portfolio_ids = process_portfolio_ids
    supplier_codes = _unique_text(
        list(normalized_processes.get("codigoproveedor", pd.Series(dtype=str)).tolist())
        + list(contracts.get("codigo_proveedor", pd.Series(dtype=str)).tolist())
    )
    supplier_nits = _unique_text(
        list(
            normalized_processes.get(
                "nit_del_proveedor_adjudicado",
                pd.Series(dtype=str),
            ).tolist()
        )
        + list(contracts.get("documento_proveedor", pd.Series(dtype=str)).tolist())
    )

    return BogotaSecopScope(
        contract_ids=contract_ids,
        process_request_ids=process_request_ids,
        process_portfolio_ids=process_portfolio_ids,
        offer_process_portfolio_ids=offer_process_portfolio_ids,
        supplier_codes=supplier_codes,
        supplier_nits=supplier_nits,
    )


def load_bogota_secop_scope(data_dir: str | Path) -> BogotaSecopScope:
    base_dir = Path(data_dir)
    processes = read_csv_normalized(
        str(base_dir / "secop_ii_processes" / "secop_ii_processes.csv"),
        dtype=str,
        keep_default_na=False,
    )
    contracts = read_csv_normalized(
        str(base_dir / "secop_ii_contracts" / "secop_ii_contracts.csv"),
        dtype=str,
        keep_default_na=False,
    )
    return extract_bogota_secop_scope(processes=processes, contracts=contracts)


def load_bogota_contract_scope(data_dir: str | Path) -> BogotaSecopScope:
    base_dir = Path(data_dir)
    contracts = read_csv_normalized(
        str(base_dir / "secop_ii_contracts" / "secop_ii_contracts.csv"),
        dtype=str,
        keep_default_na=False,
    )
    return extract_bogota_secop_scope(processes=None, contracts=contracts)


def quote_soql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_in_clauses(
    *,
    column: str,
    values: Iterable[object],
    max_clause_chars: int = 6_500,
) -> list[str]:
    normalized_values = _unique_text(values)
    if not normalized_values:
        return []

    prefix = f"{column} in ("
    minimum_clause_chars = len(prefix) + len("''")
    if max_clause_chars <= minimum_clause_chars:
        raise ValueError("max_clause_chars is too small to build a valid IN clause")

    clauses: list[str] = []
    current_values: list[str] = []
    current_length = len(prefix) + 1

    for raw_value in normalized_values:
        quoted = quote_soql_literal(raw_value)
        separator_length = 2 if current_values else 0
        projected_length = current_length + separator_length + len(quoted) + 1
        if current_values and projected_length > max_clause_chars:
            clauses.append(f"{prefix}{', '.join(current_values)})")
            current_values = [quoted]
            current_length = len(prefix) + len(quoted) + 1
            continue
        current_values.append(quoted)
        current_length = projected_length

    if current_values:
        clauses.append(f"{prefix}{', '.join(current_values)})")

    return clauses
