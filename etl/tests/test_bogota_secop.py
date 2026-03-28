from __future__ import annotations

from pathlib import Path

import pandas as pd

from coacc_etl.bogota_secop import build_in_clauses, extract_bogota_secop_scope

FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_bogota_secop_scope_uses_contract_and_process_identifiers() -> None:
    processes = pd.read_csv(
        FIXTURES / "secop_ii_processes" / "secop_ii_processes.csv",
        dtype=str,
    ).fillna("")
    contracts = pd.read_csv(
        FIXTURES / "secop_ii_contracts" / "secop_ii_contracts.csv",
        dtype=str,
    ).fillna("")

    scope = extract_bogota_secop_scope(processes=processes, contracts=contracts)

    assert scope.process_request_ids == ("CO1.REQ.1001", "CO1.REQ.1002")
    assert scope.process_portfolio_ids == ("CO1.BDOS.2001", "CO1.BDOS.2002")
    assert scope.offer_process_portfolio_ids == ("CO1.BDOS.2001", "CO1.BDOS.2002")
    assert scope.contract_ids == ("CO1.PCCNTR.3001", "CO1.PCCNTR.3002")
    assert scope.supplier_codes == ("700001", "700002")
    assert scope.supplier_nits == ("900123456", "830456789")


def test_extract_bogota_secop_scope_limits_offer_scope_to_processes_with_responses() -> None:
    processes = pd.DataFrame(
        [
            {
                "id_del_proceso": "CO1.REQ.1001",
                "id_del_portafolio": "CO1.BDOS.2001",
                "respuestas_al_procedimiento": "3",
            },
            {
                "id_del_proceso": "CO1.REQ.1002",
                "id_del_portafolio": "CO1.BDOS.2002",
                "respuestas_al_procedimiento": "0",
            },
        ]
    )
    contracts = pd.DataFrame(
        [
            {
                "proceso_de_compra": "CO1.BDOS.2001",
                "id_contrato": "CO1.PCCNTR.3001",
                "codigo_proveedor": "700001",
                "documento_proveedor": "900123456",
            },
            {
                "proceso_de_compra": "CO1.BDOS.2002",
                "id_contrato": "CO1.PCCNTR.3002",
                "codigo_proveedor": "700002",
                "documento_proveedor": "830456789",
            },
        ]
    )

    scope = extract_bogota_secop_scope(processes=processes, contracts=contracts)

    assert scope.process_portfolio_ids == ("CO1.BDOS.2001", "CO1.BDOS.2002")
    assert scope.offer_process_portfolio_ids == ("CO1.BDOS.2001",)


def test_build_in_clauses_chunks_and_escapes_values() -> None:
    clauses = build_in_clauses(
        column="codigo",
        values=["A", "B", "O'Brien", "B"],
        max_clause_chars=24,
    )

    assert clauses == [
        "codigo in ('A', 'B')",
        "codigo in ('O''Brien')",
    ]


def test_build_in_clauses_rejects_unusable_clause_budget() -> None:
    try:
        build_in_clauses(column="codigo", values=["A"], max_clause_chars=5)
    except ValueError as exc:
        assert "too small" in str(exc)
    else:
        raise AssertionError("Expected ValueError for an unusable clause budget")
