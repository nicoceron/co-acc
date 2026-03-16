from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.cuentas_claras_income_2019 import CuentasClarasIncome2019Pipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CuentasClarasIncome2019Pipeline:
    driver = MagicMock()
    return CuentasClarasIncome2019Pipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_donors_candidates_and_elections() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.companies) == 1
    assert len(pipeline.elections) == 2
    assert len(pipeline.candidate_rels) == 2
    assert len(pipeline.donation_rels) == 2


def test_load_creates_election_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Election {election_id: row.election_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (p)-[r:CANDIDATO_EM]->(e)" in str(call) for call in run_calls)
    assert any(
        "OPTIONAL MATCH (p:Person {document_id: row.source_key})" in str(call)
        and "OPTIONAL MATCH (c:Company {document_id: row.source_key})" in str(call)
        and "MERGE (donor)-[r:DONO_A {receipt: row.receipt}]->(e)" in str(call)
        for call in run_calls
    )
