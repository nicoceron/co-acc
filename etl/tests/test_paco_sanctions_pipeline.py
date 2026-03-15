from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.paco_sanctions import PacoSanctionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> PacoSanctionsPipeline:
    driver = MagicMock()
    return PacoSanctionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_people_companies_and_sanctions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 3
    assert len(pipeline.companies) == 3
    assert len(pipeline.contracts) == 1
    assert len(pipeline.sanctions) == 6
    assert len(pipeline.person_rels) == 3
    assert len(pipeline.company_rels) == 3
    assert len(pipeline.contract_rels) == 1
    assert len(pipeline.company_contract_rels) == 1


def test_load_creates_person_and_company_sancionada_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Sanction {sanction_id: row.sanction_id})" in str(call)
        for call in run_calls
    )
    assert any(
        "MERGE (n:Contract {contract_id: row.contract_id})" in str(call)
        for call in run_calls
    )
    assert any("MATCH (a:Company {document_id: row.source_key})" in str(call) for call in run_calls)
    assert any("MATCH (a:Person {document_id: row.source_key})" in str(call) for call in run_calls)
    assert any(
        "MATCH (b:Contract {contract_id: row.target_key})" in str(call)
        for call in run_calls
    )
