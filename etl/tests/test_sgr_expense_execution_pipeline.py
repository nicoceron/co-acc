from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.sgr_expense_execution import SgrExpenseExecutionPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SgrExpenseExecutionPipeline:
    driver = MagicMock()
    return SgrExpenseExecutionPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_finance_rows_and_third_party_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.finances) == 2
    assert len(pipeline.rels) == 2


def test_load_creates_forneceu_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Finance {finance_id: row.finance_id})" in str(call) for call in run_calls)
    assert any("MERGE (a)-[r:SUMINISTRO]->(b)" in str(call) for call in run_calls)
