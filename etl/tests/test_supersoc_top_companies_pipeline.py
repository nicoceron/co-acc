from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.supersoc_top_companies import SupersocTopCompaniesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SupersocTopCompaniesPipeline:
    driver = MagicMock()
    return SupersocTopCompaniesPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_company_finance_records() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.finances) == 2
    assert len(pipeline.rels) == 2
    first = pipeline.finances[0]
    assert first["type"] == "SUPERSOC_TOP_COMPANY"
    assert first["financial_year"] == "2024"
    assert first["value"] == 1500000000.0


def test_load_creates_declared_finance_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Finance {finance_id: row.finance_id})" in str(call) for call in run_calls)
    assert any("MERGE (a)-[r:DECLAROU_FINANCA]->(b)" in str(call) for call in run_calls)
