from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.igac_property_transactions import IgacPropertyTransactionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> IgacPropertyTransactionsPipeline:
    driver = MagicMock()
    return IgacPropertyTransactionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_market_activity() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.finances) == 2
    assert len(pipeline.rels) == 2
    counts = sorted(finance["transaction_count"] for finance in pipeline.finances)
    totals = sorted(finance["value"] for finance in pipeline.finances)
    assert all(finance["type"] == "IGAC_PROPERTY_ACTIVITY" for finance in pipeline.finances)
    assert counts == [1, 2]
    assert totals == [350000000.0, 500000000.0]


def test_load_creates_administra_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Finance {finance_id: row.finance_id})" in str(call) for call in run_calls)
    assert any("MERGE (a)-[r:ADMINISTRA]->(b)" in str(call) for call in run_calls)
