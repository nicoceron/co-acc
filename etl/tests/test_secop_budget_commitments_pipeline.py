from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_budget_commitments import SecopBudgetCommitmentsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopBudgetCommitmentsPipeline:
    driver = MagicMock()
    return SecopBudgetCommitmentsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_budget_commitments() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["commitment_item_count"] == 2)
    assert first["commitment_total_value"] == 1200.0
    assert first["commitment_balance_total"] == 1900.0
    assert first["commitment_release_total"] == 75.0
    assert first["commitment_code"] == "ABD"


def test_load_updates_contract_nodes_for_budget_commitments() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
