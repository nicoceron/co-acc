from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_contract_execution import SecopContractExecutionPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopContractExecutionPipeline:
    driver = MagicMock()
    return SecopContractExecutionPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_execution_metrics() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["execution_item_count"] == 2)
    assert first["execution_item_count"] == 2
    assert first["execution_actual_progress_max"] == 30.0


def test_load_updates_contract_nodes_for_execution() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
