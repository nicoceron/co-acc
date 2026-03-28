from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_contract_suspensions import SecopContractSuspensionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopContractSuspensionsPipeline:
    driver = MagicMock()
    return SecopContractSuspensionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_contract_suspensions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["suspension_event_count"] == 2)
    assert first["latest_suspension_type"] == "Suspension"
    assert first["latest_suspension_date"] == "2025-03-05"


def test_load_updates_contract_nodes_for_suspensions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
