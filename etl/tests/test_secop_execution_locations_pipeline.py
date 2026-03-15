from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_execution_locations import SecopExecutionLocationsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopExecutionLocationsPipeline:
    driver = MagicMock()
    return SecopExecutionLocationsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_execution_locations() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["execution_location_count"] == 2)
    assert set(first["execution_locations"]) == {"Bogota", "Soacha"}


def test_load_updates_contract_nodes_for_execution_locations() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
