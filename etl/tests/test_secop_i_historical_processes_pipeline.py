from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_i_historical_processes import (
    SecopIHistoricalProcessesPipeline,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopIHistoricalProcessesPipeline:
    driver = MagicMock()
    return SecopIHistoricalProcessesPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_historical_contracts_and_officers() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 4
    assert len(pipeline.people) == 2
    assert len(pipeline.officer_rels) == 2
    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["adjudication_id"] == "6105142")
    second = next(item for item in pipeline.procurements if item["adjudication_id"] == "6713364")
    assert first["secop_platform"] == "SECOP_I"
    assert first["historical"] is True
    assert first["bpin_code"] == "2017003630001"
    assert first["total_value"] == 106000000
    assert second["total_value"] == 542640
    assert second["initial_contract_value"] == 542640


def test_load_merges_historical_contract_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier)" in str(call)
        for call in session_mock.run.call_args_list
    )
    assert any(
        "MERGE (p)-[r:OFFICER_OF]->(c)" in str(call)
        for call in session_mock.run.call_args_list
    )
