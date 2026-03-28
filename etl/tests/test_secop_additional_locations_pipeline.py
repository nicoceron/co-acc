from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_additional_locations import SecopAdditionalLocationsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopAdditionalLocationsPipeline:
    driver = MagicMock()
    return SecopAdditionalLocationsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_additional_locations() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["additional_location_count"] == 2)
    assert "SOACHA, CUNDINAMARCA" in {value.upper() for value in first["additional_locations"]}
    assert "CARRERA 7 # 10-20" in first["additional_locations"]


def test_load_updates_contract_nodes_for_additional_locations() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "additional_location_count" in str(call)
        and "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
