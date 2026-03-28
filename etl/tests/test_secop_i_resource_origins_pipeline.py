from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_i_resource_origins import SecopIResourceOriginsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopIResourceOriginsPipeline:
    driver = MagicMock()
    return SecopIResourceOriginsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_attaches_resource_origins_to_historical_summaries() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["bpin_code"] == "2017003630001")
    assert first["resource_origin_count"] == 2
    assert first["resource_origin_total"] == 156000000
    assert "Recursos Propios" in first["resource_origins"]


def test_load_updates_historical_contract_nodes_for_resource_origins() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "resource_origin_count" in str(call)
        and "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
