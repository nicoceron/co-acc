from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_cdp_requests import SecopCdpRequestsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopCdpRequestsPipeline:
    driver = MagicMock()
    return SecopCdpRequestsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_cdp_requests() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["cdp_request_count"] == 2)
    assert first["cdp_used_value_total"] == 300.0
    assert first["pgn_budget_total"] == 800.0
    assert first["sgr_budget_total"] == 100.0
    assert first["own_resources_total"] == 145.0
    assert first["registered_in_siif"] is True
    assert first["latest_siif_status"] == "APROBADO"


def test_load_updates_contract_nodes_for_cdp_requests() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
