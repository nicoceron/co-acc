from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_process_bpin import SecopProcessBpinPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopProcessBpinPipeline:
    driver = MagicMock()
    return SecopProcessBpinPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_bpin_links_by_contract_or_portfolio() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["bpin_code"] == "202500000001")
    second = next(item for item in pipeline.procurements if item["bpin_code"] == "202500000002")
    assert first["bpin_validated_count"] == 1
    assert second["bpin_unvalidated_count"] == 1


def test_load_updates_contract_nodes_for_bpin_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
