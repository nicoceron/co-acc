from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_ii_processes import SecopIiProcessesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopIiProcessesPipeline:
    driver = MagicMock()
    return SecopIiProcessesPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_secop_ii_processes_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_award_summaries() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.procurement_awards) == 2
    assert {row["summary_id"] for row in pipeline.procurement_awards}


def test_load_creates_award_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (buyer)-[r:ADJUDICOU_A {summary_id: row.summary_id}]->(supplier)"
        in str(call)
        for call in run_calls
    )
