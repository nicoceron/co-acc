from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.sgr_projects import SgrProjectsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SgrProjectsPipeline:
    driver = MagicMock()
    return SgrProjectsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_projects_and_executor_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 1
    assert len(pipeline.projects) == 2
    assert len(pipeline.rels) == 2


def test_load_creates_administra_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Convenio {convenio_id: row.convenio_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (a)-[r:ADMINISTRA]->(b)" in str(call) for call in run_calls)
