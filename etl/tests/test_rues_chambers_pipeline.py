from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.rues_chambers import RuesChambersPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> RuesChambersPipeline:
    driver = MagicMock()
    return RuesChambersPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_chamber_companies() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    first = pipeline.companies[0]
    assert first["company_type"] == "CHAMBER_OF_COMMERCE"
    assert first["camera_code"] == "04"


def test_load_creates_company_nodes() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Company {document_id: row.document_id})" in str(call) for call in run_calls
    )
