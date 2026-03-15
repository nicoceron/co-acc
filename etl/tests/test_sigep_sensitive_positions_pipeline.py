from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.sigep_sensitive_positions import SigepSensitivePositionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SigepSensitivePositionsPipeline:
    driver = MagicMock()
    return SigepSensitivePositionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_sigep_sensitive_positions_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_marks_sensitive_positions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.offices) == 2
    assert all(office["sensitive_position"] is True for office in pipeline.offices)


def test_load_creates_sensitive_salary_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:PublicOffice {office_id: row.office_id})" in str(call)
        for call in run_calls
    )
    assert any("r.sensitive_position = row.sensitive_position" in str(call) for call in run_calls)
