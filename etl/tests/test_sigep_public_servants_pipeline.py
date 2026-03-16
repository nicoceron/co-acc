from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.sigep_public_servants import SigepPublicServantsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SigepPublicServantsPipeline:
    driver = MagicMock()
    return SigepPublicServantsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_sigep_public_servants_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_people_and_public_offices() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.offices) == 2
    assert len(pipeline.office_rels) == 2
    assert all(office["office_id"].startswith("office_") for office in pipeline.offices)


def test_load_creates_salary_relationships() -> None:
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
    assert any("MERGE (p)-[r:RECIBIO_SALARIO]->(o)" in str(call) for call in run_calls)
