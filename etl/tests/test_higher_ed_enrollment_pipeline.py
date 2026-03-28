from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.higher_ed_enrollment import HigherEdEnrollmentPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> HigherEdEnrollmentPipeline:
    driver = MagicMock()
    return HigherEdEnrollmentPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_enrollment_by_program_period() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 1
    assert pipeline.companies[0]["education_institution_code"] == "2833"
    assert pipeline.companies[0]["document_id"] == "coedu_2833"
    assert len(pipeline.education_nodes) == 1
    assert pipeline.education_nodes[0]["enrolled_total"] == 4
    assert len(pipeline.rels) == 1


def test_load_matches_company_by_education_code_and_creates_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (c:Company {education_institution_code: row.education_institution_code})" in str(call) for call in run_calls)
    assert any("MERGE (n:Education {school_id: row.school_id})" in str(call) for call in run_calls)
    assert any("MATCH (c:Company {education_institution_code: row.source_key})" in str(call) for call in run_calls)
    assert any("MERGE (c)-[r:MANTIENE_A]->(e)" in str(call) for call in run_calls)
