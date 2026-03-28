from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.higher_ed_institutions import HigherEdInstitutionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> HigherEdInstitutionsPipeline:
    driver = MagicMock()
    return HigherEdInstitutionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_keeps_real_nit_and_institution_code() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    first = pipeline.companies[0]
    assert first["document_id"] == "8605242195"
    assert first["education_institution_code"] == "4702"
    assert first["company_type"] == "INSTITUCION_EDUCACION_SUPERIOR"


def test_load_merges_company_by_document_id_and_education_code() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (c:Company {document_id: row.document_id})" in str(call) for call in run_calls)
    assert any("c.education_institution_code = row.education_institution_code" in str(call) for call in run_calls)
    assert any("MERGE (edu)-[r:SAME_AS]->(other)" in str(call) for call in run_calls)
    assert any("exact_name_numeric_prefix" in str(call) for call in run_calls)
