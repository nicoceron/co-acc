from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.company_registry_c82u import CompanyRegistryC82uPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CompanyRegistryC82uPipeline:
    driver = MagicMock()
    return CompanyRegistryC82uPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_company_registry_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_companies_people_and_legal_rep_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.people) == 1
    assert len(pipeline.officer_rels) == 1
    assert {company["document_id"] for company in pipeline.companies} == {
        "900123456",
        "52123456",
    }
    acme = next(company for company in pipeline.companies if company["document_id"] == "900123456")
    assert acme["registry_status"] == "ACTIVA"
    assert acme["registered_at"] == "2021-01-15"


def test_load_uses_officer_relationship() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Company {document_id: row.document_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (p)-[r:OFFICER_OF]->(c)" in str(call) for call in run_calls)
