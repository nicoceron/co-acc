from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_suppliers import SecopSuppliersPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopSuppliersPipeline:
    driver = MagicMock()
    return SecopSuppliersPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_secop_suppliers_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_companies_people_and_legal_rep_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.people) == 2
    assert len(pipeline.officer_rels) == 2
    assert {company["document_id"] for company in pipeline.companies} == {
        "900123456",
        "830456789",
    }


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
