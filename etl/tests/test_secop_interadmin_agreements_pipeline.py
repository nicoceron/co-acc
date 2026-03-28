from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_interadmin_agreements import SecopInteradminAgreementsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopInteradminAgreementsPipeline:
    driver = MagicMock()
    return SecopInteradminAgreementsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_companies_and_interadmin_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) >= 3
    assert len(pipeline.agreements) == 2
    fondecun = next(
        company
        for company in pipeline.companies
        if company["document_id"] == "900258772"
    )
    assert fondecun["name"] == "FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA"


def test_load_uses_interadmin_relationship_type() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (buyer)-[r:CELEBRO_CONVENIO_INTERADMIN {summary_id: row.summary_id}]->(counterparty)"
        in str(call)
        for call in run_calls
    )
