from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.company_branches_nb3d import CompanyBranchesNb3dPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CompanyBranchesNb3dPipeline:
    driver = MagicMock()
    return CompanyBranchesNb3dPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_branch_nodes_and_owner_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.branches) == 2
    assert len(pipeline.owner_links) == 2
    first = pipeline.branches[0]
    assert first["company_kind"] == "ESTABLISHMENT"
    assert first["registry_source"] == "nb3d-v3n7"


def test_load_creates_owner_link_queries() -> None:
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
    assert any(
        "MERGE (owner_person)-[rp:OFFICER_OF]->(branch)" in str(call)
        for call in run_calls
    )
