from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_ii_contracts import SecopIiContractsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopIiContractsPipeline:
    driver = MagicMock()
    return SecopIiContractsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_secop_ii_contracts_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_procurement_summaries_and_officer_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.people) == 2
    assert len(pipeline.procurements) == 2
    assert len(pipeline.officer_rels) == 2
    assert len(pipeline.summary_mappings) == 2


def test_load_creates_contract_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier)"
        in str(call)
        for call in run_calls
    )
    assert any("MERGE (p)-[r:OFFICER_OF]->(c)" in str(call) for call in run_calls)
