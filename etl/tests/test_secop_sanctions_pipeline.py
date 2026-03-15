from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_sanctions import SecopSanctionsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopSanctionsPipeline:
    driver = MagicMock()
    return SecopSanctionsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_both_secop_sanction_feeds() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline.secop_i_raw) == 1
    assert len(pipeline.secop_ii_raw) == 1


def test_transform_builds_sanctions_and_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.contracts) == 2
    assert len(pipeline.sanctions) == 2
    assert len(pipeline.company_rels) == 2
    assert len(pipeline.contract_rels) == 2
    assert len(pipeline.company_contract_rels) == 2


def test_load_creates_sancionada_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Sanction {sanction_id: row.sanction_id})" in str(call)
        for call in run_calls
    )
    assert any(
        "MERGE (n:Contract {contract_id: row.contract_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (c)-[r:SANCIONADA]->(s)" in str(call) for call in run_calls)
    assert any(
        "MATCH (b:Contract {contract_id: row.target_key})" in str(call)
        for call in run_calls
    )
