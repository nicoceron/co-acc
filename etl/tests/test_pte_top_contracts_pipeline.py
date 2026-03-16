from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.pte_top_contracts import PteTopContractsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> PteTopContractsPipeline:
    driver = MagicMock()
    return PteTopContractsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_finance_nodes_and_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 4
    assert len(pipeline.finances) == 2
    assert len(pipeline.admin_relations) == 2
    assert len(pipeline.beneficiary_relations) == 2
    assert len(pipeline.sector_relations) == 2
    first = pipeline.finances[0]
    assert first["type"] == "PTE_TOP_CONTRACT"
    assert first["value_paid"] == 0.0


def test_load_creates_administra_beneficiou_and_referente_a_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (a)-[r:ADMINISTRA]->(b)" in str(call) for call in run_calls)
    assert any("MERGE (a)-[r:BENEFICIO]->(b)" in str(call) for call in run_calls)
    assert any("MERGE (contract)-[r:REFERENTE_A]->(sector)" in str(call) for call in run_calls)
