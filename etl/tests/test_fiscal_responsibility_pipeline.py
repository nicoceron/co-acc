from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.fiscal_responsibility import FiscalResponsibilityPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> FiscalResponsibilityPipeline:
    driver = MagicMock()
    return FiscalResponsibilityPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_company_fiscal_responsibility_sanctions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.sanctions) == 2
    assert len(pipeline.company_rels) == 2
    first = pipeline.sanctions[0]
    assert first["type"] == "CO_FISCAL_RESPONSIBILITY"
    assert first["sanction_domain"] == "FISCAL"


def test_load_creates_company_sanction_relationships() -> None:
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
    assert any("MERGE (c)-[r:SANCIONADA]->(s)" in str(call) for call in run_calls)

