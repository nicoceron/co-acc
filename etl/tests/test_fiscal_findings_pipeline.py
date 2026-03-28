from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.fiscal_findings import FiscalFindingsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> FiscalFindingsPipeline:
    driver = MagicMock()
    return FiscalFindingsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_company_audit_findings() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.findings) == 2
    assert len(pipeline.company_rels) == 2
    first = pipeline.findings[0]
    assert first["type"] == "CO_FISCAL_FINDING"
    assert first["radicado"] == "202501200252533"


def test_load_creates_hallazgo_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Finding {finding_id: row.finding_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (c)-[r:TIENE_HALLAZGO]->(f)" in str(call) for call in run_calls)

