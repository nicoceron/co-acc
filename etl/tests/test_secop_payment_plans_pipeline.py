from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_payment_plans import SecopPaymentPlansPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopPaymentPlansPipeline:
    driver = MagicMock()
    return SecopPaymentPlansPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_payment_plans_and_supervisors() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["payment_plan_count"] == 2)
    assert first["payment_actual_count"] == 1
    assert first["payment_delay_count"] == 1
    assert first["payment_total_value"] == 1200.0
    assert first["latest_payment_supervisor_document"] == "52123456"
    assert len(pipeline.people) == 2
    assert len(pipeline.supervisor_rels) == 2


def test_load_updates_contract_nodes_and_supervisor_relationships_for_payment_plans() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in run_calls
    )
    assert any(
        "MERGE (p)-[r:SUPERVISA_PAGO {summary_id: row.summary_id}]->(c)" in str(call)
        for call in run_calls
    )
