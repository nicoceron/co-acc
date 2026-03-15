from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_invoices import SecopInvoicesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopInvoicesPipeline:
    driver = MagicMock()
    return SecopInvoicesPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_aggregates_invoices() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(item for item in pipeline.procurements if item["invoice_count"] == 2)
    assert first["paid_invoice_count"] == 1
    assert first["invoice_total_value"] == 2100.0
    assert first["invoice_net_total"] == 1700.0
    assert first["latest_invoice_status"] == "Pagado"


def test_load_updates_contract_nodes_for_invoices() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
