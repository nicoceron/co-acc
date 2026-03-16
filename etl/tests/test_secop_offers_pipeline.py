from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_offers import SecopOffersPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopOffersPipeline:
    driver = MagicMock()
    return SecopOffersPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_bids_and_bidder_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.bids) == 2
    assert len(pipeline.buyer_bid_rels) == 2
    assert len(pipeline.supplier_bid_rels) == 3

    bid = next(item for item in pipeline.bids if item["bid_id"] == "CO1.BDOS.4001")
    assert bid["offer_count"] == 3
    assert bid["total_offer_value"] == 3150.0

    supplier_bid = next(
        item
        for item in pipeline.supplier_bid_rels
        if item["source_key"] == "900123456" and item["target_key"] == "CO1.BDOS.4001"
    )
    assert supplier_bid["offer_count"] == 2
    assert supplier_bid["offer_value_total"] == 2100.0
    assert supplier_bid["latest_offer_id"] == "OFF-3"


def test_load_creates_bid_and_participation_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (b:Bid {bid_id: row.bid_id})" in str(call) for call in run_calls)
    assert any("MERGE (buyer)-[r:LICITO]->(bid)" in str(call) for call in run_calls)
    assert any(
        "MERGE (supplier)-[r:SUMINISTRO_LICITACAO]->(bid)" in str(call)
        for call in run_calls
    )
