from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.asset_disclosures import AssetDisclosuresPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> AssetDisclosuresPipeline:
    driver = MagicMock()
    return AssetDisclosuresPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_asset_disclosures_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_people_and_declared_assets() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.assets) == 2
    assert len(pipeline.asset_rels) == 2
    assert all(asset["asset_id"].startswith("ley2013_asset_") for asset in pipeline.assets)


def test_load_creates_declared_asset_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:DeclaredAsset {asset_id: row.asset_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (p)-[r:DECLAROU_BEM]->(a)" in str(call) for call in run_calls)
