from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.health_providers import HealthProvidersPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> HealthProvidersPipeline:
    driver = MagicMock()
    return HealthProvidersPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_health_sites_and_provider_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 1
    assert len(pipeline.health_sites) == 2
    assert len(pipeline.rels) == 2


def test_load_creates_operates_unit_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Health {reps_code: row.reps_code})" in str(call) for call in run_calls)
    assert any("MERGE (a)-[r:OPERA_UNIDAD]->(b)" in str(call) for call in run_calls)
