from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.mapa_inversiones_projects import MapaInversionesProjectsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> MapaInversionesProjectsPipeline:
    driver = MagicMock()
    return MapaInversionesProjectsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_companies_projects_and_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 2
    assert len(pipeline.projects) == 3
    assert len(pipeline.rels) == 3
    first = pipeline.projects[0]
    assert first["status"] == "En Ejecucion"
    assert first["value"] == 1250000000.0


def test_load_creates_administra_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Convenio {convenio_id: row.convenio_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (a)-[r:ADMINISTRA]->(b)" in str(call) for call in run_calls)
