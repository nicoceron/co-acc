from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.registraduria_death_status_checks import (
    RegistraduriaDeathStatusChecksPipeline,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> RegistraduriaDeathStatusChecksPipeline:
    driver = MagicMock()
    return RegistraduriaDeathStatusChecksPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_person_status_records() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    deceased = next(person for person in pipeline.people if person["document_id"] == "12345678")
    assert deceased["is_deceased"] is True
    assert deceased["identity_status"] == "Cancelada por muerte"


def test_load_updates_person_nodes() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Person {document_id: row.document_id})" in str(call) for call in run_calls)
