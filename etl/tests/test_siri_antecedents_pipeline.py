from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.siri_antecedents import SiriAntecedentsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SiriAntecedentsPipeline:
    driver = MagicMock()
    return SiriAntecedentsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_people_and_siri_sanctions() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.sanctions) == 2
    assert len(pipeline.person_rels) == 2
    first = pipeline.sanctions[0]
    assert first["type"] == "SIRI_ANTECEDENT"
    assert first["sanction_domain"] == "DISCIPLINARIO"


def test_load_creates_person_sanction_relationships() -> None:
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
    assert any("MERGE (p)-[r:SANCIONADA]->(s)" in str(call) for call in run_calls)

