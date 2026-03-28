from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.higher_ed_directors import HigherEdDirectorsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> HigherEdDirectorsPipeline:
    driver = MagicMock()
    return HigherEdDirectorsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_director_people_and_admin_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 4
    assert len(pipeline.rels) == 4
    assert len(pipeline.family_rels) == 1
    assert pipeline.people[0]["is_education_director"] is True
    assert pipeline.rels[0]["source"] == "higher_ed_directors"
    assert pipeline.family_rels[0]["shared_surnames"] == ["ZARATE", "GIRALDO"]


def test_load_creates_admin_links_and_probable_name_query() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Person {document_id: row.document_id})" in str(call) for call in run_calls)
    assert any("MERGE (p)-[r:ADMINISTRA]->(c)" in str(call) for call in run_calls)
    assert any("MERGE (left)-[r:POSSIBLE_FAMILY_TIE]->(right)" in str(call) for call in run_calls)
    assert any("MERGE (d)-[r:POSSIBLY_SAME_AS]->(matched_person)" in str(call) for call in run_calls)
