from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.official_case_bulletins import OfficialCaseBulletinsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> OfficialCaseBulletinsPipeline:
    driver = MagicMock()
    return OfficialCaseBulletinsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_inquiries_and_subject_links() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.inquiries) == 2
    assert len(pipeline.people_with_document) == 1
    assert len(pipeline.people_placeholders) == 1
    assert len(pipeline.person_rels) == 2
    assert len(pipeline.company_placeholders) == 1
    assert pipeline.inquiries[0]["source"] == "official_case_bulletins"
    assert pipeline.people_placeholders[0]["case_bulletin_placeholder"] is True


def test_load_creates_inquiries_reference_links_and_probable_match_queries() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Inquiry {inquiry_id: row.inquiry_id})" in str(call) for call in run_calls)
    assert any("MERGE (p)-[r:REFERENTE_A]->(i)" in str(call) for call in run_calls)
    assert any("MERGE (c)-[r:REFERENTE_A]->(i)" in str(call) for call in run_calls)
    assert any("MERGE (case_person)-[r:POSSIBLY_SAME_AS]->(matched_person)" in str(call) for call in run_calls)
    assert any("MERGE (case_person)-[r:POSSIBLY_SAME_AS]->(matched_company)" in str(call) for call in run_calls)
    assert any("size(matches) >= 1 AND size(matches) <= 3" in str(call) for call in run_calls)
