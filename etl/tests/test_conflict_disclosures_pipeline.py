from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.conflict_disclosures import ConflictDisclosuresPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> ConflictDisclosuresPipeline:
    driver = MagicMock()
    return ConflictDisclosuresPipeline(driver=driver, data_dir=str(FIXTURES))


def test_extract_reads_conflict_disclosures_fixture() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()

    assert len(pipeline._raw) == 2


def test_transform_builds_finance_disclosures() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.people) == 2
    assert len(pipeline.disclosures) == 2
    assert len(pipeline.disclosure_rels) == 2
    assert len(pipeline.contracts) == 1
    assert len(pipeline.company_document_refs) == 1
    assert len(pipeline.company_name_refs) == 1
    assert len(pipeline.contract_refs) == 1
    assert all(item["finance_id"].startswith("ley2013_conflict_") for item in pipeline.disclosures)
    targeted = next(
        disclosure
        for disclosure in pipeline.disclosures
        if disclosure["finance_id"] == "ley2013_conflict_769455-01"
    )
    assert targeted["mentioned_document_ids"] == ["900123456"]
    assert targeted["mentioned_company_names"] == ["CONSORCIO ANDINO S.A.S"]
    assert targeted["matched_contract_ids"] == ["co_proc_ani_001_2024"]
    assert targeted["legal_role_term_count"] == 1
    assert targeted["litigation_term_count"] == 1


def test_load_creates_finance_disclosure_relationships() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any("MERGE (n:Finance {finance_id: row.finance_id})" in str(call) for call in run_calls)
    assert any(
        "MERGE (n:Contract {contract_id: row.contract_id})" in str(call)
        for call in run_calls
    )
    assert any("MERGE (p)-[r:DECLARO_FINANZAS]->(f)" in str(call) for call in run_calls)
    assert any("MATCH (a:Finance {finance_id: row.source_key})" in str(call) for call in run_calls)
    assert any(
        "MATCH (c:Company {razon_social: row.target_name})" in str(call)
        for call in run_calls
    )
    assert any(
        "MATCH (b:Contract {contract_id: row.target_key})" in str(call)
        for call in run_calls
    )
