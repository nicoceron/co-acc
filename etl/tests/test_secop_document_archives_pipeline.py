from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.secop_document_archives import SecopDocumentArchivesPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopDocumentArchivesPipeline:
    driver = MagicMock()
    return SecopDocumentArchivesPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_attaches_archive_documents_to_live_summaries() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.procurements) == 2
    first = next(
        item
        for item in pipeline.procurements
        if item["summary_id"] == "co_proc_b735577d9be24627"
    )
    assert first["archive_document_count"] == 2
    assert first["archive_supervision_document_count"] == 1
    assert first["archive_assignment_document_count"] == 1
    assert "Designacion de supervisor" in first["archive_document_names"]
    assert "DOC-1002" in first["archive_document_refs"]
    assert "pdf" in first["archive_document_extensions"]
    assert first["archive_document_last_upload"] == "2025-02-01"


def test_load_updates_contract_nodes_for_archive_documents() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "archive_document_count" in str(call)
        and "archive_supervision_document_count" in str(call)
        and "MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()" in str(call)
        for call in session_mock.run.call_args_list
    )
