from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from coacc_etl.pipelines.secop_integrado import SecopIntegratedPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> SecopIntegratedPipeline:
    driver = MagicMock()
    return SecopIntegratedPipeline(driver=driver, data_dir=str(FIXTURES))


def _load_fixture_data(pipeline: SecopIntegratedPipeline) -> None:
    pipeline._raw = pd.read_csv(
        FIXTURES / "secop_integrado" / "secop_integrado.csv",
        dtype=str,
        keep_default_na=False,
    )


def test_transform_creates_company_nodes_and_procurement_edges() -> None:
    pipeline = _make_pipeline()
    _load_fixture_data(pipeline)
    pipeline.transform()

    assert len(pipeline.companies) == 4
    assert len(pipeline.procurements) == 2


def test_transform_normalizes_company_documents() -> None:
    pipeline = _make_pipeline()
    _load_fixture_data(pipeline)
    pipeline.transform()

    document_ids = {row["document_id"] for row in pipeline.companies}
    assert "901234567" in document_ids
    assert "800765432" in document_ids


def test_load_uses_company_document_id() -> None:
    pipeline = _make_pipeline()
    _load_fixture_data(pipeline)
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    run_calls = session_mock.run.call_args_list
    assert any(
        "MERGE (n:Company {document_id: row.document_id})" in str(call)
        for call in run_calls
    )
    assert any(
        "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier)"
        in str(call)
        for call in run_calls
    )
