from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.pte_sector_commitments import PteSectorCommitmentsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> PteSectorCommitmentsPipeline:
    driver = MagicMock()
    return PteSectorCommitmentsPipeline(driver=driver, data_dir=str(FIXTURES))


def test_transform_builds_finance_nodes_for_sector_commitments() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.finances) == 2
    first = pipeline.finances[0]
    assert first["type"] == "PTE_SECTOR_COMMITMENT"
    assert first["value"] == 606841833734.73
    assert first["execution_ratio"] == 0.17


def test_load_creates_finance_nodes_for_sector_commitments() -> None:
    pipeline = _make_pipeline()
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    session_mock = pipeline.driver.session.return_value.__enter__.return_value
    assert any(
        "MERGE (n:Finance {finance_id: row.finance_id})" in str(call)
        for call in session_mock.run.call_args_list
    )
