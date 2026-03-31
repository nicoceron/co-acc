from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.administrative_acts import AdministrativeActsPipeline
from coacc_etl.pipelines.control_politico_requirements import (
    ControlPoliticoRequirementsPipeline,
)
from coacc_etl.pipelines.control_politico_sessions import ControlPoliticoSessionsPipeline
from coacc_etl.pipelines.environmental_files import EnvironmentalFilesPipeline
from coacc_etl.pipelines.judicial_cases import JudicialCasesPipeline
from coacc_etl.pipelines.territorial_gazettes import TerritorialGazettesPipeline
from coacc_etl.pipelines.tvec_orders import TvecOrdersPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def test_judicial_cases_pipeline_builds_cases_and_documents() -> None:
    pipeline = JudicialCasesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.cases) == 1
    assert len(pipeline.documents) == 1
    assert pipeline.cases[0]["radicado"] == "11001-00-00-000-2024-00001-00"


def test_administrative_and_gazette_pipelines_build_documentary_nodes() -> None:
    act_pipeline = AdministrativeActsPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    act_pipeline.extract()
    act_pipeline.transform()
    assert len(act_pipeline.acts) == 1
    assert act_pipeline.acts[0]["number"] == "123"

    gazette_pipeline = TerritorialGazettesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    gazette_pipeline.extract()
    gazette_pipeline.transform()
    assert len(gazette_pipeline.gazettes) == 1
    assert gazette_pipeline.gazettes[0]["number"] == "45"


def test_control_politico_pipelines_build_inquiry_subnodes() -> None:
    requirement_pipeline = ControlPoliticoRequirementsPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
    )
    requirement_pipeline.extract()
    requirement_pipeline.transform()
    assert len(requirement_pipeline.inquiries) == 1
    assert len(requirement_pipeline.requirements) == 1
    assert requirement_pipeline.requirements[0]["code"] == "PROP-001"

    session_pipeline = ControlPoliticoSessionsPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
    )
    session_pipeline.extract()
    session_pipeline.transform()
    assert len(session_pipeline.inquiries) == 1
    assert len(session_pipeline.sessions) == 1
    assert session_pipeline.sessions[0]["session_no"] == "12"


def test_tvec_orders_pipeline_builds_supplier_capture_rows() -> None:
    pipeline = TvecOrdersPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.companies) == 3
    assert len(pipeline.orders) == 2
    assert pipeline.orders[0]["aggregation"] == "Acuerdo Marco Nube"
    assert pipeline.orders[0]["supplier_document_id"] == "860012336"


def test_environmental_files_pipeline_extracts_subject_names_for_exact_matching() -> None:
    pipeline = EnvironmentalFilesPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.files) == 1
    assert len(pipeline.company_file_rows) == 1
    assert pipeline.company_file_rows[0]["subject_name"] == "ICONTEC"
