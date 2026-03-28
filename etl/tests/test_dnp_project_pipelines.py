from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from coacc_etl.pipelines.dnp_project_beneficiary_characterization import (
    DnpProjectBeneficiaryCharacterizationPipeline,
)
from coacc_etl.pipelines.dnp_project_beneficiary_locations import (
    DnpProjectBeneficiaryLocationsPipeline,
)
from coacc_etl.pipelines.dnp_project_contract_links import DnpProjectContractLinksPipeline
from coacc_etl.pipelines.dnp_project_executors import DnpProjectExecutorsPipeline
from coacc_etl.pipelines.dnp_project_locations import DnpProjectLocationsPipeline

FIXTURES = Path(__file__).parent / "fixtures"


def test_dnp_project_executors_build_projects_and_admin_relations() -> None:
    pipeline = DnpProjectExecutorsPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.projects) == 2
    assert len(pipeline.companies) == 2
    assert len(pipeline.rels) == 2
    first = next(
        project for project in pipeline.projects if project["convenio_id"] == "20210214000125"
    )
    assert first["executor_entity_name"] == "CUNDINAMARCA"


def test_dnp_project_locations_aggregate_location_fields() -> None:
    pipeline = DnpProjectLocationsPipeline(driver=MagicMock(), data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()

    assert len(pipeline.projects) == 2
    first = next(
        project for project in pipeline.projects if project["convenio_id"] == "20210214000125"
    )
    assert first["project_location_count"] == 2
    assert "ORITO, PUTUMAYO" in first["project_locations"]
    assert "Putumayo" in first["project_regions"]


def test_dnp_project_beneficiary_layers_attach_beneficiary_signals() -> None:
    location_pipeline = DnpProjectBeneficiaryLocationsPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
    )
    location_pipeline.extract()
    location_pipeline.transform()

    project = next(
        item
        for item in location_pipeline.projects
        if item["convenio_id"] == "20210214000125"
    )
    assert project["beneficiary_location_count"] == 2
    assert project["beneficiary_total_reported"] == 120

    characterization_pipeline = DnpProjectBeneficiaryCharacterizationPipeline(
        driver=MagicMock(),
        data_dir=str(FIXTURES),
    )
    characterization_pipeline.extract()
    characterization_pipeline.transform()
    characterization = next(
        item
        for item in characterization_pipeline.projects
        if item["convenio_id"] == "20210214000125"
    )
    assert characterization["beneficiary_characterization_count"] == 2
    assert characterization["beneficiary_quantity_total"] == 120


def test_dnp_project_contract_links_query_and_load() -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    session.run.return_value = [
        {
            "buyer_document_id": "900100200",
            "supplier_document_id": "800100300",
            "summary_id": "contract-1",
            "bpin_code": "20210214000125",
        }
    ]

    pipeline = DnpProjectContractLinksPipeline(driver=driver, data_dir=str(FIXTURES))
    pipeline.extract()
    pipeline.transform()
    pipeline.load()

    assert len(pipeline.reference_rows) == 1
    run_calls = session.run.call_args_list
    assert any(
        "MATCH (buyer:Company)-[award:CONTRATOU]->(supplier:Company)" in str(call)
        for call in run_calls
    )
    assert any("BPIN_BUYER" in str(call) for call in run_calls)
