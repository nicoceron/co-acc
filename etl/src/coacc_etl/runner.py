import importlib
import logging
import os
from datetime import UTC, datetime

import click
from neo4j import GraphDatabase

from coacc_etl.linking_hooks import run_post_load_hooks
from coacc_etl.pipelines.actos_administrativos import ActosAdministrativosPipeline
from coacc_etl.pipelines.administrative_acts import AdministrativeActsPipeline
from coacc_etl.pipelines.adverse_media import AdverseMediaPipeline
from coacc_etl.pipelines.asset_disclosures import AssetDisclosuresPipeline
from coacc_etl.pipelines.company_branches_nb3d import CompanyBranchesNb3dPipeline
from coacc_etl.pipelines.company_registry_c82u import CompanyRegistryC82uPipeline
from coacc_etl.pipelines.conflict_disclosures import ConflictDisclosuresPipeline
from coacc_etl.pipelines.control_politico import ControlPoliticoPipeline
from coacc_etl.pipelines.control_politico_requirements import (
    ControlPoliticoRequirementsPipeline,
)
from coacc_etl.pipelines.control_politico_sessions import ControlPoliticoSessionsPipeline
from coacc_etl.pipelines.cuentas_claras_income_2019 import CuentasClarasIncome2019Pipeline
from coacc_etl.pipelines.dnp_project_beneficiary_characterization import (
    DnpProjectBeneficiaryCharacterizationPipeline,
)
from coacc_etl.pipelines.dnp_project_beneficiary_locations import (
    DnpProjectBeneficiaryLocationsPipeline,
)
from coacc_etl.pipelines.dnp_project_contract_links import DnpProjectContractLinksPipeline
from coacc_etl.pipelines.dnp_project_executors import DnpProjectExecutorsPipeline
from coacc_etl.pipelines.dnp_project_locations import DnpProjectLocationsPipeline
from coacc_etl.pipelines.environmental_files import EnvironmentalFilesPipeline
from coacc_etl.pipelines.environmental_files_corantioquia import (
    EnvironmentalFilesCorantioquiaPipeline,
)
from coacc_etl.pipelines.fiscal_findings import FiscalFindingsPipeline
from coacc_etl.pipelines.fiscal_responsibility import FiscalResponsibilityPipeline
from coacc_etl.pipelines.gacetas_territoriales import GacetasTerritorialesPipeline
from coacc_etl.pipelines.health_providers import HealthProvidersPipeline
from coacc_etl.pipelines.higher_ed_directors import HigherEdDirectorsPipeline
from coacc_etl.pipelines.higher_ed_enrollment import HigherEdEnrollmentPipeline
from coacc_etl.pipelines.higher_ed_institutions import HigherEdInstitutionsPipeline
from coacc_etl.pipelines.igac_property_transactions import IgacPropertyTransactionsPipeline
from coacc_etl.pipelines.judicial_cases import JudicialCasesPipeline
from coacc_etl.pipelines.judicial_providencias import JudicialProvidenciasPipeline
from coacc_etl.pipelines.mapa_inversiones_projects import MapaInversionesProjectsPipeline
from coacc_etl.pipelines.official_case_bulletins import OfficialCaseBulletinsPipeline
from coacc_etl.pipelines.paco_sanctions import PacoSanctionsPipeline
from coacc_etl.pipelines.pte_sector_commitments import PteSectorCommitmentsPipeline
from coacc_etl.pipelines.pte_top_contracts import PteTopContractsPipeline
from coacc_etl.pipelines.registraduria_death_status_checks import (
    RegistraduriaDeathStatusChecksPipeline,
)
from coacc_etl.pipelines.rub_beneficial_owners import RubBeneficialOwnersPipeline
from coacc_etl.pipelines.rues_chambers import RuesChambersPipeline
from coacc_etl.pipelines.secop_additional_locations import SecopAdditionalLocationsPipeline
from coacc_etl.pipelines.secop_budget_commitments import SecopBudgetCommitmentsPipeline
from coacc_etl.pipelines.secop_budget_items import SecopBudgetItemsPipeline
from coacc_etl.pipelines.secop_cdp_requests import SecopCdpRequestsPipeline
from coacc_etl.pipelines.secop_contract_additions import SecopContractAdditionsPipeline
from coacc_etl.pipelines.secop_contract_execution import SecopContractExecutionPipeline
from coacc_etl.pipelines.secop_contract_modifications import SecopContractModificationsPipeline
from coacc_etl.pipelines.secop_contract_suspensions import SecopContractSuspensionsPipeline
from coacc_etl.pipelines.secop_document_archives import SecopDocumentArchivesPipeline
from coacc_etl.pipelines.secop_execution_locations import SecopExecutionLocationsPipeline
from coacc_etl.pipelines.secop_i_historical_processes import SecopIHistoricalProcessesPipeline
from coacc_etl.pipelines.secop_i_resource_origins import SecopIResourceOriginsPipeline
from coacc_etl.pipelines.secop_ii_contracts import SecopIiContractsPipeline
from coacc_etl.pipelines.secop_ii_processes import SecopIiProcessesPipeline
from coacc_etl.pipelines.secop_integrado import SecopIntegratedPipeline
from coacc_etl.pipelines.secop_interadmin_agreements import SecopInteradminAgreementsPipeline
from coacc_etl.pipelines.secop_invoices import SecopInvoicesPipeline
from coacc_etl.pipelines.secop_offers import SecopOffersPipeline
from coacc_etl.pipelines.secop_payment_plans import SecopPaymentPlansPipeline
from coacc_etl.pipelines.secop_process_bpin import SecopProcessBpinPipeline
from coacc_etl.pipelines.secop_sanctions import SecopSanctionsPipeline
from coacc_etl.pipelines.secop_suppliers import SecopSuppliersPipeline
from coacc_etl.pipelines.sgr_expense_execution import SgrExpenseExecutionPipeline
from coacc_etl.pipelines.sgr_projects import SgrProjectsPipeline
from coacc_etl.pipelines.sigep_public_servants import SigepPublicServantsPipeline
from coacc_etl.pipelines.sigep_sensitive_positions import SigepSensitivePositionsPipeline
from coacc_etl.pipelines.siri_antecedents import SiriAntecedentsPipeline
from coacc_etl.pipelines.supersoc_top_companies import SupersocTopCompaniesPipeline
from coacc_etl.pipelines.territorial_gazettes import TerritorialGazettesPipeline
from coacc_etl.pipelines.tvec_orders import TvecOrdersPipeline
from coacc_etl.pipelines.tvec_orders_consolidated import TvecOrdersConsolidatedPipeline

PIPELINES: dict[str, type] = {
    "actos_administrativos": ActosAdministrativosPipeline,
    "adverse_media": AdverseMediaPipeline,
    "asset_disclosures": AssetDisclosuresPipeline,
    "company_branches_nb3d": CompanyBranchesNb3dPipeline,
    "conflict_disclosures": ConflictDisclosuresPipeline,
    "company_registry_c82u": CompanyRegistryC82uPipeline,
    "cuentas_claras_income_2019": CuentasClarasIncome2019Pipeline,
    "control_politico": ControlPoliticoPipeline,
    "control_politico_requirements": ControlPoliticoRequirementsPipeline,
    "control_politico_sessions": ControlPoliticoSessionsPipeline,
    "dnp_project_beneficiary_characterization": DnpProjectBeneficiaryCharacterizationPipeline,
    "dnp_project_beneficiary_locations": DnpProjectBeneficiaryLocationsPipeline,
    "dnp_project_contract_links": DnpProjectContractLinksPipeline,
    "dnp_project_executors": DnpProjectExecutorsPipeline,
    "dnp_project_locations": DnpProjectLocationsPipeline,
    "environmental_files_corantioquia": EnvironmentalFilesCorantioquiaPipeline,
    "environmental_files": EnvironmentalFilesPipeline,
    "fiscal_findings": FiscalFindingsPipeline,
    "fiscal_responsibility": FiscalResponsibilityPipeline,
    "gacetas_territoriales": GacetasTerritorialesPipeline,
    "health_providers": HealthProvidersPipeline,
    "higher_ed_directors": HigherEdDirectorsPipeline,
    "higher_ed_enrollment": HigherEdEnrollmentPipeline,
    "higher_ed_institutions": HigherEdInstitutionsPipeline,
    "igac_property_transactions": IgacPropertyTransactionsPipeline,
    "judicial_providencias": JudicialProvidenciasPipeline,
    "judicial_cases": JudicialCasesPipeline,
    "mapa_inversiones_projects": MapaInversionesProjectsPipeline,
    "official_case_bulletins": OfficialCaseBulletinsPipeline,
    "paco_sanctions": PacoSanctionsPipeline,
    "pte_sector_commitments": PteSectorCommitmentsPipeline,
    "pte_top_contracts": PteTopContractsPipeline,
    "registraduria_death_status_checks": RegistraduriaDeathStatusChecksPipeline,
    "rub_beneficial_owners": RubBeneficialOwnersPipeline,
    "rues_chambers": RuesChambersPipeline,
    "sgr_expense_execution": SgrExpenseExecutionPipeline,
    "sgr_projects": SgrProjectsPipeline,
    "secop_budget_commitments": SecopBudgetCommitmentsPipeline,
    "secop_budget_items": SecopBudgetItemsPipeline,
    "secop_cdp_requests": SecopCdpRequestsPipeline,
    "secop_additional_locations": SecopAdditionalLocationsPipeline,
    "secop_contract_additions": SecopContractAdditionsPipeline,
    "secop_contract_execution": SecopContractExecutionPipeline,
    "secop_contract_modifications": SecopContractModificationsPipeline,
    "secop_contract_suspensions": SecopContractSuspensionsPipeline,
    "secop_document_archives": SecopDocumentArchivesPipeline,
    "secop_execution_locations": SecopExecutionLocationsPipeline,
    "secop_i_historical_processes": SecopIHistoricalProcessesPipeline,
    "secop_i_resource_origins": SecopIResourceOriginsPipeline,
    "secop_interadmin_agreements": SecopInteradminAgreementsPipeline,
    "secop_integrado": SecopIntegratedPipeline,
    "secop_invoices": SecopInvoicesPipeline,
    "secop_ii_contracts": SecopIiContractsPipeline,
    "secop_ii_processes": SecopIiProcessesPipeline,
    "secop_offers": SecopOffersPipeline,
    "secop_payment_plans": SecopPaymentPlansPipeline,
    "secop_process_bpin": SecopProcessBpinPipeline,
    "secop_sanctions": SecopSanctionsPipeline,
    "secop_suppliers": SecopSuppliersPipeline,
    "sigep_public_servants": SigepPublicServantsPipeline,
    "sigep_sensitive_positions": SigepSensitivePositionsPipeline,
    "siri_antecedents": SiriAntecedentsPipeline,
    "administrative_acts": AdministrativeActsPipeline,
    "territorial_gazettes": TerritorialGazettesPipeline,
    "tvec_orders_consolidated": TvecOrdersConsolidatedPipeline,
    "tvec_orders": TvecOrdersPipeline,
    "supersoc_top_companies": SupersocTopCompaniesPipeline,
}

LAKE_ONLY_PIPELINE_MODULES = {
    "ungrd_emergencias": "coacc_etl.pipelines.ungrd_emergencias",
    "ideam_gei_inventario": "coacc_etl.pipelines.ideam_gei_inventario",
    "ideam_meteo_historica": "coacc_etl.pipelines.ideam_meteo_historica",
    "anla_concesiones": "coacc_etl.pipelines.anla_concesiones",
    "parques_naturales_areas_protegidas": "coacc_etl.pipelines.parques_naturales_areas_protegidas",
    "upme_energia_geovisor": "coacc_etl.pipelines.upme_energia_geovisor",
    "upme_subsidios_foes": "coacc_etl.pipelines.upme_subsidios_foes",
    "servicio_geologico_minerales": "coacc_etl.pipelines.servicio_geologico_minerales",
    "anm_titulos_mineros": "coacc_etl.pipelines.anm_titulos_mineros",
    "dane_censo_poblacion": "coacc_etl.pipelines.dane_censo_poblacion",
    "dane_pobreza_monetaria": "coacc_etl.pipelines.dane_pobreza_monetaria",
    "dane_ipm_municipal": "coacc_etl.pipelines.dane_ipm_municipal",
    "dane_violencia_genero": "coacc_etl.pipelines.dane_violencia_genero",
    "igac_catastro_alfanumerico": "coacc_etl.pipelines.igac_catastro_alfanumerico",
    "igac_ortoimagenes": "coacc_etl.pipelines.igac_ortoimagenes",
    "servicios_postales_direcciones": "coacc_etl.pipelines.servicios_postales_direcciones",
    "minsalud_cobertura_seguros": "coacc_etl.pipelines.minsalud_cobertura_seguros",
    "minsalud_centros_atencion": "coacc_etl.pipelines.minsalud_centros_atencion",
    "invima_sanciones": "coacc_etl.pipelines.invima_sanciones",
    "dapre_consejos_consultivos": "coacc_etl.pipelines.dapre_consejos_consultivos",
    "secretaria_transparencia_aip": "coacc_etl.pipelines.secretaria_transparencia_aip",
    "presidencia_iniciativas_33007": "coacc_etl.pipelines.presidencia_iniciativas_33007",
    "presidencia_normativa_sanciones": "coacc_etl.pipelines.presidencia_normativa_sanciones",
    "dnp_obras_prioritarias": "coacc_etl.pipelines.dnp_obras_prioritarias",
    "contratistas_sancionados_cce": "coacc_etl.pipelines.contratistas_sancionados_cce",
    "dian_registro_organizaciones_beneficas": (
        "coacc_etl.pipelines.dian_registro_organizaciones_beneficas"
    ),
    "dian_impuestos_publico": "coacc_etl.pipelines.dian_impuestos_publico",
    "snr_propiedad_tierras": "coacc_etl.pipelines.snr_propiedad_tierras",
    "cne_financiamiento_partidos": "coacc_etl.pipelines.cne_financiamiento_partidos",
    "rnec_promesas_campana": "coacc_etl.pipelines.rnec_promesas_campana",
    "camara_registro_cabilderos": "coacc_etl.pipelines.camara_registro_cabilderos",
    "senado_votaciones": "coacc_etl.pipelines.senado_votaciones",
    "senado_proyectos_ley": "coacc_etl.pipelines.senado_proyectos_ley",
    "sirr_reincorporacion": "coacc_etl.pipelines.sirr_reincorporacion",
    "pnis_beneficiarios": "coacc_etl.pipelines.pnis_beneficiarios",
    "pdet_municipios": "coacc_etl.pipelines.pdet_municipios",
    "anim_inmuebles": "coacc_etl.pipelines.anim_inmuebles",
    "dane_micronegocios": "coacc_etl.pipelines.dane_micronegocios",
    "mindeporte_actores": "coacc_etl.pipelines.mindeporte_actores",
}


def resolve_pipeline(source: str) -> type | None:
    if source in PIPELINES:
        return PIPELINES[source]
    module_name = LAKE_ONLY_PIPELINE_MODULES.get(source)
    if module_name is None:
        return None
    module = importlib.import_module(module_name)
    pipeline_cls = module.PIPELINE_CLASS
    PIPELINES[source] = pipeline_cls
    return pipeline_cls


@click.group()
def cli() -> None:
    """CO-ACC ETL — Data ingestion pipelines for Colombian public data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@cli.command()
@click.option(
    "--source",
    "--pipeline",
    "source",
    required=True,
    help="Pipeline name (see 'sources' command)",
)
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j", help="Neo4j user")
@click.option("--neo4j-password", default=None, help="Neo4j password")
@click.option("--neo4j-database", default="neo4j", help="Neo4j database")
@click.option("--data-dir", default="./data", help="Directory for downloaded data")
@click.option("--limit", type=int, default=None, help="Limit rows processed")
@click.option("--chunk-size", type=int, default=50_000, help="Chunk size for batch processing")
@click.option(
    "--linking-tier",
    type=click.Choice(["community", "full"]),
    default=os.getenv("LINKING_TIER", "full"),
    show_default=True,
    help="Post-load linking strategy tier",
)
@click.option("--streaming/--no-streaming", default=False, help="Streaming mode")
@click.option("--start-phase", type=int, default=1, help="Skip to phase N")
@click.option("--history/--no-history", default=False, help="Enable history mode when supported")
@click.option(
    "--to-lake/--to-neo4j",
    default=False,
    help="Write pipeline output to the Parquet lake",
)
@click.option("--full-refresh/--incremental", default=False, help="Ignore lake watermarks")
def run(
    source: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    neo4j_database: str,
    data_dir: str,
    limit: int | None,
    chunk_size: int,
    linking_tier: str,
    streaming: bool,
    start_phase: int,
    history: bool,
    to_lake: bool,
    full_refresh: bool,
) -> None:
    """Run an ETL pipeline."""
    os.environ["NEO4J_DATABASE"] = neo4j_database

    pipeline_cls = resolve_pipeline(source)
    if pipeline_cls is None:
        available = ", ".join(sorted({*PIPELINES, *LAKE_ONLY_PIPELINE_MODULES}))
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")

    if to_lake:
        pipeline = pipeline_cls(
            driver=None,
            data_dir=data_dir,
            limit=limit,
            chunk_size=chunk_size,
            history=history,
        )
        if not hasattr(pipeline, "run_to_lake"):
            raise click.ClickException(f"Pipeline {source} does not implement --to-lake")
        delta = pipeline.run_to_lake(full_refresh=full_refresh)
        click.echo(
            f"{delta.source}: wrote {delta.rows:,} rows "
            f"(batch={delta.batch_id}, advanced={delta.advanced})"
        )
        return

    if not neo4j_password:
        neo4j_password = os.environ.get("NEO4J_PASSWORD")
    if not neo4j_password:
        raise click.ClickException("--neo4j-password or NEO4J_PASSWORD env var required")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        pipeline = pipeline_cls(
            driver=driver,
            data_dir=data_dir,
            limit=limit,
            chunk_size=chunk_size,
            history=history,
        )

        if streaming and hasattr(pipeline, "run_streaming"):
            started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            pipeline._upsert_ingestion_run(status="running", started_at=started_at)
            try:
                pipeline.run_streaming(start_phase=start_phase)
                finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                pipeline._upsert_ingestion_run(
                    status="loaded",
                    started_at=started_at,
                    finished_at=finished_at,
                )
            except Exception as exc:
                finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                pipeline._upsert_ingestion_run(
                    status="quality_fail",
                    started_at=started_at,
                    finished_at=finished_at,
                    error=str(exc)[:1000],
                )
                raise
        else:
            pipeline.run()

        run_post_load_hooks(
            driver=driver,
            source=source,
            neo4j_database=neo4j_database,
            linking_tier=linking_tier,
        )
    finally:
        driver.close()


@cli.command()
@click.option("--status", "show_status", is_flag=True, help="Show ingestion status from Neo4j")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j")
@click.option("--neo4j-password", default=None)
def sources(show_status: bool, neo4j_uri: str, neo4j_user: str, neo4j_password: str | None) -> None:
    """List available data sources."""
    if not show_status:
        click.echo("Available pipelines:")
        for name in sorted({*PIPELINES, *LAKE_ONLY_PIPELINE_MODULES}):
            click.echo(f"  {name}")
        return

    if not neo4j_password:
        neo4j_password = os.environ.get("NEO4J_PASSWORD", "")
    if not neo4j_password:
        raise click.ClickException(
            "--neo4j-password or NEO4J_PASSWORD env var required for --status"
        )

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as session:
            result = session.run(
                "MATCH (r:IngestionRun) "
                "WITH r ORDER BY r.started_at DESC "
                "WITH r.source_id AS sid, collect(r)[0] AS latest "
                "RETURN latest ORDER BY sid"
            )
            runs = {r["latest"]["source_id"]: dict(r["latest"]) for r in result}

        click.echo(
            f"{'Source':<20} {'Status':<15} {'Rows In':>10} {'Loaded':>10} "
            f"{'Started':<20} {'Finished':<20}"
        )
        click.echo("-" * 100)

        for name in sorted({*PIPELINES, *LAKE_ONLY_PIPELINE_MODULES}):
            run = runs.get(name, {})
            click.echo(
                f"{name:<20} "
                f"{run.get('status', '-'):<15} "
                f"{run.get('rows_in', 0):>10,} "
                f"{run.get('rows_loaded', 0):>10,} "
                f"{str(run.get('started_at', '-')):<20} "
                f"{str(run.get('finished_at', '-')):<20}"
            )
    finally:
        driver.close()


if __name__ == "__main__":
    cli()
