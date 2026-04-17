from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DianRegistroOrganizacionesBeneficasPipeline(LakeCsvPipeline):
    name = "dian_registro_organizaciones_beneficas"
    source_id = "dian_registro_organizaciones_beneficas"
    socrata_dataset_id_env = "COACC_DATASET_DIAN_ORGANIZACIONES_BENEFICAS"


PIPELINE_CLASS = DianRegistroOrganizacionesBeneficasPipeline
