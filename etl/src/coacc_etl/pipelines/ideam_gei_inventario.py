from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class IdeamGeiInventarioPipeline(LakeCsvPipeline):
    name = "ideam_gei_inventario"
    source_id = "ideam_gei_inventario"
    socrata_dataset_id_env = "COACC_DATASET_IDEAM_GEI_INVENTARIO"


PIPELINE_CLASS = IdeamGeiInventarioPipeline
