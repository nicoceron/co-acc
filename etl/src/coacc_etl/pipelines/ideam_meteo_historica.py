from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class IdeamMeteoHistoricaPipeline(LakeCsvPipeline):
    name = "ideam_meteo_historica"
    source_id = "ideam_meteo_historica"
    socrata_dataset_id_env = "COACC_DATASET_IDEAM_METEO_HISTORICA"


PIPELINE_CLASS = IdeamMeteoHistoricaPipeline
