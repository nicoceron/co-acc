from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class PresidenciaNormativaSancionesPipeline(LakeCsvPipeline):
    name = "presidencia_normativa_sanciones"
    source_id = "presidencia_normativa_sanciones"
    socrata_dataset_id_env = "COACC_DATASET_PRESIDENCIA_NORMATIVA_SANCIONES"


PIPELINE_CLASS = PresidenciaNormativaSancionesPipeline
