from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class PresidenciaIniciativas33007Pipeline(LakeCsvPipeline):
    name = "presidencia_iniciativas_33007"
    source_id = "presidencia_iniciativas_33007"
    socrata_dataset_id_env = "COACC_DATASET_PRESIDENCIA_INICIATIVAS_33007"


PIPELINE_CLASS = PresidenciaIniciativas33007Pipeline
