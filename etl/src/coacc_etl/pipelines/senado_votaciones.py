from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class SenadoVotacionesPipeline(LakeCsvPipeline):
    name = "senado_votaciones"
    source_id = "senado_votaciones"
    socrata_dataset_id_env = "COACC_DATASET_SENADO_VOTACIONES"


PIPELINE_CLASS = SenadoVotacionesPipeline
