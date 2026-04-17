from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class InvimaSancionesPipeline(LakeCsvPipeline):
    name = "invima_sanciones"
    source_id = "invima_sanciones"
    socrata_dataset_id_env = "COACC_DATASET_INVIMA_SANCIONES"


PIPELINE_CLASS = InvimaSancionesPipeline
