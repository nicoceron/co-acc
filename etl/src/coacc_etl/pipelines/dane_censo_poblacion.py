from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DaneCensoPoblacionPipeline(LakeCsvPipeline):
    name = "dane_censo_poblacion"
    source_id = "dane_censo_poblacion"
    socrata_dataset_id_env = "COACC_DATASET_DANE_CENSO_POBLACION"


PIPELINE_CLASS = DaneCensoPoblacionPipeline
