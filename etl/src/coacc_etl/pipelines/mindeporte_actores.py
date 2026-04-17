from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class MindeporteActoresPipeline(LakeCsvPipeline):
    name = "mindeporte_actores"
    source_id = "mindeporte_actores"
    socrata_dataset_id_env = "COACC_DATASET_MINDEPORTE_ACTORES"


PIPELINE_CLASS = MindeporteActoresPipeline
