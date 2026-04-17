from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class ContratistasSancionadosCcePipeline(LakeCsvPipeline):
    name = "contratistas_sancionados_cce"
    source_id = "secop_sanctions"
    socrata_dataset_id_env = "COACC_DATASET_CONTRATISTAS_SANCIONADOS_CCE"


PIPELINE_CLASS = ContratistasSancionadosCcePipeline
