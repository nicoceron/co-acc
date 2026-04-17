from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DianImpuestosPublicoPipeline(LakeCsvPipeline):
    name = "dian_impuestos_publico"
    source_id = "dian_impuestos_publico"
    socrata_dataset_id_env = "COACC_DATASET_DIAN_IMPUESTOS_PUBLICO"


PIPELINE_CLASS = DianImpuestosPublicoPipeline
