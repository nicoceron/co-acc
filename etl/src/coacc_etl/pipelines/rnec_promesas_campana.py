from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class RnecPromesasCampanaPipeline(LakeCsvPipeline):
    name = "rnec_promesas_campana"
    source_id = "rnec_promesas_campana"
    socrata_dataset_id_env = "COACC_DATASET_RNEC_PROMESAS_CAMPANA"


PIPELINE_CLASS = RnecPromesasCampanaPipeline
