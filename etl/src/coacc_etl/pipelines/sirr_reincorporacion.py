from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class SirrReincorporacionPipeline(LakeCsvPipeline):
    name = "sirr_reincorporacion"
    source_id = "sirr_reincorporacion"
    socrata_dataset_id_env = "COACC_DATASET_SIRR_REINCORPORACION"


PIPELINE_CLASS = SirrReincorporacionPipeline
