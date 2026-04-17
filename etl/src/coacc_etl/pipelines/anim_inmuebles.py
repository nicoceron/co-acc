from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class AnimInmueblesPipeline(LakeCsvPipeline):
    name = "anim_inmuebles"
    source_id = "anim_inmuebles"
    socrata_dataset_id_env = "COACC_DATASET_ANIM_INMUEBLES"


PIPELINE_CLASS = AnimInmueblesPipeline
