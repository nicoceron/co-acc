from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class IgacOrtoimagenesPipeline(LakeCsvPipeline):
    name = "igac_ortoimagenes"
    source_id = "igac_ortoimagenes"
    socrata_dataset_id_env = "COACC_DATASET_IGAC_ORTOIMAGENES"


PIPELINE_CLASS = IgacOrtoimagenesPipeline
