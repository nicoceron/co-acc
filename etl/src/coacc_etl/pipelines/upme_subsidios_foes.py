from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class UpmeSubsidiosFoesPipeline(LakeCsvPipeline):
    name = "upme_subsidios_foes"
    source_id = "upme_subsidios"
    socrata_dataset_id_env = "COACC_DATASET_UPME_SUBSIDIOS"


PIPELINE_CLASS = UpmeSubsidiosFoesPipeline
