from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DnpObrasPrioritariasPipeline(LakeCsvPipeline):
    name = "dnp_obras_prioritarias"
    source_id = "dnp_obras_prioritarias"
    socrata_dataset_id_env = "COACC_DATASET_DNP_OBRAS_PRIORITARIAS"


PIPELINE_CLASS = DnpObrasPrioritariasPipeline
