from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class UngrdEmergenciasPipeline(LakeCsvPipeline):
    name = "ungrd_emergencias"
    source_id = "ungrd_damnificados"
    socrata_dataset_id_env = "COACC_DATASET_UNGRD_DAMNIFICADOS"


PIPELINE_CLASS = UngrdEmergenciasPipeline
