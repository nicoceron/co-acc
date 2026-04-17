from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class UpmeEnergiaGeovisorPipeline(LakeCsvPipeline):
    name = "upme_energia_geovisor"
    source_id = "upme_energia_geovisor"
    socrata_dataset_id_env = "COACC_DATASET_UPME_ENERGIA_GEOVISOR"


PIPELINE_CLASS = UpmeEnergiaGeovisorPipeline
