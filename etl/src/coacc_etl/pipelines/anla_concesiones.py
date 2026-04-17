from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class AnlaConcesionesPipeline(LakeCsvPipeline):
    name = "anla_concesiones"
    source_id = "anla_licencias"
    socrata_dataset_id_env = "COACC_DATASET_ANLA_LICENCIAS"


PIPELINE_CLASS = AnlaConcesionesPipeline
