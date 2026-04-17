from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class PdetMunicipiosPipeline(LakeCsvPipeline):
    name = "pdet_municipios"
    source_id = "pdet_municipios"
    socrata_dataset_id_env = "COACC_DATASET_PDET_MUNICIPIOS"


PIPELINE_CLASS = PdetMunicipiosPipeline
