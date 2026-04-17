from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class CamaraRegistroCabilderosPipeline(LakeCsvPipeline):
    name = "camara_registro_cabilderos"
    source_id = "camara_registro_cabilderos"
    socrata_dataset_id_env = "COACC_DATASET_CAMARA_REGISTRO_CABILDEROS"


PIPELINE_CLASS = CamaraRegistroCabilderosPipeline
