from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class ServiciosPostalesDireccionesPipeline(LakeCsvPipeline):
    name = "servicios_postales_direcciones"
    source_id = "servicios_postales_direcciones"
    socrata_dataset_id_env = "COACC_DATASET_SERVICIOS_POSTALES_DIRECCIONES"


PIPELINE_CLASS = ServiciosPostalesDireccionesPipeline
