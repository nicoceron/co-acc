from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class SecretariaTransparenciaAipPipeline(LakeCsvPipeline):
    name = "secretaria_transparencia_aip"
    source_id = "secretaria_transparencia_aip"
    socrata_dataset_id_env = "COACC_DATASET_SECRETARIA_TRANSPARENCIA_AIP"


PIPELINE_CLASS = SecretariaTransparenciaAipPipeline
