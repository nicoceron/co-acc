from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class ServicioGeologicoMineralesPipeline(LakeCsvPipeline):
    name = "servicio_geologico_minerales"
    source_id = "servicio_geologico_minerales"
    socrata_dataset_id_env = "COACC_DATASET_SERVICIO_GEOLOGICO_MINERALES"


PIPELINE_CLASS = ServicioGeologicoMineralesPipeline
