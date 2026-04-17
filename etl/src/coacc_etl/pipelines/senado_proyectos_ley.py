from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class SenadoProyectosLeyPipeline(LakeCsvPipeline):
    name = "senado_proyectos_ley"
    source_id = "senado_proyectos_ley"
    socrata_dataset_id_env = "COACC_DATASET_SENADO_PROYECTOS_LEY"


PIPELINE_CLASS = SenadoProyectosLeyPipeline
