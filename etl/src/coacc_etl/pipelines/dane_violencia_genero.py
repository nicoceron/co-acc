from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DaneViolenciaGeneroPipeline(LakeCsvPipeline):
    name = "dane_violencia_genero"
    source_id = "dane_violencia_genero"
    socrata_dataset_id_env = "COACC_DATASET_DANE_VIOLENCIA_GENERO"


PIPELINE_CLASS = DaneViolenciaGeneroPipeline
