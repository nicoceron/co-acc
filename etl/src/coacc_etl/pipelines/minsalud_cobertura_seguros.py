from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class MinsaludCoberturaSegurosPipeline(LakeCsvPipeline):
    name = "minsalud_cobertura_seguros"
    source_id = "minsalud_cobertura_seguros"
    socrata_dataset_id_env = "COACC_DATASET_MINSALUD_COBERTURA_SEGUROS"


PIPELINE_CLASS = MinsaludCoberturaSegurosPipeline
