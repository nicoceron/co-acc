from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DapreConsejosConsultivosPipeline(LakeCsvPipeline):
    name = "dapre_consejos_consultivos"
    source_id = "dapre_consejos_consultivos"
    socrata_dataset_id_env = "COACC_DATASET_DAPRE_CONSEJOS_CONSULTIVOS"


PIPELINE_CLASS = DapreConsejosConsultivosPipeline
