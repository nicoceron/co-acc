from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DaneMicronegociosPipeline(LakeCsvPipeline):
    name = "dane_micronegocios"
    source_id = "dane_micronegocios"
    socrata_dataset_id_env = "COACC_DATASET_DANE_MICRONEGOCIOS"


PIPELINE_CLASS = DaneMicronegociosPipeline
