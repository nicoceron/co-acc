from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DanePobrezaMonetariaPipeline(LakeCsvPipeline):
    name = "dane_pobreza_monetaria"
    source_id = "dane_pobreza_monetaria"
    socrata_dataset_id_env = "COACC_DATASET_DANE_POBREZA_MONETARIA"


PIPELINE_CLASS = DanePobrezaMonetariaPipeline
