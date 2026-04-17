from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class IgacCatastroAlfanumericoPipeline(LakeCsvPipeline):
    name = "igac_catastro_alfanumerico"
    source_id = "igac_parcelas"
    socrata_dataset_id_env = "COACC_DATASET_IGAC_PARCELAS"


PIPELINE_CLASS = IgacCatastroAlfanumericoPipeline
