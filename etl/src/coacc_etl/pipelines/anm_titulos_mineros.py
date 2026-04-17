from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class AnmTitulosMinerosPipeline(LakeCsvPipeline):
    name = "anm_titulos_mineros"
    source_id = "anm_titulos"
    socrata_dataset_id_env = "COACC_DATASET_ANM_TITULOS"


PIPELINE_CLASS = AnmTitulosMinerosPipeline
