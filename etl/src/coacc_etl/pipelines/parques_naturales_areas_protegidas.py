from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class ParquesNaturalesAreasProtegidasPipeline(LakeCsvPipeline):
    name = "parques_naturales_areas_protegidas"
    source_id = "parques_naturales_areas_protegidas"
    socrata_dataset_id_env = "COACC_DATASET_PARQUES_NATURALES_AREAS_PROTEGIDAS"


PIPELINE_CLASS = ParquesNaturalesAreasProtegidasPipeline
