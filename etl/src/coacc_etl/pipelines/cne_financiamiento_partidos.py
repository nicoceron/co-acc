from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class CneFinanciamientoPartidosPipeline(LakeCsvPipeline):
    name = "cne_financiamiento_partidos"
    source_id = "cne_financiamiento_partidos"
    socrata_dataset_id_env = "COACC_DATASET_CNE_FINANCIAMIENTO_PARTIDOS"


PIPELINE_CLASS = CneFinanciamientoPartidosPipeline
