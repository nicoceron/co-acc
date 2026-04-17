from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class SnrPropiedadTierrasPipeline(LakeCsvPipeline):
    name = "snr_propiedad_tierras"
    source_id = "snr_propiedad_tierras"
    socrata_dataset_id_env = "COACC_DATASET_SNR_PROPIEDAD_TIERRAS"


PIPELINE_CLASS = SnrPropiedadTierrasPipeline
