from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class PnisBeneficiariosPipeline(LakeCsvPipeline):
    name = "pnis_beneficiarios"
    source_id = "pnis_beneficiarios"
    socrata_dataset_id_env = "COACC_DATASET_PNIS_BENEFICIARIOS"


PIPELINE_CLASS = PnisBeneficiariosPipeline
