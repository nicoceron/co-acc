from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class DaneIpmMunicipalPipeline(LakeCsvPipeline):
    name = "dane_ipm_municipal"
    source_id = "dane_ipm"
    socrata_dataset_id_env = "COACC_DATASET_DANE_IPM"


PIPELINE_CLASS = DaneIpmMunicipalPipeline
