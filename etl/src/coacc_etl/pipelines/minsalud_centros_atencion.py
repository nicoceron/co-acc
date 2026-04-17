from __future__ import annotations

from coacc_etl.pipelines.lake_template import LakeCsvPipeline


class MinsaludCentrosAtencionPipeline(LakeCsvPipeline):
    name = "minsalud_centros_atencion"
    source_id = "minsalud_centros_atencion"
    socrata_dataset_id_env = "COACC_DATASET_MINSALUD_CENTROS_ATENCION"


PIPELINE_CLASS = MinsaludCentrosAtencionPipeline
