from __future__ import annotations

from coacc_etl.pipelines.judicial_cases import JudicialCasesPipeline


class JudicialProvidenciasPipeline(JudicialCasesPipeline):
    name = "judicial_providencias"
    source_id = "judicial_providencias"
