from __future__ import annotations

from coacc_etl.pipelines.administrative_acts import AdministrativeActsPipeline


class ActosAdministrativosPipeline(AdministrativeActsPipeline):
    name = "actos_administrativos"
    source_id = "actos_administrativos"
