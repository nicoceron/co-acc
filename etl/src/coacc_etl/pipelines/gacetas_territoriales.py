from __future__ import annotations

from coacc_etl.pipelines.territorial_gazettes import TerritorialGazettesPipeline


class GacetasTerritorialesPipeline(TerritorialGazettesPipeline):
    name = "gacetas_territoriales"
    source_id = "gacetas_territoriales"
