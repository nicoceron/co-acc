from __future__ import annotations

from coacc_etl.pipelines.tvec_orders import TvecOrdersPipeline


class TvecOrdersConsolidatedPipeline(TvecOrdersPipeline):
    name = "tvec_orders_consolidated"
    source_id = "tvec_orders_consolidated"
