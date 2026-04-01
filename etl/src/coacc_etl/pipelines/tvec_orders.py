from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    build_company_row,
    make_company_document_id,
    merge_company,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    parse_iso_date,
    read_csv_normalized_with_fallback,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _first_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


class TvecOrdersPipeline(Pipeline):
    """Load Tienda Virtual del Estado Colombiano orders as first-class order nodes."""

    name = "tvec_orders"
    source_id = "tvec_orders_consolidated"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: pd.DataFrame = pd.DataFrame()
        self.companies: list[dict[str, Any]] = []
        self.orders: list[dict[str, Any]] = []
        self.buyer_rels: list[dict[str, Any]] = []
        self.supplier_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "tvec_orders" / "tvec_orders.csv"
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized_with_fallback(
            str(csv_path),
            dtype=str,
            keep_default_na=False,
        )
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        orders: list[dict[str, Any]] = []
        buyer_rels: list[dict[str, Any]] = []
        supplier_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            order_id = _first_value(row, "identificador_de_la_orden", "solicitud")
            buyer_name = clean_name(row.get("entidad"))
            supplier_name = clean_name(row.get("proveedor"))
            if not order_id or not buyer_name or not supplier_name:
                continue

            buyer_document_id = make_company_document_id(
                _first_value(row, "nit_entidad"),
                buyer_name,
                kind="tvec-buyer",
            )
            supplier_document_id = make_company_document_id(
                _first_value(row, "nit_proveedor"),
                supplier_name,
                kind="tvec-supplier",
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=buyer_document_id,
                    name=buyer_name,
                    source=self.source_id,
                    entity_type="PUBLIC_BUYER",
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=supplier_document_id,
                    name=supplier_name,
                    source=self.source_id,
                    actividad_economica=_first_value(row, "actividad_economica_proveedor"),
                ),
            )

            date = parse_iso_date(row.get("fecha"))
            orders.append(
                {
                    "order_id": order_id,
                    "title": f"Orden TVEC {order_id}",
                    "name": f"Orden TVEC {order_id}",
                    "year": _first_value(row, "a_o", "ano"),
                    "date": date,
                    "valid_until": parse_iso_date(row.get("fecha_vence")),
                    "status": clean_text(row.get("estado")),
                    "entity_name": buyer_name,
                    "buyer_entity_name": buyer_name,
                    "supplier_name": supplier_name,
                    "buyer_document_id": buyer_document_id,
                    "supplier_document_id": supplier_document_id,
                    "aggregation": clean_text(row.get("agregacion")),
                    "request_id": clean_text(row.get("solicitud")),
                    "sector": clean_text(row.get("sector_de_la_entidad")),
                    "branch": clean_text(row.get("rama_de_la_entidad")),
                    "entity_order": clean_text(row.get("orden_de_la_entidad")),
                    "city": clean_text(row.get("ciudad")),
                    "items": clean_text(row.get("items")),
                    "total": parse_amount(row.get("total")),
                    "search_text": " ".join(
                        value
                        for value in (
                            order_id,
                            buyer_name,
                            supplier_name,
                            clean_text(row.get("agregacion")),
                            clean_text(row.get("sector_de_la_entidad")),
                        )
                        if value
                    ),
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            buyer_rels.append(
                {"source_key": buyer_document_id, "target_key": order_id, "source": self.source_id}
            )
            supplier_rels.append(
                {
                    "source_key": supplier_document_id,
                    "target_key": order_id,
                    "source": self.source_id,
                }
            )

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.orders = deduplicate_rows(orders, ["order_id"])
        self.buyer_rels = deduplicate_rows(buyer_rels, ["source_key", "target_key"])
        self.supplier_rels = deduplicate_rows(supplier_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.orders:
            loaded += loader.load_nodes("TVECOrder", self.orders, key_field="order_id")
        if self.buyer_rels:
            loaded += loader.load_relationships(
                rel_type="ORDENO_TVEC",
                rows=self.buyer_rels,
                source_label="Company",
                source_key="document_id",
                target_label="TVECOrder",
                target_key="order_id",
                properties=["source"],
            )
        if self.supplier_rels:
            loaded += loader.load_relationships(
                rel_type="PROVEYO_TVEC",
                rows=self.supplier_rels,
                source_label="Company",
                source_key="document_id",
                target_label="TVECOrder",
                target_key="order_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
