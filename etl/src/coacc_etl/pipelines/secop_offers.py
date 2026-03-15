from __future__ import annotations

# ruff: noqa: E501
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
    merge_limited_unique,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopOffersPipeline(Pipeline):
    """Load SECOP II offers as bid nodes plus buyer and bidder participation edges."""

    name = "secop_offers"
    source_id = "secop_offers"

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
        self.bids: list[dict[str, Any]] = []
        self.buyer_bid_rels: list[dict[str, Any]] = []
        self.supplier_bid_rels: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_offers" / "secop_offers.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _transform_frame(
        self,
        frame: pd.DataFrame,
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        company_map: dict[str, dict[str, Any]] = {}
        bid_map: dict[str, dict[str, Any]] = {}
        buyer_bid_map: dict[str, dict[str, Any]] = {}
        supplier_bid_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            bid_id = clean_text(row.get("id_del_proceso_de_compra"))
            if not bid_id:
                continue

            buyer_name = clean_name(row.get("entidad_compradora"))
            buyer_document = make_company_document_id(
                row.get("nit_entidad_compradora"),
                buyer_name,
                kind="buyer",
            )
            if not buyer_document or not buyer_name:
                continue

            supplier_name = clean_name(row.get("nombre_proveedor"))
            supplier_document = make_company_document_id(
                row.get("nit_del_proveedor"),
                supplier_name,
                kind="supplier",
            )
            entity_code = clean_text(row.get("c_digo_entidad"))
            provider_code = clean_text(row.get("c_digo_proveedor"))
            currency = clean_text(row.get("moneda"))
            modality = clean_text(row.get("modalidad"))
            offer_date = parse_iso_date(row.get("fecha_de_registro"))
            offer_id = clean_text(row.get("identificador_de_la_oferta"))
            offer_reference = clean_text(row.get("referencia_de_la_oferta"))
            offer_value = parse_amount(row.get("valor_de_la_oferta")) or 0.0
            direct_invitation = parse_flag(row.get("invitacion_directa"))
            process_reference = clean_text(row.get("referencia_del_proceso"))
            procedure_description = clean_text(row.get("descripcion_del_procedimiento"))

            merge_company(
                company_map,
                build_company_row(
                    document_id=buyer_document,
                    name=buyer_name,
                    source="secop_offers",
                    entity_code=entity_code,
                ),
            )
            if supplier_document and supplier_name:
                merge_company(
                    company_map,
                    build_company_row(
                        document_id=supplier_document,
                        name=supplier_name,
                        source="secop_offers",
                        supplier_registry_code=provider_code,
                    ),
                )

            bid = bid_map.setdefault(
                bid_id,
                {
                    "bid_id": bid_id,
                    "name": procedure_description or process_reference or bid_id,
                    "reference": process_reference,
                    "procedure_description": procedure_description,
                    "modality": modality,
                    "currency": currency,
                    "buyer_document_id": buyer_document,
                    "buyer_name": buyer_name,
                    "entity_code": entity_code,
                    "direct_invitation": bool(direct_invitation) if direct_invitation is not None else False,
                    "offer_count": 0,
                    "total_offer_value": 0.0,
                    "average_offer_value": None,
                    "first_offer_date": offer_date,
                    "last_offer_date": offer_date,
                    "source": "secop_offers",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            bid["offer_count"] += 1
            bid["total_offer_value"] += offer_value
            bid["average_offer_value"] = bid["total_offer_value"] / float(bid["offer_count"])
            if offer_date and (not bid.get("first_offer_date") or offer_date < bid["first_offer_date"]):
                bid["first_offer_date"] = offer_date
            if offer_date and (not bid.get("last_offer_date") or offer_date > bid["last_offer_date"]):
                bid["last_offer_date"] = offer_date
            if procedure_description and not bid.get("procedure_description"):
                bid["procedure_description"] = procedure_description
                bid["name"] = procedure_description
            if process_reference and not bid.get("reference"):
                bid["reference"] = process_reference
            if modality and not bid.get("modality"):
                bid["modality"] = modality
            if currency and not bid.get("currency"):
                bid["currency"] = currency
            if entity_code and not bid.get("entity_code"):
                bid["entity_code"] = entity_code
            if direct_invitation is not None:
                bid["direct_invitation"] = bool(bid.get("direct_invitation")) or direct_invitation
            bid["evidence_refs"] = merge_limited_unique(
                list(bid.get("evidence_refs", [])),
                bid_id,
                offer_id,
                offer_reference,
            )

            buyer_bid_map[f"{buyer_document}|{bid_id}"] = {
                "source_key": buyer_document,
                "target_key": bid_id,
                "source": "secop_offers",
                "country": "CO",
                "buyer_document_id": buyer_document,
                "buyer_name": buyer_name,
                "entity_code": entity_code,
            }

            if supplier_document and supplier_name:
                supplier_bid = supplier_bid_map.setdefault(
                    f"{supplier_document}|{bid_id}",
                    {
                        "source_key": supplier_document,
                        "target_key": bid_id,
                        "source": "secop_offers",
                        "country": "CO",
                        "supplier_document_id": supplier_document,
                        "supplier_name": supplier_name,
                        "provider_code": provider_code,
                        "currency": currency,
                        "offer_count": 0,
                        "offer_value_total": 0.0,
                        "average_offer_value": None,
                        "first_offer_date": offer_date,
                        "last_offer_date": offer_date,
                        "latest_offer_id": offer_id,
                        "latest_offer_reference": offer_reference,
                        "evidence_refs": [],
                    },
                )
                supplier_bid["offer_count"] += 1
                supplier_bid["offer_value_total"] += offer_value
                supplier_bid["average_offer_value"] = (
                    supplier_bid["offer_value_total"] / float(supplier_bid["offer_count"])
                )
                if offer_date and (
                    not supplier_bid.get("first_offer_date")
                    or offer_date < supplier_bid["first_offer_date"]
                ):
                    supplier_bid["first_offer_date"] = offer_date
                if offer_date and (
                    not supplier_bid.get("last_offer_date")
                    or offer_date >= supplier_bid["last_offer_date"]
                ):
                    supplier_bid["last_offer_date"] = offer_date
                    if offer_id:
                        supplier_bid["latest_offer_id"] = offer_id
                    if offer_reference:
                        supplier_bid["latest_offer_reference"] = offer_reference
                if provider_code and not supplier_bid.get("provider_code"):
                    supplier_bid["provider_code"] = provider_code
                if currency and not supplier_bid.get("currency"):
                    supplier_bid["currency"] = currency
                supplier_bid["evidence_refs"] = merge_limited_unique(
                    list(supplier_bid.get("evidence_refs", [])),
                    offer_id,
                    offer_reference,
                    bid_id,
                )

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(bid_map.values()), ["bid_id"]),
            deduplicate_rows(list(buyer_bid_map.values()), ["source_key", "target_key"]),
            deduplicate_rows(list(supplier_bid_map.values()), ["source_key", "target_key"]),
        )

    def transform(self) -> None:
        (
            self.companies,
            self.bids,
            self.buyer_bid_rels,
            self.supplier_bid_rels,
        ) = self._transform_frame(self._raw)

    def _bid_query(self) -> str:
        return """
            UNWIND $rows AS row
            MERGE (b:Bid {bid_id: row.bid_id})
            WITH b, row,
                 coalesce(b.offer_count, 0) AS prev_count,
                 coalesce(b.total_offer_value, 0.0) AS prev_total,
                 b.first_offer_date AS prev_first_date,
                 b.last_offer_date AS prev_last_date,
                 coalesce(b.evidence_refs, []) AS prev_refs
            SET b.source = row.source,
                b.country = row.country,
                b.name = CASE WHEN row.name <> '' THEN row.name ELSE coalesce(b.name, row.name) END,
                b.reference = CASE
                    WHEN row.reference <> '' THEN row.reference
                    ELSE b.reference
                END,
                b.procedure_description = CASE
                    WHEN row.procedure_description <> '' THEN row.procedure_description
                    ELSE b.procedure_description
                END,
                b.modality = CASE
                    WHEN row.modality <> '' THEN row.modality
                    ELSE b.modality
                END,
                b.currency = CASE
                    WHEN row.currency <> '' THEN row.currency
                    ELSE b.currency
                END,
                b.buyer_document_id = CASE
                    WHEN row.buyer_document_id <> '' THEN row.buyer_document_id
                    ELSE b.buyer_document_id
                END,
                b.buyer_name = CASE
                    WHEN row.buyer_name <> '' THEN row.buyer_name
                    ELSE b.buyer_name
                END,
                b.entity_code = CASE
                    WHEN row.entity_code <> '' THEN row.entity_code
                    ELSE b.entity_code
                END,
                b.direct_invitation =
                    coalesce(b.direct_invitation, false)
                    OR coalesce(row.direct_invitation, false),
                b.offer_count = prev_count + row.offer_count,
                b.total_offer_value = prev_total + row.total_offer_value,
                b.average_offer_value = CASE
                    WHEN (prev_count + row.offer_count) > 0
                    THEN (prev_total + row.total_offer_value) / toFloat(prev_count + row.offer_count)
                    ELSE NULL
                END,
                b.first_offer_date = CASE
                    WHEN prev_first_date IS NULL
                      OR (row.first_offer_date IS NOT NULL AND row.first_offer_date < prev_first_date)
                    THEN row.first_offer_date
                    ELSE prev_first_date
                END,
                b.last_offer_date = CASE
                    WHEN prev_last_date IS NULL
                      OR (row.last_offer_date IS NOT NULL AND row.last_offer_date > prev_last_date)
                    THEN row.last_offer_date
                    ELSE prev_last_date
                END,
                b.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
        """

    def _buyer_bid_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH (buyer:Company {document_id: row.source_key})
            MATCH (bid:Bid {bid_id: row.target_key})
            MERGE (buyer)-[r:LICITOU]->(bid)
            SET r.source = row.source,
                r.country = row.country,
                r.buyer_document_id = row.buyer_document_id,
                r.buyer_name = row.buyer_name,
                r.entity_code = CASE
                    WHEN row.entity_code <> '' THEN row.entity_code
                    ELSE r.entity_code
                END
        """

    def _supplier_bid_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH (supplier:Company {document_id: row.source_key})
            MATCH (bid:Bid {bid_id: row.target_key})
            MERGE (supplier)-[r:FORNECEU_LICITACAO]->(bid)
            WITH r, row,
                 coalesce(r.offer_count, 0) AS prev_count,
                 coalesce(r.offer_value_total, 0.0) AS prev_total,
                 r.last_offer_date AS prev_last_date,
                 r.first_offer_date AS prev_first_date,
                 coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.supplier_document_id = row.supplier_document_id,
                r.supplier_name = row.supplier_name,
                r.provider_code = CASE
                    WHEN row.provider_code <> '' THEN row.provider_code
                    ELSE r.provider_code
                END,
                r.currency = CASE
                    WHEN row.currency <> '' THEN row.currency
                    ELSE r.currency
                END,
                r.offer_count = prev_count + row.offer_count,
                r.offer_value_total = prev_total + row.offer_value_total,
                r.average_offer_value = CASE
                    WHEN (prev_count + row.offer_count) > 0
                    THEN (prev_total + row.offer_value_total) / toFloat(prev_count + row.offer_count)
                    ELSE NULL
                END,
                r.first_offer_date = CASE
                    WHEN prev_first_date IS NULL
                      OR (row.first_offer_date IS NOT NULL AND row.first_offer_date < prev_first_date)
                    THEN row.first_offer_date
                    ELSE prev_first_date
                END,
                r.last_offer_date = CASE
                    WHEN prev_last_date IS NULL
                      OR (row.last_offer_date IS NOT NULL AND row.last_offer_date >= prev_last_date)
                    THEN row.last_offer_date
                    ELSE prev_last_date
                END,
                r.latest_offer_id = CASE
                    WHEN row.latest_offer_id <> ''
                      AND (
                        prev_last_date IS NULL
                        OR row.last_offer_date IS NULL
                        OR row.last_offer_date >= prev_last_date
                      )
                    THEN row.latest_offer_id
                    ELSE coalesce(r.latest_offer_id, row.latest_offer_id)
                END,
                r.latest_offer_reference = CASE
                    WHEN row.latest_offer_reference <> ''
                      AND (
                        prev_last_date IS NULL
                        OR row.last_offer_date IS NULL
                        OR row.last_offer_date >= prev_last_date
                      )
                    THEN row.latest_offer_reference
                    ELSE coalesce(r.latest_offer_reference, row.latest_offer_reference)
                END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
        """

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.bids:
            loaded += loader.run_query(self._bid_query(), self.bids)
        if self.buyer_bid_rels:
            loaded += loader.run_query(self._buyer_bid_query(), self.buyer_bid_rels)
        if self.supplier_bid_rels:
            loaded += loader.run_query(self._supplier_bid_query(), self.supplier_bid_rels)

        self.rows_loaded = loaded

    def run_streaming(self, start_phase: int = 1) -> None:
        if start_phase > 1:
            logger.info(
                "[%s] start_phase=%s ignored for single-phase streaming",
                self.name,
                start_phase,
            )

        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        loader = Neo4jBatchLoader(self.driver)
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        bid_query = self._bid_query()
        buyer_bid_query = self._buyer_bid_query()
        supplier_bid_query = self._supplier_bid_query()

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            companies, bids, buyer_bid_rels, supplier_bid_rels = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if bids:
                loaded += loader.run_query(bid_query, bids)
            if buyer_bid_rels:
                loaded += loader.run_query(buyer_bid_query, buyer_bid_rels)
            if supplier_bid_rels:
                loaded += loader.run_query(supplier_bid_query, supplier_bid_rels)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
