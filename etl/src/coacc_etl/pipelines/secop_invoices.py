from __future__ import annotations

# ruff: noqa: E501
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    ContractSummaryLookup,
    merge_limited_unique,
    summary_map_csv_path,
)
from coacc_etl.pipelines.colombia_shared import (
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


def _is_paid(*, status: object, paid_flag: object) -> bool:
    parsed = parse_flag(paid_flag)
    if parsed is not None:
        return parsed
    return "pagad" in clean_text(status).lower()


class SecopInvoicesPipeline(Pipeline):
    """Aggregate SECOP II invoice rows back onto contract summaries."""

    name = "secop_invoices"
    source_id = "secop_invoices"

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
        self.procurements: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_invoices" / "secop_invoices.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _aggregate_frame(
        self,
        frame: pd.DataFrame,
        summary_lookup: dict[str, str],
    ) -> list[dict[str, Any]]:
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_contrato"))
            if not contract_id:
                continue
            summary_id = summary_lookup.get(contract_id)
            if not summary_id:
                continue

            invoice_date = parse_iso_date(row.get("fecha_factura"))
            delivery_date = parse_iso_date(row.get("fecha_de_entrega"))
            payment_estimate = parse_iso_date(row.get("fecha_estiamda_de_pago"))
            invoice_status = clean_text(row.get("estado"))
            invoice_number = clean_text(row.get("numero_de_factura"))
            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "invoice_count": 0,
                    "paid_invoice_count": 0,
                    "invoice_total_value": 0.0,
                    "invoice_net_total": 0.0,
                    "invoice_amount_due_total": 0.0,
                    "latest_invoice_date": None,
                    "latest_delivery_date": None,
                    "latest_payment_estimate": None,
                    "latest_invoice_status": "",
                    "last_invoice_number": "",
                    "source": "secop_invoices",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            current["invoice_count"] += 1
            current["paid_invoice_count"] += int(
                _is_paid(status=invoice_status, paid_flag=row.get("pago_confirmado"))
            )
            current["invoice_total_value"] += parse_amount(row.get("valor_total")) or 0.0
            current["invoice_net_total"] += parse_amount(row.get("valor_neto")) or 0.0
            current["invoice_amount_due_total"] += parse_amount(row.get("valor_a_pagar")) or 0.0

            if invoice_date and (
                current["latest_invoice_date"] is None
                or invoice_date >= current["latest_invoice_date"]
            ):
                current["latest_invoice_date"] = invoice_date
                if invoice_status:
                    current["latest_invoice_status"] = invoice_status
                if invoice_number:
                    current["last_invoice_number"] = invoice_number
            else:
                if invoice_status and not current["latest_invoice_status"]:
                    current["latest_invoice_status"] = invoice_status
                if invoice_number and not current["last_invoice_number"]:
                    current["last_invoice_number"] = invoice_number

            if delivery_date:
                current["latest_delivery_date"] = max(
                    current["latest_delivery_date"] or delivery_date,
                    delivery_date,
                )
            if payment_estimate:
                current["latest_payment_estimate"] = max(
                    current["latest_payment_estimate"] or payment_estimate,
                    payment_estimate,
                )

            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                contract_id,
                row.get("id_pago"),
                invoice_number,
                row.get("radicado"),
            )

        return deduplicate_rows(list(procurement_map.values()), ["summary_id"])

    def transform(self) -> None:
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        try:
            contract_ids = [
                clean_text(value)
                for value in self._raw.get("id_contrato", pd.Series(dtype=str)).tolist()
            ]
            self.procurements = self._aggregate_frame(self._raw, lookup.lookup_many(contract_ids))
        except FileNotFoundError:
            logger.warning("[%s] contract summary map not found; skipping", self.name)
            self.procurements = []
        finally:
            lookup.close()

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row, r.latest_invoice_date AS prev_latest_date, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.invoice_count = coalesce(r.invoice_count, 0) + row.invoice_count,
                r.paid_invoice_count = coalesce(r.paid_invoice_count, 0) + row.paid_invoice_count,
                r.invoice_total_value =
                    coalesce(r.invoice_total_value, 0.0) + row.invoice_total_value,
                r.invoice_net_total =
                    coalesce(r.invoice_net_total, 0.0) + row.invoice_net_total,
                r.invoice_amount_due_total =
                    coalesce(r.invoice_amount_due_total, 0.0) + row.invoice_amount_due_total,
                r.latest_invoice_date = CASE
                    WHEN prev_latest_date IS NULL
                      OR (
                        row.latest_invoice_date IS NOT NULL
                        AND row.latest_invoice_date >= prev_latest_date
                      )
                    THEN row.latest_invoice_date
                    ELSE prev_latest_date
                END,
                r.latest_delivery_date = CASE
                    WHEN r.latest_delivery_date IS NULL
                      OR (
                        row.latest_delivery_date IS NOT NULL
                        AND row.latest_delivery_date > r.latest_delivery_date
                      )
                    THEN row.latest_delivery_date
                    ELSE r.latest_delivery_date
                END,
                r.latest_payment_estimate = CASE
                    WHEN r.latest_payment_estimate IS NULL
                      OR (
                        row.latest_payment_estimate IS NOT NULL
                        AND row.latest_payment_estimate > r.latest_payment_estimate
                      )
                    THEN row.latest_payment_estimate
                    ELSE r.latest_payment_estimate
                END,
                r.latest_invoice_status = CASE
                    WHEN row.latest_invoice_status <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_invoice_date IS NULL
                        OR row.latest_invoice_date >= prev_latest_date
                      )
                    THEN row.latest_invoice_status
                    ELSE coalesce(r.latest_invoice_status, row.latest_invoice_status)
                END,
                r.last_invoice_number = CASE
                    WHEN row.last_invoice_number <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_invoice_date IS NULL
                        OR row.latest_invoice_date >= prev_latest_date
                      )
                    THEN row.last_invoice_number
                    ELSE coalesce(r.last_invoice_number, row.last_invoice_number)
                END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
        """

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = loader.run_query(self._load_query(), self.procurements)

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
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = self._load_query()

        try:
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                contract_ids = [
                    clean_text(value)
                    for value in chunk.get("id_contrato", pd.Series(dtype=str)).tolist()
                ]
                procurements = self._aggregate_frame(chunk, lookup.lookup_many(contract_ids))
                if procurements:
                    loaded += loader.run_query(query, procurements)

                processed += len(chunk)
                self.rows_in = processed
                self.rows_loaded = loaded
                if processed >= next_log_at:
                    logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                    next_log_at += self.chunk_size * 10
        finally:
            lookup.close()
