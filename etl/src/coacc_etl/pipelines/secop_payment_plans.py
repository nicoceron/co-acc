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
    clean_name,
    clean_text,
    parse_amount,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _event_date(row: dict[str, str]) -> str | None:
    return (
        parse_iso_date(row.get("fecha_real_de_pago"))
        or parse_iso_date(row.get("fecha_estimada_de_pago"))
        or parse_iso_date(row.get("fecha_de_recepcion"))
        or parse_iso_date(row.get("fecha_de_emision"))
    )


class SecopPaymentPlansPipeline(Pipeline):
    """Aggregate SECOP II payment-plan rows back onto contract summaries."""

    name = "secop_payment_plans"
    source_id = "secop_payment_plans"

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
        self.people: list[dict[str, Any]] = []
        self.supervisor_rels: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_payment_plans" / "secop_payment_plans.csv"

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
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        procurement_map: dict[str, dict[str, Any]] = {}
        person_map: dict[str, dict[str, Any]] = {}
        supervisor_rels: list[dict[str, Any]] = []

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_del_contrato"))
            if not contract_id:
                continue
            summary_id = summary_lookup.get(contract_id)
            if not summary_id:
                continue

            estimated_payment_date = parse_iso_date(row.get("fecha_estimada_de_pago"))
            actual_payment_date = parse_iso_date(row.get("fecha_real_de_pago"))
            event_date = _event_date(row)
            invoice_number = clean_text(row.get("numero_de_factura"))
            payment_status = clean_text(row.get("estado"))
            value_to_pay = (
                parse_amount(row.get("valor_a_pagar"))
                or parse_amount(row.get("valor_total"))
                or parse_amount(row.get("valor_total_de_la_factura"))
                or 0.0
            )
            supervisor_document = strip_document(clean_text(row.get("documento_supervisor")))
            supervisor_name = clean_name(row.get("nombre_supervisor"))
            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "payment_plan_count": 0,
                    "payment_actual_count": 0,
                    "payment_pending_count": 0,
                    "payment_delay_count": 0,
                    "payment_total_value": 0.0,
                    "payment_actual_total": 0.0,
                    "latest_payment_event_date": None,
                    "latest_payment_estimate": None,
                    "latest_payment_actual_date": None,
                    "latest_payment_status": "",
                    "latest_payment_approver": "",
                    "latest_payment_invoice_number": "",
                    "latest_payment_supervisor_document": "",
                    "latest_payment_supervisor_name": "",
                    "latest_payment_supervisor_type": "",
                    "latest_payment_radicado": "",
                    "latest_payment_cufe": "",
                    "source": "secop_payment_plans",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            current["payment_plan_count"] += 1
            current["payment_actual_count"] += int(actual_payment_date is not None)
            current["payment_pending_count"] += int(actual_payment_date is None)
            current["payment_delay_count"] += int(
                estimated_payment_date is not None
                and actual_payment_date is not None
                and actual_payment_date > estimated_payment_date
            )
            current["payment_total_value"] += value_to_pay
            if actual_payment_date is not None:
                current["payment_actual_total"] += value_to_pay

            if event_date and (
                current["latest_payment_event_date"] is None
                or event_date >= current["latest_payment_event_date"]
            ):
                current["latest_payment_event_date"] = event_date
                current["latest_payment_estimate"] = estimated_payment_date
                current["latest_payment_actual_date"] = actual_payment_date
                current["latest_payment_status"] = payment_status
                current["latest_payment_approver"] = clean_text(row.get("aprobado_por"))
                current["latest_payment_invoice_number"] = invoice_number
                current["latest_payment_supervisor_document"] = supervisor_document
                current["latest_payment_supervisor_name"] = supervisor_name
                current["latest_payment_supervisor_type"] = clean_text(
                    row.get("tipo_documento_supervisor")
                )
                current["latest_payment_radicado"] = clean_text(row.get("n_mero_de_radicaci_n"))
                current["latest_payment_cufe"] = clean_text(row.get("cufe"))
            else:
                if payment_status and not current["latest_payment_status"]:
                    current["latest_payment_status"] = payment_status
                if invoice_number and not current["latest_payment_invoice_number"]:
                    current["latest_payment_invoice_number"] = invoice_number
                if supervisor_document and not current["latest_payment_supervisor_document"]:
                    current["latest_payment_supervisor_document"] = supervisor_document
                if supervisor_name and not current["latest_payment_supervisor_name"]:
                    current["latest_payment_supervisor_name"] = supervisor_name

            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                contract_id,
                row.get("id_de_pago"),
                invoice_number,
                row.get("n_mero_de_radicaci_n"),
                row.get("cufe"),
            )

            if supervisor_document and supervisor_name:
                person_map[supervisor_document] = {
                    "document_id": supervisor_document,
                    "cedula": supervisor_document,
                    "name": supervisor_name,
                    "document_type": clean_text(row.get("tipo_documento_supervisor")),
                    "source": "secop_payment_plans",
                    "country": "CO",
                }
                supervisor_rels.append({
                    "source_key": supervisor_document,
                    "summary_id": summary_id,
                    "source": "secop_payment_plans",
                    "role": "PAYMENT_SUPERVISOR",
                })

        return (
            deduplicate_rows(list(procurement_map.values()), ["summary_id"]),
            deduplicate_rows(list(person_map.values()), ["document_id"]),
            deduplicate_rows(supervisor_rels, ["source_key", "summary_id"]),
        )

    def transform(self) -> None:
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        try:
            contract_ids = [
                clean_text(value)
                for value in self._raw.get("id_del_contrato", pd.Series(dtype=str)).tolist()
            ]
            (
                self.procurements,
                self.people,
                self.supervisor_rels,
            ) = self._aggregate_frame(self._raw, lookup.lookup_many(contract_ids))
        except FileNotFoundError:
            logger.warning("[%s] contract summary map not found; skipping", self.name)
            self.procurements = []
            self.people = []
            self.supervisor_rels = []
        finally:
            lookup.close()

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row,
                 coalesce(r.latest_payment_event_date, r.latest_payment_actual_date, r.latest_payment_estimate) AS prev_event_date,
                 coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.payment_plan_count = coalesce(r.payment_plan_count, 0) + row.payment_plan_count,
                r.payment_actual_count = coalesce(r.payment_actual_count, 0) + row.payment_actual_count,
                r.payment_pending_count = coalesce(r.payment_pending_count, 0) + row.payment_pending_count,
                r.payment_delay_count = coalesce(r.payment_delay_count, 0) + row.payment_delay_count,
                r.payment_total_value = coalesce(r.payment_total_value, 0.0) + row.payment_total_value,
                r.payment_actual_total = coalesce(r.payment_actual_total, 0.0) + row.payment_actual_total,
                r.latest_payment_event_date = CASE
                    WHEN prev_event_date IS NULL
                      OR (
                        row.latest_payment_event_date IS NOT NULL
                        AND row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_event_date
                    ELSE prev_event_date
                END,
                r.latest_payment_estimate = CASE
                    WHEN prev_event_date IS NULL
                      OR (
                        row.latest_payment_event_date IS NOT NULL
                        AND row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_estimate
                    ELSE r.latest_payment_estimate
                END,
                r.latest_payment_actual_date = CASE
                    WHEN prev_event_date IS NULL
                      OR (
                        row.latest_payment_event_date IS NOT NULL
                        AND row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_actual_date
                    ELSE r.latest_payment_actual_date
                END,
                r.latest_payment_status = CASE
                    WHEN row.latest_payment_status <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_status
                    ELSE coalesce(r.latest_payment_status, row.latest_payment_status)
                END,
                r.latest_payment_approver = CASE
                    WHEN row.latest_payment_approver <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_approver
                    ELSE coalesce(r.latest_payment_approver, row.latest_payment_approver)
                END,
                r.latest_payment_invoice_number = CASE
                    WHEN row.latest_payment_invoice_number <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_invoice_number
                    ELSE coalesce(
                        r.latest_payment_invoice_number,
                        row.latest_payment_invoice_number
                    )
                END,
                r.latest_payment_supervisor_document = CASE
                    WHEN row.latest_payment_supervisor_document <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_supervisor_document
                    ELSE coalesce(
                        r.latest_payment_supervisor_document,
                        row.latest_payment_supervisor_document
                    )
                END,
                r.latest_payment_supervisor_name = CASE
                    WHEN row.latest_payment_supervisor_name <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_supervisor_name
                    ELSE coalesce(
                        r.latest_payment_supervisor_name,
                        row.latest_payment_supervisor_name
                    )
                END,
                r.latest_payment_supervisor_type = CASE
                    WHEN row.latest_payment_supervisor_type <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_supervisor_type
                    ELSE coalesce(
                        r.latest_payment_supervisor_type,
                        row.latest_payment_supervisor_type
                    )
                END,
                r.latest_payment_radicado = CASE
                    WHEN row.latest_payment_radicado <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_radicado
                    ELSE coalesce(r.latest_payment_radicado, row.latest_payment_radicado)
                END,
                r.latest_payment_cufe = CASE
                    WHEN row.latest_payment_cufe <> ''
                      AND (
                        prev_event_date IS NULL
                        OR row.latest_payment_event_date IS NULL
                        OR row.latest_payment_event_date >= prev_event_date
                      )
                    THEN row.latest_payment_cufe
                    ELSE coalesce(r.latest_payment_cufe, row.latest_payment_cufe)
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
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.procurements:
            loaded += loader.run_query(self._load_query(), self.procurements)
        if self.supervisor_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH ()-[award:CONTRATOU {summary_id: row.summary_id}]->(c:Company) "
                "MERGE (p)-[r:SUPERVISA_PAGO {summary_id: row.summary_id}]->(c) "
                "SET r.source = row.source, "
                "    r.role = row.role"
            )
            loaded += loader.run_query(query, self.supervisor_rels)
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
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = self._load_query()
        supervisor_query = (
            "UNWIND $rows AS row "
            "MATCH (p:Person {document_id: row.source_key}) "
            "MATCH ()-[award:CONTRATOU {summary_id: row.summary_id}]->(c:Company) "
            "MERGE (p)-[r:SUPERVISA_PAGO {summary_id: row.summary_id}]->(c) "
            "SET r.source = row.source, "
            "    r.role = row.role"
        )

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
                    for value in chunk.get("id_del_contrato", pd.Series(dtype=str)).tolist()
                ]
                procurements, people, supervisor_rels = self._aggregate_frame(
                    chunk,
                    lookup.lookup_many(contract_ids),
                )
                if people:
                    loaded += loader.load_nodes("Person", people, key_field="document_id")
                if procurements:
                    loaded += loader.run_query(query, procurements)
                if supervisor_rels:
                    loaded += loader.run_query(supervisor_query, supervisor_rels)

                processed += len(chunk)
                self.rows_in = processed
                self.rows_loaded = loaded
                if processed >= next_log_at:
                    logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                    next_log_at += self.chunk_size * 10
        finally:
            lookup.close()
