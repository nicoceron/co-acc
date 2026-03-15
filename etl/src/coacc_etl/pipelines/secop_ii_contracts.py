from __future__ import annotations

# ruff: noqa: E501
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    append_summary_map,
    build_company_row,
    make_company_document_id,
    merge_company,
    merge_limited_unique,
    procurement_relation_id,
    procurement_year,
    reset_summary_map,
    summary_map_csv_path,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    extract_url,
    parse_amount,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopIiContractsPipeline(Pipeline):
    """Load SECOP II contracts and link them back to SECOP II bids."""

    name = "secop_ii_contracts"
    source_id = "secop_ii_contracts"

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
        self.people: list[dict[str, Any]] = []
        self.procurements: list[dict[str, Any]] = []
        self.officer_rels: list[dict[str, Any]] = []
        self.summary_mappings: list[tuple[str, str]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_ii_contracts" / "secop_ii_contracts.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_ii_contracts] file not found: %s", csv_path)
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
        list[tuple[str, str]],
    ]:
        company_map: dict[str, dict[str, Any]] = {}
        person_map: dict[str, dict[str, Any]] = {}
        procurement_map: dict[str, dict[str, Any]] = {}
        officer_rels: list[dict[str, Any]] = []
        summary_mappings: list[tuple[str, str]] = []

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_contrato"))
            if not contract_id:
                continue

            entity_name = clean_name(row.get("nombre_entidad"))
            entity_document = make_company_document_id(
                row.get("nit_entidad"),
                entity_name,
                kind="buyer",
            )
            supplier_name = clean_name(row.get("proveedor_adjudicado"))
            supplier_document = make_company_document_id(
                row.get("documento_proveedor"),
                supplier_name,
                kind="supplier",
            )
            if not entity_document or not entity_name or not supplier_document or not supplier_name:
                continue

            merge_company(
                company_map,
                build_company_row(
                    document_id=entity_document,
                    name=entity_name,
                    source="secop_ii_contracts",
                    entity_order=clean_text(row.get("orden")),
                    sector=clean_text(row.get("sector")),
                    branch=clean_text(row.get("rama")),
                    entity_centralized=clean_text(row.get("entidad_centralizada")),
                    department=clean_text(row.get("departamento")),
                    municipality=clean_text(row.get("ciudad")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=supplier_document,
                    name=supplier_name,
                    source="secop_ii_contracts",
                    supplier_document_type=clean_text(row.get("tipodocproveedor")),
                    is_group=clean_text(row.get("es_grupo")),
                    is_pyme=clean_text(row.get("es_pyme")),
                    supplier_code=clean_text(row.get("codigo_proveedor")),
                ),
            )

            signed_at = parse_iso_date(row.get("fecha_de_firma"))
            year = procurement_year(row.get("fecha_de_firma"))
            summary_id = procurement_relation_id(
                "secop_ii_contracts",
                entity_document,
                supplier_document,
                year,
            )
            summary = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "source_key": entity_document,
                    "target_key": supplier_document,
                    "source": "secop_ii_contracts",
                    "country": "CO",
                    "year": year,
                    "buyer_document_id": entity_document,
                    "buyer_name": entity_name,
                    "supplier_document_id": supplier_document,
                    "supplier_name": supplier_name,
                    "contract_count": 0,
                    "total_value": 0.0,
                    "average_value": None,
                    "value_paid_total": 0.0,
                    "value_invoiced_total": 0.0,
                    "value_pending_payment_total": 0.0,
                    "value_pending_execution_total": 0.0,
                    "first_date": signed_at,
                    "last_date": signed_at,
                    "department": clean_text(row.get("departamento")),
                    "city": clean_text(row.get("ciudad")),
                    "status": clean_text(row.get("estado_contrato")),
                    "category_code": clean_text(row.get("codigo_de_categoria_principal")),
                    "contract_type": clean_text(row.get("tipo_de_contrato")),
                    "modality": clean_text(row.get("modalidad_de_contratacion")),
                    "modality_justification": clean_text(
                        row.get("justificacion_modalidad_de")
                    ),
                    "reference": clean_text(row.get("referencia_del_contrato")),
                    "url": extract_url(row.get("urlproceso")),
                    "evidence_refs": [],
                },
            )
            summary["contract_count"] += 1
            summary["total_value"] += parse_amount(row.get("valor_del_contrato")) or 0.0
            summary["average_value"] = summary["total_value"] / float(summary["contract_count"])
            summary["value_paid_total"] += parse_amount(row.get("valor_pagado")) or 0.0
            summary["value_invoiced_total"] += parse_amount(row.get("valor_facturado")) or 0.0
            summary["value_pending_payment_total"] += (
                parse_amount(row.get("valor_pendiente_de_pago")) or 0.0
            )
            summary["value_pending_execution_total"] += (
                parse_amount(row.get("valor_pendiente_de_ejecucion")) or 0.0
            )
            if signed_at and (not summary.get("first_date") or signed_at < summary["first_date"]):
                summary["first_date"] = signed_at
            if signed_at and (not summary.get("last_date") or signed_at > summary["last_date"]):
                summary["last_date"] = signed_at
            if not summary.get("status"):
                summary["status"] = clean_text(row.get("estado_contrato"))
            if not summary.get("category_code"):
                summary["category_code"] = clean_text(row.get("codigo_de_categoria_principal"))
            if not summary.get("contract_type"):
                summary["contract_type"] = clean_text(row.get("tipo_de_contrato"))
            if not summary.get("modality"):
                summary["modality"] = clean_text(row.get("modalidad_de_contratacion"))
            if not summary.get("modality_justification"):
                summary["modality_justification"] = clean_text(
                    row.get("justificacion_modalidad_de")
                )
            if not summary.get("url"):
                summary["url"] = extract_url(row.get("urlproceso"))
            summary["evidence_refs"] = merge_limited_unique(
                list(summary.get("evidence_refs", [])),
                contract_id,
                clean_text(row.get("proceso_de_compra")),
            )
            summary_mappings.append((contract_id, summary_id))

            representative_document = strip_document(
                clean_text(row.get("identificaci_n_representante_legal"))
            )
            representative_name = clean_name(row.get("nombre_representante_legal"))
            if supplier_document and representative_document and representative_name:
                person_map[representative_document] = {
                    "document_id": representative_document,
                    "cedula": representative_document,
                    "name": representative_name,
                    "document_type": clean_text(
                        row.get("tipo_de_identificaci_n_representante_legal")
                    ),
                    "nationality": clean_text(row.get("nacionalidad_representante_legal")),
                    "source": "secop_ii_contracts",
                    "country": "CO",
                }
                officer_rels.append({
                    "source_key": representative_document,
                    "target_key": supplier_document,
                    "source": "secop_ii_contracts",
                    "role": "LEGAL_REPRESENTATIVE",
                })

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(person_map.values()), ["document_id"]),
            deduplicate_rows(list(procurement_map.values()), ["summary_id"]),
            deduplicate_rows(officer_rels, ["source_key", "target_key"]),
            summary_mappings,
        )

    def transform(self) -> None:
        (
            self.companies,
            self.people,
            self.procurements,
            self.officer_rels,
            self.summary_mappings,
        ) = self._transform_frame(self._raw)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.procurements:
            procurement_query = (
                "UNWIND $rows AS row "
                "MATCH (buyer:Company {document_id: row.source_key}) "
                "MATCH (supplier:Company {document_id: row.target_key}) "
                "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier) "
                "WITH r, row, "
                "     coalesce(r.contract_count, 0) AS prev_count, "
                "     coalesce(r.total_value, 0.0) AS prev_total, "
                "     coalesce(r.value_paid_total, 0.0) AS prev_paid, "
                "     coalesce(r.value_invoiced_total, 0.0) AS prev_invoiced, "
                "     coalesce(r.value_pending_payment_total, 0.0) AS prev_pending_payment, "
                "     coalesce(r.value_pending_execution_total, 0.0) AS prev_pending_execution, "
                "     r.first_date AS prev_first_date, "
                "     r.last_date AS prev_last_date, "
                "     coalesce(r.evidence_refs, []) AS prev_refs "
                "SET r.source = row.source, "
                "    r.country = row.country, "
                "    r.year = coalesce(row.year, r.year), "
                "    r.buyer_document_id = row.buyer_document_id, "
                "    r.buyer_name = row.buyer_name, "
                "    r.supplier_document_id = row.supplier_document_id, "
                "    r.supplier_name = row.supplier_name, "
                "    r.department = coalesce(row.department, r.department), "
                "    r.city = coalesce(row.city, r.city), "
                "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
                "    r.category_code = CASE "
                "      WHEN row.category_code <> '' THEN row.category_code ELSE r.category_code END, "
                "    r.contract_type = CASE "
                "      WHEN row.contract_type <> '' THEN row.contract_type ELSE r.contract_type END, "
                "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
                "    r.modality_justification = CASE "
                "      WHEN row.modality_justification <> '' "
                "      THEN row.modality_justification "
                "      ELSE r.modality_justification "
                "    END, "
                "    r.reference = CASE WHEN row.reference <> '' THEN row.reference ELSE r.reference END, "
                "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
                "    r.contract_count = prev_count + row.contract_count, "
                "    r.total_value = prev_total + row.total_value, "
                "    r.average_value = CASE "
                "      WHEN (prev_count + row.contract_count) > 0 "
                "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.contract_count) "
                "      ELSE NULL "
                "    END, "
                "    r.value_paid_total = prev_paid + row.value_paid_total, "
                "    r.value_invoiced_total = prev_invoiced + row.value_invoiced_total, "
                "    r.value_pending_payment_total = "
                "      prev_pending_payment + row.value_pending_payment_total, "
                "    r.value_pending_execution_total = "
                "      prev_pending_execution + row.value_pending_execution_total, "
                "    r.first_date = CASE "
                "      WHEN prev_first_date IS NULL "
                "        OR (row.first_date IS NOT NULL AND row.first_date < prev_first_date) "
                "      THEN row.first_date ELSE prev_first_date END, "
                "    r.last_date = CASE "
                "      WHEN prev_last_date IS NULL "
                "        OR (row.last_date IS NOT NULL AND row.last_date > prev_last_date) "
                "      THEN row.last_date ELSE prev_last_date END, "
                "    r.evidence_refs = reduce("
                "      acc = [], "
                "      item IN (prev_refs + row.evidence_refs) | "
                "        CASE WHEN item IN acc THEN acc ELSE acc + item END"
                "    )[0..5]"
            )
            loaded += loader.run_query(procurement_query, self.procurements)
        if self.officer_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (c:Company {document_id: row.target_key}) "
                "MERGE (p)-[r:OFFICER_OF]->(c) "
                "SET r.source = row.source, "
                "    r.role = row.role"
            )
            loaded += loader.run_query(query, self.officer_rels)
        mapping_path = summary_map_csv_path(self.data_dir)
        reset_summary_map(mapping_path)
        append_summary_map(mapping_path, self.summary_mappings)

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
        mapping_path = summary_map_csv_path(self.data_dir)
        reset_summary_map(mapping_path)
        officer_query = (
            "UNWIND $rows AS row "
            "MATCH (p:Person {document_id: row.source_key}) "
            "MATCH (c:Company {document_id: row.target_key}) "
            "MERGE (p)-[r:OFFICER_OF]->(c) "
            "SET r.source = row.source, "
            "    r.role = row.role"
        )
        procurement_query = (
            "UNWIND $rows AS row "
            "MATCH (buyer:Company {document_id: row.source_key}) "
            "MATCH (supplier:Company {document_id: row.target_key}) "
            "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier) "
            "WITH r, row, "
            "     coalesce(r.contract_count, 0) AS prev_count, "
            "     coalesce(r.total_value, 0.0) AS prev_total, "
            "     coalesce(r.value_paid_total, 0.0) AS prev_paid, "
            "     coalesce(r.value_invoiced_total, 0.0) AS prev_invoiced, "
            "     coalesce(r.value_pending_payment_total, 0.0) AS prev_pending_payment, "
            "     coalesce(r.value_pending_execution_total, 0.0) AS prev_pending_execution, "
            "     r.first_date AS prev_first_date, "
            "     r.last_date AS prev_last_date, "
            "     coalesce(r.evidence_refs, []) AS prev_refs "
            "SET r.source = row.source, "
            "    r.country = row.country, "
            "    r.year = coalesce(row.year, r.year), "
            "    r.buyer_document_id = row.buyer_document_id, "
            "    r.buyer_name = row.buyer_name, "
            "    r.supplier_document_id = row.supplier_document_id, "
            "    r.supplier_name = row.supplier_name, "
            "    r.department = coalesce(row.department, r.department), "
            "    r.city = coalesce(row.city, r.city), "
            "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
            "    r.category_code = CASE "
            "      WHEN row.category_code <> '' THEN row.category_code ELSE r.category_code END, "
            "    r.contract_type = CASE "
            "      WHEN row.contract_type <> '' THEN row.contract_type ELSE r.contract_type END, "
            "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
            "    r.modality_justification = CASE "
            "      WHEN row.modality_justification <> '' "
            "      THEN row.modality_justification "
            "      ELSE r.modality_justification "
            "    END, "
            "    r.reference = CASE WHEN row.reference <> '' THEN row.reference ELSE r.reference END, "
            "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
            "    r.contract_count = prev_count + row.contract_count, "
            "    r.total_value = prev_total + row.total_value, "
            "    r.average_value = CASE "
            "      WHEN (prev_count + row.contract_count) > 0 "
            "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.contract_count) "
            "      ELSE NULL "
            "    END, "
            "    r.value_paid_total = prev_paid + row.value_paid_total, "
            "    r.value_invoiced_total = prev_invoiced + row.value_invoiced_total, "
            "    r.value_pending_payment_total = "
            "      prev_pending_payment + row.value_pending_payment_total, "
            "    r.value_pending_execution_total = "
            "      prev_pending_execution + row.value_pending_execution_total, "
            "    r.first_date = CASE "
            "      WHEN prev_first_date IS NULL "
            "        OR (row.first_date IS NOT NULL AND row.first_date < prev_first_date) "
            "      THEN row.first_date ELSE prev_first_date END, "
            "    r.last_date = CASE "
            "      WHEN prev_last_date IS NULL "
            "        OR (row.last_date IS NOT NULL AND row.last_date > prev_last_date) "
            "      THEN row.last_date ELSE prev_last_date END, "
            "    r.evidence_refs = reduce("
            "      acc = [], "
            "      item IN (prev_refs + row.evidence_refs) | "
            "        CASE WHEN item IN acc THEN acc ELSE acc + item END"
            "    )[0..5]"
        )

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            (
                companies,
                people,
                procurements,
                officer_rels,
                summary_mappings,
            ) = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if people:
                loaded += loader.load_nodes("Person", people, key_field="document_id")
            if procurements:
                loaded += loader.run_query(procurement_query, procurements)
            if officer_rels:
                loaded += loader.run_query(officer_query, officer_rels)
            if summary_mappings:
                append_summary_map(mapping_path, summary_mappings)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
