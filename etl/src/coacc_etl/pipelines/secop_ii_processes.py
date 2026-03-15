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
    procurement_relation_id,
    procurement_year,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    extract_url,
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


def _looks_like_srp(*values: object) -> bool:
    haystack = " ".join(clean_text(value).lower() for value in values if clean_text(value))
    return "acuerdo marco" in haystack or "srp" in haystack


class SecopIiProcessesPipeline(Pipeline):
    """Load SECOP II procurement procedures as buyer-to-awarded-supplier summaries."""

    name = "secop_ii_processes"
    source_id = "secop_ii_processes"

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
        self.procurement_awards: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_ii_processes" / "secop_ii_processes.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_ii_processes] file not found: %s", csv_path)
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
    ]:
        company_map: dict[str, dict[str, Any]] = {}
        award_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            bid_id = clean_text(row.get("id_del_proceso"))
            if not bid_id:
                continue

            entity_name = clean_name(row.get("entidad"))
            entity_document = make_company_document_id(
                row.get("nit_entidad"),
                entity_name,
                kind="buyer",
            )

            supplier_name = clean_name(row.get("nombre_del_proveedor"))
            supplier_document = make_company_document_id(
                row.get("nit_del_proveedor_adjudicado"),
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
                    source="secop_ii_processes",
                    entity_order=clean_text(row.get("ordenentidad")),
                    entity_classification=clean_text(row.get("codigo_pci")),
                    department=clean_text(row.get("departamento_entidad")),
                    municipality=clean_text(row.get("ciudad_entidad")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=supplier_document,
                    name=supplier_name,
                    source="secop_ii_processes",
                    supplier_registry_code=clean_text(row.get("codigoproveedor")),
                ),
            )

            award_date = parse_iso_date(row.get("fecha_adjudicacion"))
            year = procurement_year(row.get("fecha_adjudicacion"))
            summary_id = procurement_relation_id(
                "secop_ii_processes",
                entity_document,
                supplier_document,
                year,
            )
            summary = award_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "source_key": entity_document,
                    "target_key": supplier_document,
                    "source": "secop_ii_processes",
                    "country": "CO",
                    "year": year,
                    "buyer_document_id": entity_document,
                    "buyer_name": entity_name,
                    "supplier_document_id": supplier_document,
                    "supplier_name": supplier_name,
                    "process_count": 0,
                    "total_value": 0.0,
                    "average_value": None,
                    "price_base_total": 0.0,
                    "first_date": award_date,
                    "last_date": award_date,
                    "status": clean_text(row.get("estado_del_procedimiento")),
                    "summary_status": clean_text(row.get("estado_resumen")),
                    "opening_status": clean_text(row.get("estado_de_apertura_del_proceso")),
                    "modality": clean_text(row.get("modalidad_de_contratacion")),
                    "justification": clean_text(row.get("justificaci_n_modalidad_de")),
                    "contract_type": clean_text(row.get("tipo_de_contrato")),
                    "contract_subtype": clean_text(row.get("subtipo_de_contrato")),
                    "department": clean_text(row.get("departamento_entidad")),
                    "city": clean_text(row.get("ciudad_entidad")),
                    "entity_code": clean_text(row.get("codigo_entidad")),
                    "portfolio_id": clean_text(row.get("id_del_portafolio")),
                    "provider_code": clean_text(row.get("codigoproveedor")),
                    "is_awarded": parse_flag(row.get("adjudicado")),
                    "srp": _looks_like_srp(
                        row.get("nombre_del_procedimiento"),
                        row.get("referencia_del_proceso"),
                        row.get("descripci_n_del_procedimiento"),
                        row.get("modalidad_de_contratacion"),
                    ),
                    "evidence_refs": [],
                    "url": extract_url(row.get("urlproceso")),
                },
            )
            summary["process_count"] += 1
            summary["total_value"] += parse_amount(row.get("valor_total_adjudicacion")) or 0.0
            summary["average_value"] = summary["total_value"] / float(summary["process_count"])
            summary["price_base_total"] += parse_amount(row.get("precio_base")) or 0.0
            if award_date and (not summary.get("first_date") or award_date < summary["first_date"]):
                summary["first_date"] = award_date
            if award_date and (not summary.get("last_date") or award_date > summary["last_date"]):
                summary["last_date"] = award_date
            if not summary.get("status"):
                summary["status"] = clean_text(row.get("estado_del_procedimiento"))
            if not summary.get("summary_status"):
                summary["summary_status"] = clean_text(row.get("estado_resumen"))
            if not summary.get("opening_status"):
                summary["opening_status"] = clean_text(row.get("estado_de_apertura_del_proceso"))
            if not summary.get("modality"):
                summary["modality"] = clean_text(row.get("modalidad_de_contratacion"))
            if not summary.get("justification"):
                summary["justification"] = clean_text(row.get("justificaci_n_modalidad_de"))
            if not summary.get("contract_type"):
                summary["contract_type"] = clean_text(row.get("tipo_de_contrato"))
            if not summary.get("contract_subtype"):
                summary["contract_subtype"] = clean_text(row.get("subtipo_de_contrato"))
            if not summary.get("url"):
                summary["url"] = extract_url(row.get("urlproceso"))
            summary["is_awarded"] = bool(summary.get("is_awarded")) or bool(parse_flag(row.get("adjudicado")))
            summary["srp"] = bool(summary.get("srp")) or _looks_like_srp(
                row.get("nombre_del_procedimiento"),
                row.get("referencia_del_proceso"),
                row.get("descripci_n_del_procedimiento"),
                row.get("modalidad_de_contratacion"),
            )
            summary["evidence_refs"] = merge_limited_unique(
                list(summary.get("evidence_refs", [])),
                bid_id,
                clean_text(row.get("id_del_portafolio")),
            )

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(award_map.values()), ["summary_id"]),
        )

    def transform(self) -> None:
        (
            self.companies,
            self.procurement_awards,
        ) = self._transform_frame(self._raw)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.procurement_awards:
            query = (
                "UNWIND $rows AS row "
                "MATCH (buyer:Company {document_id: row.source_key}) "
                "MATCH (supplier:Company {document_id: row.target_key}) "
                "MERGE (buyer)-[r:ADJUDICOU_A {summary_id: row.summary_id}]->(supplier) "
                "WITH r, row, "
                "     coalesce(r.process_count, 0) AS prev_count, "
                "     coalesce(r.total_value, 0.0) AS prev_total, "
                "     coalesce(r.price_base_total, 0.0) AS prev_price_base, "
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
                "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
                "    r.summary_status = CASE "
                "      WHEN row.summary_status <> '' THEN row.summary_status ELSE r.summary_status END, "
                "    r.opening_status = CASE "
                "      WHEN row.opening_status <> '' THEN row.opening_status ELSE r.opening_status END, "
                "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
                "    r.justification = CASE "
                "      WHEN row.justification <> '' THEN row.justification ELSE r.justification END, "
                "    r.contract_type = CASE "
                "      WHEN row.contract_type <> '' THEN row.contract_type ELSE r.contract_type END, "
                "    r.contract_subtype = CASE "
                "      WHEN row.contract_subtype <> '' THEN row.contract_subtype ELSE r.contract_subtype END, "
                "    r.department = coalesce(row.department, r.department), "
                "    r.city = coalesce(row.city, r.city), "
                "    r.entity_code = CASE "
                "      WHEN row.entity_code <> '' THEN row.entity_code ELSE r.entity_code END, "
                "    r.portfolio_id = CASE "
                "      WHEN row.portfolio_id <> '' THEN row.portfolio_id ELSE r.portfolio_id END, "
                "    r.provider_code = CASE "
                "      WHEN row.provider_code <> '' THEN row.provider_code ELSE r.provider_code END, "
                "    r.is_awarded = coalesce(r.is_awarded, false) OR coalesce(row.is_awarded, false), "
                "    r.srp = coalesce(r.srp, false) OR coalesce(row.srp, false), "
                "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
                "    r.process_count = prev_count + row.process_count, "
                "    r.total_value = prev_total + row.total_value, "
                "    r.average_value = CASE "
                "      WHEN (prev_count + row.process_count) > 0 "
                "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.process_count) "
                "      ELSE NULL "
                "    END, "
                "    r.price_base_total = prev_price_base + row.price_base_total, "
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
            loaded += loader.run_query(query, self.procurement_awards)

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

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            companies, procurement_awards = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if procurement_awards:
                query = (
                    "UNWIND $rows AS row "
                    "MATCH (buyer:Company {document_id: row.source_key}) "
                    "MATCH (supplier:Company {document_id: row.target_key}) "
                    "MERGE (buyer)-[r:ADJUDICOU_A {summary_id: row.summary_id}]->(supplier) "
                    "WITH r, row, "
                    "     coalesce(r.process_count, 0) AS prev_count, "
                    "     coalesce(r.total_value, 0.0) AS prev_total, "
                    "     coalesce(r.price_base_total, 0.0) AS prev_price_base, "
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
                    "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
                    "    r.summary_status = CASE "
                    "      WHEN row.summary_status <> '' THEN row.summary_status ELSE r.summary_status END, "
                    "    r.opening_status = CASE "
                    "      WHEN row.opening_status <> '' THEN row.opening_status ELSE r.opening_status END, "
                    "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
                    "    r.justification = CASE "
                    "      WHEN row.justification <> '' THEN row.justification ELSE r.justification END, "
                    "    r.contract_type = CASE "
                    "      WHEN row.contract_type <> '' THEN row.contract_type ELSE r.contract_type END, "
                    "    r.contract_subtype = CASE "
                    "      WHEN row.contract_subtype <> '' THEN row.contract_subtype ELSE r.contract_subtype END, "
                    "    r.department = coalesce(row.department, r.department), "
                    "    r.city = coalesce(row.city, r.city), "
                    "    r.entity_code = CASE "
                    "      WHEN row.entity_code <> '' THEN row.entity_code ELSE r.entity_code END, "
                    "    r.portfolio_id = CASE "
                    "      WHEN row.portfolio_id <> '' THEN row.portfolio_id ELSE r.portfolio_id END, "
                    "    r.provider_code = CASE "
                    "      WHEN row.provider_code <> '' THEN row.provider_code ELSE r.provider_code END, "
                    "    r.is_awarded = coalesce(r.is_awarded, false) OR coalesce(row.is_awarded, false), "
                    "    r.srp = coalesce(r.srp, false) OR coalesce(row.srp, false), "
                    "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
                    "    r.process_count = prev_count + row.process_count, "
                    "    r.total_value = prev_total + row.total_value, "
                    "    r.average_value = CASE "
                    "      WHEN (prev_count + row.process_count) > 0 "
                    "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.process_count) "
                    "      ELSE NULL "
                    "    END, "
                    "    r.price_base_total = prev_price_base + row.price_base_total, "
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
                loaded += loader.run_query(query, procurement_awards)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
