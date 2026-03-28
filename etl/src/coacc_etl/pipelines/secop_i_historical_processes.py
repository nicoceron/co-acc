from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    build_company_row,
    merge_company,
    merge_limited_unique,
    procurement_year,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    extract_url,
    parse_amount,
    parse_iso_date,
    read_csv_normalized,
    stable_id,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def secop_i_historical_summary_id(row: dict[str, Any]) -> str:
    return stable_id(
        "co_secop_i_hist",
        row.get("id_adjudicacion"),
        row.get("numero_de_constancia"),
        row.get("numero_de_contrato"),
        row.get("uid"),
    )


def _company_document(*values: object) -> str:
    for value in values:
        digits = strip_document(clean_text(value))
        if digits:
            return digits
    return ""


class SecopIHistoricalProcessesPipeline(Pipeline):
    """Load connected SECOP I historical awarded processes into the graph."""

    name = "secop_i_historical_processes"
    source_id = "secop_i_historical_processes"

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
        self.officer_rels: list[dict[str, Any]] = []
        self.procurements: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return (
            Path(self.data_dir)
            / "secop_i_historical_processes"
            / "secop_i_historical_processes.csv"
        )

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
        person_map: dict[str, dict[str, Any]] = {}
        officer_rels: list[dict[str, Any]] = []
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            buyer_name = clean_name(row.get("nombre_entidad"))
            buyer_document = _company_document(row.get("nit_de_la_entidad"))
            supplier_name = clean_name(row.get("nom_razon_social_contratista"))
            supplier_document = _company_document(row.get("identificacion_del_contratista"))
            if not buyer_name or not buyer_document or not supplier_name or not supplier_document:
                continue

            merge_company(
                company_map,
                build_company_row(
                    document_id=buyer_document,
                    name=buyer_name,
                    source=self.source_id,
                    department=clean_text(row.get("departamento_entidad")),
                    municipality=clean_text(row.get("municipio_entidad")),
                    entity_order=clean_text(row.get("orden_entidad")),
                    entity_scope=clean_text(row.get("nivel_entidad")),
                    entity_code=clean_text(row.get("c_digo_de_la_entidad"))
                    or clean_text(row.get("codigo_de_la_entidad")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=supplier_document,
                    name=supplier_name,
                    source=self.source_id,
                    department=clean_text(row.get("dpto_y_muni_contratista")),
                ),
            )

            representative_document = _company_document(row.get("identific_representante_legal"))
            representative_name = clean_name(row.get("nombre_del_represen_legal"))
            if representative_document and representative_name:
                person_map[representative_document] = {
                    "document_id": representative_document,
                    "cedula": representative_document,
                    "name": representative_name,
                    "document_type": clean_text(row.get("tipo_doc_representante_legal")),
                    "source": self.source_id,
                    "country": "CO",
                }
                officer_rels.append(
                    {
                        "source_key": representative_document,
                        "target_key": supplier_document,
                        "source": self.source_id,
                        "role": "LEGAL_REPRESENTATIVE",
                        "adjudication_id": clean_text(row.get("id_adjudicacion")),
                    }
                )

            summary_id = secop_i_historical_summary_id(row)
            contract_value = (
                parse_amount(row.get("valor_contrato_con_adiciones"))
                or parse_amount(row.get("cuantia_contrato"))
                or parse_amount(row.get("cuantia_proceso"))
                or 0.0
            )
            added_value = parse_amount(row.get("valor_total_de_adiciones")) or 0.0
            first_date = parse_iso_date(row.get("fecha_de_firma_del_contrato"))
            last_date = parse_iso_date(row.get("fecha_fin_ejec_contrato")) or first_date

            procurement_map[summary_id] = {
                "summary_id": summary_id,
                "source_key": buyer_document,
                "target_key": supplier_document,
                "source": self.source_id,
                "country": "CO",
                "secop_platform": "SECOP_I",
                "historical": True,
                "year": procurement_year(row.get("fecha_de_firma_del_contrato"))
                or procurement_year(row.get("fecha_de_cargue_en_el_secop")),
                "buyer_document_id": buyer_document,
                "buyer_name": buyer_name,
                "supplier_document_id": supplier_document,
                "supplier_name": supplier_name,
                "contract_count": 1,
                "total_value": contract_value,
                "average_value": contract_value,
                "initial_contract_value": parse_amount(row.get("cuantia_contrato"))
                or parse_amount(row.get("cuantia_proceso"))
                or contract_value,
                "added_value_total": added_value,
                "first_date": first_date,
                "last_date": last_date,
                "status": clean_text(row.get("estado_del_proceso")),
                "modality": clean_text(row.get("modalidad_de_contratacion")),
                "contract_type": clean_text(row.get("tipo_de_contrato")),
                "department": clean_text(row.get("departamento_entidad")),
                "city": clean_text(row.get("municipio_entidad")),
                "entity_code": clean_text(row.get("c_digo_de_la_entidad"))
                or clean_text(row.get("codigo_de_la_entidad")),
                "process_id": clean_text(row.get("numero_de_proceso")),
                "contract_id": clean_text(row.get("numero_de_contrato")),
                "reference": clean_text(row.get("numero_de_constancia")),
                "adjudication_id": clean_text(row.get("id_adjudicacion")),
                "bpin_code": strip_document(clean_text(row.get("codigo_bpin"))),
                "spending_destination": clean_text(row.get("destino_gasto")),
                "evidence_refs": merge_limited_unique(
                    [],
                    row.get("uid"),
                    row.get("numero_de_constancia"),
                    row.get("numero_de_proceso"),
                    row.get("numero_de_contrato"),
                    extract_url(row.get("ruta_proceso_en_secop_i")),
                    limit=6,
                ),
                "url": extract_url(row.get("ruta_proceso_en_secop_i")),
            }

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(person_map.values()), ["document_id"]),
            deduplicate_rows(officer_rels, ["source_key", "target_key", "adjudication_id"]),
            deduplicate_rows(list(procurement_map.values()), ["summary_id"]),
        )

    def transform(self) -> None:
        (
            self.companies,
            self.people,
            self.officer_rels,
            self.procurements,
        ) = self._transform_frame(self._raw)

    def _procurement_query(self) -> str:
        return (
            "UNWIND $rows AS row "
            "MATCH (buyer:Company {document_id: row.source_key}) "
            "MATCH (supplier:Company {document_id: row.target_key}) "
            "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier) "
            "SET r.source = row.source, "
            "    r.country = row.country, "
            "    r.secop_platform = row.secop_platform, "
            "    r.historical = row.historical, "
            "    r.year = row.year, "
            "    r.buyer_document_id = row.buyer_document_id, "
            "    r.buyer_name = row.buyer_name, "
            "    r.supplier_document_id = row.supplier_document_id, "
            "    r.supplier_name = row.supplier_name, "
            "    r.contract_count = row.contract_count, "
            "    r.total_value = row.total_value, "
            "    r.average_value = row.average_value, "
            "    r.initial_contract_value = row.initial_contract_value, "
            "    r.added_value_total = row.added_value_total, "
            "    r.first_date = row.first_date, "
            "    r.last_date = row.last_date, "
            "    r.status = row.status, "
            "    r.modality = row.modality, "
            "    r.contract_type = row.contract_type, "
            "    r.department = row.department, "
            "    r.city = row.city, "
            "    r.entity_code = row.entity_code, "
            "    r.process_id = row.process_id, "
            "    r.contract_id = row.contract_id, "
            "    r.reference = row.reference, "
            "    r.adjudication_id = row.adjudication_id, "
            "    r.bpin_code = CASE "
            "      WHEN row.bpin_code <> '' THEN row.bpin_code "
            "      ELSE r.bpin_code "
            "    END, "
            "    r.latest_spending_destination = CASE "
            "      WHEN row.spending_destination <> '' THEN row.spending_destination "
            "      ELSE r.latest_spending_destination END, "
            "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
            "    r.evidence_refs = row.evidence_refs"
        )

    def _officer_query(self) -> str:
        return (
            "UNWIND $rows AS row "
            "MATCH (p:Person {document_id: row.source_key}) "
            "MATCH (c:Company {document_id: row.target_key}) "
            "MERGE (p)-[r:OFFICER_OF]->(c) "
            "SET r.source = row.source, "
            "    r.role = row.role, "
            "    r.adjudication_id = row.adjudication_id"
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.officer_rels:
            loaded += loader.run_query(self._officer_query(), self.officer_rels)
        if self.procurements:
            loaded += loader.run_query(self._procurement_query(), self.procurements)
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
            companies, people, officer_rels, procurements = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if people:
                loaded += loader.load_nodes("Person", people, key_field="document_id")
            if officer_rels:
                loaded += loader.run_query(self._officer_query(), officer_rels)
            if procurements:
                loaded += loader.run_query(self._procurement_query(), procurements)
            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
