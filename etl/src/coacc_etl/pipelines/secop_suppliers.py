from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopSuppliersPipeline(Pipeline):
    """Load the SECOP II supplier registry and legal representatives."""

    name = "secop_suppliers"
    source_id = "secop_suppliers"

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

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_suppliers" / "secop_suppliers.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_suppliers] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _transform_frame(
        self,
        frame: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        company_map: dict[str, dict[str, Any]] = {}
        person_map: dict[str, dict[str, Any]] = {}
        officer_rels: list[dict[str, Any]] = []

        for row in frame.to_dict(orient="records"):
            supplier_document = strip_document(clean_text(row.get("nit")))
            if not supplier_document:
                continue

            supplier_name = clean_name(row.get("nombre")) or supplier_document
            company_map[supplier_document] = {
                "document_id": supplier_document,
                "nit": supplier_document,
                "name": supplier_name,
                "razao_social": supplier_name,
                "supplier_code": clean_text(row.get("codigo")),
                "company_type": clean_text(row.get("tipo_empresa")),
                "is_active": parse_flag(row.get("esta_activa")),
                "is_group": parse_flag(row.get("es_grupo")),
                "is_entity": parse_flag(row.get("es_entidad")),
                "is_pyme": parse_flag(row.get("espyme")),
                "created_at": parse_iso_date(row.get("fecha_creacion")),
                "category_code": clean_text(row.get("codigo_categoria_principal")),
                "category_name": clean_text(row.get("descripcion_categoria_principal")),
                "phone": clean_text(row.get("telefono")),
                "email": clean_text(row.get("correo")),
                "address": clean_text(row.get("direccion")),
                "website": clean_text(row.get("sitio_web")),
                "department": clean_text(row.get("departamento")),
                "municipality": clean_text(row.get("municipio")),
                "country": clean_text(row.get("pais")) or "CO",
                "source": "secop_suppliers",
            }

            representative_document = strip_document(
                clean_text(row.get("n_mero_doc_representante_legal"))
            )
            representative_name = clean_name(row.get("nombre_representante_legal"))

            if representative_document and representative_name:
                person_map[representative_document] = {
                    "document_id": representative_document,
                    "cedula": representative_document,
                    "name": representative_name,
                    "document_type": clean_text(row.get("tipo_doc_representante_legal")),
                    "phone": clean_text(row.get("telefono_representante_legal")),
                    "email": clean_text(row.get("correo_representante_legal")),
                    "source": "secop_suppliers",
                    "country": "CO",
                }
                officer_rels.append({
                    "source_key": representative_document,
                    "target_key": supplier_document,
                    "source": "secop_suppliers",
                    "role": "LEGAL_REPRESENTATIVE",
                })

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(person_map.values()), ["document_id"]),
            deduplicate_rows(officer_rels, ["source_key", "target_key"]),
        )

    def transform(self) -> None:
        self.companies, self.people, self.officer_rels = self._transform_frame(self._raw)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
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
        officer_query = (
            "UNWIND $rows AS row "
            "MATCH (p:Person {document_id: row.source_key}) "
            "MATCH (c:Company {document_id: row.target_key}) "
            "MERGE (p)-[r:OFFICER_OF]->(c) "
            "SET r.source = row.source, "
            "    r.role = row.role"
        )

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            companies, people, officer_rels = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if people:
                loaded += loader.load_nodes("Person", people, key_field="document_id")
            if officer_rels:
                loaded += loader.run_query(officer_query, officer_rels)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
