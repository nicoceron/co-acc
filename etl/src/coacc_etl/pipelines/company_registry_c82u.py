from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    build_person_name,
    clean_name,
    clean_text,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class CompanyRegistryC82uPipeline(Pipeline):
    """Load chamber-registry entities and legal representatives from c82u-588k."""

    name = "company_registry_c82u"
    source_id = "company_registry_c82u"

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
        return Path(self.data_dir) / "company_registry_c82u" / "company_registry_c82u.csv"

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
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        company_map: dict[str, dict[str, Any]] = {}
        person_map: dict[str, dict[str, Any]] = {}
        officer_rels: list[dict[str, Any]] = []

        for row in frame.to_dict(orient="records"):
            registry_document = strip_document(clean_text(row.get("numero_identificacion")))
            if not registry_document:
                registry_document = strip_document(clean_text(row.get("nit")))
            if not registry_document:
                continue

            company_name = (
                clean_name(row.get("razon_social"))
                or build_person_name(
                    row.get("primer_nombre"),
                    row.get("segundo_nombre"),
                    row.get("primer_apellido"),
                    row.get("segundo_apellido"),
                )
                or registry_document
            )

            nit = strip_document(clean_text(row.get("nit")))
            company_map[registry_document] = {
                "document_id": registry_document,
                "nit": nit or None,
                "name": company_name,
                "razon_social": company_name,
                "registry_source": "c82u-588k",
                "registry_chamber_code": clean_text(row.get("codigo_camara")),
                "registry_chamber_name": clean_text(row.get("camara_comercio")),
                "registry_number": clean_text(row.get("matricula")),
                "registry_proponent_registration": clean_text(row.get("inscripcion_proponente")),
                "registry_identification_class": clean_text(row.get("clase_identificacion")),
                "registry_identification_class_code": clean_text(
                    row.get("codigo_clase_identificacion")
                ),
                "registry_status": clean_text(row.get("estado_matricula")),
                "registry_status_code": clean_text(row.get("codigo_estado_matricula")),
                "registry_society_type": clean_text(row.get("tipo_sociedad")),
                "registry_society_type_code": clean_text(row.get("codigo_tipo_sociedad")),
                "registry_legal_organization": clean_text(row.get("organizacion_juridica")),
                "registry_legal_organization_code": clean_text(
                    row.get("codigo_organizacion_juridica")
                ),
                "registry_category": clean_text(row.get("categoria_matricula")),
                "registry_category_code": clean_text(row.get("codigo_categoria_matricula")),
                "registry_sigla": clean_text(row.get("sigla")),
                "primary_ciiu_code": clean_text(row.get("cod_ciiu_act_econ_pri")),
                "secondary_ciiu_code": clean_text(row.get("cod_ciiu_act_econ_sec")),
                "ciiu3": clean_text(row.get("ciiu3")),
                "ciiu4": clean_text(row.get("ciiu4")),
                "registered_at": parse_iso_date(row.get("fecha_matricula")),
                "renewed_at": parse_iso_date(row.get("fecha_renovacion")),
                "registry_last_renewed_year": clean_text(row.get("ultimo_ano_renovado")),
                "registry_valid_until": parse_iso_date(row.get("fecha_vigencia")),
                "registry_cancelled_at": parse_iso_date(row.get("fecha_cancelacion")),
                "registry_updated_at": parse_iso_date(row.get("fecha_actualizacion")),
                "source": "company_registry_c82u",
                "country": "CO",
            }

            representative_document = strip_document(
                clean_text(row.get("num_identificacion_representante_legal"))
            )
            representative_name = clean_name(row.get("representante_legal"))
            if representative_document and representative_name:
                person_map[representative_document] = {
                    "document_id": representative_document,
                    "cedula": representative_document,
                    "name": representative_name,
                    "document_type": clean_text(row.get("clase_identificacion_rl")),
                    "source": "company_registry_c82u",
                    "country": "CO",
                }
                officer_rels.append({
                    "source_key": representative_document,
                    "target_key": registry_document,
                    "source": "company_registry_c82u",
                    "role": "LEGAL_REPRESENTATIVE",
                    "registry_number": clean_text(row.get("matricula")),
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
                "    r.role = row.role, "
                "    r.registry_number = row.registry_number"
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
            "    r.role = row.role, "
            "    r.registry_number = row.registry_number"
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
