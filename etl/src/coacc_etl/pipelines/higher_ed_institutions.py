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
    read_csv_normalized_with_fallback,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _row_value(row: pd.Series, *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


class HigherEdInstitutionsPipeline(Pipeline):
    """Load MEN higher-education institutions as real legal entities."""

    name = "higher_ed_institutions"
    source_id = "higher_ed_institutions"

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

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "higher_ed_institutions"
            / "higher_ed_institutions.csv"
        )
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

        for _, row in self._raw.iterrows():
            institution_code = _row_value(
                row,
                "c_digo_instituci_n",
                "codigo_instituci_n",
                "codigoinstitucion",
            )
            institution_name = clean_name(
                _row_value(
                    row,
                    "nombre_instituci_n",
                    "nombreinstitucion",
                )
            )
            document_id = strip_document(
                _row_value(
                    row,
                    "n_mero_identificaci_n",
                    "numero_identificacion",
                )
            )
            if not institution_code or not institution_name or not document_id:
                continue

            company_map[document_id] = {
                "document_id": document_id,
                "nit": document_id,
                "name": institution_name,
                "razon_social": institution_name,
                "education_institution_code": institution_code,
                "company_type": "INSTITUCION_EDUCACION_SUPERIOR",
                "principal_sectional": _row_value(
                    row,
                    "principal_seccional",
                ),
                "legal_nature": _row_value(
                    row,
                    "naturaleza_jur_dica",
                    "naturaleza_juridica",
                ),
                "education_sector": _row_value(row, "sector"),
                "academic_character": _row_value(
                    row,
                    "car_cter_acad_mico",
                    "caracter_academico",
                ),
                "department": _row_value(row, "departamento_domicilio"),
                "municipality": _row_value(row, "municipio_domicilio"),
                "address": _row_value(row, "direcci_n_domicilio", "direccion_domicilio"),
                "website": _row_value(row, "p_gina_web", "pagina_web"),
                "institution_status": _row_value(row, "estado"),
                "education_updated_at": _row_value(
                    row,
                    "fecha_actualizacion",
                    "fecha_actualizaci_n",
                ),
                "source": "higher_ed_institutions",
                "country": "CO",
            }

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])

    def load(self) -> None:
        if not self.companies:
            self.rows_loaded = 0
            return

        loader = Neo4jBatchLoader(self.driver)
        upsert_query = """
            UNWIND $rows AS row
            MERGE (c:Company {document_id: row.document_id})
            SET c.nit = coalesce(c.nit, row.nit),
                c.name = coalesce(c.name, row.name),
                c.razon_social = coalesce(c.razon_social, row.razon_social),
                c.company_type = coalesce(c.company_type, row.company_type),
                c.country = coalesce(c.country, row.country),
                c.source = coalesce(c.source, row.source),
                c.education_institution_code = row.education_institution_code,
                c.principal_sectional = row.principal_sectional,
                c.legal_nature = row.legal_nature,
                c.education_sector = row.education_sector,
                c.academic_character = row.academic_character,
                c.department = coalesce(c.department, row.department),
                c.municipality = coalesce(c.municipality, row.municipality),
                c.address = coalesce(c.address, row.address),
                c.website = CASE
                    WHEN row.website <> '' THEN row.website
                    ELSE c.website
                END,
                c.institution_status = row.institution_status,
                c.education_updated_at = row.education_updated_at
        """
        loaded = loader.run_query(upsert_query, self.companies)

        link_query = """
            MATCH (edu:Company)
            WHERE edu.source = 'higher_ed_institutions'
              AND edu.document_id =~ '^[0-9]+$'
              AND coalesce(edu.name, edu.razon_social, '') <> ''
            WITH edu,
                 toUpper(
                   replace(
                     replace(
                       replace(
                         replace(
                           replace(
                             replace(
                               replace(coalesce(edu.razon_social, edu.name, ''), ' ', ''),
                               '.', ''
                             ),
                             ',', ''
                           ),
                           '-', ''
                         ),
                         '_', ''
                       ),
                       '(', ''
                     ),
                     ')', ''
                   )
                 ) AS edu_name_key
            MATCH (other:Company)
            WHERE other.document_id <> edu.document_id
              AND other.document_id =~ '^[0-9]+$'
              AND coalesce(other.name, other.razon_social, '') <> ''
              AND coalesce(other.source, '') <> 'higher_ed_institutions'
            WITH edu,
                 edu_name_key,
                 other,
                 toUpper(
                   replace(
                     replace(
                       replace(
                         replace(
                           replace(
                             replace(
                               replace(coalesce(other.razon_social, other.name, ''), ' ', ''),
                               '.', ''
                             ),
                             ',', ''
                           ),
                           '-', ''
                         ),
                         '_', ''
                       ),
                       '(', ''
                     ),
                     ')', ''
                   )
                 ) AS other_name_key
            WHERE edu_name_key = other_name_key
              AND abs(size(edu.document_id) - size(other.document_id)) <= 2
              AND (
                edu.document_id STARTS WITH other.document_id
                OR other.document_id STARTS WITH edu.document_id
              )
            MERGE (edu)-[r:SAME_AS]->(other)
            SET r.source = 'higher_ed_institutions',
                r.match_reason = 'exact_name_numeric_prefix',
                r.confidence = 0.94
        """
        with self.driver.session(database=self.neo4j_database) as session:
            session.run(link_query)

        self.rows_loaded = loaded
