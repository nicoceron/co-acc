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
    parse_integer,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _row_value(row: pd.Series, *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


class HigherEdEnrollmentPipeline(Pipeline):
    """Load MEN higher-education enrollment by institution/program/period."""

    name = "higher_ed_enrollment"
    source_id = "higher_ed_enrollment"

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
        self.education_nodes: list[dict[str, Any]] = []
        self.rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "higher_ed_enrollment"
            / "higher_ed_enrollment.csv"
        )
        if not csv_path.exists():
            logger.warning("[higher_ed_enrollment] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        education_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            institution_code = _row_value(
                row,
                "c_digo_de_la_instituci_n",
                "codigo_de_la_institucion",
            )
            program_code = _row_value(
                row,
                "c_digo_snies_delprograma",
                "codigo_snies_delprograma",
            )
            year = _row_value(row, "a_o", "ano")
            semester = _row_value(row, "semestre")
            municipality_code = _row_value(
                row,
                "c_digo_del_municipio_programa",
                "codigo_del_municipio_programa",
            )
            if not institution_code or not program_code or not year or not semester:
                continue

            school_id = f"{program_code}_{year}_{semester}_{municipality_code or 'na'}"
            institution_name = clean_name(
                _row_value(
                    row,
                    "instituci_n_de_educaci_n_superior_ies",
                    "institucion_de_educacion_superior_ies",
                )
            )
            company_map[institution_code] = {
                "document_id": f"coedu_{institution_code}",
                "name": institution_name or institution_code,
                "razon_social": institution_name or institution_code,
                "education_institution_code": institution_code,
                "department": _row_value(row, "departamento_de_domicilio_de_la_ies"),
                "municipality": _row_value(row, "municipio_dedomicilio_de_la_ies"),
                "source": "higher_ed_enrollment",
                "country": "CO",
                "synthetic_document_id": True,
            }

            enrolled = parse_integer(_row_value(row, "matriculados_2015", "total_matriculados")) or 0
            current = education_map.setdefault(
                school_id,
                {
                    "school_id": school_id,
                    "name": clean_name(
                        _row_value(row, "programa_acad_mico", "programa_academico")
                    ) or school_id,
                    "institution_name": institution_name,
                    "knowledge_area": _row_value(
                        row,
                        "n_cleo_b_sico_del_conocimiento_nbc",
                        "nucleo_basico_del_conocimiento_nbc",
                    ),
                    "department": _row_value(row, "departamento_de_oferta_del_programa"),
                    "municipality": _row_value(row, "municipio_de_oferta_del_programa"),
                    "year": parse_integer(year),
                    "semester": semester,
                    "enrolled_total": 0,
                    "source": "higher_ed_enrollment",
                    "country": "CO",
                },
            )
            current["enrolled_total"] += enrolled

            rels.append({
                "source_key": institution_code,
                "target_key": school_id,
                "source": "higher_ed_enrollment",
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.education_nodes = deduplicate_rows(list(education_map.values()), ["school_id"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            company_query = """
                UNWIND $rows AS row
                MERGE (c:Company {education_institution_code: row.education_institution_code})
                ON CREATE SET c.document_id = row.document_id,
                              c.synthetic_document_id = true
                SET c.name = coalesce(c.name, row.name),
                    c.razon_social = coalesce(c.razon_social, row.razon_social),
                    c.department = coalesce(c.department, row.department),
                    c.municipality = coalesce(c.municipality, row.municipality),
                    c.country = coalesce(c.country, row.country),
                    c.source = CASE
                        WHEN coalesce(c.source, '') = '' THEN row.source
                        ELSE c.source
                    END
            """
            loaded += loader.run_query(company_query, self.companies)
        if self.education_nodes:
            loaded += loader.load_nodes("Education", self.education_nodes, key_field="school_id")
        if self.rels:
            rel_query = """
                UNWIND $rows AS row
                MATCH (c:Company {education_institution_code: row.source_key})
                MATCH (e:Education {school_id: row.target_key})
                MERGE (c)-[r:MANTIENE_A]->(e)
                SET r.source = row.source
            """
            loaded += loader.run_query(rel_query, self.rels)

        self.rows_loaded = loaded
