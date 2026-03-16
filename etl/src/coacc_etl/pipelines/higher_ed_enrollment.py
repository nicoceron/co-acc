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
            institution_code = clean_text(row.get("c_digo_de_la_instituci_n"))
            program_code = clean_text(row.get("c_digo_snies_delprograma"))
            year = clean_text(row.get("a_o"))
            semester = clean_text(row.get("semestre"))
            municipality_code = clean_text(row.get("c_digo_del_municipio_programa"))
            if not institution_code or not program_code or not year or not semester:
                continue

            school_id = f"{program_code}_{year}_{semester}_{municipality_code or 'na'}"
            institution_name = clean_name(row.get("instituci_n_de_educaci_n_superior_ies"))
            company_map[institution_code] = {
                "document_id": institution_code,
                "name": institution_name or institution_code,
                "razon_social": institution_name or institution_code,
                "department": clean_text(row.get("departamento_de_domicilio_de_la_ies")),
                "municipality": clean_text(row.get("municipio_dedomicilio_de_la_ies")),
                "source": "higher_ed_enrollment",
                "country": "CO",
            }

            enrolled = parse_integer(row.get("matriculados_2015")) or 0
            current = education_map.setdefault(
                school_id,
                {
                    "school_id": school_id,
                    "name": clean_name(row.get("programa_acad_mico")) or school_id,
                    "institution_name": institution_name,
                    "knowledge_area": clean_text(
                        row.get("n_cleo_b_sico_del_conocimiento_nbc")
                    ),
                    "department": clean_text(row.get("departamento_de_oferta_del_programa")),
                    "municipality": clean_text(row.get("municipio_de_oferta_del_programa")),
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
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.education_nodes:
            loaded += loader.load_nodes("Education", self.education_nodes, key_field="school_id")
        if self.rels:
            loaded += loader.load_relationships(
                rel_type="MANTIENE_A",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Education",
                target_key="school_id",
                properties=["source"],
            )

        self.rows_loaded = loaded
