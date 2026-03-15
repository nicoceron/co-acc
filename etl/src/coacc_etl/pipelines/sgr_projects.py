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
    parse_amount,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SgrProjectsPipeline(Pipeline):
    """Load SGR projects as Convenio-like investment nodes."""

    name = "sgr_projects"
    source_id = "sgr_projects"

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
        self.projects: list[dict[str, Any]] = []
        self.rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "sgr_projects" / "sgr_projects.csv"
        if not csv_path.exists():
            logger.warning("[sgr_projects] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        project_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            project_id = clean_text(row.get("codigobpin"))
            executor_code = clean_text(row.get("codejecutor"))
            if not project_id:
                continue

            executor_name = clean_name(row.get("entidadejecutora"))
            if executor_code and executor_name:
                company_map[executor_code] = {
                    "document_id": executor_code,
                    "name": executor_name,
                    "razao_social": executor_name,
                    "department": clean_text(row.get("departamento")),
                    "source": "sgr_projects",
                    "country": "CO",
                }
                rels.append({
                    "source_key": executor_code,
                    "target_key": project_id,
                    "source": "sgr_projects",
                })

            project_map[project_id] = {
                "convenio_id": project_id,
                "name": clean_name(row.get("nombre")) or project_id,
                "object": clean_text(row.get("nombre")),
                "value": parse_amount(row.get("valortotal")),
                "status": clean_text(row.get("estado")),
                "sector": clean_text(row.get("sector")),
                "department": clean_text(row.get("departamento")),
                "ocad_name": clean_text(row.get("nomocad")),
                "execution_physical": parse_amount(row.get("ejecucionfisica")),
                "execution_financial": parse_amount(row.get("ejecucionfinanciera")),
                "peace_project": clean_text(row.get("proyecto_paz")),
                "ethnic_project": clean_text(row.get("proyecto_grupo_etnico")),
                "covid_project": clean_text(row.get("proyecto_covid")),
                "source": "sgr_projects",
                "country": "CO",
            }

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.projects = deduplicate_rows(list(project_map.values()), ["convenio_id"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.projects:
            loaded += loader.load_nodes("Convenio", self.projects, key_field="convenio_id")
        if self.rels:
            loaded += loader.load_relationships(
                rel_type="ADMINISTRA",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Convenio",
                target_key="convenio_id",
                properties=["source"],
            )

        self.rows_loaded = loaded
