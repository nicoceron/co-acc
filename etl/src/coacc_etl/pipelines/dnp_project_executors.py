from __future__ import annotations

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
)
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, read_csv_normalized
from coacc_etl.pipelines.project_graph import build_project_row, load_project_nodes, load_project_relationships
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _clean_bpin(raw: object) -> str:
    return strip_document(clean_text(raw))


class DnpProjectExecutorsPipeline(Pipeline):
    """Load DNP project executors onto Convenio nodes keyed by BPIN."""

    name = "dnp_project_executors"
    source_id = "dnp_project_executors"

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
        csv_path = Path(self.data_dir) / "dnp_project_executors" / "dnp_project_executors.csv"
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        project_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            project_id = _clean_bpin(row.get("bpin"))
            project_name = clean_name(row.get("nombreproyecto"))
            executor_name = clean_name(row.get("entidadejecutora"))
            executor_code = strip_document(clean_text(row.get("codigoentidadejecutora")))
            if not project_id:
                continue

            project_map[project_id] = build_project_row(
                project_id,
                name=project_name or project_id,
                object=clean_text(row.get("nombreproyecto")) or project_name or project_id,
                executor_entity_code=executor_code,
                executor_entity_name=executor_name,
                source=self.source_id,
                country="CO",
            )

            if not executor_name:
                continue

            executor_document = make_company_document_id(
                executor_code,
                executor_name,
                kind="dnp-executor",
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=executor_document,
                    name=executor_name,
                    source=self.source_id,
                    entity_code=executor_code,
                ),
            )
            rels.append({
                "source_key": executor_document,
                "target_key": project_id,
                "source": self.source_id,
                "role": "PROJECT_EXECUTOR",
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.projects = deduplicate_rows(list(project_map.values()), ["project_id"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.projects:
            loaded += load_project_nodes(loader, self.projects)
        if self.rels:
            loaded += load_project_relationships(
                loader,
                rel_type="ADMINISTRA",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                properties=["source", "role"],
            )
        self.rows_loaded = loaded
