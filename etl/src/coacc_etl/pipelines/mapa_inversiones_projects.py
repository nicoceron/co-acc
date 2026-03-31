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
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    parse_integer,
    read_csv_normalized_with_fallback,
)
from coacc_etl.pipelines.project_graph import build_project_row, load_project_nodes, load_project_relationships
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _clean_bpin(raw: object) -> str:
    return clean_text(raw).lstrip("'")


def _split_place(raw_name: str) -> tuple[str, str]:
    if "," not in raw_name:
        return "", ""
    municipality, department = [part.strip() for part in raw_name.rsplit(",", 1)]
    return municipality, department


class MapaInversionesProjectsPipeline(Pipeline):
    """Load MapaInversiones project basics as Convenio nodes tied to responsible entities."""

    name = "mapa_inversiones_projects"
    source_id = "mapa_inversiones_projects"

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
        csv_path = (
            Path(self.data_dir)
            / "mapa_inversiones_projects"
            / "mapa_inversiones_projects.csv"
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
        project_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            project_id = _clean_bpin(row.get("bpin"))
            project_name = clean_name(row.get("nombreproyecto"))
            entity_name = clean_name(row.get("entidadresponsable"))
            if not project_id or not project_name or not entity_name:
                continue

            municipality, department = _split_place(entity_name)
            entity_document = make_company_document_id(
                "",
                entity_name,
                kind="mapa-entity",
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=entity_document,
                    name=entity_name,
                    source=self.source_id,
                    department=department,
                    municipality=municipality,
                ),
            )

            project_map[project_id] = build_project_row(
                project_id,
                name=project_name,
                object=clean_text(row.get("nombreproyecto")),
                value=parse_amount(row.get("valortotalproyecto")),
                requested_value=parse_amount(row.get("valorsolicitadoproyecto")),
                executed_value=parse_amount(row.get("valorejecutadoproyecto")),
                execution_physical=parse_amount(row.get("avancefisico")),
                execution_financial=parse_amount(row.get("avancefinanciero")),
                beneficiaries=parse_integer(row.get("beneficiarios")),
                status=clean_text(row.get("estadoproyecto")),
                sub_status=clean_text(row.get("subestadoproyecto")),
                sector=clean_text(row.get("sectorproyecto")),
                project_type=clean_text(row.get("tipoproyecto")),
                horizon=clean_text(row.get("horizonteproyecto")),
                ocad_name=clean_text(row.get("ocad")),
                source=self.source_id,
                country="CO",
            )

            rels.append({
                "source_key": entity_document,
                "target_key": project_id,
                "source": self.source_id,
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
                properties=["source"],
            )

        self.rows_loaded = loaded
