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
    merge_limited_unique,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_integer,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _clean_bpin(raw: object) -> str:
    return strip_document(clean_text(raw))


class DnpProjectBeneficiaryCharacterizationPipeline(Pipeline):
    """Load DNP beneficiary demographic characterization keyed by BPIN."""

    name = "dnp_project_beneficiary_characterization"
    source_id = "dnp_project_beneficiary_characterization"

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
            / "dnp_project_beneficiary_characterization"
            / "dnp_project_beneficiary_characterization.csv"
        )
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
            if not project_id:
                continue

            project_name = clean_name(row.get("nombreproyecto"))
            entity_name = clean_name(row.get("entidadresponsable"))
            characteristic = clean_text(row.get("caracteristicademografica"))
            quantity = parse_integer(row.get("cantidad")) or 0
            beneficiary_total = parse_integer(row.get("totalbeneficiario")) or 0

            current = project_map.setdefault(
                project_id,
                {
                    "convenio_id": project_id,
                    "name": project_name or project_id,
                    "object": clean_text(row.get("nombreproyecto")) or project_name or project_id,
                    "beneficiary_characteristics": [],
                    "beneficiary_characterization_count": 0,
                    "beneficiary_quantity_total": 0,
                    "beneficiary_total_reported": 0,
                    "source": self.source_id,
                    "country": "CO",
                },
            )
            current["beneficiary_entity_name"] = entity_name or current.get(
                "beneficiary_entity_name",
                "",
            )
            current["beneficiary_sector"] = clean_text(row.get("sector")) or current.get(
                "beneficiary_sector",
                "",
            )
            current["beneficiary_characteristics"] = merge_limited_unique(
                list(current.get("beneficiary_characteristics", [])),
                characteristic,
                limit=20,
            )
            current["beneficiary_characterization_count"] = len(
                current["beneficiary_characteristics"]
            )
            current["beneficiary_quantity_total"] = int(
                current.get("beneficiary_quantity_total") or 0
            ) + int(quantity)
            current["beneficiary_total_reported"] = max(
                int(current.get("beneficiary_total_reported") or 0),
                int(beneficiary_total),
            )

            if not entity_name:
                continue

            entity_document = make_company_document_id(
                "",
                entity_name,
                kind="dnp-characterization-entity",
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=entity_document,
                    name=entity_name,
                    source=self.source_id,
                ),
            )
            rels.append({
                "source_key": entity_document,
                "target_key": project_id,
                "source": self.source_id,
                "role": "PROJECT_BENEFICIARY_ENTITY",
            })

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
                properties=["source", "role"],
            )
        self.rows_loaded = loaded
