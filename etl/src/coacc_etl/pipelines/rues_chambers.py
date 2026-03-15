from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, read_csv_normalized
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class RuesChambersPipeline(Pipeline):
    """Load the public RUES chamber directory into Company nodes."""

    name = "rues_chambers"
    source_id = "rues_chambers"

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

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "rues_chambers" / "rues_chambers.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[rues_chambers] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        companies: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            camera_code = clean_text(row.get("camera_code"))
            nit = clean_text(row.get("nit"))
            document_id = nit or f"rues_chamber_{camera_code}"
            if not document_id:
                continue

            chamber_name = clean_name(row.get("chamber_name_full")) or clean_name(
                row.get("chamber_name")
            )
            municipality = clean_name(row.get("chamber_name"))
            companies.append(
                {
                    "document_id": document_id,
                    "nit": nit or None,
                    "name": chamber_name or document_id,
                    "razao_social": chamber_name or document_id,
                    "camera_code": camera_code,
                    "company_type": "CHAMBER_OF_COMMERCE",
                    "municipality": municipality,
                    "phone": clean_text(row.get("phone")),
                    "email": clean_text(row.get("email")),
                    "address": clean_text(row.get("address")),
                    "website": clean_text(row.get("website")),
                    "privacy_policy_url": clean_text(row.get("privacy_policy_url")),
                    "responsible_contact": clean_text(row.get("responsible_contact")),
                    "correspondence_address": clean_text(row.get("correspondence_address")),
                    "source": "rues_chambers",
                    "country": "CO",
                }
            )

        self.companies = deduplicate_rows(companies, ["document_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = 0
        if self.companies:
            self.rows_loaded += loader.load_nodes(
                "Company",
                self.companies,
                key_field="document_id",
            )
