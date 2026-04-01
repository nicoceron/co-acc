from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class ControlPoliticoRequirementsPipeline(Pipeline):
    """Load control-politico propositions as Inquiry and requirement nodes."""

    name = "control_politico_requirements"
    source_id = "control_politico"

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
        self.inquiries: list[dict[str, Any]] = []
        self.requirements: list[dict[str, Any]] = []
        self.requirement_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "control_politico_requirements"
            / "control_politico_requirements.csv"
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
        inquiries: list[dict[str, Any]] = []
        requirements: list[dict[str, Any]] = []
        requirement_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            code = clean_text(row.get("codigo"))
            tema = clean_text(row.get("tema"))
            description = clean_text(row.get("descripci_n")) or clean_text(row.get("descripcion"))
            debate = clean_text(row.get("debate_control_politico"))
            period = clean_text(row.get("periodo"))
            approval_date = parse_iso_date(row.get("fecha_aprobacion"))
            inquiry_title = debate if debate and debate.lower() != "no aplica" else (tema or code)
            if not inquiry_title:
                continue

            inquiry_id = stable_id("inquiry_cp", inquiry_title, period, approval_date)
            search_text = " ".join(
                value for value in (inquiry_title, tema, description, code, period) if value
            )
            inquiries.append(
                {
                    "inquiry_id": inquiry_id,
                    "title": inquiry_title,
                    "summary": description or tema,
                    "type": "CONTROL_POLITICO",
                    "tema": tema,
                    "status": clean_text(row.get("vigencia")),
                    "event_date": approval_date,
                    "approval_date": approval_date,
                    "source_url": "",
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )

            requirement_id = stable_id("inquiry_req", code, inquiry_id, description)
            requirements.append(
                {
                    "requirement_id": requirement_id,
                    "title": code or inquiry_title,
                    "name": code or inquiry_title,
                    "summary": description,
                    "code": code,
                    "tema": tema,
                    "type": clean_text(row.get("tipo")),
                    "scope": clean_text(row.get("alcance")),
                    "period": period,
                    "debate_control_politico": debate,
                    "approval_date": approval_date,
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            requirement_rels.append(
                {
                    "source_key": requirement_id,
                    "target_key": inquiry_id,
                    "source": self.source_id,
                }
            )

        self.inquiries = deduplicate_rows(inquiries, ["inquiry_id"])
        self.requirements = deduplicate_rows(requirements, ["requirement_id"])
        self.requirement_rels = deduplicate_rows(
            requirement_rels,
            ["source_key", "target_key"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.inquiries:
            loaded += loader.load_nodes("Inquiry", self.inquiries, key_field="inquiry_id")
        if self.requirements:
            loaded += loader.load_nodes(
                "InquiryRequirement",
                self.requirements,
                key_field="requirement_id",
            )
        if self.requirement_rels:
            loaded += loader.load_relationships(
                rel_type="PARTE_DE",
                rows=self.requirement_rels,
                source_label="InquiryRequirement",
                source_key="requirement_id",
                target_label="Inquiry",
                target_key="inquiry_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
