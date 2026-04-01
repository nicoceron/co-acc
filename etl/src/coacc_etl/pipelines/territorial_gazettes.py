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


class TerritorialGazettesPipeline(Pipeline):
    """Load territorial gazettes and acuerdos as documentary evidence."""

    name = "territorial_gazettes"
    source_id = "gacetas_territoriales"

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
        self.gazettes: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.gazette_doc_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "territorial_gazettes" / "territorial_gazettes.csv"
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
        gazettes: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
        gazette_doc_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            number = clean_text(row.get("numero_de_acuerdo"))
            title = clean_text(row.get("titulo")) or number
            if not title:
                continue

            publication_date = parse_iso_date(row.get("fecha_de_sancion"))
            gazette_id = stable_id(
                "gaceta",
                number,
                title,
                clean_text(row.get("gaceta_no")),
                publication_date,
            )
            summary = clean_text(row.get("tema"))
            gazettes.append(
                {
                    "gaceta_id": gazette_id,
                    "title": title,
                    "name": title,
                    "summary": summary,
                    "number": number,
                    "year": clean_text(row.get("a_o")) or clean_text(row.get("ano")),
                    "gaceta_number": clean_text(row.get("gaceta_no")),
                    "tema": summary,
                    "commission": clean_text(row.get("comision")),
                    "fecha_de_sancion": publication_date,
                    "publication_date": publication_date,
                    "search_text": " ".join(
                        value
                        for value in (title, summary, clean_text(row.get("comision")), number)
                        if value
                    ),
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            doc_id = stable_id("gaceta_doc", gazette_id, title)
            documents.append(
                {
                    "doc_id": doc_id,
                    "title": title,
                    "name": title,
                    "summary": summary,
                    "publication_date": publication_date,
                    "document_kind": "territorial_gazette",
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            gazette_doc_rels.append(
                {"source_key": gazette_id, "target_key": doc_id, "source": self.source_id}
            )

        self.gazettes = deduplicate_rows(gazettes, ["gaceta_id"])
        self.documents = deduplicate_rows(documents, ["doc_id"])
        self.gazette_doc_rels = deduplicate_rows(
            gazette_doc_rels,
            ["source_key", "target_key"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.gazettes:
            loaded += loader.load_nodes("GacetaTerritorial", self.gazettes, key_field="gaceta_id")
        if self.documents:
            loaded += loader.load_nodes("SourceDocument", self.documents, key_field="doc_id")
        if self.gazette_doc_rels:
            loaded += loader.load_relationships(
                rel_type="RESPALDADO_POR",
                rows=self.gazette_doc_rels,
                source_label="GacetaTerritorial",
                source_key="gaceta_id",
                target_label="SourceDocument",
                target_key="doc_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
