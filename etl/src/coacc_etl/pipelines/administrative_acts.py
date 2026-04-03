from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    extract_url,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class AdministrativeActsPipeline(Pipeline):
    """Load public administrative acts as first-class graph evidence."""

    name = "administrative_acts"
    source_id = "actos_administrativos"

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
        self.acts: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.act_doc_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "administrative_acts" / "administrative_acts.csv"
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
        acts: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
        act_doc_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            act_type = clean_text(row.get("acto_administrativo"))
            number = clean_text(row.get("n_mero")) or clean_text(row.get("numero"))
            description = clean_text(row.get("descripci_n")) or clean_text(row.get("descripcion"))
            year = clean_text(row.get("a_o")) or clean_text(row.get("ano"))
            title = " ".join(value for value in (act_type, number, description) if value).strip()
            if not title:
                continue

            act_id = stable_id("acto", act_type, number, year, description)
            publication_date = parse_iso_date(row.get("fecha"))
            source_url = extract_url(row.get("link_sitio_web"))
            search_text = " ".join(value for value in (title, description, year) if value)
            acts.append(
                {
                    "act_id": act_id,
                    "title": title,
                    "name": title,
                    "summary": description,
                    "number": number,
                    "year": year,
                    "type": act_type,
                    "publication_date": publication_date,
                    "source_url": source_url,
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            doc_id = stable_id("acto_doc", act_id, source_url, title)
            documents.append(
                {
                    "doc_id": doc_id,
                    "title": title,
                    "name": title,
                    "summary": description,
                    "source_url": source_url,
                    "publication_date": publication_date,
                    "document_kind": "administrative_act",
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            act_doc_rels.append(
                {"source_key": act_id, "target_key": doc_id, "source": self.source_id}
            )

        self.acts = deduplicate_rows(acts, ["act_id"])
        self.documents = deduplicate_rows(documents, ["doc_id"])
        self.act_doc_rels = deduplicate_rows(act_doc_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.acts:
            loaded += loader.load_nodes("ActoAdministrativo", self.acts, key_field="act_id")
        if self.documents:
            loaded += loader.load_nodes("SourceDocument", self.documents, key_field="doc_id")
        if self.act_doc_rels:
            loaded += loader.load_relationships(
                rel_type="RESPALDADO_POR",
                rows=self.act_doc_rels,
                source_label="ActoAdministrativo",
                source_key="act_id",
                target_label="SourceDocument",
                target_key="doc_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
