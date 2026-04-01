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


class JudicialCasesPipeline(Pipeline):
    """Load public judicial-decision records as documentary evidence nodes."""

    name = "judicial_cases"
    source_id = "judicial_providencias"

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
        self.cases: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.case_doc_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "judicial_cases" / "judicial_cases.csv"
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
        cases: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
        case_doc_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            radicado = clean_text(row.get("radicado"))
            title = (
                clean_text(row.get("nombre"))
                or clean_text(row.get("asunto"))
                or radicado
            )
            if not title:
                continue

            publication_date = parse_iso_date(row.get("fecha_providencia"))
            despacho = clean_text(row.get("despacho"))
            source_url = extract_url(row.get("hipervinculo"))
            case_id = stable_id(
                "judicial_case",
                radicado,
                title,
                publication_date,
                despacho,
            )
            search_text = " ".join(
                value
                for value in (
                    title,
                    clean_text(row.get("asunto")),
                    despacho,
                    clean_text(row.get("macro_caso")),
                    radicado,
                )
                if value
            )
            cases.append(
                {
                    "case_id": case_id,
                    "title": title,
                    "name": title,
                    "summary": clean_text(row.get("asunto")),
                    "radicado": radicado,
                    "document_type": clean_text(row.get("tipo_documento")),
                    "dispatch": despacho,
                    "macro_case": clean_text(row.get("macro_caso")),
                    "orfeo_reference": clean_text(row.get("radicado_orfeo_conti")),
                    "publication_date": publication_date,
                    "source_url": source_url,
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            document_id = stable_id("judicial_case_doc", case_id, source_url, title)
            documents.append(
                {
                    "doc_id": document_id,
                    "title": title,
                    "name": title,
                    "summary": clean_text(row.get("asunto")),
                    "source_url": source_url,
                    "publication_date": publication_date,
                    "document_kind": "judicial_case",
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            case_doc_rels.append(
                {"source_key": case_id, "target_key": document_id, "source": self.source_id}
            )

        self.cases = deduplicate_rows(cases, ["case_id"])
        self.documents = deduplicate_rows(documents, ["doc_id"])
        self.case_doc_rels = deduplicate_rows(case_doc_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.cases:
            loaded += loader.load_nodes("JudicialCase", self.cases, key_field="case_id")
        if self.documents:
            loaded += loader.load_nodes("SourceDocument", self.documents, key_field="doc_id")
        if self.case_doc_rels:
            loaded += loader.load_relationships(
                rel_type="RESPALDADO_POR",
                rows=self.case_doc_rels,
                source_label="JudicialCase",
                source_key="case_id",
                target_label="SourceDocument",
                target_key="doc_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
