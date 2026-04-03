from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import build_company_row
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, parse_iso_date, read_csv_normalized_with_fallback, stable_id
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class AdverseMediaPipeline(Pipeline):
    """Load reviewer-only adverse-media records with exact-identity linking when available."""

    name = "adverse_media"
    source_id = "adverse_media"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._rows: list[dict[str, Any]] = []
        self.media_items: list[dict[str, Any]] = []
        self.companies: list[dict[str, Any]] = []
        self.people: list[dict[str, Any]] = []
        self.company_mentions: list[dict[str, Any]] = []
        self.person_mentions: list[dict[str, Any]] = []

    def extract(self) -> None:
        json_path = Path(self.data_dir) / "adverse_media" / "adverse_media.json"
        csv_path = Path(self.data_dir) / "adverse_media" / "adverse_media.csv"
        rows: list[dict[str, Any]] = []
        if json_path.exists():
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
        elif csv_path.exists():
            frame = read_csv_normalized_with_fallback(
                str(csv_path),
                dtype=str,
                keep_default_na=False,
            )
            rows = frame.to_dict(orient="records")
        else:
            logger.warning("[%s] no adverse media input found under data/adverse_media", self.name)
            return

        if self.limit is not None:
            rows = rows[: self.limit]
        self._rows = rows
        self.rows_in = len(rows)

    def transform(self) -> None:
        media_items: list[dict[str, Any]] = []
        companies: list[dict[str, Any]] = []
        people: list[dict[str, Any]] = []
        company_mentions: list[dict[str, Any]] = []
        person_mentions: list[dict[str, Any]] = []

        for row in self._rows:
            title = clean_text(row.get("title"))
            url = clean_text(row.get("url"))
            if not title or not url:
                continue

            media_id = stable_id("media", title, url, row.get("published_at"))
            media_items.append(
                {
                    "media_id": media_id,
                    "title": title,
                    "name": title,
                    "summary": clean_text(row.get("summary") or row.get("excerpt")),
                    "url": url,
                    "published_at": parse_iso_date(row.get("published_at")),
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "public_safe": False,
                    "reviewer_only": True,
                    "country": "CO",
                }
            )

            company_document_id = strip_document(
                row.get("company_nit") or row.get("nit") or row.get("company_document_id")
            )
            if company_document_id:
                company_name = clean_name(row.get("company_name") or row.get("razon_social")) or company_document_id
                companies.append(
                    build_company_row(
                        document_id=company_document_id,
                        nit=company_document_id,
                        name=company_name,
                        source=self.source_id,
                        reviewer_only_source=True,
                    )
                )
                company_mentions.append(
                    {
                        "source_key": company_document_id,
                        "target_key": media_id,
                        "source": self.source_id,
                        "public_safe": False,
                    }
                )

            person_document_id = strip_document(
                row.get("person_document_id") or row.get("document_id") or row.get("numero_documento")
            )
            if person_document_id:
                person_name = clean_name(row.get("person_name") or row.get("name")) or person_document_id
                people.append(
                    {
                        "document_id": person_document_id,
                        "cedula": person_document_id,
                        "name": person_name,
                        "source": self.source_id,
                        "country": "CO",
                        "reviewer_only_source": True,
                    }
                )
                person_mentions.append(
                    {
                        "source_key": person_document_id,
                        "target_key": media_id,
                        "source": self.source_id,
                        "public_safe": False,
                    }
                )

        self.media_items = deduplicate_rows(media_items, ["media_id"])
        self.companies = deduplicate_rows(companies, ["document_id"])
        self.people = deduplicate_rows(people, ["document_id"])
        self.company_mentions = deduplicate_rows(company_mentions, ["source_key", "target_key"])
        self.person_mentions = deduplicate_rows(person_mentions, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.media_items:
            loaded += loader.load_nodes("MediaItem", self.media_items, key_field="media_id")
        if self.company_mentions:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {document_id: row.source_key}) "
                "MATCH (m:MediaItem {media_id: row.target_key}) "
                "MERGE (c)-[r:MENCIONADO_EN]->(m) "
                "SET r.source = row.source, "
                "    r.public_safe = row.public_safe"
            )
            loaded += loader.run_query(query, self.company_mentions)
        if self.person_mentions:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (m:MediaItem {media_id: row.target_key}) "
                "MERGE (p)-[r:MENCIONADO_EN]->(m) "
                "SET r.source = row.source, "
                "    r.public_safe = row.public_safe"
            )
            loaded += loader.run_query(query, self.person_mentions)
        self.rows_loaded = loaded
