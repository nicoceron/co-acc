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


class ControlPoliticoSessionsPipeline(Pipeline):
    """Load plenary-session records as oversight-session subnodes."""

    name = "control_politico_sessions"
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
        self.sessions: list[dict[str, Any]] = []
        self.session_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "control_politico_sessions"
            / "control_politico_sessions.csv"
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
        sessions: list[dict[str, Any]] = []
        session_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            session_no = clean_text(row.get("sesion_no"))
            date = parse_iso_date(row.get("fecha"))
            year = clean_text(row.get("a_o")) or clean_text(row.get("ano"))
            title = f"Sesión plenaria {session_no}".strip() if session_no else clean_text(row.get("detalle"))
            if not title:
                continue

            inquiry_id = stable_id("inquiry_session", year, session_no, date)
            search_text = " ".join(
                value
                for value in (
                    title,
                    clean_text(row.get("detalle")),
                    clean_text(row.get("lugar")),
                    year,
                )
                if value
            )
            inquiries.append(
                {
                    "inquiry_id": inquiry_id,
                    "title": title,
                    "summary": clean_text(row.get("detalle")),
                    "type": "CONTROL_POLITICO_SESSION",
                    "event_date": date,
                    "source_url": "",
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )

            session_id = stable_id("inquiry_session_node", inquiry_id, session_no, date)
            sessions.append(
                {
                    "session_id": session_id,
                    "title": title,
                    "name": title,
                    "summary": clean_text(row.get("detalle")),
                    "session_no": session_no,
                    "date": date,
                    "year": year,
                    "location": clean_text(row.get("lugar")),
                    "search_text": search_text,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            session_rels.append(
                {"source_key": session_id, "target_key": inquiry_id, "source": self.source_id}
            )

        self.inquiries = deduplicate_rows(inquiries, ["inquiry_id"])
        self.sessions = deduplicate_rows(sessions, ["session_id"])
        self.session_rels = deduplicate_rows(session_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.inquiries:
            loaded += loader.load_nodes("Inquiry", self.inquiries, key_field="inquiry_id")
        if self.sessions:
            loaded += loader.load_nodes("InquirySession", self.sessions, key_field="session_id")
        if self.session_rels:
            loaded += loader.load_relationships(
                rel_type="PARTE_DE",
                rows=self.session_rels,
                source_label="InquirySession",
                source_key="session_id",
                target_label="Inquiry",
                target_key="inquiry_id",
                properties=["source"],
            )
        self.rows_loaded = loaded
