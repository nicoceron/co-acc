from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _infer_deceased(status: str, status_detail: str, raw_value: object) -> bool | None:
    explicit = parse_flag(raw_value)
    if explicit is not None:
        return explicit

    combined = f"{status} {status_detail}".upper()
    if not combined.strip():
        return None
    if any(token in combined for token in ("MUERTE", "FALLECID", "DEFUNCION")):
        return True
    if any(token in combined for token in ("VIGENTE", "ACTIVA", "VALIDA")):
        return False
    return None


class RegistraduriaDeathStatusChecksPipeline(Pipeline):
    """Load normalized Registraduria document-status checks into Person nodes."""

    name = "registraduria_death_status_checks"
    source_id = "registraduria_death_status_checks"

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
        self.people: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return (
            Path(self.data_dir)
            / "registraduria_death_status_checks"
            / "registraduria_death_status_checks.csv"
        )

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[registraduria_death_status_checks] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        people: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            document_id = strip_document(clean_text(row.get("document_id")))
            status = clean_text(row.get("status"))
            if not document_id or not status:
                continue

            status_detail = clean_text(row.get("status_detail"))
            people.append(
                {
                    "document_id": document_id,
                    "cedula": document_id,
                    "identity_status": status,
                    "identity_status_detail": status_detail,
                    "death_status_checked_at": parse_iso_date(row.get("checked_at")),
                    "is_deceased": _infer_deceased(status, status_detail, row.get("is_deceased")),
                    "status_source_url": clean_text(row.get("source_url")),
                    "source": "registraduria_death_status_checks",
                    "country": "CO",
                }
            )

        self.people = deduplicate_rows(people, ["document_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = 0
        if self.people:
            self.rows_loaded += loader.load_nodes("Person", self.people, key_field="document_id")
