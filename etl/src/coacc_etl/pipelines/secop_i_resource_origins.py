from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import merge_limited_unique
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_amount,
    read_csv_normalized,
    stable_id,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document


def secop_i_historical_summary_id(row: dict[str, Any]) -> str:
    return stable_id(
        "co_secop_i_hist",
        row.get("id_adjudicacion"),
        row.get("numero_de_constancia"),
        row.get("numero_de_contrato"),
        row.get("uid"),
    )

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopIResourceOriginsPipeline(Pipeline):
    """Attach SECOP I resource-origin rows to historical SECOP I contract summaries."""

    name = "secop_i_resource_origins"
    source_id = "secop_i_resource_origins"

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
        self.procurements: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_i_resource_origins" / "secop_i_resource_origins.csv"

    def _historical_csv_path(self) -> Path:
        return (
            Path(self.data_dir)
            / "secop_i_historical_processes"
            / "secop_i_historical_processes.csv"
        )

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _adjudication_summary_lookup(self) -> dict[str, str]:
        historical_csv_path = self._historical_csv_path()
        if not historical_csv_path.exists():
            raise FileNotFoundError(historical_csv_path)

        lookup: dict[str, str] = {}
        with historical_csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                adjudication_id = clean_text(row.get("id_adjudicacion"))
                if not adjudication_id:
                    continue
                lookup[adjudication_id] = secop_i_historical_summary_id(row)
        return lookup

    def _aggregate_frame(
        self,
        frame: pd.DataFrame,
        summary_lookup: dict[str, str],
    ) -> list[dict[str, Any]]:
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            adjudication_id = clean_text(row.get("id_adjudicacion"))
            if not adjudication_id:
                continue
            summary_id = summary_lookup.get(adjudication_id)
            if not summary_id:
                continue

            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "resource_origin_count": 0,
                    "resource_origins": [],
                    "resource_origin_total": 0.0,
                    "resource_origin_descriptions": [],
                    "bpin_code": "",
                    "source": self.source_id,
                    "evidence_refs": [],
                },
            )
            current["resource_origin_count"] += 1
            current["resource_origin_total"] += parse_amount(row.get("valor")) or 0.0
            current["resource_origins"] = merge_limited_unique(
                list(current.get("resource_origins", [])),
                row.get("orig_rec_nombre"),
                limit=8,
            )
            current["resource_origin_descriptions"] = merge_limited_unique(
                list(current.get("resource_origin_descriptions", [])),
                row.get("descripcion_otros_recursos"),
                limit=8,
            )
            if not current.get("bpin_code"):
                current["bpin_code"] = strip_document(clean_text(row.get("codigo_bpin")))
            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                adjudication_id,
                row.get("identificador"),
                limit=6,
            )

        return deduplicate_rows(list(procurement_map.values()), ["summary_id"])

    def transform(self) -> None:
        try:
            self.procurements = self._aggregate_frame(
                self._raw,
                self._adjudication_summary_lookup(),
            )
        except FileNotFoundError:
            logger.warning("[%s] SECOP I historical file not found; skipping", self.name)
            self.procurements = []

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row,
                 coalesce(r.resource_origins, []) AS prev_origins,
                 coalesce(r.resource_origin_descriptions, []) AS prev_descriptions,
                 coalesce(r.evidence_refs, []) AS prev_refs
            SET r.resource_origin_count =
                    coalesce(r.resource_origin_count, 0) + row.resource_origin_count,
                r.resource_origins = reduce(
                  acc = prev_origins,
                  item IN row.resource_origins |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..8],
                r.resource_origin_descriptions = reduce(
                  acc = prev_descriptions,
                  item IN row.resource_origin_descriptions |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..8],
                r.resource_origin_total =
                    coalesce(r.resource_origin_total, 0.0) + row.resource_origin_total,
                r.bpin_code = CASE WHEN row.bpin_code <> '' THEN row.bpin_code ELSE r.bpin_code END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..6]
        """

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = loader.run_query(self._load_query(), self.procurements)

    def run_streaming(self, start_phase: int = 1) -> None:
        if start_phase > 1:
            logger.info(
                "[%s] start_phase=%s ignored for single-phase streaming",
                self.name,
                start_phase,
            )

        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        try:
            summary_lookup = self._adjudication_summary_lookup()
        except FileNotFoundError:
            logger.warning("[%s] SECOP I historical file not found; skipping", self.name)
            return

        loader = Neo4jBatchLoader(self.driver)
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = self._load_query()

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            procurements = self._aggregate_frame(chunk, summary_lookup)
            if procurements:
                loaded += loader.run_query(query, procurements)
            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
