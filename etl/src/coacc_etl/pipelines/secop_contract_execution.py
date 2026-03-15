from __future__ import annotations

# ruff: noqa: E501
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    ContractSummaryLookup,
    merge_limited_unique,
    summary_map_csv_path,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_amount,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopContractExecutionPipeline(Pipeline):
    """Aggregate SECOP II contract execution milestones onto Contract nodes."""

    name = "secop_contract_execution"
    source_id = "secop_contract_execution"

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
        return Path(self.data_dir) / "secop_contract_execution" / "secop_contract_execution.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_contract_execution] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _aggregate_frame(
        self,
        frame: pd.DataFrame,
        summary_lookup: dict[str, str],
    ) -> list[dict[str, Any]]:
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("identificadorcontrato"))
            if not contract_id:
                continue
            summary_id = summary_lookup.get(contract_id)
            if not summary_id:
                continue

            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "execution_item_count": 0,
                    "execution_expected_progress_max": None,
                    "execution_actual_progress_max": None,
                    "execution_latest_expected_date": None,
                    "execution_latest_actual_date": None,
                    "execution_status": "",
                    "source": "secop_contract_execution",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            current["execution_item_count"] += 1
            expected = parse_amount(row.get("porcentajedeavanceesperado"))
            actual = parse_amount(row.get("porcentaje_de_avance_real"))
            if expected is not None:
                current["execution_expected_progress_max"] = max(
                    float(current["execution_expected_progress_max"] or 0),
                    expected,
                )
            if actual is not None:
                current["execution_actual_progress_max"] = max(
                    float(current["execution_actual_progress_max"] or 0),
                    actual,
                )

            expected_date = parse_iso_date(row.get("fechadeentregaesperada"))
            actual_date = parse_iso_date(row.get("fechadeentregareal"))
            if expected_date:
                current["execution_latest_expected_date"] = max(
                    current["execution_latest_expected_date"] or expected_date,
                    expected_date,
                )
            if actual_date:
                current["execution_latest_actual_date"] = max(
                    current["execution_latest_actual_date"] or actual_date,
                    actual_date,
                )

            status = clean_text(row.get("estado_del_contrato"))
            if status:
                current["execution_status"] = status
            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                contract_id,
            )

        return deduplicate_rows(list(procurement_map.values()), ["summary_id"])

    def transform(self) -> None:
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        try:
            contract_ids = [
                clean_text(value)
                for value in self._raw.get("identificadorcontrato", pd.Series(dtype=str)).tolist()
            ]
            self.procurements = self._aggregate_frame(self._raw, lookup.lookup_many(contract_ids))
        except FileNotFoundError:
            logger.warning("[%s] contract summary map not found; skipping", self.name)
            self.procurements = []
        finally:
            lookup.close()

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        query = """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.execution_item_count =
                    coalesce(r.execution_item_count, 0) + row.execution_item_count,
                r.execution_expected_progress_max = CASE
                    WHEN r.execution_expected_progress_max IS NULL
                      OR (
                        row.execution_expected_progress_max IS NOT NULL
                        AND row.execution_expected_progress_max > r.execution_expected_progress_max
                      )
                    THEN row.execution_expected_progress_max
                    ELSE r.execution_expected_progress_max
                END,
                r.execution_actual_progress_max = CASE
                    WHEN r.execution_actual_progress_max IS NULL
                      OR (
                        row.execution_actual_progress_max IS NOT NULL
                        AND row.execution_actual_progress_max > r.execution_actual_progress_max
                      )
                    THEN row.execution_actual_progress_max
                    ELSE r.execution_actual_progress_max
                END,
                r.execution_latest_expected_date = CASE
                    WHEN r.execution_latest_expected_date IS NULL
                      OR (
                        row.execution_latest_expected_date IS NOT NULL
                        AND row.execution_latest_expected_date > r.execution_latest_expected_date
                      )
                    THEN row.execution_latest_expected_date
                    ELSE r.execution_latest_expected_date
                END,
                r.execution_latest_actual_date = CASE
                    WHEN r.execution_latest_actual_date IS NULL
                      OR (
                        row.execution_latest_actual_date IS NOT NULL
                        AND row.execution_latest_actual_date > r.execution_latest_actual_date
                      )
                    THEN row.execution_latest_actual_date
                    ELSE r.execution_latest_actual_date
                END,
                r.execution_status = CASE
                    WHEN row.execution_status <> '' THEN row.execution_status
                    ELSE r.execution_status
                END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
        """
        self.rows_loaded = loader.run_query(query, self.procurements)

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

        loader = Neo4jBatchLoader(self.driver)
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.execution_item_count =
                    coalesce(r.execution_item_count, 0) + row.execution_item_count,
                r.execution_expected_progress_max = CASE
                    WHEN r.execution_expected_progress_max IS NULL
                      OR (
                        row.execution_expected_progress_max IS NOT NULL
                        AND row.execution_expected_progress_max > r.execution_expected_progress_max
                      )
                    THEN row.execution_expected_progress_max
                    ELSE r.execution_expected_progress_max
                END,
                r.execution_actual_progress_max = CASE
                    WHEN r.execution_actual_progress_max IS NULL
                      OR (
                        row.execution_actual_progress_max IS NOT NULL
                        AND row.execution_actual_progress_max > r.execution_actual_progress_max
                      )
                    THEN row.execution_actual_progress_max
                    ELSE r.execution_actual_progress_max
                END,
                r.execution_latest_expected_date = CASE
                    WHEN r.execution_latest_expected_date IS NULL
                      OR (
                        row.execution_latest_expected_date IS NOT NULL
                        AND row.execution_latest_expected_date > r.execution_latest_expected_date
                      )
                    THEN row.execution_latest_expected_date
                    ELSE r.execution_latest_expected_date
                END,
                r.execution_latest_actual_date = CASE
                    WHEN r.execution_latest_actual_date IS NULL
                      OR (
                        row.execution_latest_actual_date IS NOT NULL
                        AND row.execution_latest_actual_date > r.execution_latest_actual_date
                      )
                    THEN row.execution_latest_actual_date
                    ELSE r.execution_latest_actual_date
                END,
                r.execution_status = CASE
                    WHEN row.execution_status <> '' THEN row.execution_status
                    ELSE r.execution_status
                END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
        """
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))

        try:
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                contract_ids = [
                    clean_text(value)
                    for value in chunk.get("identificadorcontrato", pd.Series(dtype=str)).tolist()
                ]
                procurements = self._aggregate_frame(chunk, lookup.lookup_many(contract_ids))
                if procurements:
                    loaded += loader.run_query(query, procurements)

                processed += len(chunk)
                self.rows_in = processed
                self.rows_loaded = loaded
                if processed >= next_log_at:
                    logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                    next_log_at += self.chunk_size * 10
        except FileNotFoundError:
            logger.warning("[%s] contract summary map not found; skipping", self.name)
        finally:
            lookup.close()
