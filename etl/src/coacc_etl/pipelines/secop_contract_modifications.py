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
    parse_integer,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopContractModificationsPipeline(Pipeline):
    """Aggregate SECOP II modification metadata onto Contract nodes."""

    name = "secop_contract_modifications"
    source_id = "secop_contract_modifications"

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
        return (
            Path(self.data_dir)
            / "secop_contract_modifications"
            / "secop_contract_modifications.csv"
        )

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_contract_modifications] file not found: %s", csv_path)
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
            contract_id = clean_text(row.get("id_contrato"))
            if not contract_id:
                continue
            summary_id = summary_lookup.get(contract_id)
            if not summary_id:
                continue

            purpose = clean_text(row.get("proposito_modificacion"))
            status = clean_text(row.get("estado_modificacion"))
            days_extended = parse_integer(row.get("dias_extendidos"))
            modification_date = parse_iso_date(row.get("fecha_de_aprobacion")) or parse_iso_date(
                row.get("fecha_version")
            )
            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "modification_event_count": 0,
                    "modification_total_value": 0.0,
                    "latest_modification_purpose": "",
                    "latest_modification_status": "",
                    "latest_modification_date": None,
                    "max_extended_days": None,
                    "source": "secop_contract_modifications",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            current["modification_event_count"] += 1
            current["modification_total_value"] += (
                parse_amount(row.get("valor_modificacion")) or 0.0
            )

            if days_extended is not None:
                current["max_extended_days"] = max(
                    int(current["max_extended_days"] or 0),
                    days_extended,
                )
            if modification_date and (
                current["latest_modification_date"] is None
                or modification_date >= current["latest_modification_date"]
            ):
                current["latest_modification_date"] = modification_date
                if purpose:
                    current["latest_modification_purpose"] = purpose
                if status:
                    current["latest_modification_status"] = status
            else:
                if purpose and not current["latest_modification_purpose"]:
                    current["latest_modification_purpose"] = purpose
                if status and not current["latest_modification_status"]:
                    current["latest_modification_status"] = status
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
                for value in self._raw.get("id_contrato", pd.Series(dtype=str)).tolist()
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
            WITH r, row, r.latest_modification_date AS prev_latest_date, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.modification_event_count =
                    coalesce(r.modification_event_count, 0) + row.modification_event_count,
                r.modification_total_value =
                    coalesce(r.modification_total_value, 0.0) + row.modification_total_value,
                r.latest_modification_date = CASE
                    WHEN prev_latest_date IS NULL
                      OR (
                        row.latest_modification_date IS NOT NULL
                        AND row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_date
                    ELSE prev_latest_date
                END,
                r.latest_modification_purpose = CASE
                    WHEN row.latest_modification_purpose <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_modification_date IS NULL
                        OR row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_purpose
                    ELSE coalesce(
                        r.latest_modification_purpose,
                        row.latest_modification_purpose
                    )
                END,
                r.latest_modification_status = CASE
                    WHEN row.latest_modification_status <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_modification_date IS NULL
                        OR row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_status
                    ELSE coalesce(
                        r.latest_modification_status,
                        row.latest_modification_status
                    )
                END,
                r.max_extended_days = CASE
                    WHEN r.max_extended_days IS NULL
                      OR (
                        row.max_extended_days IS NOT NULL
                        AND row.max_extended_days > r.max_extended_days
                      )
                    THEN row.max_extended_days
                    ELSE r.max_extended_days
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
            WITH r, row, r.latest_modification_date AS prev_latest_date, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.modification_event_count =
                    coalesce(r.modification_event_count, 0) + row.modification_event_count,
                r.modification_total_value =
                    coalesce(r.modification_total_value, 0.0) + row.modification_total_value,
                r.latest_modification_date = CASE
                    WHEN prev_latest_date IS NULL
                      OR (
                        row.latest_modification_date IS NOT NULL
                        AND row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_date
                    ELSE prev_latest_date
                END,
                r.latest_modification_purpose = CASE
                    WHEN row.latest_modification_purpose <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_modification_date IS NULL
                        OR row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_purpose
                    ELSE coalesce(
                        r.latest_modification_purpose,
                        row.latest_modification_purpose
                    )
                END,
                r.latest_modification_status = CASE
                    WHEN row.latest_modification_status <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_modification_date IS NULL
                        OR row.latest_modification_date >= prev_latest_date
                      )
                    THEN row.latest_modification_status
                    ELSE coalesce(
                        r.latest_modification_status,
                        row.latest_modification_status
                    )
                END,
                r.max_extended_days = CASE
                    WHEN r.max_extended_days IS NULL
                      OR (
                        row.max_extended_days IS NOT NULL
                        AND row.max_extended_days > r.max_extended_days
                      )
                    THEN row.max_extended_days
                    ELSE r.max_extended_days
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
                    for value in chunk.get("id_contrato", pd.Series(dtype=str)).tolist()
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
