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
from coacc_etl.pipelines.colombia_shared import clean_text, read_csv_normalized
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _validated_counts(raw_status: str) -> tuple[int, int]:
    status = clean_text(raw_status).lower()
    if not status:
        return 0, 0
    if "no valid" in status:
        return 0, 1
    if "valid" in status:
        return 1, 0
    return 0, 0


class SecopProcessBpinPipeline(Pipeline):
    """Aggregate SECOP II BPIN-by-process rows back onto contract summaries."""

    name = "secop_process_bpin"
    source_id = "secop_process_bpin"

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
        return Path(self.data_dir) / "secop_process_bpin" / "secop_process_bpin.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _portfolio_summary_map(self, summary_lookup: ContractSummaryLookup) -> dict[str, tuple[str, ...]]:
        contracts_path = Path(self.data_dir) / "secop_ii_contracts" / "secop_ii_contracts.csv"
        if not contracts_path.exists():
            return {}

        contracts = read_csv_normalized(str(contracts_path), dtype=str, keep_default_na=False)
        contract_ids = [
            clean_text(value)
            for value in contracts.get("id_contrato", pd.Series(dtype=str)).tolist()
        ]
        summary_by_contract = summary_lookup.lookup_many(contract_ids)
        portfolio_map: dict[str, set[str]] = {}

        for row in contracts.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_contrato"))
            portfolio_id = clean_text(row.get("proceso_de_compra"))
            summary_id = summary_by_contract.get(contract_id)
            if not portfolio_id or not summary_id:
                continue
            portfolio_map.setdefault(portfolio_id, set()).add(summary_id)

        return {key: tuple(sorted(value)) for key, value in portfolio_map.items()}

    def _aggregate_frame(
        self,
        frame: pd.DataFrame,
        summary_lookup: dict[str, str],
        portfolio_summary_map: dict[str, tuple[str, ...]],
    ) -> list[dict[str, Any]]:
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_contracto"))
            portfolio_id = clean_text(row.get("id_portafolio"))
            summary_ids: set[str] = set()
            if contract_id and contract_id in summary_lookup:
                summary_ids.add(summary_lookup[contract_id])
            if portfolio_id:
                summary_ids.update(portfolio_summary_map.get(portfolio_id, ()))
            if not summary_ids:
                continue

            bpin_code = clean_text(row.get("codigo_bpin"))
            bpin_year = clean_text(row.get("anno_bpin"))
            validation_status = clean_text(row.get("validacion_bpin"))
            validated_count, unvalidated_count = _validated_counts(validation_status)

            for summary_id in summary_ids:
                current = procurement_map.setdefault(
                    summary_id,
                    {
                        "summary_id": summary_id,
                        "bpin_link_count": 0,
                        "bpin_validated_count": 0,
                        "bpin_unvalidated_count": 0,
                        "bpin_code": "",
                        "bpin_year": "",
                        "bpin_validation_status": "",
                        "source": "secop_process_bpin",
                        "country": "CO",
                        "evidence_refs": [],
                    },
                )
                current["bpin_link_count"] += 1
                current["bpin_validated_count"] += validated_count
                current["bpin_unvalidated_count"] += unvalidated_count
                if bpin_code and bpin_code != "0":
                    current["bpin_code"] = bpin_code
                if bpin_year:
                    current["bpin_year"] = bpin_year
                if validation_status:
                    current["bpin_validation_status"] = validation_status
                current["evidence_refs"] = merge_limited_unique(
                    list(current.get("evidence_refs", [])),
                    contract_id,
                    row.get("id_proceso"),
                    portfolio_id,
                    bpin_code,
                )

        return deduplicate_rows(list(procurement_map.values()), ["summary_id"])

    def transform(self) -> None:
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        try:
            contract_ids = [
                clean_text(value)
                for value in self._raw.get("id_contracto", pd.Series(dtype=str)).tolist()
            ]
            summary_lookup = lookup.lookup_many(contract_ids)
            portfolio_summary_map = self._portfolio_summary_map(lookup)
            self.procurements = self._aggregate_frame(
                self._raw,
                summary_lookup,
                portfolio_summary_map,
            )
        except FileNotFoundError:
            logger.warning("[%s] contract summary inputs not found; skipping", self.name)
            self.procurements = []
        finally:
            lookup.close()

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.bpin_link_count = coalesce(r.bpin_link_count, 0) + row.bpin_link_count,
                r.bpin_validated_count = coalesce(r.bpin_validated_count, 0) + row.bpin_validated_count,
                r.bpin_unvalidated_count = coalesce(r.bpin_unvalidated_count, 0) + row.bpin_unvalidated_count,
                r.bpin_code = CASE
                    WHEN row.bpin_code <> '' AND row.bpin_code <> '0' THEN row.bpin_code
                    ELSE r.bpin_code
                END,
                r.bpin_year = CASE
                    WHEN row.bpin_year <> '' THEN row.bpin_year
                    ELSE r.bpin_year
                END,
                r.bpin_validation_status = CASE
                    WHEN row.bpin_validation_status <> '' THEN row.bpin_validation_status
                    ELSE r.bpin_validation_status
                END,
                r.evidence_refs = reduce(
                  acc = [],
                  item IN (prev_refs + row.evidence_refs) |
                    CASE WHEN item IN acc THEN acc ELSE acc + item END
                )[0..5]
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

        loader = Neo4jBatchLoader(self.driver)
        lookup = ContractSummaryLookup(summary_map_csv_path(self.data_dir))
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = self._load_query()

        try:
            portfolio_summary_map = self._portfolio_summary_map(lookup)
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                contract_ids = [
                    clean_text(value)
                    for value in chunk.get("id_contracto", pd.Series(dtype=str)).tolist()
                ]
                procurements = self._aggregate_frame(
                    chunk,
                    lookup.lookup_many(contract_ids),
                    portfolio_summary_map,
                )
                if procurements:
                    loaded += loader.run_query(query, procurements)

                processed += len(chunk)
                self.rows_in = processed
                self.rows_loaded = loaded
                if processed >= next_log_at:
                    logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                    next_log_at += self.chunk_size * 10
        finally:
            lookup.close()
