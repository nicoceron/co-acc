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
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopCdpRequestsPipeline(Pipeline):
    """Aggregate SECOP II CDP / SIIF request rows back onto contract summaries."""

    name = "secop_cdp_requests"
    source_id = "secop_cdp_requests"

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
        return Path(self.data_dir) / "secop_cdp_requests" / "secop_cdp_requests.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
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

            siif_date = parse_iso_date(row.get("fecha_consulta_siif")) or parse_iso_date(
                row.get("ultima_consulta_siif")
            )
            current = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "cdp_request_count": 0,
                    "cdp_commit_available_total": 0.0,
                    "cdp_balance_total": 0.0,
                    "cdp_future_balance_total": 0.0,
                    "cdp_used_value_total": 0.0,
                    "pgn_budget_total": 0.0,
                    "sgr_budget_total": 0.0,
                    "own_resources_total": 0.0,
                    "credit_resources_total": 0.0,
                    "latest_siif_check_date": None,
                    "latest_siif_status": "",
                    "latest_spending_destination": "",
                    "latest_cdp_code": "",
                    "registered_in_siif": False,
                    "bpin_code": "",
                    "source": "secop_cdp_requests",
                    "country": "CO",
                    "evidence_refs": [],
                },
            )
            current["cdp_request_count"] += 1
            current["cdp_commit_available_total"] += (
                parse_amount(row.get("saldo_total_a_comprometer")) or 0.0
            )
            current["cdp_balance_total"] += parse_amount(row.get("saldo_cdp")) or 0.0
            current["cdp_future_balance_total"] += (
                parse_amount(row.get("saldo_vigencias_futuras")) or 0.0
            )
            current["cdp_used_value_total"] += parse_amount(row.get("valor_utilizado")) or 0.0
            current["pgn_budget_total"] += (
                parse_amount(row.get("presupuesto_general_estado")) or 0.0
            )
            current["sgr_budget_total"] += (
                parse_amount(row.get("sistema_general_de_regal_as")) or 0.0
            )
            current["own_resources_total"] += (
                parse_amount(row.get("recursos_propios")) or 0.0
            ) + (parse_amount(row.get("recursos_propios_agri")) or 0.0)
            current["credit_resources_total"] += (
                parse_amount(row.get("recursos_de_credito")) or 0.0
            )

            siif_status = clean_text(row.get("estado_siif"))
            spending_destination = clean_text(row.get("destino_del_gasto"))
            cdp_code = clean_text(row.get("c_digo_cdp"))
            if siif_date and (
                current["latest_siif_check_date"] is None
                or siif_date >= current["latest_siif_check_date"]
            ):
                current["latest_siif_check_date"] = siif_date
                if siif_status:
                    current["latest_siif_status"] = siif_status
                if spending_destination:
                    current["latest_spending_destination"] = spending_destination
                if cdp_code:
                    current["latest_cdp_code"] = cdp_code
            else:
                if siif_status and not current["latest_siif_status"]:
                    current["latest_siif_status"] = siif_status
                if spending_destination and not current["latest_spending_destination"]:
                    current["latest_spending_destination"] = spending_destination
                if cdp_code and not current["latest_cdp_code"]:
                    current["latest_cdp_code"] = cdp_code

            current["registered_in_siif"] = bool(current["registered_in_siif"]) or bool(
                parse_flag(row.get("registrado_en_siif"))
            )
            if not current["bpin_code"]:
                current["bpin_code"] = clean_text(row.get("bpin_codigo"))

            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                contract_id,
                row.get("id_siif"),
                cdp_code,
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

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            WITH r, row, r.latest_siif_check_date AS prev_latest_date, coalesce(r.evidence_refs, []) AS prev_refs
            SET r.source = row.source,
                r.country = row.country,
                r.cdp_request_count =
                    coalesce(r.cdp_request_count, 0) + row.cdp_request_count,
                r.cdp_commit_available_total =
                    coalesce(r.cdp_commit_available_total, 0.0) + row.cdp_commit_available_total,
                r.cdp_balance_total =
                    coalesce(r.cdp_balance_total, 0.0) + row.cdp_balance_total,
                r.cdp_future_balance_total =
                    coalesce(r.cdp_future_balance_total, 0.0) + row.cdp_future_balance_total,
                r.cdp_used_value_total =
                    coalesce(r.cdp_used_value_total, 0.0) + row.cdp_used_value_total,
                r.pgn_budget_total =
                    coalesce(r.pgn_budget_total, 0.0) + row.pgn_budget_total,
                r.sgr_budget_total =
                    coalesce(r.sgr_budget_total, 0.0) + row.sgr_budget_total,
                r.own_resources_total =
                    coalesce(r.own_resources_total, 0.0) + row.own_resources_total,
                r.credit_resources_total =
                    coalesce(r.credit_resources_total, 0.0) + row.credit_resources_total,
                r.latest_siif_check_date = CASE
                    WHEN prev_latest_date IS NULL
                      OR (
                        row.latest_siif_check_date IS NOT NULL
                        AND row.latest_siif_check_date >= prev_latest_date
                      )
                    THEN row.latest_siif_check_date
                    ELSE prev_latest_date
                END,
                r.latest_siif_status = CASE
                    WHEN row.latest_siif_status <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_siif_check_date IS NULL
                        OR row.latest_siif_check_date >= prev_latest_date
                      )
                    THEN row.latest_siif_status
                    ELSE coalesce(r.latest_siif_status, row.latest_siif_status)
                END,
                r.latest_spending_destination = CASE
                    WHEN row.latest_spending_destination <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_siif_check_date IS NULL
                        OR row.latest_siif_check_date >= prev_latest_date
                      )
                    THEN row.latest_spending_destination
                    ELSE coalesce(
                        r.latest_spending_destination,
                        row.latest_spending_destination
                    )
                END,
                r.latest_cdp_code = CASE
                    WHEN row.latest_cdp_code <> ''
                      AND (
                        prev_latest_date IS NULL
                        OR row.latest_siif_check_date IS NULL
                        OR row.latest_siif_check_date >= prev_latest_date
                      )
                    THEN row.latest_cdp_code
                    ELSE coalesce(r.latest_cdp_code, row.latest_cdp_code)
                END,
                r.registered_in_siif =
                    coalesce(r.registered_in_siif, false)
                    OR coalesce(row.registered_in_siif, false),
                r.bpin_code = CASE
                    WHEN row.bpin_code <> '' THEN row.bpin_code
                    ELSE r.bpin_code
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
        finally:
            lookup.close()
