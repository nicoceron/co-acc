from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import build_company_row, merge_company
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    parse_integer,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_YEAR_SUFFIX = re.compile(r"(20\d{2})$")


def _detect_years(columns: list[str]) -> tuple[int | None, int | None]:
    years = sorted({
        int(match.group(1))
        for column in columns
        if (match := _YEAR_SUFFIX.search(column))
    })
    if not years:
        return None, None
    current = years[-1]
    previous = years[-2] if len(years) > 1 else None
    return current, previous


def _column_for_year(columns: set[str], base: str, year: int | None) -> str | None:
    if year is None:
        return None
    candidate = f"{base}_{year}"
    return candidate if candidate in columns else None


class SupersocTopCompaniesPipeline(Pipeline):
    """Load Supersociedades top-company financial filings into Company and Finance nodes."""

    name = "supersoc_top_companies"
    source_id = "supersoc_top_companies"

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
        self.companies: list[dict[str, Any]] = []
        self.finances: list[dict[str, Any]] = []
        self.rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "supersoc_top_companies" / "supersoc_top_companies.csv"
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
        company_map: dict[str, dict[str, Any]] = {}
        finances: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        current_year, previous_year = _detect_years(list(self._raw.columns))
        column_set = set(self._raw.columns)

        revenue_current_col = _column_for_year(column_set, "ingresos_operacionales", current_year)
        revenue_previous_col = _column_for_year(column_set, "ingresos_operacionales", previous_year)
        profit_current_col = _column_for_year(column_set, "ganancia_perdida", current_year)
        profit_previous_col = _column_for_year(column_set, "ganancia_perdida", previous_year)
        assets_current_col = _column_for_year(column_set, "total_activos", current_year)
        assets_previous_col = _column_for_year(column_set, "total_activos", previous_year)
        liabilities_current_col = _column_for_year(column_set, "total_pasivos", current_year)
        liabilities_previous_col = _column_for_year(column_set, "total_pasivos", previous_year)
        equity_current_col = _column_for_year(column_set, "total_patrimonio", current_year)
        equity_previous_col = _column_for_year(column_set, "total_patrimonio", previous_year)

        raw_records: list[dict[str, Any]] = self._raw.to_dict(orient="records")  # type: ignore[assignment]
        for row in raw_records:
            document_id = strip_document(clean_text(row.get("nit")))
            name = clean_name(row.get("razon_social"))
            if not document_id or not name:
                continue

            ranking = parse_integer(row.get("no"))
            operating_revenue_current = self._parse_metric(row, revenue_current_col)
            total_assets_current = self._parse_metric(row, assets_current_col)
            total_liabilities_current = self._parse_metric(row, liabilities_current_col)
            total_equity_current = self._parse_metric(row, equity_current_col)
            net_profit_current = self._parse_metric(row, profit_current_col)

            merge_company(
                company_map,
                build_company_row(
                    document_id=document_id,
                    name=name,
                    source=self.source_id,
                    department=clean_text(row.get("departamento_domicilio")),
                    municipality=clean_text(row.get("ciudad_domicilio")),
                    sector=clean_text(row.get("macrosector")),
                    ciiu=clean_text(row.get("ciiu")),
                    supervisor=clean_text(row.get("supervisor")),
                    supersoc_company_rank=ranking,
                    supersoc_financial_year=str(current_year) if current_year else "",
                    supersoc_operating_revenue=operating_revenue_current,
                    supersoc_total_assets=total_assets_current,
                    supersoc_total_liabilities=total_liabilities_current,
                    supersoc_total_equity=total_equity_current,
                    supersoc_net_profit=net_profit_current,
                ),
            )

            finance_id = stable_id("supersoc_top_company", document_id, current_year or "")
            finances.append({
                "finance_id": finance_id,
                "name": f"Supersociedades top company {name}",
                "type": "SUPERSOC_TOP_COMPANY",
                "company_name": name,
                "company_rank": ranking,
                "financial_year": str(current_year) if current_year else None,
                "comparison_year": str(previous_year) if previous_year else None,
                "supervisor": clean_text(row.get("supervisor")),
                "region": clean_text(row.get("region")),
                "department": clean_text(row.get("departamento_domicilio")),
                "city": clean_text(row.get("ciudad_domicilio")),
                "ciiu": clean_text(row.get("ciiu")),
                "macrosector": clean_text(row.get("macrosector")),
                "ifrs_group": clean_text(row.get("grupo_en_niif")),
                "operating_revenue_current": operating_revenue_current,
                "operating_revenue_previous": self._parse_metric(row, revenue_previous_col),
                "net_profit_current": net_profit_current,
                "net_profit_previous": self._parse_metric(row, profit_previous_col),
                "total_assets_current": total_assets_current,
                "total_assets_previous": self._parse_metric(row, assets_previous_col),
                "total_liabilities_current": total_liabilities_current,
                "total_liabilities_previous": self._parse_metric(row, liabilities_previous_col),
                "total_equity_current": total_equity_current,
                "total_equity_previous": self._parse_metric(row, equity_previous_col),
                "value": operating_revenue_current,
                "date": f"{current_year}-12-31" if current_year else None,
                "source": self.source_id,
                "country": "CO",
            })
            rels.append({
                "source_key": document_id,
                "target_key": finance_id,
                "source": self.source_id,
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.finances = deduplicate_rows(finances, ["finance_id"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.finances:
            loaded += loader.load_nodes("Finance", self.finances, key_field="finance_id")
        if self.rels:
            loaded += loader.load_relationships(
                rel_type="DECLAROU_FINANCA",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Finance",
                target_key="finance_id",
                properties=["source"],
            )

        self.rows_loaded = loaded

    @staticmethod
    def _parse_metric(row: dict[str, Any], column_name: str | None) -> float | None:
        if not column_name:
            return None
        return parse_amount(row.get(column_name))
