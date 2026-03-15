from __future__ import annotations

import logging
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
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _territory_name(municipality: str, department: str) -> str:
    if municipality and department:
        return f"{municipality}, {department}"
    return municipality or department


class IgacPropertyTransactionsPipeline(Pipeline):
    """Aggregate IGAC property transactions into municipality-level market activity nodes."""

    name = "igac_property_transactions"
    source_id = "igac_property_transactions"

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
        csv_path = (
            Path(self.data_dir)
            / "igac_property_transactions"
            / "igac_property_transactions.csv"
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
        if self._raw.empty:
            self.companies = []
            self.finances = []
            self.rels = []
            return

        frame = self._raw.copy()
        for column in (
            "year_radica",
            "divipola",
            "departamento",
            "municipio",
            "nombre_natujur",
            "cod_natujur",
            "tipo_predio_zona",
            "categoria_ruralidad_2024",
        ):
            col_series = frame.get(column)
            if col_series is not None and hasattr(col_series, "map"):
                frame[column] = col_series.map(clean_text)
            else:
                frame[column] = ""

        def _map_col(name: str, func: Any) -> pd.Series:
            col = frame.get(name)
            if col is not None and hasattr(col, "map"):
                return col.map(func)
            return pd.Series([None] * len(frame))

        frame["valor_num"] = _map_col("valor", parse_amount).fillna(0.0)
        frame["count_a_num"] = _map_col("count_a", parse_integer).fillna(0)
        frame["count_de_num"] = _map_col("count_de", parse_integer).fillna(0)
        frame["predios_nuevos_num"] = _map_col("predios_nuevos", parse_integer).fillna(0)

        grouped = (
            frame.groupby(
                [
                    "year_radica",
                    "divipola",
                    "departamento",
                    "municipio",
                    "nombre_natujur",
                    "cod_natujur",
                    "tipo_predio_zona",
                    "categoria_ruralidad_2024",
                ],
                dropna=False,
            )
            .agg(
                transaction_count=("pk", "count"),
                total_value=("valor_num", "sum"),
                from_party_count=("count_a_num", "sum"),
                to_party_count=("count_de_num", "sum"),
                new_property_count=("predios_nuevos_num", "sum"),
            )
            .reset_index()
        )

        company_map: dict[str, dict[str, Any]] = {}
        finances: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for row in grouped.to_dict(orient="records"):
            year = clean_text(row.get("year_radica"))
            divipola = clean_text(row.get("divipola"))
            department = clean_name(row.get("departamento"))
            municipality = clean_name(row.get("municipio"))
            nature_name = clean_text(row.get("nombre_natujur"))
            territory = _territory_name(municipality, department)
            if not year or not territory:
                continue

            territory_document = stable_id("igac_territory", divipola, territory)
            merge_company(
                company_map,
                build_company_row(
                    document_id=territory_document,
                    name=territory,
                    source=self.source_id,
                    department=department,
                    municipality=municipality,
                    territory_code=divipola,
                    entity_type="territory",
                ),
            )

            transaction_count = int(row.get("transaction_count") or 0)
            total_value = float(row.get("total_value") or 0.0)
            average_value = total_value / transaction_count if transaction_count else None
            finance_id = stable_id(
                "igac_market_activity",
                year,
                divipola,
                nature_name,
                clean_text(row.get("tipo_predio_zona")),
                clean_text(row.get("categoria_ruralidad_2024")),
            )
            finances.append({
                "finance_id": finance_id,
                "name": f"IGAC property activity {territory} {year}",
                "type": "IGAC_PROPERTY_ACTIVITY",
                "date": f"{year}-12-31",
                "year": year,
                "territory_code": divipola,
                "department": department,
                "municipality": municipality,
                "legal_nature_code": clean_text(row.get("cod_natujur")),
                "legal_nature_name": nature_name,
                "zone_type": clean_text(row.get("tipo_predio_zona")),
                "rurality_category": clean_text(row.get("categoria_ruralidad_2024")),
                "transaction_count": transaction_count,
                "from_party_count": int(row.get("from_party_count") or 0),
                "to_party_count": int(row.get("to_party_count") or 0),
                "new_property_count": int(row.get("new_property_count") or 0),
                "average_value": average_value,
                "value": total_value,
                "source": self.source_id,
                "country": "CO",
            })
            rels.append({
                "source_key": territory_document,
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
                rel_type="ADMINISTRA",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Finance",
                target_key="finance_id",
                properties=["source"],
            )

        self.rows_loaded = loaded
