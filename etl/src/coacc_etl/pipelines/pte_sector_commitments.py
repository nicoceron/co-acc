from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_amount,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class PteSectorCommitmentsPipeline(Pipeline):
    """Load current-year PGN sector commitments from PTE as Finance nodes."""

    name = "pte_sector_commitments"
    source_id = "pte_sector_commitments"

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
        self.finances: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "pte_sector_commitments" / "pte_sector_commitments.csv"
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
        finances: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            sector_code = clean_text(row.get("codigosector"))
            sector_name = clean_text(row.get("nombresector"))
            year = clean_text(row.get("anio"))
            if not sector_code or not sector_name or not year:
                continue

            finance_id = stable_id("pte_sector", sector_name, year)
            finances.append({
                "finance_id": finance_id,
                "name": f"PGN sector commitments {sector_name} {year}",
                "type": "PTE_SECTOR_COMMITMENT",
                "sector_code": sector_code,
                "sector_name": sector_name,
                "year": year,
                "contract_count": int(parse_amount(row.get("cantidadcontratos")) or 0),
                "value": parse_amount(row.get("valorcontratos")),
                "value_paid": parse_amount(row.get("valorejecutado")),
                "execution_ratio": parse_amount(row.get("porcentajeejecutado")),
                "source": "pte_sector_commitments",
                "country": "CO",
            })

        self.finances = deduplicate_rows(finances, ["finance_id"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = loader.load_nodes("Finance", self.finances, key_field="finance_id")
