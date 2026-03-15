from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _period_to_date(period: str) -> str | None:
    value = clean_text(period)
    if len(value) == 8:
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return None


class SgrExpenseExecutionPipeline(Pipeline):
    """Load SGR expense execution rows as Finance nodes tied to third parties."""

    name = "sgr_expense_execution"
    source_id = "sgr_expense_execution"

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
            / "sgr_expense_execution"
            / "sgr_expense_execution.csv"
        )
        if not csv_path.exists():
            logger.warning("[sgr_expense_execution] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        finance_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            third_party_id = strip_document(clean_text(row.get("cod_terceros")))
            bpin = clean_text(row.get("bpin"))
            period = clean_text(row.get("periodo"))
            account = clean_text(row.get("cuenta"))
            if not third_party_id:
                continue

            finance_id = f"sgr_{period}_{bpin or 'na'}_{third_party_id}_{account or 'na'}"
            third_party_name = clean_name(row.get("nom_terceros")) or third_party_id
            company_map[third_party_id] = {
                "document_id": third_party_id,
                "nit": third_party_id,
                "name": third_party_name,
                "razao_social": third_party_name,
                "source": "sgr_expense_execution",
                "country": "CO",
            }

            finance_map[finance_id] = {
                "finance_id": finance_id,
                "name": clean_text(row.get("nombre_cuenta")) or finance_id,
                "type": "SGR_EXPENSE_EXECUTION",
                "value": parse_amount(row.get("pagos"))
                or parse_amount(row.get("obligaciones"))
                or parse_amount(row.get("compromisos")),
                "date": _period_to_date(period),
                "entity_name": clean_text(row.get("nombre_entidad")),
                "project_id": bpin,
                "program_name": clean_text(row.get("nom_programatico_mga")),
                "sector_name": clean_text(row.get("nom_sector")),
                "funding_source": clean_text(row.get("nom_fuentes_financiacion")),
                "resource_type": clean_text(row.get("nom_tipo_recursos")),
                "account_code": account,
                "account_name": clean_text(row.get("nombre_cuenta")),
                "source": "sgr_expense_execution",
                "country": "CO",
            }

            rels.append({
                "source_key": third_party_id,
                "target_key": finance_id,
                "source": "sgr_expense_execution",
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.finances = deduplicate_rows(list(finance_map.values()), ["finance_id"])
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
                rel_type="FORNECEU",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Finance",
                target_key="finance_id",
                properties=["source"],
            )

        self.rows_loaded = loaded
