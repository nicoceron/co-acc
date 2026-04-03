from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import build_company_row
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, parse_amount, read_csv_normalized_with_fallback
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class RubBeneficialOwnersPipeline(Pipeline):
    """Load reviewer-only beneficial ownership records from RUB exports."""

    name = "rub_beneficial_owners"
    source_id = "rub_beneficial_owners"

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
        self.people: list[dict[str, Any]] = []
        self.owner_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "rub_beneficial_owners" / "rub_beneficial_owners.csv"
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
        companies: list[dict[str, Any]] = []
        people: list[dict[str, Any]] = []
        owner_rels: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            company_document_id = strip_document(
                row.get("company_nit") or row.get("nit") or row.get("numero_identificacion")
            )
            owner_document_id = strip_document(
                row.get("owner_document_id") or row.get("document_id") or row.get("numero_documento")
            )
            if not company_document_id or not owner_document_id:
                continue

            company_name = clean_name(row.get("company_name") or row.get("razon_social")) or company_document_id
            owner_name = clean_name(row.get("owner_name") or row.get("nombre")) or owner_document_id
            companies.append(
                build_company_row(
                    document_id=company_document_id,
                    nit=company_document_id,
                    name=company_name,
                    source=self.source_id,
                    reviewer_only_source=True,
                )
            )
            people.append(
                {
                    "document_id": owner_document_id,
                    "cedula": owner_document_id,
                    "name": owner_name,
                    "document_type": clean_text(row.get("owner_document_type") or row.get("tipo_documento")),
                    "source": self.source_id,
                    "country": "CO",
                    "reviewer_only_source": True,
                }
            )
            owner_rels.append(
                {
                    "source_key": company_document_id,
                    "target_key": owner_document_id,
                    "source": self.source_id,
                    "ownership_percentage": parse_amount(row.get("ownership_percentage") or row.get("participacion")),
                    "public_safe": False,
                    "reviewer_only": True,
                }
            )

        self.companies = deduplicate_rows(companies, ["document_id"])
        self.people = deduplicate_rows(people, ["document_id"])
        self.owner_rels = deduplicate_rows(owner_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.owner_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {document_id: row.source_key}) "
                "MATCH (p:Person {document_id: row.target_key}) "
                "MERGE (c)-[r:BENEFICIARIO_FINAL]->(p) "
                "SET r.source = row.source, "
                "    r.ownership_percentage = row.ownership_percentage, "
                "    r.public_safe = row.public_safe, "
                "    r.reviewer_only = row.reviewer_only"
            )
            loaded += loader.run_query(query, self.owner_rels)
        self.rows_loaded = loaded
