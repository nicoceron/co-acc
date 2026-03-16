from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    build_company_row,
    make_company_document_id,
    merge_company,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_SPANISH_MONTHS = {
    "ENERO": "01",
    "FEBRERO": "02",
    "MARZO": "03",
    "ABRIL": "04",
    "MAYO": "05",
    "JUNIO": "06",
    "JULIO": "07",
    "AGOSTO": "08",
    "SEPTIEMBRE": "09",
    "OCTUBRE": "10",
    "NOVIEMBRE": "11",
    "DICIEMBRE": "12",
}


def _parse_pte_date(raw: object) -> str | None:
    value = (
        clean_text(raw)
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
    )
    if not value or "," not in value:
        return None
    _, rest = value.split(",", 1)
    parts = rest.strip().split()
    if len(parts) < 4:
        return None
    month = _SPANISH_MONTHS.get(parts[0].upper())
    day = parts[1].zfill(2)
    year = parts[3]
    if not month or len(year) != 4:
        return None
    return f"{year}-{month}-{day}"


class PteTopContractsPipeline(Pipeline):
    """Load PTE's top PGN contracts as Finance nodes linked to entities and beneficiaries."""

    name = "pte_top_contracts"
    source_id = "pte_top_contracts"

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
        self.admin_relations: list[dict[str, Any]] = []
        self.beneficiary_relations: list[dict[str, Any]] = []
        self.sector_relations: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "pte_top_contracts" / "pte_top_contracts.csv"
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
        admin_relations: list[dict[str, Any]] = []
        beneficiary_relations: list[dict[str, Any]] = []
        sector_relations: list[dict[str, Any]] = []

        for row in self._raw.to_dict(orient="records"):
            entity_name = clean_name(row.get("entidad"))
            beneficiary_name = clean_name(row.get("beneficiario"))
            entity_code = clean_text(row.get("codigoentidad"))
            commitment_number = clean_text(row.get("numerocompromiso"))
            year = clean_text(row.get("anio"))
            month = clean_text(row.get("mes"))
            sector_name = clean_text(row.get("sector"))
            date_registered = _parse_pte_date(row.get("fecharegistro"))
            if not entity_name or not beneficiary_name or not year:
                continue

            buyer_document = make_company_document_id(entity_code, entity_name, kind="buyer")
            beneficiary_document = make_company_document_id(
                "",
                beneficiary_name,
                kind="pte-beneficiary",
            )
            finance_id = stable_id(
                "pte_top_contract",
                entity_code,
                entity_name,
                beneficiary_name,
                commitment_number,
                year,
                month,
            )

            merge_company(
                company_map,
                build_company_row(
                    document_id=buyer_document,
                    name=entity_name,
                    source="pte_top_contracts",
                    entity_code=entity_code,
                    sector=sector_name,
                    subunit=clean_text(row.get("subunidad")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=beneficiary_document,
                    name=beneficiary_name,
                    source="pte_top_contracts",
                ),
            )

            finances.append({
                "finance_id": finance_id,
                "name": f"PGN top contract {beneficiary_name}",
                "type": "PTE_TOP_CONTRACT",
                "buyer_name": entity_name,
                "beneficiary_name": beneficiary_name,
                "entity_code": entity_code,
                "subunit": clean_text(row.get("subunidad")),
                "sector_name": sector_name,
                "commitment_number": commitment_number,
                "year": year,
                "month": month,
                "date": date_registered,
                "value": parse_amount(row.get("valorcontrato")),
                "value_paid": parse_amount(row.get("valorpagado")),
                "execution_ratio": parse_amount(row.get("porcentajepagado")),
                "is_contract": clean_text(row.get("escontrato")),
                "source": "pte_top_contracts",
                "country": "CO",
            })

            admin_relations.append({
                "source_key": buyer_document,
                "target_key": finance_id,
                "source": "pte_top_contracts",
            })
            beneficiary_relations.append({
                "source_key": beneficiary_document,
                "target_key": finance_id,
                "source": "pte_top_contracts",
            })

            if sector_name and year:
                sector_relations.append({
                    "source_key": finance_id,
                    "target_key": stable_id("pte_sector", sector_name, year),
                    "source": "pte_top_contracts",
                })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.finances = deduplicate_rows(finances, ["finance_id"])
        self.admin_relations = deduplicate_rows(admin_relations, ["source_key", "target_key"])
        self.beneficiary_relations = deduplicate_rows(
            beneficiary_relations,
            ["source_key", "target_key"],
        )
        self.sector_relations = deduplicate_rows(sector_relations, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.finances:
            loaded += loader.load_nodes("Finance", self.finances, key_field="finance_id")
        if self.admin_relations:
            loaded += loader.load_relationships(
                rel_type="ADMINISTRA",
                rows=self.admin_relations,
                source_label="Company",
                source_key="document_id",
                target_label="Finance",
                target_key="finance_id",
                properties=["source"],
            )
        if self.beneficiary_relations:
            loaded += loader.load_relationships(
                rel_type="BENEFICIO",
                rows=self.beneficiary_relations,
                source_label="Company",
                source_key="document_id",
                target_label="Finance",
                target_key="finance_id",
                properties=["source"],
            )
        if self.sector_relations:
            query = (
                "UNWIND $rows AS row "
                "MATCH (contract:Finance {finance_id: row.source_key}) "
                "MATCH (sector:Finance {finance_id: row.target_key}) "
                "MERGE (contract)-[r:REFERENTE_A]->(sector) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.sector_relations)

        self.rows_loaded = loaded
