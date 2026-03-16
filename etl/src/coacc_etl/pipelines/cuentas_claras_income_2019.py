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
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class CuentasClarasIncome2019Pipeline(Pipeline):
    """Load 2019 campaign income into Election nodes and DONO_A/CANDIDATO_EM relationships."""

    name = "cuentas_claras_income_2019"
    source_id = "cuentas_claras_income_2019"

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
        self.people: list[dict[str, Any]] = []
        self.companies: list[dict[str, Any]] = []
        self.elections: list[dict[str, Any]] = []
        self.candidate_rels: list[dict[str, Any]] = []
        self.donation_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "cuentas_claras_income_2019"
            / "cuentas_claras_income_2019.csv"
        )
        if not csv_path.exists():
            logger.warning("[cuentas_claras_income_2019] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        people_map: dict[str, dict[str, Any]] = {}
        company_map: dict[str, dict[str, Any]] = {}
        election_map: dict[str, dict[str, Any]] = {}
        candidate_rels: list[dict[str, Any]] = []
        donation_rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            candidate_id = strip_document(clean_text(row.get("can_identificacion")))
            candidate_name = clean_name(row.get("nombre_candidato"))
            donor_id = strip_document(clean_text(row.get("ing_identificacion")))
            donor_name = clean_name(row.get("nombre_persona"))
            office_name = clean_text(row.get("cnd_nombre"))
            election_class = clean_text(row.get("cla_nombre"))
            department = clean_text(row.get("dep_nombre"))
            municipality = clean_text(row.get("mun_nombre"))
            election_id = f"cc2019_{candidate_id}_{clean_text(row.get('cco_id'))}"

            if not candidate_id or not donor_id:
                continue

            people_map[candidate_id] = {
                "document_id": candidate_id,
                "cedula": candidate_id,
                "name": candidate_name or candidate_id,
                "source": "cuentas_claras_income_2019",
                "country": "CO",
            }

            donor_type = clean_text(row.get("tpe_nombre")).lower()
            if "natural" in donor_type:
                people_map[donor_id] = {
                    "document_id": donor_id,
                    "cedula": donor_id,
                    "name": donor_name or donor_id,
                    "source": "cuentas_claras_income_2019",
                    "country": "CO",
                }
            else:
                company_map[donor_id] = {
                    "document_id": donor_id,
                    "nit": donor_id,
                    "name": donor_name or donor_id,
                    "razon_social": donor_name or donor_id,
                    "source": "cuentas_claras_income_2019",
                    "country": "CO",
                }

            election_map[election_id] = {
                "election_id": election_id,
                "name": candidate_name or election_id,
                "candidate_name": candidate_name,
                "candidate_document_id": candidate_id,
                "year": 2019,
                "cargo": office_name,
                "uf": department,
                "municipio": municipality,
                "party": clean_text(row.get("org_nombre")),
                "classification": election_class,
                "party_coalition": clean_text(row.get("partido_coalicion")),
                "source": "cuentas_claras_income_2019",
                "country": "CO",
            }

            candidate_rels.append({
                "source_key": candidate_id,
                "target_key": election_id,
                "source": "cuentas_claras_income_2019",
            })
            donation_rels.append({
                "source_key": donor_id,
                "target_key": election_id,
                "source": "cuentas_claras_income_2019",
                "value": parse_amount(row.get("ing_valor")),
                "date": parse_iso_date(row.get("ing_fecha_comprobante")),
                "type": clean_text(row.get("tdo_nombre")),
                "receipt": clean_text(row.get("ing_comprobante")),
                "concept": clean_text(row.get("ing_concepto")),
            })

        self.people = deduplicate_rows(list(people_map.values()), ["document_id"])
        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.elections = deduplicate_rows(list(election_map.values()), ["election_id"])
        self.candidate_rels = deduplicate_rows(candidate_rels, ["source_key", "target_key"])
        self.donation_rels = deduplicate_rows(
            donation_rels,
            ["source_key", "target_key", "receipt"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.elections:
            loaded += loader.load_nodes("Election", self.elections, key_field="election_id")
        if self.candidate_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (e:Election {election_id: row.target_key}) "
                "MERGE (p)-[r:CANDIDATO_EM]->(e) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.candidate_rels)
        if self.donation_rels:
            query = (
                "UNWIND $rows AS row "
                "OPTIONAL MATCH (p:Person {document_id: row.source_key}) "
                "OPTIONAL MATCH (c:Company {document_id: row.source_key}) "
                "WITH row, coalesce(p, c) AS donor "
                "WHERE donor IS NOT NULL "
                "MATCH (e:Election {election_id: row.target_key}) "
                "MERGE (donor)-[r:DONO_A {receipt: row.receipt}]->(e) "
                "SET r.source = row.source, "
                "    r.value = row.value, "
                "    r.date = row.date, "
                "    r.type = row.type, "
                "    r.concept = row.concept"
            )
            loaded += loader.run_query(query, self.donation_rels)

        self.rows_loaded = loaded
