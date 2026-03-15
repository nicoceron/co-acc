from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import ContractSummaryLookup, summary_map_csv_path
from coacc_etl.pipelines.colombia_shared import clean_text, read_csv_normalized
from coacc_etl.transforms import deduplicate_rows, normalize_name, parse_date, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _clean_value(raw: object) -> str:
    value = str(raw or "").strip()
    if value.upper() in {"NO DEFINIDO", "N/D", "NULL", "NONE", "NAN"}:
        return ""
    return value


def _parse_amount(raw: object) -> float | None:
    value = _clean_value(raw).replace(",", "").replace("$", "")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


class SecopSanctionsPipeline(Pipeline):
    """Load SECOP I and SECOP II sanctions into the graph."""

    name = "secop_sanctions"
    source_id = "secop_sanctions"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.secop_i_raw: pd.DataFrame = pd.DataFrame()
        self.secop_ii_raw: pd.DataFrame = pd.DataFrame()
        self.companies: list[dict[str, Any]] = []
        self.contracts: list[dict[str, Any]] = []
        self.sanctions: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []
        self.contract_rels: list[dict[str, Any]] = []
        self.company_contract_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        sanctions_dir = Path(self.data_dir) / "secop_sanctions"
        secop_i_path = sanctions_dir / "secop_i_sanctions.csv"
        secop_ii_path = sanctions_dir / "secop_ii_sanctions.csv"

        if secop_i_path.exists():
            self.secop_i_raw = read_csv_normalized(
                str(secop_i_path), dtype=str, keep_default_na=False
            )
        if secop_ii_path.exists():
            self.secop_ii_raw = read_csv_normalized(
                str(secop_ii_path), dtype=str, keep_default_na=False
            )

        if self.limit:
            self.secop_i_raw = self.secop_i_raw.head(self.limit)
            self.secop_ii_raw = self.secop_ii_raw.head(self.limit)

        self.rows_in = len(self.secop_i_raw) + len(self.secop_ii_raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        contract_map: dict[str, dict[str, Any]] = {}
        sanctions: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        contract_rels: list[dict[str, Any]] = []
        company_contract_rels: list[dict[str, Any]] = []

        summary_lookup: ContractSummaryLookup | None = None
        summary_path = summary_map_csv_path(self.data_dir)
        if summary_path.exists():
            summary_lookup = ContractSummaryLookup(summary_path)

        def attach_contract_reference(
            *,
            sanction_id: str,
            raw_contract_id: object,
            supplier_document_id: str,
            source: str,
        ) -> None:
            contract_id = _clean_value(raw_contract_id)
            if not contract_id:
                return

            canonical_contract_id = contract_id
            if summary_lookup is not None:
                summary_id = clean_text(summary_lookup.lookup_many([contract_id]).get(contract_id))
                if summary_id:
                    canonical_contract_id = summary_id

            contract_map[canonical_contract_id] = {
                "contract_id": canonical_contract_id,
                "name": contract_id,
                "summary_id": canonical_contract_id if canonical_contract_id != contract_id else "",
                "legacy_contract_id": contract_id,
                "source": source,
                "country": "CO",
                "reference_origin": "SANCTION_RECORD",
            }
            contract_rels.append({
                "source_key": sanction_id,
                "target_key": canonical_contract_id,
                "source": source,
                "reference_type": "sanction_contract",
            })
            company_contract_rels.append({
                "source_key": supplier_document_id,
                "target_key": canonical_contract_id,
                "source": source,
                "reference_type": "sanction_supplier_contract",
            })

        try:
            for _, row in self.secop_i_raw.iterrows():
                contractor_document = strip_document(_clean_value(row.get("documento_contratista")))
                contractor_name = normalize_name(_clean_value(row.get("nombre_contratista")))
                sanction_id = (
                    f"secop_i_{_clean_value(row.get('numero_de_contrato'))}_"
                    f"{_clean_value(row.get('numero_de_resolucion'))}"
                ).strip("_")
                if not sanction_id or not contractor_document:
                    continue

                company_map[contractor_document] = {
                    "document_id": contractor_document,
                    "nit": contractor_document,
                    "name": contractor_name or contractor_document,
                    "razao_social": contractor_name or contractor_document,
                    "source": "secop_sanctions",
                    "country": "CO",
                }
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": _clean_value(row.get("numero_de_resolucion")) or sanction_id,
                    "type": "SECOP_I_SANCTION",
                    "penalty_type": _clean_value(row.get("numero_de_resolucion")),
                    "value": _parse_amount(row.get("valor_sancion")),
                    "date_start": parse_date(_clean_value(row.get("fecha_de_firmeza"))),
                    "issuing_entity": _clean_value(row.get("nombre_entidad")),
                    "municipality": _clean_value(row.get("municipio")),
                    "contract_id": _clean_value(row.get("numero_de_contrato")),
                    "url": _clean_value(row.get("ruta_de_proceso")),
                    "source": "secop_sanctions",
                    "country": "CO",
                })
                company_rels.append({
                    "source_key": contractor_document,
                    "target_key": sanction_id,
                    "source": "secop_sanctions",
                })
                attach_contract_reference(
                    sanction_id=sanction_id,
                    raw_contract_id=row.get("numero_de_contrato"),
                    supplier_document_id=contractor_document,
                    source="secop_sanctions",
                )

            for _, row in self.secop_ii_raw.iterrows():
                supplier_code = strip_document(_clean_value(row.get("as_codigo_proveedor_objeto")))
                supplier_name = normalize_name(_clean_value(row.get("nombre_proveedor_objeto_de")))
                sanction_id = (
                    f"secop_ii_{_clean_value(row.get('id_proceso'))}_"
                    f"{_clean_value(row.get('numero_de_acto'))}"
                ).strip("_")
                if not sanction_id or not supplier_code:
                    continue

                company_map[supplier_code] = {
                    "document_id": supplier_code,
                    "nit": supplier_code,
                    "name": supplier_name or supplier_code,
                    "razao_social": supplier_name or supplier_code,
                    "source": "secop_sanctions",
                    "country": "CO",
                }
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": _clean_value(row.get("tipo_de_sancion")) or sanction_id,
                    "type": "SECOP_II_SANCTION",
                    "penalty_type": _clean_value(row.get("tipo_de_sancion")),
                    "value": _parse_amount(row.get("valor")),
                    "value_paid": _parse_amount(row.get("valor_pagado")),
                    "date_start": parse_date(_clean_value(row.get("fecha_evento"))),
                    "issuing_entity": _clean_value(row.get("nombre_entidad_creadora")),
                    "contract_id": _clean_value(row.get("id_contrato")),
                    "process_id": _clean_value(row.get("id_proceso")),
                    "event_type": _clean_value(row.get("tipo")),
                    "source": "secop_sanctions",
                    "country": "CO",
                })
                company_rels.append({
                    "source_key": supplier_code,
                    "target_key": sanction_id,
                    "source": "secop_sanctions",
                })
                attach_contract_reference(
                    sanction_id=sanction_id,
                    raw_contract_id=row.get("id_contrato"),
                    supplier_document_id=supplier_code,
                    source="secop_sanctions",
                )
        finally:
            if summary_lookup is not None:
                summary_lookup.close()

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.contracts = deduplicate_rows(list(contract_map.values()), ["contract_id"])
        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])
        self.company_rels = deduplicate_rows(company_rels, ["source_key", "target_key"])
        self.contract_rels = deduplicate_rows(contract_rels, ["source_key", "target_key"])
        self.company_contract_rels = deduplicate_rows(
            company_contract_rels,
            ["source_key", "target_key", "reference_type"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.contracts:
            loaded += loader.load_nodes("Contract", self.contracts, key_field="contract_id")
        if self.sanctions:
            loaded += loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")
        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {document_id: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (c)-[r:SANCIONADA]->(s) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.company_rels)
        if self.contract_rels:
            loaded += loader.load_relationships(
                rel_type="REFERENTE_A",
                rows=self.contract_rels,
                source_label="Sanction",
                source_key="sanction_id",
                target_label="Contract",
                target_key="contract_id",
                properties=["source", "reference_type"],
            )
        if self.company_contract_rels:
            loaded += loader.load_relationships(
                rel_type="VENCEU",
                rows=self.company_contract_rels,
                source_label="Company",
                source_key="document_id",
                target_label="Contract",
                target_key="contract_id",
                properties=["source", "reference_type"],
            )

        self.rows_loaded = loaded
