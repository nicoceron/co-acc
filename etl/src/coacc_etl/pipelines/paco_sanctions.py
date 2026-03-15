from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    ContractSummaryLookup,
    build_company_row,
    summary_map_csv_path,
)
from coacc_etl.pipelines.colombia_shared import (
    build_person_name,
    clean_name,
    clean_text,
    parse_amount,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

_MULTAS_SECOP_COLUMNS = [
    "entity_name",
    "entity_nit",
    "territory_scope",
    "entity_level",
    "resolution",
    "contractor_document_id",
    "contractor_name",
    "contract_id",
    "sanction_value",
    "decision_date",
    "source_url",
    "extra_1",
    "extra_2",
    "department",
    "municipality",
]

_SIRI_COLUMNS = [
    "record_id",
    "case_type",
    "subject_type",
    "subject_order",
    "document_type",
    "document_id",
    "surname_1",
    "surname_2",
    "given_name_1",
    "given_name_2",
    "position_name",
    "position_department",
    "position_municipality",
    "sanction_type",
    "duration_years",
    "duration_months",
    "duration_days",
    "instance",
    "issuing_authority",
    "decision_date",
    "process_number",
    "entity_name",
    "authority_department",
    "authority_municipality",
    "decision_year",
    "decision_month",
    "decision_day",
    "duration_text",
]

_COMPANY_MARKERS = (
    " S.A.S",
    " SAS",
    " S.A.",
    " S.A",
    " LTDA",
    " LIMITADA",
    " CIA",
    " COMPAÑ",
    " COMPANIA",
    " COOPERATIVA",
    " CONSORCIO",
    " UNION TEMPORAL",
    " FUNDACION",
    " FUNDACIÓN",
    " ASOCIACION",
    " ASOCIACIÓN",
    " CORPORACION",
    " CORPORACIÓN",
    " E.S.P",
    " IPS",
    " ESE",
    " S EN C",
)


def _looks_like_company(name: str) -> bool:
    normalized = clean_text(name).upper()
    if not normalized:
        return False
    return any(marker in normalized for marker in _COMPANY_MARKERS)


class PacoSanctionsPipeline(Pipeline):
    """Load PACO sanctions and public risk feeds into person/company sanction edges."""

    name = "paco_sanctions"
    source_id = "paco_sanctions"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.fiscal_raw: pd.DataFrame = pd.DataFrame()
        self.collusion_raw: pd.DataFrame = pd.DataFrame()
        self.siri_raw: pd.DataFrame = pd.DataFrame()
        self.secop_fines_raw: pd.DataFrame = pd.DataFrame()
        self.people: list[dict[str, Any]] = []
        self.companies: list[dict[str, Any]] = []
        self.contracts: list[dict[str, Any]] = []
        self.sanctions: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []
        self.contract_rels: list[dict[str, Any]] = []
        self.company_contract_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        base_dir = Path(self.data_dir) / "paco_sanctions"
        fiscal_path = base_dir / "responsabilidades_fiscales.csv"
        collusion_path = base_dir / "colusiones_en_contratacion.csv"
        siri_path = base_dir / "antecedentes_siri_sanciones.csv"
        secop_fines_path = base_dir / "multas_secop.csv"

        if fiscal_path.exists():
            self.fiscal_raw = read_csv_normalized_with_fallback(
                str(fiscal_path),
                dtype=str,
                keep_default_na=False,
            )
        if collusion_path.exists():
            self.collusion_raw = read_csv_normalized_with_fallback(
                str(collusion_path),
                dtype=str,
                keep_default_na=False,
            )
        if siri_path.exists():
            self.siri_raw = read_csv_normalized_with_fallback(
                str(siri_path),
                dtype=str,
                keep_default_na=False,
                header=None,
                names=_SIRI_COLUMNS,
            )
        if secop_fines_path.exists():
            self.secop_fines_raw = read_csv_normalized_with_fallback(
                str(secop_fines_path),
                dtype=str,
                keep_default_na=False,
                header=None,
                names=_MULTAS_SECOP_COLUMNS,
            )

        if self.limit:
            self.fiscal_raw = self.fiscal_raw.head(self.limit)
            self.collusion_raw = self.collusion_raw.head(self.limit)
            self.siri_raw = self.siri_raw.head(self.limit)
            self.secop_fines_raw = self.secop_fines_raw.head(self.limit)

        self.rows_in = sum(
            len(frame)
            for frame in (
                self.fiscal_raw,
                self.collusion_raw,
                self.siri_raw,
                self.secop_fines_raw,
            )
        )

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        person_map: dict[str, dict[str, Any]] = {}
        contract_map: dict[str, dict[str, Any]] = {}
        sanctions: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        contract_rels: list[dict[str, Any]] = []
        company_contract_rels: list[dict[str, Any]] = []

        summary_lookup: ContractSummaryLookup | None = None
        summary_path = summary_map_csv_path(self.data_dir)
        if summary_path.exists():
            summary_lookup = ContractSummaryLookup(summary_path)

        def attach_company(document_id: str, name: str, **extra: object) -> None:
            company_map[document_id] = build_company_row(
                document_id=document_id,
                name=clean_name(name) or clean_text(name) or document_id,
                source="paco_sanctions",
                **extra,
            )

        def attach_person(document_id: str, name: str, *, document_type: str = "") -> None:
            person_map[document_id] = {
                "document_id": document_id,
                "cedula": document_id,
                "name": clean_name(name) or clean_text(name) or document_id,
                "document_type": clean_text(document_type),
                "source": "paco_sanctions",
                "country": "CO",
            }

        def attach_contract_reference(
            *,
            sanction_id: str,
            raw_contract_id: object,
            company_document_id: str | None = None,
        ) -> None:
            contract_id = clean_text(raw_contract_id)
            if not contract_id:
                return

            canonical_contract_id = contract_id
            if summary_lookup is not None:
                summary_id = clean_text(summary_lookup.lookup_many([contract_id]).get(contract_id))
                if summary_id:
                    canonical_contract_id = summary_id

            contract_map[canonical_contract_id] = {
                "contract_id": canonical_contract_id,
                "name": f"Referenced contract {contract_id}",
                "summary_id": canonical_contract_id if canonical_contract_id != contract_id else "",
                "legacy_contract_id": contract_id,
                "source": "paco_sanctions",
                "country": "CO",
                "reference_origin": "SANCTION_RECORD",
            }
            contract_rels.append({
                "source_key": sanction_id,
                "target_key": canonical_contract_id,
                "source": "paco_sanctions",
                "reference_type": "sanction_contract",
            })
            if company_document_id:
                company_contract_rels.append({
                    "source_key": company_document_id,
                    "target_key": canonical_contract_id,
                    "source": "paco_sanctions",
                    "reference_type": "sanction_supplier_contract",
                })

        def link_entity(
            *,
            sanction_id: str,
            document_id: str,
            name: str,
            prefer_company: bool,
            document_type: str = "",
            **extra: object,
        ) -> None:
            if prefer_company:
                attach_company(document_id, name, **extra)
                company_rels.append({
                    "source_key": document_id,
                    "target_key": sanction_id,
                    "source": "paco_sanctions",
                })
                return
            attach_person(document_id, name, document_type=document_type)
            person_rels.append({
                "source_key": document_id,
                "target_key": sanction_id,
                "source": "paco_sanctions",
            })

        try:
            for row in self.fiscal_raw.to_dict(orient="records"):
                document_id = strip_document(row.get("tipo_y_num_docuemento"))
                subject_name = clean_text(row.get("responsable_fiscal"))
                if not document_id or not subject_name:
                    continue

                sanction_id = stable_id(
                    "paco_fiscal",
                    document_id,
                    row.get("entidad_afectada"),
                    row.get("ente_que_reporta"),
                    row.get("r"),
                )
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": "Fiscal responsibility finding",
                    "type": "PACO_FISCAL_RESPONSIBILITY",
                    "penalty_type": clean_text(row.get("tr")),
                    "issuing_entity": clean_text(row.get("ente_que_reporta")),
                    "entity_affected": clean_text(row.get("entidad_afectada")),
                    "department": clean_text(row.get("departamento")),
                    "municipality": clean_text(row.get("municipio")),
                    "case_rank": clean_text(row.get("r")),
                    "source": "paco_sanctions",
                    "country": "CO",
                })
                link_entity(
                    sanction_id=sanction_id,
                    document_id=document_id,
                    name=subject_name,
                    prefer_company=_looks_like_company(subject_name),
                    paco_entity_type="FISCAL_RESPONSIBILITY",
                )

            for row in self.collusion_raw.to_dict(orient="records"):
                document_id = strip_document(row.get("identificacion"))
                subject_name = clean_text(row.get("personas_sancionadas"))
                if not document_id or not subject_name:
                    continue

                person_type = clean_text(row.get("tipo_de_persona_sancionada")).upper()
                prefer_company = "JURID" in person_type or _looks_like_company(subject_name)
                sanction_id = stable_id(
                    "paco_collusion",
                    row.get("radicado"),
                    document_id,
                    row.get("resolucion_de_sancion"),
                )
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": clean_text(row.get("caso")) or "Procurement collusion finding",
                    "type": "PACO_PROCUREMENT_COLLUSION",
                    "penalty_type": clean_text(row.get("falta_que_origina_la_sancion")),
                    "value": parse_amount(row.get("multa_inicial")),
                    "date_start": parse_iso_date(row.get("fecha_de_radicacion")),
                    "issuing_entity": "Superintendencia de Industria y Comercio",
                    "case_id": clean_text(row.get("radicado")),
                    "case_name": clean_text(row.get("caso")),
                    "opening_resolution": clean_text(row.get("resolucion_de_apertura")),
                    "sanction_resolution": clean_text(row.get("resolucion_de_sancion")),
                    "source": "paco_sanctions",
                    "country": "CO",
                })
                link_entity(
                    sanction_id=sanction_id,
                    document_id=document_id,
                    name=subject_name,
                    prefer_company=prefer_company,
                    paco_entity_type="PROCUREMENT_COLLUSION",
                )

            for row in self.siri_raw.to_dict(orient="records"):
                document_id = strip_document(row.get("document_id"))
                if not document_id:
                    continue

                person_name = build_person_name(
                    row.get("given_name_1"),
                    row.get("given_name_2"),
                    row.get("surname_1"),
                    row.get("surname_2"),
                )
                sanction_type = clean_text(row.get("sanction_type"))
                sanction_id = stable_id(
                    "paco_siri",
                    row.get("record_id"),
                    document_id,
                    sanction_type,
                    row.get("process_number"),
                    row.get("instance"),
                )
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": sanction_type or "Disciplinary sanction",
                    "type": "PACO_DISCIPLINARY_SANCTION",
                    "penalty_type": sanction_type,
                    "date_start": parse_iso_date(row.get("decision_date")),
                    "issuing_entity": clean_text(row.get("issuing_authority")),
                    "entity_affected": clean_text(row.get("entity_name")),
                    "department": clean_text(row.get("authority_department")),
                    "municipality": clean_text(row.get("authority_municipality")),
                    "instance": clean_text(row.get("instance")),
                    "process_number": clean_text(row.get("process_number")),
                    "position_name": clean_text(row.get("position_name")),
                    "duration_text": clean_text(row.get("duration_text")),
                    "source": "paco_sanctions",
                    "country": "CO",
                })
                link_entity(
                    sanction_id=sanction_id,
                    document_id=document_id,
                    name=person_name,
                    document_type=clean_text(row.get("document_type")),
                    prefer_company=False,
                )

            for row in self.secop_fines_raw.to_dict(orient="records"):
                document_id = strip_document(row.get("contractor_document_id"))
                subject_name = clean_text(row.get("contractor_name"))
                if not document_id or not subject_name:
                    continue

                sanction_id = stable_id(
                    "paco_secop",
                    row.get("entity_nit"),
                    document_id,
                    row.get("contract_id"),
                    row.get("resolution"),
                )
                sanctions.append({
                    "sanction_id": sanction_id,
                    "name": clean_text(row.get("resolution")) or "SECOP fine",
                    "type": "PACO_SECOP_FINE",
                    "value": parse_amount(row.get("sanction_value")),
                    "date_start": parse_iso_date(row.get("decision_date")),
                    "issuing_entity": clean_text(row.get("entity_name")),
                    "contract_id": clean_text(row.get("contract_id")),
                    "url": clean_text(row.get("source_url")),
                    "department": clean_text(row.get("department")),
                    "municipality": clean_text(row.get("municipality")),
                    "source": "paco_sanctions",
                    "country": "CO",
                })
                link_entity(
                    sanction_id=sanction_id,
                    document_id=document_id,
                    name=subject_name,
                    prefer_company=_looks_like_company(subject_name),
                    paco_entity_type="SECOP_FINE",
                )
                attach_contract_reference(
                    sanction_id=sanction_id,
                    raw_contract_id=row.get("contract_id"),
                    company_document_id=document_id,
                )
        finally:
            if summary_lookup is not None:
                summary_lookup.close()

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.contracts = deduplicate_rows(list(contract_map.values()), ["contract_id"])
        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])
        self.company_rels = deduplicate_rows(company_rels, ["source_key", "target_key"])
        self.person_rels = deduplicate_rows(person_rels, ["source_key", "target_key"])
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
        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.contracts:
            loaded += loader.load_nodes("Contract", self.contracts, key_field="contract_id")
        if self.sanctions:
            loaded += loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")
        if self.company_rels:
            loaded += loader.load_relationships(
                rel_type="SANCIONADA",
                rows=self.company_rels,
                source_label="Company",
                source_key="document_id",
                target_label="Sanction",
                target_key="sanction_id",
                properties=["source"],
            )
        if self.person_rels:
            loaded += loader.load_relationships(
                rel_type="SANCIONADA",
                rows=self.person_rels,
                source_label="Person",
                source_key="document_id",
                target_label="Sanction",
                target_key="sanction_id",
                properties=["source"],
            )
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
