from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import ContractSummaryLookup, summary_map_csv_path
from coacc_etl.pipelines.colombia_shared import (
    build_person_name,
    clean_text,
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.pipelines.disclosure_mining import extract_disclosure_references
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _pick_value(row: pd.Series, *keys: str) -> object:
    for key in keys:
        value = row.get(key)
        if clean_text(value):
            return value
    return ""


class ConflictDisclosuresPipeline(Pipeline):
    """Load Ley 2013 conflict-of-interest disclosures as Finance nodes."""

    name = "conflict_disclosures"
    source_id = "conflict_disclosures"

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
        self.disclosures: list[dict[str, Any]] = []
        self.disclosure_rels: list[dict[str, Any]] = []
        self.contracts: list[dict[str, Any]] = []
        self.company_document_refs: list[dict[str, Any]] = []
        self.company_name_refs: list[dict[str, Any]] = []
        self.contract_refs: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "conflict_disclosures" / "conflict_disclosures.csv"
        if not csv_path.exists():
            logger.warning("[conflict_disclosures] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        person_map: dict[str, dict[str, Any]] = {}
        disclosure_map: dict[str, dict[str, Any]] = {}
        contract_map: dict[str, dict[str, Any]] = {}
        disclosure_rels: list[dict[str, Any]] = []
        company_document_refs: list[dict[str, Any]] = []
        company_name_refs: list[dict[str, Any]] = []
        contract_refs: list[dict[str, Any]] = []

        summary_lookup: ContractSummaryLookup | None = None
        summary_path = summary_map_csv_path(self.data_dir)
        if summary_path.exists():
            summary_lookup = ContractSummaryLookup(summary_path)

        try:
            for _, row in self._raw.iterrows():
                document_id = strip_document(clean_text(row.get("numero_documento")))
                form_id = clean_text(row.get("numero_formulario"))
                if not document_id or not form_id:
                    continue

                person_name = build_person_name(
                    _pick_value(row, "primer_nombre_declarante_pn", "primer_nombre_declarante"),
                    _pick_value(row, "segundo_nombre_declarante_pn", "segundo_nombre_declarante"),
                    _pick_value(
                        row,
                        "primer_apellido_declarante_pn",
                        "primer_apellido_declarante",
                    ),
                    _pick_value(
                        row,
                        "segundo_apellido_declarante_pn",
                        "segundo_apellido_declarante",
                    ),
                ) or document_id
                finance_id = f"ley2013_conflict_{form_id}"
                note_text = clean_text(
                    _pick_value(
                        row,
                        "descripc_otros_posibles_conflictos_interes_info",
                        "descripc_otros_posibles",
                    )
                )
                extracted = extract_disclosure_references(note_text)

                mentioned_process_refs = extracted["mentioned_process_refs"]
                summary_matches: dict[str, str] = {}
                if summary_lookup is not None and mentioned_process_refs:
                    summary_matches = summary_lookup.lookup_many(mentioned_process_refs)

                matched_summary_ids: list[str] = []
                for process_ref in mentioned_process_refs:
                    summary_id = clean_text(summary_matches.get(process_ref))
                    if not summary_id or summary_id in matched_summary_ids:
                        continue
                    matched_summary_ids.append(summary_id)
                    contract_map[summary_id] = {
                        "contract_id": summary_id,
                        "name": f"Referenced contract {summary_id}",
                        "summary_id": summary_id,
                        "source": "conflict_disclosures",
                        "country": "CO",
                        "reference_origin": "DISCLOSURE_NOTE",
                    }
                    contract_refs.append({
                        "source_key": finance_id,
                        "target_key": summary_id,
                        "source": "conflict_disclosures",
                        "reference_type": "mentioned_contract",
                    })

                person_map[document_id] = {
                    "document_id": document_id,
                    "cedula": document_id,
                    "name": person_name,
                    "document_type": clean_text(row.get("tipo_documento")),
                    "source": "conflict_disclosures",
                    "country": "CO",
                }

                disclosure_map[finance_id] = {
                    "finance_id": finance_id,
                    "name": f"Conflict disclosure {form_id}",
                    "type": "CONFLICT_DISCLOSURE",
                    "form_id": form_id,
                    "declaration_type": clean_text(row.get("tipo_declaracion")),
                    "declaration_status": clean_text(row.get("estado_declaracion")),
                    "date": parse_iso_date(row.get("fecha_publicac_declarac")),
                    "entity_name": clean_text(row.get("nombre_entidad")),
                    "declarant_role": clean_text(row.get("cargo_declarante")),
                    "tax_year": clean_text(row.get("ano_gravable_declarac_iyr")),
                    "is_contractor": parse_flag(row.get("declarante_es_contratista")),
                    "has_partner": parse_flag(
                        _pick_value(
                            row,
                            "tiene_conyuge_companero_permanente",
                            "tiene_conyuge_companero",
                        )
                    ),
                    "partner_involved": parse_flag(
                        _pick_value(
                            row,
                            "conyuge_companero_permanente_podria_generar_conflicto_interes",
                            "conyuge_companero_permanente",
                        )
                    ),
                    "family_conflicts": parse_flag(row.get("parientes_conflictos_interes")),
                    "direct_interests": parse_flag(
                        _pick_value(
                            row,
                            "intereses_directos_actuaciones_ci",
                            "intereses_directos_actuaciones",
                        )
                    ),
                    "trust_conflicts": parse_flag(row.get("fideicomisos_conflicto_interes")),
                    "other_investments": parse_flag(
                        _pick_value(
                            row,
                            "otras_inversiones_conflicto_interes",
                            "otras_inversiones_conflicto",
                        )
                    ),
                    "donations_conflicts": parse_flag(row.get("donaciones_confl_interes")),
                    "other_possible_conflicts": parse_flag(
                        _pick_value(
                            row,
                            "otros_posibles_conflictos_interes",
                            "otros_posibles_conflictos",
                        )
                    ),
                    "other_conflict_notes": note_text,
                    "mentioned_document_ids": extracted["mentioned_document_ids"],
                    "mentioned_process_refs": mentioned_process_refs,
                    "mentioned_company_names": extracted["mentioned_company_names"],
                    "legal_role_terms": extracted["legal_role_terms"],
                    "family_terms": extracted["family_terms"],
                    "litigation_terms": extracted["litigation_terms"],
                    "company_document_mention_count": extracted["company_document_mention_count"],
                    "process_reference_count": extracted["process_reference_count"],
                    "company_name_mention_count": extracted["company_name_mention_count"],
                    "legal_role_term_count": extracted["legal_role_term_count"],
                    "family_term_count": extracted["family_term_count"],
                    "litigation_term_count": extracted["litigation_term_count"],
                    "matched_contract_ids": matched_summary_ids,
                    "source": "conflict_disclosures",
                    "country": "CO",
                }

                for mentioned_document_id in extracted["mentioned_document_ids"]:
                    if mentioned_document_id == document_id:
                        continue
                    company_document_refs.append({
                        "source_key": finance_id,
                        "target_key": mentioned_document_id,
                        "source": "conflict_disclosures",
                        "reference_type": "mentioned_document_id",
                    })

                for company_name in extracted["mentioned_company_names"]:
                    company_name_refs.append({
                        "source_key": finance_id,
                        "target_name": company_name,
                        "source": "conflict_disclosures",
                        "reference_type": "mentioned_company_name",
                    })

                disclosure_rels.append({
                    "source_key": document_id,
                    "target_key": finance_id,
                    "source": "conflict_disclosures",
                })
        finally:
            if summary_lookup is not None:
                summary_lookup.close()

        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.disclosures = deduplicate_rows(list(disclosure_map.values()), ["finance_id"])
        self.contracts = deduplicate_rows(list(contract_map.values()), ["contract_id"])
        self.disclosure_rels = deduplicate_rows(
            disclosure_rels,
            ["source_key", "target_key"],
        )
        self.company_document_refs = deduplicate_rows(
            company_document_refs,
            ["source_key", "target_key", "reference_type"],
        )
        self.company_name_refs = deduplicate_rows(
            company_name_refs,
            ["source_key", "target_name", "reference_type"],
        )
        self.contract_refs = deduplicate_rows(
            contract_refs,
            ["source_key", "target_key", "reference_type"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.disclosures:
            loaded += loader.load_nodes("Finance", self.disclosures, key_field="finance_id")
        if self.contracts:
            loaded += loader.load_nodes("Contract", self.contracts, key_field="contract_id")
        if self.disclosure_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (f:Finance {finance_id: row.target_key}) "
                "MERGE (p)-[r:DECLARO_FINANZAS]->(f) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.disclosure_rels)
        if self.company_document_refs:
            loaded += loader.load_relationships(
                rel_type="REFERENTE_A",
                rows=self.company_document_refs,
                source_label="Finance",
                source_key="finance_id",
                target_label="Company",
                target_key="document_id",
                properties=["source", "reference_type"],
            )
        if self.company_name_refs:
            query = (
                "UNWIND $rows AS row "
                "MATCH (f:Finance {finance_id: row.source_key}) "
                "CALL (row) { "
                "  MATCH (c:Company {razon_social: row.target_name}) "
                "  WITH collect(c) AS matches "
                "  WHERE size(matches) = 1 "
                "  RETURN matches[0] AS company "
                "} "
                "MERGE (f)-[r:REFERENTE_A]->(company) "
                "SET r.source = row.source, "
                "    r.reference_type = row.reference_type"
            )
            loaded += loader.run_query(query, self.company_name_refs)
        if self.contract_refs:
            loaded += loader.load_relationships(
                rel_type="REFERENTE_A",
                rows=self.contract_refs,
                source_label="Finance",
                source_key="finance_id",
                target_label="Contract",
                target_key="contract_id",
                properties=["source", "reference_type"],
            )

        self.rows_loaded = loaded
