from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    build_person_name,
    clean_name,
    clean_text,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver


class SiriAntecedentsPipeline(Pipeline):
    """Load official SIRI antecedents as person sanction records."""

    name = "siri_antecedents"
    source_id = "siri_antecedents"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.raw: pd.DataFrame = pd.DataFrame()
        self.people: list[dict[str, Any]] = []
        self.sanctions: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        path = Path(self.data_dir) / "siri_antecedents" / "siri_antecedents.csv"
        if path.exists():
            self.raw = read_csv_normalized_with_fallback(
                str(path),
                dtype=str,
                keep_default_na=False,
            )
        if self.limit:
            self.raw = self.raw.head(self.limit)
        self.rows_in = len(self.raw)

    def transform(self) -> None:
        person_map: dict[str, dict[str, Any]] = {}
        sanctions: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        for _, row in self.raw.iterrows():
            document_id = strip_document(row.get("numero_identificacion"))
            siri_number = clean_text(row.get("numero_siri"))
            if not document_id or not siri_number:
                continue

            full_name = build_person_name(
                row.get("primer_nombre"),
                row.get("segundo_nombre"),
                row.get("primer_apellido"),
                row.get("segundo_apellido"),
            )
            if not full_name:
                continue

            sanction_id = stable_id(
                "siri",
                siri_number,
                document_id,
                row.get("numero_proceso"),
                row.get("fecha_efectos_juridicos"),
                row.get("sanciones"),
            )

            person_map[document_id] = {
                "document_id": document_id,
                "cedula": document_id,
                "name": full_name,
                "document_type": clean_text(row.get("nombre_tipo_identificacion")),
                "source": "siri_antecedents",
                "country": "CO",
            }

            sanctions.append({
                "sanction_id": sanction_id,
                "name": clean_text(row.get("sanciones")) or siri_number,
                "type": "SIRI_ANTECEDENT",
                "sanction_domain": clean_text(row.get("tipo_inhabilidad")),
                "penalty_type": clean_text(row.get("sanciones")),
                "subject_type": clean_text(row.get("calidad_persona")),
                "document_type": clean_text(row.get("nombre_tipo_identificacion")),
                "record_id": siri_number,
                "process_number": clean_text(row.get("numero_proceso")),
                "decision_level": clean_text(row.get("providencia")),
                "issuing_entity": clean_text(row.get("autoridad")),
                "entity_name": clean_text(row.get("entidad_sancionado")),
                "department": clean_text(row.get("lugar_hechos_departamento")),
                "municipality": clean_text(row.get("lugar_hechos_municipio")),
                "authority_department": clean_text(row.get("entidad_departamento")),
                "authority_municipality": clean_text(row.get("entidad_municipio")),
                "date_start": parse_iso_date(row.get("fecha_efectos_juridicos")),
                "duration_years": clean_text(row.get("duracion_anos")),
                "duration_months": clean_text(row.get("duracion_mes")),
                "duration_days": clean_text(row.get("duracion_dias")),
                "source": "siri_antecedents",
                "country": "CO",
            })
            rels.append({
                "source_key": document_id,
                "target_key": sanction_id,
                "source": "siri_antecedents",
            })

        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])
        self.person_rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.sanctions:
            loaded += loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")
        if self.person_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (p)-[r:SANCIONADA]->(s) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.person_rels)

        self.rows_loaded = loaded

