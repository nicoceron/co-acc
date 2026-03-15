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
    make_public_office_id,
    parse_amount,
    parse_integer,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SigepSensitivePositionsPipeline(Pipeline):
    """Load SIGEP positions flagged for heightened integrity exposure."""

    name = "sigep_sensitive_positions"
    source_id = "sigep_sensitive_positions"

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
        self.offices: list[dict[str, Any]] = []
        self.office_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = (
            Path(self.data_dir)
            / "sigep_sensitive_positions"
            / "sigep_sensitive_positions.csv"
        )
        if not csv_path.exists():
            logger.warning("[sigep_sensitive_positions] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        person_map: dict[str, dict[str, Any]] = {}
        office_map: dict[str, dict[str, Any]] = {}
        office_rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            document_id = strip_document(clean_text(row.get("identificacion_funcionario")))
            if not document_id:
                continue

            person_name = clean_name(row.get("nombre_completo")) or document_id
            institution_name = clean_name(row.get("nombre_institucion"))
            role_name = clean_name(row.get("denominacion_empleo_actual"))
            dependency_name = clean_name(row.get("dependencia_empleo_actual"))
            office_id = make_public_office_id(
                document_id,
                row.get("cod_institucion"),
                institution_name,
                role_name,
                dependency_name,
            )

            person_map[document_id] = {
                "document_id": document_id,
                "cedula": document_id,
                "name": person_name,
                "sex": clean_text(row.get("sexo")),
                "nationality": clean_text(row.get("nacionalidad")),
                "birth_department": clean_text(row.get("dpto_nacimiento")),
                "birth_municipality": clean_text(row.get("municipio_nacimiento")),
                "public_experience_months": parse_integer(
                    row.get("meses_experiencia_publico")
                ),
                "private_experience_months": parse_integer(
                    row.get("meses_experiencia_privado")
                ),
                "independent_experience_months": parse_integer(
                    row.get("meses_exp_neg_propio")
                ),
                "teaching_experience_months": parse_integer(
                    row.get("meses_experiencia_docente")
                ),
                "education_level": clean_text(row.get("nivel_academico")),
                "education_track": clean_text(row.get("nivel_formacion")),
                "source": "sigep_sensitive_positions",
                "country": "CO",
            }

            office_map[office_id] = {
                "office_id": office_id,
                "name": role_name or dependency_name or institution_name or office_id,
                "role_name": role_name,
                "org": institution_name,
                "dependency": dependency_name,
                "salary": parse_amount(row.get("asig_basica")),
                "start_date": parse_iso_date(row.get("fecha_vinculacion")),
                "institution_code": clean_text(row.get("cod_institucion")),
                "order": clean_text(row.get("orden")),
                "sector": clean_text(row.get("sector_admtivo")),
                "legal_nature": clean_text(row.get("naturaleza_juridica")),
                "organic_classification": clean_text(row.get("clasificacion_organica")),
                "appointment_type": clean_text(row.get("tipo_nombramiento")),
                "hierarchy_level": clean_text(row.get("nivel_jerarquico_empleo")),
                "sensitive_position": True,
                "source": "sigep_sensitive_positions",
                "country": "CO",
            }

            office_rels.append({
                "source_key": document_id,
                "target_key": office_id,
                "source": "sigep_sensitive_positions",
                "salary": parse_amount(row.get("asig_basica")),
                "start_date": parse_iso_date(row.get("fecha_vinculacion")),
                "sensitive_position": True,
            })

        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.offices = deduplicate_rows(list(office_map.values()), ["office_id"])
        self.office_rels = deduplicate_rows(office_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.offices:
            loaded += loader.load_nodes("PublicOffice", self.offices, key_field="office_id")
        if self.office_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (o:PublicOffice {office_id: row.target_key}) "
                "MERGE (p)-[r:RECEBEU_SALARIO]->(o) "
                "SET r.source = row.source, "
                "    r.salary = row.salary, "
                "    r.start_date = row.start_date, "
                "    r.sensitive_position = row.sensitive_position"
            )
            loaded += loader.run_query(query, self.office_rels)

        self.rows_loaded = loaded
