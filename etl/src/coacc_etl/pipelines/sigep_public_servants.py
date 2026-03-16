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


class SigepPublicServantsPipeline(Pipeline):
    """Load SIGEP public servants and their current public offices."""

    name = "sigep_public_servants"
    source_id = "sigep_public_servants"

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
        csv_path = Path(self.data_dir) / "sigep_public_servants" / "sigep_public_servants.csv"
        if not csv_path.exists():
            logger.warning("[sigep_public_servants] file not found: %s", csv_path)
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
            document_id = strip_document(clean_text(row.get("numerodeidentificacion")))
            if not document_id:
                continue

            person_name = clean_name(row.get("nombre")) or document_id
            institution_name = clean_name(row.get("nombreentidad"))
            role_name = clean_name(row.get("denominacionempleoactual"))
            dependency_name = clean_name(row.get("dependenciaempleoactual"))
            office_id = make_public_office_id(
                document_id,
                row.get("codigosigep"),
                institution_name,
                role_name,
                dependency_name,
            )

            person_map[document_id] = {
                "document_id": document_id,
                "cedula": document_id,
                "name": person_name,
                "sex": clean_text(row.get("sexo")),
                "birth_department": clean_text(row.get("departamentodenacimiento")),
                "birth_municipality": clean_text(row.get("municipiodenacimiento")),
                "public_experience_months": parse_integer(
                    row.get("mesesdeexperienciapublico")
                ),
                "private_experience_months": parse_integer(
                    row.get("mesesdeexperienciaprivado")
                ),
                "teaching_experience_months": parse_integer(
                    row.get("mesesdeexperienciadocente")
                ),
                "independent_experience_months": parse_integer(
                    row.get("mesesdeexperienciaindependiente")
                ),
                "education_level": clean_text(row.get("niveleducativo")),
                "source": "sigep_public_servants",
                "country": "CO",
            }

            office_map[office_id] = {
                "office_id": office_id,
                "name": role_name or dependency_name or institution_name or office_id,
                "role_name": role_name,
                "org": institution_name,
                "dependency": dependency_name,
                "salary": parse_amount(row.get("asignacionbasicasalarial")),
                "start_date": parse_iso_date(row.get("fecha_de_vinculaci_n")),
                "institution_code": clean_text(row.get("codigosigep")),
                "order": clean_text(row.get("orden")),
                "legal_nature": clean_text(row.get("naturalezajuridica")),
                "appointment_type": clean_text(row.get("tipodenombramiento")),
                "hierarchy_level": clean_text(row.get("niveljerarquicoempleo")),
                "source": "sigep_public_servants",
                "country": "CO",
            }

            office_rels.append({
                "source_key": document_id,
                "target_key": office_id,
                "source": "sigep_public_servants",
                "salary": parse_amount(row.get("asignacionbasicasalarial")),
                "start_date": parse_iso_date(row.get("fecha_de_vinculaci_n")),
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
                "MERGE (p)-[r:RECIBIO_SALARIO]->(o) "
                "SET r.source = row.source, "
                "    r.salary = row.salary, "
                "    r.start_date = row.start_date"
            )
            loaded += loader.run_query(query, self.office_rels)

        self.rows_loaded = loaded
