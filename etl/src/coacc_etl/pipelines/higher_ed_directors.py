from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    build_person_name,
    clean_text,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _row_value(row: pd.Series, *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


def _surname_pair(full_name: str) -> tuple[str, str] | None:
    tokens = [token for token in clean_text(full_name).upper().split() if token]
    if len(tokens) < 2:
        return None
    return tokens[-2], tokens[-1]


class HigherEdDirectorsPipeline(Pipeline):
    """Load MEN higher-education directors and connect them to institutions."""

    name = "higher_ed_directors"
    source_id = "higher_ed_directors"

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
        self.rels: list[dict[str, Any]] = []
        self.family_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "higher_ed_directors" / "higher_ed_directors.csv"
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
        person_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []
        institution_directors: dict[str, list[dict[str, Any]]] = {}

        for _, row in self._raw.iterrows():
            institution_code = _row_value(row, "codigoinsitucion", "codigoinstitucion")
            institution_name = _row_value(row, "nombreinstitucion", "nombre_instituci_n")
            role_name = _row_value(row, "nombrecargo")
            full_name = build_person_name(
                row.get("nombresdirectivo"),
                row.get("apellidosdirectivo"),
            )
            if not institution_code or not institution_name or not full_name:
                continue

            director_id = stable_id(
                "edu_dir",
                institution_code,
                full_name,
                role_name,
            )
            person_record = {
                "document_id": director_id,
                "name": full_name,
                "role_name": role_name,
                "email": _row_value(row, "correoelectronico"),
                "phone": _row_value(row, "telefono"),
                "address": _row_value(row, "direccion"),
                "education_institution_code": institution_code,
                "education_institution_name": institution_name,
                "is_education_director": True,
                "source": "higher_ed_directors",
                "country": "CO",
            }
            person_map[director_id] = person_record
            institution_directors.setdefault(institution_code, []).append(person_record)
            rels.append({
                "source_key": director_id,
                "target_key": institution_code,
                "source": "higher_ed_directors",
                "role": role_name,
            })

        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key", "role"])
        family_rels: list[dict[str, Any]] = []
        for institution_code, directors in institution_directors.items():
            deduped_directors = deduplicate_rows(directors, ["document_id"])
            for index, left in enumerate(deduped_directors):
                left_surnames = _surname_pair(str(left.get("name") or ""))
                if left_surnames is None:
                    continue
                for right in deduped_directors[index + 1 :]:
                    right_surnames = _surname_pair(str(right.get("name") or ""))
                    if right_surnames is None:
                        continue
                    if left_surnames != right_surnames:
                        continue
                    if clean_text(left.get("name")) == clean_text(right.get("name")):
                        continue
                    shared_contact = any(
                        clean_text(left.get(key)) and clean_text(left.get(key)) == clean_text(right.get(key))
                        for key in ("email", "phone", "address")
                    )
                    family_rels.append({
                        "source_key": str(left["document_id"]),
                        "target_key": str(right["document_id"]),
                        "source": "higher_ed_directors",
                        "match_reason": (
                            "same_institution_exact_surnames_shared_contact"
                            if shared_contact
                            else "same_institution_exact_surnames"
                        ),
                        "confidence": 0.72 if shared_contact else 0.66,
                        "shared_surnames": list(left_surnames),
                        "education_institution_code": institution_code,
                    })
        self.family_rels = deduplicate_rows(
            family_rels,
            ["source_key", "target_key", "education_institution_code"],
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.rels:
            query = """
                UNWIND $rows AS row
                MATCH (p:Person {document_id: row.source_key})
                MATCH (c:Company {education_institution_code: row.target_key})
                MERGE (p)-[r:ADMINISTRA]->(c)
                SET r.source = row.source,
                    r.role = row.role
            """
            loaded += loader.run_query(query, self.rels)
        if self.family_rels:
            family_query = """
                UNWIND $rows AS row
                MATCH (left:Person {document_id: row.source_key})
                MATCH (right:Person {document_id: row.target_key})
                MERGE (left)-[r:POSSIBLE_FAMILY_TIE]->(right)
                SET r.source = row.source,
                    r.match_reason = row.match_reason,
                    r.confidence = row.confidence,
                    r.shared_surnames = row.shared_surnames,
                    r.same_institution = true,
                    r.education_institution_code = row.education_institution_code
            """
            loaded += loader.run_query(family_query, self.family_rels)

        probable_link_query = """
            MATCH (d:Person)
            WHERE coalesce(d.is_education_director, false)
              AND d.source = 'higher_ed_directors'
              AND coalesce(d.name, '') <> ''
            WITH d, toUpper(d.name) AS director_name
            MATCH (p:Person)
            WHERE p.document_id <> d.document_id
              AND NOT coalesce(p.is_education_director, false)
              AND toUpper(coalesce(p.name, '')) = director_name
            WITH d, collect(DISTINCT p) AS matches
            WHERE size(matches) = 1
            WITH d, matches[0] AS matched_person
            MERGE (d)-[r:POSSIBLY_SAME_AS]->(matched_person)
            SET r.source = 'higher_ed_directors',
                r.match_reason = 'unique_exact_full_name',
                r.confidence = 0.55
        """
        with self.driver.session(database=self.neo4j_database) as session:
            session.run(probable_link_query)

        self.rows_loaded = loaded
