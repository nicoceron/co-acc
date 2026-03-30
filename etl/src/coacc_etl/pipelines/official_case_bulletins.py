from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import build_company_row
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, parse_iso_date, stable_id
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _case_person_id(inquiry_id: str, person: dict[str, Any]) -> str:
    explicit_id = clean_text(person.get("case_person_id"))
    if explicit_id:
        return explicit_id
    return stable_id("case_person", inquiry_id, person.get("name"))


def _case_company_id(inquiry_id: str, company: dict[str, Any]) -> str:
    explicit_id = clean_text(company.get("case_company_id"))
    if explicit_id:
        return explicit_id
    return stable_id("case_company", inquiry_id, company.get("name"))


class OfficialCaseBulletinsPipeline(Pipeline):
    """Load curated official Colombia case bulletins as Inquiry evidence."""

    name = "official_case_bulletins"
    source_id = "official_case_bulletins"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._raw: list[dict[str, Any]] = []
        self.inquiries: list[dict[str, Any]] = []
        self.people_with_document: list[dict[str, Any]] = []
        self.people_placeholders: list[dict[str, Any]] = []
        self.companies_with_document: list[dict[str, Any]] = []
        self.company_placeholders: list[dict[str, Any]] = []
        self.person_rels: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.inquiry_doc_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        path = (
            Path(self.data_dir)
            / "official_case_bulletins"
            / "official_case_bulletins.json"
        )
        if not path.exists():
            logger.warning("[%s] file not found: %s", self.name, path)
            self._raw = []
            self.rows_in = 0
            return

        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise TypeError(f"Expected list payload in {path}, got {type(payload)}")

        rows = [row for row in payload if isinstance(row, dict)]
        if self.limit is not None:
            rows = rows[: self.limit]
        self._raw = rows
        self.rows_in = len(rows)

    def transform(self) -> None:
        inquiries: list[dict[str, Any]] = []
        people_with_document: list[dict[str, Any]] = []
        people_placeholders: list[dict[str, Any]] = []
        companies_with_document: list[dict[str, Any]] = []
        company_placeholders: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        documents: list[dict[str, Any]] = []
        inquiry_doc_rels: list[dict[str, Any]] = []

        for case in self._raw:
            title = clean_text(case.get("title"))
            source_url = clean_text(case.get("source_url"))
            if not title or not source_url:
                continue

            inquiry_id = clean_text(case.get("inquiry_id")) or stable_id(
                "inq",
                title,
                case.get("event_date"),
                source_url,
            )
            inquiries.append(
                {
                    "inquiry_id": inquiry_id,
                    "title": title,
                    "type": clean_text(case.get("type")) or "OFFICIAL_CASE_BULLETIN",
                    "case_domain": clean_text(case.get("case_domain")),
                    "case_category": clean_text(case.get("case_category")),
                    "status": clean_text(case.get("status")),
                    "event_date": parse_iso_date(case.get("event_date")),
                    "summary": clean_text(case.get("summary")),
                    "source_url": source_url,
                    "issuing_entity": clean_text(case.get("issuing_entity")),
                    "source": "official_case_bulletins",
                    "country": "CO",
                }
            )
            document_id = stable_id(
                "official_case_bulletin_doc",
                title,
                case.get("event_date"),
                source_url,
            )
            documents.append(
                {
                    "doc_id": document_id,
                    "title": title,
                    "name": title,
                    "summary": clean_text(case.get("summary")),
                    "source_url": source_url,
                    "publication_date": parse_iso_date(case.get("event_date")),
                    "document_kind": "official_case_bulletin",
                    "issuing_entity": clean_text(case.get("issuing_entity")),
                    "case_category": clean_text(case.get("case_category")),
                    "case_domain": clean_text(case.get("case_domain")),
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
            inquiry_doc_rels.append(
                {
                    "source_key": inquiry_id,
                    "target_key": document_id,
                    "source": self.source_id,
                }
            )

            for person in case.get("people") or []:
                if not isinstance(person, dict):
                    continue
                name = clean_name(person.get("name"))
                if not name:
                    continue
                document_id = strip_document(person.get("document_id"))
                roles = [
                    clean_text(role)
                    for role in (person.get("roles") or [])
                    if clean_text(role)
                ]
                if document_id:
                    people_with_document.append(
                        {
                            "document_id": document_id,
                            "name": name,
                            "country": "CO",
                            "case_bulletin_subject": True,
                            "case_bulletin_source": "official_case_bulletins",
                        }
                    )
                    person_rels.append(
                        {
                            "source_key": document_id,
                            "target_key": inquiry_id,
                            "source": "official_case_bulletins",
                            "case_role": ", ".join(roles),
                            "case_roles": roles,
                            "subject_match": "document_id",
                        }
                    )
                    continue

                case_person_id = _case_person_id(inquiry_id, person)
                people_placeholders.append(
                    {
                        "case_person_id": case_person_id,
                        "name": name,
                        "country": "CO",
                        "source": "official_case_bulletins",
                        "case_bulletin_placeholder": True,
                        "case_bulletin_subject": True,
                    }
                )
                person_rels.append(
                    {
                        "source_key": case_person_id,
                        "target_key": inquiry_id,
                        "source": "official_case_bulletins",
                        "case_role": ", ".join(roles),
                        "case_roles": roles,
                        "subject_match": "official_name_only",
                    }
                )

            for company in case.get("companies") or []:
                if not isinstance(company, dict):
                    continue
                name = clean_name(company.get("name"))
                if not name:
                    continue
                document_id = strip_document(company.get("document_id"))
                roles = [
                    clean_text(role)
                    for role in (company.get("roles") or [])
                    if clean_text(role)
                ]
                if document_id:
                    companies_with_document.append(
                        build_company_row(
                            document_id=document_id,
                            name=name,
                            source="official_case_bulletins",
                            case_bulletin_subject=True,
                            case_bulletin_source="official_case_bulletins",
                        )
                    )
                    company_rels.append(
                        {
                            "source_key": document_id,
                            "target_key": inquiry_id,
                            "source": "official_case_bulletins",
                            "case_role": ", ".join(roles),
                            "case_roles": roles,
                            "subject_match": "document_id",
                        }
                    )
                    continue

                case_company_id = _case_company_id(inquiry_id, company)
                company_placeholders.append(
                    {
                        "case_company_id": case_company_id,
                        "name": name,
                        "razon_social": name,
                        "source": "official_case_bulletins",
                        "country": "CO",
                        "case_bulletin_placeholder": True,
                        "case_bulletin_subject": True,
                    }
                )
                company_rels.append(
                    {
                        "source_key": case_company_id,
                        "target_key": inquiry_id,
                        "source": "official_case_bulletins",
                        "case_role": ", ".join(roles),
                        "case_roles": roles,
                        "subject_match": "official_name_only",
                    }
                )

        self.inquiries = deduplicate_rows(inquiries, ["inquiry_id"])
        self.people_with_document = deduplicate_rows(people_with_document, ["document_id"])
        self.people_placeholders = deduplicate_rows(people_placeholders, ["case_person_id"])
        self.companies_with_document = deduplicate_rows(companies_with_document, ["document_id"])
        self.company_placeholders = deduplicate_rows(company_placeholders, ["case_company_id"])
        self.person_rels = deduplicate_rows(person_rels, ["source_key", "target_key"])
        self.company_rels = deduplicate_rows(company_rels, ["source_key", "target_key"])
        self.documents = deduplicate_rows(documents, ["doc_id"])
        self.inquiry_doc_rels = deduplicate_rows(inquiry_doc_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.inquiries:
            loaded += loader.load_nodes("Inquiry", self.inquiries, key_field="inquiry_id")
        if self.documents:
            loaded += loader.load_nodes("SourceDocument", self.documents, key_field="doc_id")

        if self.people_with_document:
            person_query = """
                UNWIND $rows AS row
                MERGE (p:Person {document_id: row.document_id})
                SET p.name = coalesce(p.name, row.name),
                    p.country = coalesce(p.country, row.country),
                    p.case_bulletin_subject = true,
                    p.case_bulletin_source = row.case_bulletin_source
            """
            loaded += loader.run_query(person_query, self.people_with_document)

        if self.people_placeholders:
            placeholder_query = """
                UNWIND $rows AS row
                MERGE (p:Person {case_person_id: row.case_person_id})
                SET p.name = row.name,
                    p.country = row.country,
                    p.source = row.source,
                    p.case_bulletin_placeholder = true,
                    p.case_bulletin_subject = true
            """
            loaded += loader.run_query(placeholder_query, self.people_placeholders)

        if self.companies_with_document:
            company_query = """
                UNWIND $rows AS row
                MERGE (c:Company {document_id: row.document_id})
                SET c.name = coalesce(c.name, row.name),
                    c.razon_social = coalesce(c.razon_social, row.razon_social, row.name),
                    c.country = coalesce(c.country, row.country),
                    c.case_bulletin_subject = true,
                    c.case_bulletin_source = row.case_bulletin_source,
                    c.nit = coalesce(c.nit, row.nit)
            """
            loaded += loader.run_query(company_query, self.companies_with_document)

        if self.company_placeholders:
            company_placeholder_query = """
                UNWIND $rows AS row
                MERGE (c:Company {case_company_id: row.case_company_id})
                SET c.name = row.name,
                    c.razon_social = row.razon_social,
                    c.source = row.source,
                    c.country = row.country,
                    c.case_bulletin_placeholder = true,
                    c.case_bulletin_subject = true
            """
            loaded += loader.run_query(company_placeholder_query, self.company_placeholders)

        if self.person_rels:
            person_rel_query = """
                UNWIND $rows AS row
                MATCH (p:Person)
                WHERE p.document_id = row.source_key
                   OR p.case_person_id = row.source_key
                MATCH (i:Inquiry {inquiry_id: row.target_key})
                MERGE (p)-[r:REFERENTE_A]->(i)
                SET r.source = row.source,
                    r.case_role = row.case_role,
                    r.case_roles = row.case_roles,
                    r.subject_match = row.subject_match
            """
            loaded += loader.run_query(person_rel_query, self.person_rels)

        if self.company_rels:
            company_rel_query = """
                UNWIND $rows AS row
                MATCH (c:Company)
                WHERE c.document_id = row.source_key
                   OR c.case_company_id = row.source_key
                MATCH (i:Inquiry {inquiry_id: row.target_key})
                MERGE (c)-[r:REFERENTE_A]->(i)
                SET r.source = row.source,
                    r.case_role = row.case_role,
                    r.case_roles = row.case_roles,
                    r.subject_match = row.subject_match
            """
            loaded += loader.run_query(company_rel_query, self.company_rels)

        if self.inquiry_doc_rels:
            inquiry_doc_query = """
                UNWIND $rows AS row
                MATCH (i:Inquiry {inquiry_id: row.source_key})
                MATCH (d:SourceDocument {doc_id: row.target_key})
                MERGE (i)-[r:REFERENTE_A]->(d)
                SET r.source = row.source
            """
            loaded += loader.run_query(inquiry_doc_query, self.inquiry_doc_rels)

        probable_person_query = """
            MATCH (case_person:Person)
            WHERE coalesce(case_person.case_bulletin_placeholder, false)
              AND case_person.source = 'official_case_bulletins'
              AND coalesce(case_person.name, '') <> ''
            WITH case_person, toUpper(case_person.name) AS case_name
            MATCH (p:Person)
            WHERE p.case_person_id IS NULL
              AND p.document_id IS NOT NULL
              AND toUpper(coalesce(p.name, '')) = case_name
            WITH case_person, collect(DISTINCT p) AS matches
            WHERE size(matches) = 1
            WITH case_person, matches[0] AS matched_person
            MERGE (case_person)-[r:POSSIBLY_SAME_AS]->(matched_person)
            SET r.source = 'official_case_bulletins',
                r.match_reason = 'unique_exact_full_name_person',
                r.confidence = 0.6
        """

        probable_company_query = """
            MATCH (case_person:Person)
            WHERE coalesce(case_person.case_bulletin_placeholder, false)
              AND case_person.source = 'official_case_bulletins'
              AND coalesce(case_person.name, '') <> ''
            WITH case_person, toUpper(case_person.name) AS case_name
            MATCH (c:Company)
            WHERE c.document_id IS NOT NULL
              AND toUpper(coalesce(c.name, c.razon_social, '')) = case_name
            WITH case_person, collect(DISTINCT c) AS matches
            WHERE size(matches) >= 1 AND size(matches) <= 3
            UNWIND matches AS matched_company
            MERGE (case_person)-[r:POSSIBLY_SAME_AS]->(matched_company)
            SET r.source = 'official_case_bulletins',
                r.match_reason = CASE
                  WHEN size(matches) = 1
                  THEN 'unique_exact_full_name_company_form'
                  ELSE 'exact_full_name_company_form_cluster'
                END,
                r.match_count = size(matches),
                r.confidence = CASE
                  WHEN size(matches) = 1 THEN 0.58
                  ELSE 0.51
                END
        """

        with self.driver.session(database=self.neo4j_database) as session:
            session.run(probable_person_query)
            session.run(probable_company_query)

        self.rows_loaded = loaded
