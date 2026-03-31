from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import clean_text
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class DnpProjectContractLinksPipeline(Pipeline):
    """Create BPIN-backed reference links from procurement exposure to DNP project nodes."""

    name = "dnp_project_contract_links"
    source_id = "dnp_project_contract_links"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self.reference_rows: list[dict[str, Any]] = []

    def extract(self) -> None:
        self.rows_in = 0

    def transform(self) -> None:
        query = """
            MATCH (buyer:Company)-[award:CONTRATOU]->(supplier:Company)
            WHERE coalesce(award.bpin_code, '') <> ''
              AND coalesce(award.summary_id, '') <> ''
            MATCH (project:Project {project_id: award.bpin_code})
            RETURN DISTINCT buyer.document_id AS buyer_document_id,
                            supplier.document_id AS supplier_document_id,
                            award.summary_id AS summary_id,
                            award.bpin_code AS bpin_code
        """
        rows: list[dict[str, Any]] = []
        with self.driver.session(database=self.neo4j_database) as session:
            for record in session.run(query):
                buyer_document_id = clean_text(record["buyer_document_id"])
                supplier_document_id = clean_text(record["supplier_document_id"])
                summary_id = clean_text(record["summary_id"])
                bpin_code = clean_text(record["bpin_code"])
                if (
                    not buyer_document_id
                    or not supplier_document_id
                    or not summary_id
                    or not bpin_code
                ):
                    continue
                rows.append({
                    "buyer_document_id": buyer_document_id,
                    "supplier_document_id": supplier_document_id,
                    "summary_id": summary_id,
                    "bpin_code": bpin_code,
                    "source": self.source_id,
                    "country": "CO",
                })

        self.reference_rows = deduplicate_rows(
            rows,
            ["buyer_document_id", "supplier_document_id", "summary_id", "bpin_code"],
        )
        if self.limit:
            self.reference_rows = self.reference_rows[: self.limit]
        self.rows_in = len(self.reference_rows)

    def load(self) -> None:
        if not self.reference_rows:
            self.rows_loaded = 0
            return

        query = """
            UNWIND $rows AS row
            MATCH (buyer:Company {document_id: row.buyer_document_id})
            MATCH (supplier:Company {document_id: row.supplier_document_id})
            OPTIONAL MATCH (project:Project {project_id: row.bpin_code})
            OPTIONAL MATCH (legacy:Convenio {convenio_id: row.bpin_code})
            WITH row, buyer, supplier, [n IN [project, legacy] WHERE n IS NOT NULL] AS targets
            FOREACH (target IN targets |
                MERGE (buyer)-[
                    rb:REFERENTE_A {summary_id: row.summary_id, reference_kind: 'BPIN_BUYER'}
                ]->(target)
                SET rb.source = row.source,
                    rb.country = row.country,
                    rb.bpin_code = row.bpin_code,
                    rb.contract_role = 'BUYER'
            )
            FOREACH (target IN targets |
                MERGE (supplier)-[
                    rs:REFERENTE_A {
                        summary_id: row.summary_id,
                        reference_kind: 'BPIN_SUPPLIER'
                    }
                ]->(target)
                SET rs.source = row.source,
                    rs.country = row.country,
                    rs.bpin_code = row.bpin_code,
                    rs.contract_role = 'SUPPLIER'
            )
            WITH row, targets
            OPTIONAL MATCH (contract:Contract {contract_id: row.summary_id})
            FOREACH (target IN CASE WHEN contract IS NULL THEN [] ELSE targets END |
                MERGE (contract)-[rc:REFERENTE_A {reference_kind: 'BPIN_CONTRACT'}]->(target)
                SET rc.source = row.source,
                    rc.country = row.country,
                    rc.bpin_code = row.bpin_code
            )
        """
        loader = Neo4jBatchLoader(self.driver)
        self.rows_loaded = loader.run_query(query, self.reference_rows)
