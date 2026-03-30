from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    build_company_row,
    make_company_document_id,
    merge_company,
    merge_limited_unique,
    procurement_relation_id,
    procurement_year,
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    extract_url,
    parse_iso_date,
    read_csv_normalized,
    stable_id,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _archive_label(row: dict[str, Any]) -> str:
    return clean_text(row.get("descripci_n")) or clean_text(row.get("nombre_archivo"))


def _archive_type_flags(*values: object) -> dict[str, bool]:
    parts = [clean_text(value).lower() for value in values if clean_text(value)]
    text = " ".join(parts)
    return {
        "supervision": any(token in text for token in ("supervis", "interventor", "interventoria")),
        "payment": any(token in text for token in ("pago", "factura", "cuenta de cobro", "cufe")),
        "assignment": any(token in text for token in ("designaci", "delegaci", "apoyo a la supervision")),
        "start_record": any(token in text for token in ("acta de inicio", "oficio de inicio")),
        "resume": "hoja de vida" in text,
        "report": any(token in text for token in ("informe", "acta de supervis")),
    }


def _archive_document_kind(flags: dict[str, bool]) -> str:
    kinds = [name for name, enabled in flags.items() if enabled]
    return ",".join(sorted(kinds)) if kinds else "supporting_document"


class SecopDocumentArchivesPipeline(Pipeline):
    """Aggregate SECOP II public document-index rows onto contract summaries."""

    name = "secop_document_archives"
    source_id = "secop_document_archives"

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
        self.procurements: list[dict[str, Any]] = []
        self.bids: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.bid_company_links: list[dict[str, Any]] = []
        self.bid_document_links: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_document_archives" / "secop_document_archives.csv"

    def _contracts_csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_ii_contracts" / "secop_ii_contracts.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _build_process_summary_lookup(self, process_ids: set[str]) -> dict[str, list[str]]:
        csv_path = self._contracts_csv_path()
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        lookup: dict[str, set[str]] = defaultdict(set)
        company_map: dict[str, dict[str, Any]] = {}

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            dtype=str,
            keep_default_na=False,
        ):
            for row in chunk.to_dict(orient="records"):
                process_id = clean_text(row.get("proceso_de_compra"))
                if not process_id or process_id not in process_ids:
                    continue

                buyer_name = clean_name(row.get("nombre_entidad"))
                buyer_document = make_company_document_id(
                    row.get("nit_entidad"),
                    buyer_name,
                    kind="buyer",
                )
                supplier_name = clean_name(row.get("proveedor_adjudicado"))
                supplier_document = make_company_document_id(
                    row.get("documento_proveedor"),
                    supplier_name,
                    kind="supplier",
                )
                if (
                    not buyer_document
                    or not supplier_document
                    or not buyer_name
                    or not supplier_name
                ):
                    continue

                merge_company(
                    company_map,
                    build_company_row(
                        document_id=buyer_document,
                        name=buyer_name,
                        source="secop_ii_contracts",
                    ),
                )
                merge_company(
                    company_map,
                    build_company_row(
                        document_id=supplier_document,
                        name=supplier_name,
                        source="secop_ii_contracts",
                    ),
                )

                summary_id = procurement_relation_id(
                    "secop_ii_contracts",
                    buyer_document,
                    supplier_document,
                    procurement_year(row.get("fecha_de_firma")),
                )
                lookup[process_id].add(summary_id)

        return {process_id: sorted(summary_ids) for process_id, summary_ids in lookup.items()}

    def _aggregate_frame(
        self,
        frame: pd.DataFrame,
        process_summary_lookup: dict[str, list[str]],
    ) -> list[dict[str, Any]]:
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            process_id = clean_text(row.get("proceso"))
            if not process_id:
                continue
            summary_ids = process_summary_lookup.get(process_id)
            if not summary_ids:
                continue

            archive_label = _archive_label(row)
            archive_name = clean_text(row.get("nombre_archivo"))
            archive_url = extract_url(row.get("url_descarga_documento"))
            archive_extension = clean_text(row.get("extensi_n")).lower()
            archive_uploaded_at = parse_iso_date(row.get("fecha_carga"))
            archive_document_id = clean_text(row.get("id_documento"))
            contract_ref = clean_text(row.get("n_mero_de_contrato"))

            for summary_id in summary_ids:
                current = procurement_map.setdefault(
                    summary_id,
                    {
                        "summary_id": summary_id,
                        "archive_document_count": 0,
                        "archive_supervision_document_count": 0,
                        "archive_payment_document_count": 0,
                        "archive_assignment_document_count": 0,
                        "archive_start_record_document_count": 0,
                        "archive_resume_document_count": 0,
                        "archive_report_document_count": 0,
                        "archive_document_names": [],
                        "archive_document_urls": [],
                        "archive_document_refs": [],
                        "archive_document_extensions": [],
                        "archive_document_last_upload": None,
                    },
                )
                current["archive_document_count"] += 1
                archive_flags = _archive_type_flags(
                    archive_label,
                    archive_name,
                    contract_ref,
                )
                if archive_flags["supervision"]:
                    current["archive_supervision_document_count"] += 1
                if archive_flags["payment"]:
                    current["archive_payment_document_count"] += 1
                if archive_flags["assignment"]:
                    current["archive_assignment_document_count"] += 1
                if archive_flags["start_record"]:
                    current["archive_start_record_document_count"] += 1
                if archive_flags["resume"]:
                    current["archive_resume_document_count"] += 1
                if archive_flags["report"]:
                    current["archive_report_document_count"] += 1
                current["archive_document_names"] = merge_limited_unique(
                    list(current.get("archive_document_names", [])),
                    archive_label,
                    archive_name,
                    limit=12,
                )
                current["archive_document_urls"] = merge_limited_unique(
                    list(current.get("archive_document_urls", [])),
                    archive_url,
                    limit=6,
                )
                current["archive_document_refs"] = merge_limited_unique(
                    list(current.get("archive_document_refs", [])),
                    archive_document_id,
                    contract_ref,
                    process_id,
                    limit=12,
                )
                current["archive_document_extensions"] = merge_limited_unique(
                    list(current.get("archive_document_extensions", [])),
                    archive_extension,
                    limit=6,
                )
                if archive_uploaded_at and (
                    not current.get("archive_document_last_upload")
                    or archive_uploaded_at > current["archive_document_last_upload"]
                ):
                    current["archive_document_last_upload"] = archive_uploaded_at

        return deduplicate_rows(list(procurement_map.values()), ["summary_id"])

    def _document_rows(
        self,
        frame: pd.DataFrame,
        process_summary_lookup: dict[str, list[str]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for row in frame.to_dict(orient="records"):
            process_id = clean_text(row.get("proceso"))
            if not process_id:
                continue
            summary_ids = process_summary_lookup.get(process_id)
            if not summary_ids:
                continue

            archive_label = _archive_label(row)
            archive_name = clean_text(row.get("nombre_archivo"))
            archive_url = extract_url(row.get("url_descarga_documento"))
            archive_extension = clean_text(row.get("extensi_n")).lower()
            archive_uploaded_at = parse_iso_date(row.get("fecha_carga"))
            archive_document_id = clean_text(row.get("id_documento"))
            contract_ref = clean_text(row.get("n_mero_de_contrato"))
            archive_flags = _archive_type_flags(archive_label, archive_name, contract_ref)
            document_kind = _archive_document_kind(archive_flags)

            doc_id = stable_id(
                "secop_archive_doc",
                process_id,
                archive_document_id or archive_url or archive_name or contract_ref,
            )
            rows.append(
                {
                    "doc_id": doc_id,
                    "bid_id": process_id,
                    "title": archive_label or archive_name or contract_ref or doc_id,
                    "name": archive_name or archive_label or doc_id,
                    "archive_label": archive_label,
                    "archive_name": archive_name,
                    "document_url": archive_url,
                    "uploaded_at": archive_uploaded_at,
                    "document_extension": archive_extension,
                    "archive_document_ref": archive_document_id,
                    "contract_reference": contract_ref,
                    "process_id": process_id,
                    "document_kind": document_kind,
                    "source_id": self.source_id,
                    "source": self.source_id,
                    "country": "CO",
                }
            )

        return deduplicate_rows(rows, ["doc_id"])

    def _bid_rows(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            process_id = clean_text(row.get("proceso"))
            if not process_id:
                continue
            archive_label = _archive_label(row)
            archive_name = clean_text(row.get("nombre_archivo"))
            contract_ref = clean_text(row.get("n_mero_de_contrato"))
            rows.append(
                {
                    "bid_id": process_id,
                    "name": archive_label or archive_name or contract_ref or process_id,
                    "reference": contract_ref or process_id,
                    "procedure_description": archive_label or archive_name or contract_ref,
                    "source": self.source_id,
                    "country": "CO",
                }
            )
        return deduplicate_rows(rows, ["bid_id"])

    def _bid_company_rows(
        self,
        process_summary_lookup: dict[str, list[str]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for process_id, summary_ids in process_summary_lookup.items():
            for summary_id in summary_ids:
                rows.append(
                    {
                        "bid_id": process_id,
                        "summary_id": summary_id,
                        "source": self.source_id,
                        "country": "CO",
                    }
                )
        return deduplicate_rows(rows, ["bid_id", "summary_id"])

    def _bid_document_rows(self, documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = [
            {
                "bid_id": row["bid_id"],
                "doc_id": row["doc_id"],
                "source": row["source"],
                "document_kind": row["document_kind"],
            }
            for row in documents
            if row.get("bid_id") and row.get("doc_id")
        ]
        return deduplicate_rows(rows, ["bid_id", "doc_id"])

    def transform(self) -> None:
        process_ids = {
            clean_text(value)
            for value in self._raw.get("proceso", pd.Series(dtype=str)).tolist()
            if clean_text(value)
        }
        if not process_ids:
            self.procurements = []
            self.bids = []
            self.documents = []
            self.bid_company_links = []
            self.bid_document_links = []
            return

        try:
            lookup = self._build_process_summary_lookup(process_ids)
        except FileNotFoundError:
            logger.warning("[%s] secop_ii_contracts source file not found; skipping", self.name)
            self.procurements = []
            self.bids = []
            self.documents = []
            self.bid_company_links = []
            self.bid_document_links = []
            return

        self.procurements = self._aggregate_frame(self._raw, lookup)
        self.bids = self._bid_rows(self._raw)
        self.documents = self._document_rows(self._raw, lookup)
        self.bid_company_links = self._bid_company_rows(lookup)
        self.bid_document_links = self._bid_document_rows(self.documents)

    def _load_query(self) -> str:
        return """
            UNWIND $rows AS row
            MATCH ()-[r:CONTRATOU {summary_id: row.summary_id}]->()
            SET r.archive_document_count = row.archive_document_count,
                r.archive_supervision_document_count = row.archive_supervision_document_count,
                r.archive_payment_document_count = row.archive_payment_document_count,
                r.archive_assignment_document_count = row.archive_assignment_document_count,
                r.archive_start_record_document_count = row.archive_start_record_document_count,
                r.archive_resume_document_count = row.archive_resume_document_count,
                r.archive_report_document_count = row.archive_report_document_count,
                r.archive_document_names = row.archive_document_names,
                r.archive_document_urls = row.archive_document_urls,
                r.archive_document_refs = row.archive_document_refs,
                r.archive_document_extensions = row.archive_document_extensions,
                r.archive_document_last_upload = row.archive_document_last_upload
        """

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        document_loader = Neo4jBatchLoader(self.driver, batch_size=500)
        loaded = 0
        if self.procurements:
            loaded += loader.run_query(self._load_query(), self.procurements)
        if self.bids:
            loaded += document_loader.run_query_with_retry(
                """
                    UNWIND $rows AS row
                    MERGE (b:Bid {bid_id: row.bid_id})
                    SET b.source = coalesce(b.source, row.source),
                        b.country = coalesce(b.country, row.country),
                        b.name = CASE
                            WHEN coalesce(b.name, '') = '' AND row.name <> '' THEN row.name
                            ELSE b.name
                        END,
                        b.reference = CASE
                            WHEN coalesce(b.reference, '') = '' AND row.reference <> '' THEN row.reference
                            ELSE b.reference
                        END,
                        b.procedure_description = CASE
                            WHEN coalesce(b.procedure_description, '') = '' AND row.procedure_description <> ''
                            THEN row.procedure_description
                            ELSE b.procedure_description
                        END
                """,
                self.bids,
            )
        if self.bid_company_links:
            loaded += document_loader.run_query_with_retry(
                """
                    UNWIND $rows AS row
                    MATCH (buyer:Company)-[award:CONTRATOU {summary_id: row.summary_id}]->(supplier:Company)
                    MERGE (bid:Bid {bid_id: row.bid_id})
                    MERGE (buyer)-[rb:LICITO]->(bid)
                    SET rb.source = row.source,
                        rb.country = row.country,
                        rb.summary_id = row.summary_id,
                        rb.buyer_document_id = coalesce(award.buyer_document_id, rb.buyer_document_id),
                        rb.buyer_name = coalesce(award.buyer_name, rb.buyer_name)
                    MERGE (supplier)-[rs:GANO]->(bid)
                    SET rs.source = row.source,
                        rs.country = row.country,
                        rs.summary_id = row.summary_id,
                        rs.supplier_document_id = coalesce(award.supplier_document_id, rs.supplier_document_id),
                        rs.supplier_name = coalesce(award.supplier_name, rs.supplier_name)
                """,
                self.bid_company_links,
            )
        if self.documents:
            loaded += document_loader.load_nodes("SourceDocument", self.documents, key_field="doc_id")
        if self.bid_document_links:
            loaded += document_loader.run_query_with_retry(
                """
                    UNWIND $rows AS row
                    MATCH (bid:Bid {bid_id: row.bid_id})
                    MATCH (doc:SourceDocument {doc_id: row.doc_id})
                    MERGE (bid)-[r:REFERENTE_A]->(doc)
                    SET r.source = row.source,
                        r.document_kind = row.document_kind
                """,
                self.bid_document_links,
            )
        self.rows_loaded = loaded
