from __future__ import annotations

# ruff: noqa: E501
import logging
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
)
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    extract_url,
    parse_amount,
    parse_iso_date,
    read_csv_normalized,
    stable_id,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class SecopInteradminAgreementsPipeline(Pipeline):
    """Load current and historical SECOP interadministrative agreements."""

    name = "secop_interadmin_agreements"
    source_id = "secop_interadmin_agreements"

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
        self.companies: list[dict[str, Any]] = []
        self.agreements: list[dict[str, Any]] = []

    def _csv_paths(self) -> tuple[Path, ...]:
        return (
            Path(self.data_dir)
            / "secop_interadmin_agreements"
            / "secop_interadmin_agreements.csv",
            Path(self.data_dir)
            / "secop_interadmin_agreements_historical"
            / "secop_interadmin_agreements_historical.csv",
        )

    def extract(self) -> None:
        frames: list[pd.DataFrame] = []
        for csv_path in self._csv_paths():
            if not csv_path.exists():
                continue
            frame = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
            if not frame.empty:
                frames.append(frame)

        if not frames:
            logger.warning("[%s] no input file found under %s", self.name, self._csv_paths())
            return

        self._raw = pd.concat(frames, ignore_index=True)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _transform_frame(
        self,
        frame: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        company_map: dict[str, dict[str, Any]] = {}
        agreement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            contract_id = clean_text(row.get("id_contrato"))
            buyer_name = clean_name(row.get("nombre_entidad"))
            buyer_document = make_company_document_id(
                row.get("id_entidad"),
                buyer_name,
                kind="buyer",
            )
            counterparty_name = clean_name(row.get("contratista"))
            counterparty_document = make_company_document_id(
                row.get("identificacion_contratista"),
                counterparty_name,
                kind="counterparty",
            )
            if not buyer_document or not buyer_name or not counterparty_document or not counterparty_name:
                continue

            merge_company(
                company_map,
                build_company_row(
                    document_id=buyer_document,
                    name=buyer_name,
                    source="secop_interadmin_agreements",
                    entity_order=clean_text(row.get("orden")),
                    department=clean_text(row.get("departamento")),
                    municipality=clean_text(row.get("municipio")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=counterparty_document,
                    name=counterparty_name,
                    source="secop_interadmin_agreements",
                    company_type=clean_text(row.get("tipo_contratista")),
                    department=clean_text(row.get("departamento")),
                    municipality=clean_text(row.get("municipio")),
                ),
            )

            signed_at = parse_iso_date(row.get("fecha_firma"))
            summary_id = stable_id(
                "co_interadmin",
                contract_id or row.get("numero_de_contrato"),
                buyer_document,
                counterparty_document,
            )
            current = agreement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "source_key": buyer_document,
                    "target_key": counterparty_document,
                    "source": "secop_interadmin_agreements",
                    "country": "CO",
                    "buyer_document_id": buyer_document,
                    "buyer_name": buyer_name,
                    "counterparty_document_id": counterparty_document,
                    "counterparty_name": counterparty_name,
                    "agreement_count": 0,
                    "total_value": 0.0,
                    "average_value": None,
                    "first_date": signed_at,
                    "last_date": signed_at,
                    "status": clean_text(row.get("estado_contrato")),
                    "modality": clean_text(row.get("modalidad_contratacion")),
                    "contract_type": clean_text(row.get("tipo_de_contrato")),
                    "modality_justification": clean_text(row.get("justificacion_modalidad")),
                    "department": clean_text(row.get("departamento")),
                    "municipality": clean_text(row.get("municipio")),
                    "object": clean_text(row.get("objeto_contractual")),
                    "resource_origin": clean_text(row.get("origen_de_los_recursos")),
                    "order_level": clean_text(row.get("orden")),
                    "url": extract_url(row.get("link")),
                    "evidence_refs": [],
                },
            )
            current["agreement_count"] += 1
            current["total_value"] += parse_amount(row.get("valor_con_adiciones")) or 0.0
            current["average_value"] = current["total_value"] / float(current["agreement_count"])
            if signed_at and (not current.get("first_date") or signed_at < current["first_date"]):
                current["first_date"] = signed_at
            if signed_at and (not current.get("last_date") or signed_at > current["last_date"]):
                current["last_date"] = signed_at
            if not current.get("status"):
                current["status"] = clean_text(row.get("estado_contrato"))
            if not current.get("modality"):
                current["modality"] = clean_text(row.get("modalidad_contratacion"))
            if not current.get("contract_type"):
                current["contract_type"] = clean_text(row.get("tipo_de_contrato"))
            if not current.get("modality_justification"):
                current["modality_justification"] = clean_text(row.get("justificacion_modalidad"))
            if not current.get("object"):
                current["object"] = clean_text(row.get("objeto_contractual"))
            if not current.get("resource_origin"):
                current["resource_origin"] = clean_text(row.get("origen_de_los_recursos"))
            if not current.get("url"):
                current["url"] = extract_url(row.get("link"))
            current["evidence_refs"] = merge_limited_unique(
                list(current.get("evidence_refs", [])),
                contract_id,
                row.get("numero_de_contrato"),
                row.get("id_proceso"),
            )

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(agreement_map.values()), ["summary_id"]),
        )

    def transform(self) -> None:
        self.companies, self.agreements = self._transform_frame(self._raw)

    def _load_query(self) -> str:
        return (
            "UNWIND $rows AS row "
            "MATCH (buyer:Company {document_id: row.source_key}) "
            "MATCH (counterparty:Company {document_id: row.target_key}) "
            "MERGE (buyer)-[r:CELEBRO_CONVENIO_INTERADMIN {summary_id: row.summary_id}]->(counterparty) "
            "WITH r, row, "
            "     coalesce(r.agreement_count, 0) AS prev_count, "
            "     coalesce(r.total_value, 0.0) AS prev_total, "
            "     r.first_date AS prev_first_date, "
            "     r.last_date AS prev_last_date, "
            "     coalesce(r.evidence_refs, []) AS prev_refs "
            "SET r.source = row.source, "
            "    r.country = row.country, "
            "    r.buyer_document_id = row.buyer_document_id, "
            "    r.buyer_name = row.buyer_name, "
            "    r.counterparty_document_id = row.counterparty_document_id, "
            "    r.counterparty_name = row.counterparty_name, "
            "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
            "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
            "    r.contract_type = CASE "
            "      WHEN row.contract_type <> '' THEN row.contract_type ELSE r.contract_type END, "
            "    r.modality_justification = CASE "
            "      WHEN row.modality_justification <> '' "
            "      THEN row.modality_justification ELSE r.modality_justification END, "
            "    r.department = CASE WHEN row.department <> '' THEN row.department ELSE r.department END, "
            "    r.municipality = CASE WHEN row.municipality <> '' THEN row.municipality ELSE r.municipality END, "
            "    r.object = CASE WHEN row.object <> '' THEN row.object ELSE r.object END, "
            "    r.resource_origin = CASE "
            "      WHEN row.resource_origin <> '' THEN row.resource_origin ELSE r.resource_origin END, "
            "    r.order_level = CASE "
            "      WHEN row.order_level <> '' THEN row.order_level ELSE r.order_level END, "
            "    r.url = CASE WHEN row.url <> '' THEN row.url ELSE r.url END, "
            "    r.agreement_count = prev_count + row.agreement_count, "
            "    r.total_value = prev_total + row.total_value, "
            "    r.average_value = CASE "
            "      WHEN (prev_count + row.agreement_count) > 0 "
            "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.agreement_count) "
            "      ELSE NULL END, "
            "    r.first_date = CASE "
            "      WHEN prev_first_date IS NULL "
            "        OR (row.first_date IS NOT NULL AND row.first_date < prev_first_date) "
            "      THEN row.first_date ELSE prev_first_date END, "
            "    r.last_date = CASE "
            "      WHEN prev_last_date IS NULL "
            "        OR (row.last_date IS NOT NULL AND row.last_date > prev_last_date) "
            "      THEN row.last_date ELSE prev_last_date END, "
            "    r.evidence_refs = reduce("
            "      acc = [], item IN (prev_refs + row.evidence_refs) | "
            "      CASE WHEN item IN acc THEN acc ELSE acc + item END"
            "    )[0..5]"
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.agreements:
            loaded += loader.run_query(self._load_query(), self.agreements)
        self.rows_loaded = loaded

    def run_streaming(self, start_phase: int = 1) -> None:
        if start_phase > 1:
            logger.info(
                "[%s] start_phase=%s ignored for single-phase streaming",
                self.name,
                start_phase,
            )

        loader = Neo4jBatchLoader(self.driver)
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        query = self._load_query()

        for csv_path in self._csv_paths():
            if not csv_path.exists():
                continue
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                companies, agreements = self._transform_frame(chunk)
                if companies:
                    loaded += loader.load_nodes("Company", companies, key_field="document_id")
                if agreements:
                    loaded += loader.run_query(query, agreements)

                processed += len(chunk)
                self.rows_in = processed
                self.rows_loaded = loaded
                if processed >= next_log_at:
                    logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                    next_log_at += self.chunk_size * 10
