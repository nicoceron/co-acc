from __future__ import annotations

# ruff: noqa: E501
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa

from coacc_etl.base import Pipeline
from coacc_etl.lakehouse import append_parquet, watermark
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import (
    build_company_row,
    make_company_document_id,
    merge_company,
    merge_limited_unique,
    procurement_relation_id,
)
from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns, read_csv_normalized
from coacc_etl.streaming import iter_csv_chunks, pipeline_stream_to_lake
from coacc_etl.transforms import deduplicate_rows, normalize_name, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

    from coacc_etl.lakehouse.watermark import WatermarkDelta

logger = logging.getLogger(__name__)
SOURCE = "secop_integrado"
SOCRATA_ID = "jbjy-vk9h"


def _clean_value(raw: object) -> str:
    value = str(raw or "").strip()
    if value.upper() in {"NO DEFINIDO", "N/D", "NULL", "NONE", "NAN"}:
        return ""
    return value


def _parse_amount(raw: object) -> float | None:
    value = _clean_value(raw).replace(",", "").replace("$", "")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def normalize(rows: list[dict[str, Any]]) -> pa.Table:
    frame = normalize_dataframe_columns(pd.DataFrame.from_records(rows))
    return normalize_frame_for_lake(frame)


def normalize_frame_for_lake(frame: pd.DataFrame) -> pa.Table:
    normalized = normalize_dataframe_columns(frame.fillna(""))

    def col(name: str) -> pd.Series:
        if name in normalized:
            return normalized[name].astype(str)
        return pd.Series([""] * len(normalized), index=normalized.index, dtype="string")

    supplier_document = col("documento_proveedor").map(strip_document)
    supplier_name = col("nom_raz_social_contratista").map(lambda value: normalize_name(value))
    buyer_name = col("nombre_de_la_entidad").map(_clean_value)
    buyer_document = [
        make_company_document_id(raw_doc, raw_name, kind="buyer")
        for raw_doc, raw_name in zip(col("nit_de_la_entidad"), buyer_name, strict=False)
    ]

    normalized = normalized.copy()
    normalized["source"] = SOURCE
    normalized["supplier_document_id"] = supplier_document
    normalized["supplier_name"] = supplier_name
    normalized["buyer_document_id"] = buyer_document
    normalized["buyer_name"] = buyer_name
    normalized["contract_id"] = col("numero_del_contrato").where(
        col("numero_del_contrato") != "",
        col("id_contrato"),
    )
    normalized["process_id"] = col("numero_de_proceso").where(
        col("numero_de_proceso") != "",
        col("id_proceso"),
    )
    normalized["department"] = col("departamento_entidad").map(_clean_value)
    normalized["municipality"] = col("municipio_entidad").map(_clean_value)
    normalized["modality"] = col("modalidad_de_contrataci_n").map(_clean_value)
    normalized["status"] = col("estado_del_proceso").map(_clean_value)
    normalized["origin"] = col("origen").map(_clean_value)
    normalized["contract_value"] = [
        _parse_amount(value) for value in col("valor_contrato")
    ]
    normalized["load_date"] = col("fecha_de_cargue_en_secop").map(_clean_value)
    normalized["object"] = col("objeto_del_contrato").map(_clean_value)
    return pa.Table.from_pandas(normalized, preserve_index=False)


class SecopIntegratedPipeline(Pipeline):
    """Load the SECOP integrated contract awards dataset into the graph."""

    name = "secop_integrado"
    source_id = "secop_integrado"

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
        self.procurements: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "secop_integrado" / "secop_integrado.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[secop_integrado] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _transform_frame(
        self,
        frame: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        company_map: dict[str, dict[str, Any]] = {}
        procurement_map: dict[str, dict[str, Any]] = {}

        for row in frame.to_dict(orient="records"):
            entity_name = _clean_value(row.get("nombre_de_la_entidad"))
            entity_nit = make_company_document_id(
                row.get("nit_de_la_entidad"),
                entity_name,
                kind="buyer",
            )
            supplier_document = strip_document(_clean_value(row.get("documento_proveedor")))
            supplier_name = normalize_name(_clean_value(row.get("nom_raz_social_contratista")))
            supplier_document = supplier_document or make_company_document_id(
                supplier_document,
                supplier_name,
                kind="supplier",
            )
            supplier_doc_type = _clean_value(row.get("tipo_documento_proveedor"))
            contract_id = _clean_value(row.get("numero_del_contrato")) or _clean_value(
                row.get("id_contrato")
            )
            process_id = _clean_value(row.get("numero_de_proceso")) or _clean_value(
                row.get("id_proceso")
            )
            origin = _clean_value(row.get("origen"))
            if not entity_nit or not entity_name or not supplier_document or not supplier_name:
                continue

            merge_company(
                company_map,
                build_company_row(
                    document_id=entity_nit,
                    name=entity_name,
                    source="secop_integrado",
                    department=_clean_value(row.get("departamento_entidad")),
                    municipality=_clean_value(row.get("municipio_entidad")),
                    entity_scope=_clean_value(row.get("nivel_entidad")),
                ),
            )
            merge_company(
                company_map,
                build_company_row(
                    document_id=supplier_document,
                    name=supplier_name,
                    source="secop_integrado",
                    document_type=supplier_doc_type,
                ),
            )

            summary_id = procurement_relation_id(
                "secop_integrado",
                entity_nit,
                supplier_document,
            )
            summary = procurement_map.setdefault(
                summary_id,
                {
                    "summary_id": summary_id,
                    "source_key": entity_nit,
                    "target_key": supplier_document,
                    "source": "secop_integrado",
                    "country": "CO",
                    "buyer_document_id": entity_nit,
                    "buyer_name": entity_name,
                    "supplier_document_id": supplier_document,
                    "supplier_name": supplier_name,
                    "contract_count": 0,
                    "total_value": 0.0,
                    "average_value": None,
                    "department": _clean_value(row.get("departamento_entidad")),
                    "city": _clean_value(row.get("municipio_entidad")),
                    "modality": _clean_value(row.get("modalidad_de_contrataci_n")),
                    "status": _clean_value(row.get("estado_del_proceso")),
                    "origin": origin,
                    "evidence_refs": [],
                },
            )
            summary["contract_count"] += 1
            summary["total_value"] += _parse_amount(row.get("valor_contrato")) or 0.0
            summary["average_value"] = summary["total_value"] / float(summary["contract_count"])
            if not summary.get("department"):
                summary["department"] = _clean_value(row.get("departamento_entidad"))
            if not summary.get("city"):
                summary["city"] = _clean_value(row.get("municipio_entidad"))
            if not summary.get("modality"):
                summary["modality"] = _clean_value(row.get("modalidad_de_contrataci_n"))
            if not summary.get("status"):
                summary["status"] = _clean_value(row.get("estado_del_proceso"))
            if not summary.get("origin"):
                summary["origin"] = origin
            summary["evidence_refs"] = merge_limited_unique(
                list(summary.get("evidence_refs", [])),
                contract_id,
                process_id,
            )

        return (
            deduplicate_rows(list(company_map.values()), ["document_id"]),
            deduplicate_rows(list(procurement_map.values()), ["summary_id"]),
        )

    def transform(self) -> None:
        self.companies, self.procurements = self._transform_frame(self._raw)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.procurements:
            query = (
                "UNWIND $rows AS row "
                "MATCH (buyer:Company {document_id: row.source_key}) "
                "MATCH (supplier:Company {document_id: row.target_key}) "
                "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier) "
                "WITH r, row, "
                "     coalesce(r.contract_count, 0) AS prev_count, "
                "     coalesce(r.total_value, 0.0) AS prev_total, "
                "     coalesce(r.evidence_refs, []) AS prev_refs "
                "SET r.source = row.source, "
                "    r.country = row.country, "
                "    r.buyer_document_id = row.buyer_document_id, "
                "    r.buyer_name = row.buyer_name, "
                "    r.supplier_document_id = row.supplier_document_id, "
                "    r.supplier_name = row.supplier_name, "
                "    r.department = coalesce(row.department, r.department), "
                "    r.city = coalesce(row.city, r.city), "
                "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
                "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
                "    r.origin = CASE WHEN row.origin <> '' THEN row.origin ELSE r.origin END, "
                "    r.contract_count = prev_count + row.contract_count, "
                "    r.total_value = prev_total + row.total_value, "
                "    r.average_value = CASE "
                "      WHEN (prev_count + row.contract_count) > 0 "
                "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.contract_count) "
                "      ELSE NULL "
                "    END, "
                "    r.evidence_refs = reduce("
                "      acc = [], "
                "      item IN (prev_refs + row.evidence_refs) | "
                "        CASE WHEN item IN acc THEN acc ELSE acc + item END"
                "    )[0..5]"
            )
            loaded += loader.run_query(query, self.procurements)

        self.rows_loaded = loaded

    def run_streaming(self, start_phase: int = 1) -> None:
        if start_phase > 1:
            logger.info(
                "[%s] start_phase=%s ignored for single-phase streaming",
                self.name,
                start_phase,
            )

        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        loader = Neo4jBatchLoader(self.driver)
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10
        procurement_query = (
            "UNWIND $rows AS row "
            "MATCH (buyer:Company {document_id: row.source_key}) "
            "MATCH (supplier:Company {document_id: row.target_key}) "
            "MERGE (buyer)-[r:CONTRATOU {summary_id: row.summary_id}]->(supplier) "
            "WITH r, row, "
            "     coalesce(r.contract_count, 0) AS prev_count, "
            "     coalesce(r.total_value, 0.0) AS prev_total, "
            "     coalesce(r.evidence_refs, []) AS prev_refs "
            "SET r.source = row.source, "
            "    r.country = row.country, "
            "    r.buyer_document_id = row.buyer_document_id, "
            "    r.buyer_name = row.buyer_name, "
            "    r.supplier_document_id = row.supplier_document_id, "
            "    r.supplier_name = row.supplier_name, "
            "    r.department = coalesce(row.department, r.department), "
            "    r.city = coalesce(row.city, r.city), "
            "    r.modality = CASE WHEN row.modality <> '' THEN row.modality ELSE r.modality END, "
            "    r.status = CASE WHEN row.status <> '' THEN row.status ELSE r.status END, "
            "    r.origin = CASE WHEN row.origin <> '' THEN row.origin ELSE r.origin END, "
            "    r.contract_count = prev_count + row.contract_count, "
            "    r.total_value = prev_total + row.total_value, "
            "    r.average_value = CASE "
            "      WHEN (prev_count + row.contract_count) > 0 "
            "      THEN (prev_total + row.total_value) / toFloat(prev_count + row.contract_count) "
            "      ELSE NULL "
            "    END, "
            "    r.evidence_refs = reduce("
            "      acc = [], "
            "      item IN (prev_refs + row.evidence_refs) | "
            "        CASE WHEN item IN acc THEN acc ELSE acc + item END"
            "    )[0..5]"
        )

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            companies, procurements = self._transform_frame(chunk)
            if companies:
                loaded += loader.load_nodes("Company", companies, key_field="document_id")
            if procurements:
                loaded += loader.run_query(procurement_query, procurements)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10

    def run_to_lake(self, *, full_refresh: bool = False) -> WatermarkDelta:
        last = watermark.get(SOURCE)
        where = None
        # Only apply incremental $where once the previous backfill has completed
        # (last_offset cleared). If last_offset is set we are mid-resume and must
        # continue the same pagination, otherwise we'd double-count rows.
        if not full_refresh and last is not None and last.last_offset is None:
            where = f"fecha_de_cargue_en_secop > '{last.last_seen_ts.isoformat()}'"

        csv_path = self._csv_path()
        batch_id = uuid.uuid4().hex
        rows_total = 0
        if csv_path.exists():
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                table = normalize_frame_for_lake(chunk)
                now = datetime.now(tz=UTC)
                append_parquet(table, SOURCE, year=now.year, month=now.month)
                rows_total += len(chunk)
            return watermark.advance(SOURCE, rows=rows_total, batch_id=batch_id)

        return pipeline_stream_to_lake(
            SOURCE,
            SOCRATA_ID,
            normalizer=normalize,
            chunk_size=self.chunk_size,
            where=where,
            full_refresh=full_refresh,
        )
