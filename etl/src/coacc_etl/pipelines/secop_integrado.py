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

COLUMN_MAP: dict[str, str] = {
    "nombre_entidad": "buyer_name",
    "nit_entidad": "buyer_document_raw",
    "proveedor_adjudicado": "supplier_name",
    "documento_proveedor": "supplier_document_raw",
    "tipodocproveedor": "supplier_doc_type",
    "valor_del_contrato": "contract_value",
    "ultima_actualizacion": "load_date",
    "fecha_de_firma": "signed_date",
    "departamento": "department",
    "ciudad": "municipality",
    "modalidad_de_contratacion": "modality",
    "estado_contrato": "status",
    "orden": "origin",
    "referencia_del_contrato": "contract_id",
    "id_contrato": "contract_id_fallback",
    "proceso_de_compra": "process_id",
    "objeto_del_contrato": "object",
}

QUALITY_NULL_THRESHOLD: dict[str, float] = {
    "buyer_name": 0.90,
    "contract_value": 0.50,
    "supplier_name": 0.80,
    "load_date": 0.90,
}


class LakeQualityError(Exception):
    def __init__(self, source: str, metrics: dict[str, float]) -> None:
        lines = [f"  {col}: {pct:.1%} non-null (threshold {QUALITY_NULL_THRESHOLD[col]:.0%})" for col, pct in sorted(metrics.items())]
        super().__init__(f"Quality gate failed for {source}:\n" + "\n".join(lines))
        self.source = source
        self.metrics = metrics


def _check_quality(df: pd.DataFrame, source: str = SOURCE) -> None:
    total = len(df)
    if total == 0:
        return
    failures: dict[str, float] = {}
    for col, threshold in QUALITY_NULL_THRESHOLD.items():
        if col not in df.columns:
            failures[col] = 0.0
            continue
        non_null = df[col].notna().sum()
        non_empty = (df[col].astype(str).str.strip() != "").sum() if df[col].dtype == object else non_null
        coverage = max(non_null, non_empty) / total
        if coverage < threshold:
            failures[col] = coverage
    if failures:
        raise LakeQualityError(source, failures)


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


def _extract_max_data_ts(table: pa.Table) -> str | None:
    for ts_col in ("ultima_actualizacion", "load_date"):
        if ts_col in table.column_names:
            col = table.column(ts_col)
            vals = [str(v) for v in col if v and str(v).strip() not in ("", "None", "nan")]
            if vals:
                return max(vals)
    return None


def _extract_signed_date_for_partition(table: pa.Table) -> tuple[int, int]:
    from coacc_etl.pipelines.colombia_shared import parse_iso_date

    col_name = "signed_date" if "signed_date" in table.column_names else "fecha_de_firma"
    if col_name not in table.column_names:
        now = datetime.now(tz=UTC)
        return now.year, now.month
    col = table.column(col_name)
    years, months = [], []
    for v in col:
        s = str(v).strip()
        if s and s not in ("None", "nan", ""):
            p = parse_iso_date(s)
            if p:
                years.append(int(p[:4]))
                months.append(int(p[5:7]))
    if years:
        from collections import Counter

        y, _ = Counter(years).most_common(1)[0]
        m, _ = Counter(months).most_common(1)[0]
        return y, m
    now = datetime.now(tz=UTC)
    return now.year, now.month


def _parse_watermark_ts(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    from coacc_etl.pipelines.colombia_shared import parse_iso_date

    parsed = parse_iso_date(ts_str)
    if not parsed:
        return None
    dt = datetime.fromisoformat(parsed) if isinstance(parsed, str) else datetime.fromisoformat(str(parsed))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def normalize(rows: list[dict[str, Any]]) -> pa.Table:
    frame = normalize_dataframe_columns(pd.DataFrame.from_records(rows))
    return normalize_frame_for_lake(frame)


def normalize_frame_for_lake(frame: pd.DataFrame) -> pa.Table:
    normalized = normalize_dataframe_columns(frame.fillna(""))

    def col(name: str) -> pd.Series:
        if name in normalized:
            return normalized[name].astype(str)
        return pd.Series([""] * len(normalized), index=normalized.index, dtype="string")

    supplier_document_raw = col("documento_proveedor").map(strip_document)
    supplier_name = col("proveedor_adjudicado").map(lambda v: normalize_name(v))
    buyer_name = col("nombre_entidad").map(_clean_value)
    buyer_document = [
        make_company_document_id(raw_doc, raw_name, kind="buyer")
        for raw_doc, raw_name in zip(col("nit_entidad"), buyer_name, strict=False)
    ]

    normalized = normalized.copy()
    normalized["source"] = SOURCE
    normalized["supplier_document_id"] = supplier_document_raw
    normalized["supplier_name"] = supplier_name
    normalized["buyer_document_id"] = buyer_document
    normalized["buyer_name"] = buyer_name
    normalized["contract_id"] = col("referencia_del_contrato").where(
        col("referencia_del_contrato") != "",
        col("id_contrato"),
    )
    normalized["process_id"] = col("proceso_de_compra").where(
        col("proceso_de_compra") != "",
        col("id_proceso"),
    )
    normalized["department"] = col("departamento").map(_clean_value)
    normalized["municipality"] = col("ciudad").map(_clean_value)
    normalized["modality"] = col("modalidad_de_contratacion").map(_clean_value)
    normalized["status"] = col("estado_contrato").map(_clean_value)
    normalized["origin"] = col("orden").map(_clean_value)
    normalized["contract_value"] = [
        _parse_amount(value) for value in col("valor_del_contrato")
    ]
    normalized["load_date"] = col("ultima_actualizacion").map(_clean_value)
    normalized["signed_date"] = col("fecha_de_firma").map(_clean_value)
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
            entity_name = _clean_value(row.get("nombre_entidad"))
            entity_nit = make_company_document_id(
                row.get("nit_entidad"),
                entity_name,
                kind="buyer",
            )
            supplier_document = strip_document(_clean_value(row.get("documento_proveedor")))
            supplier_name = normalize_name(_clean_value(row.get("proveedor_adjudicado")))
            supplier_document = supplier_document or make_company_document_id(
                supplier_document,
                supplier_name,
                kind="supplier",
            )
            supplier_doc_type = _clean_value(row.get("tipodocproveedor"))
            contract_id = _clean_value(row.get("referencia_del_contrato")) or _clean_value(
                row.get("id_contrato")
            )
            process_id = _clean_value(row.get("proceso_de_compra")) or _clean_value(
                row.get("id_proceso")
            )
            origin = _clean_value(row.get("orden"))
            if not entity_nit or not entity_name or not supplier_document or not supplier_name:
                continue

            merge_company(
                company_map,
                build_company_row(
                    document_id=entity_nit,
                    name=entity_name,
                    source="secop_integrado",
                    department=_clean_value(row.get("departamento")),
                    municipality=_clean_value(row.get("ciudad")),
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
                    "department": _clean_value(row.get("departamento")),
                    "city": _clean_value(row.get("ciudad")),
                    "modality": _clean_value(row.get("modalidad_de_contratacion")),
                    "status": _clean_value(row.get("estado_contrato")),
                    "origin": origin,
                    "evidence_refs": [],
                },
            )
            summary["contract_count"] += 1
            summary["total_value"] += _parse_amount(row.get("valor_del_contrato")) or 0.0
            summary["average_value"] = summary["total_value"] / float(summary["contract_count"])
            if not summary.get("department"):
                summary["department"] = _clean_value(row.get("departamento"))
            if not summary.get("city"):
                summary["city"] = _clean_value(row.get("ciudad"))
            if not summary.get("modality"):
                summary["modality"] = _clean_value(row.get("modalidad_de_contratacion"))
            if not summary.get("status"):
                summary["status"] = _clean_value(row.get("estado_contrato"))
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
        if not full_refresh and last is not None and last.last_offset is None:
            where = f"ultima_actualizacion > '{last.last_seen_ts.isoformat()}'"

        csv_path = self._csv_path()
        batch_id = uuid.uuid4().hex
        rows_total = 0
        table = None
        if csv_path.exists():
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                table = normalize_frame_for_lake(chunk)
                year, month = _extract_signed_date_for_partition(table)
                append_parquet(table, SOURCE, year=year, month=month)
                rows_total += len(chunk)
            max_data_ts = _extract_max_data_ts(table) if table is not None else None
            return watermark.advance(SOURCE, rows=rows_total, batch_id=batch_id, last_seen_ts=_parse_watermark_ts(max_data_ts))

        return pipeline_stream_to_lake(
            SOURCE,
            SOCRATA_ID,
            normalizer=normalize,
            chunk_size=self.chunk_size,
            where=where,
            full_refresh=full_refresh,
        )
