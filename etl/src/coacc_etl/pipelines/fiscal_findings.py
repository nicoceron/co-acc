from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_procurement import build_company_row
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_amount,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver


class FiscalFindingsPipeline(Pipeline):
    """Load official fiscal findings as factual company audit findings."""

    name = "fiscal_findings"
    source_id = "fiscal_findings"

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
        self.companies: list[dict[str, Any]] = []
        self.findings: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        path = Path(self.data_dir) / "fiscal_findings" / "fiscal_findings.csv"
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
        company_map: dict[str, dict[str, Any]] = {}
        findings: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        def value(row: pd.Series, *keys: str) -> object:
            for key in keys:
                if key in row and clean_text(row.get(key)):
                    return row.get(key)
            return ""

        for _, row in self.raw.iterrows():
            document_id = strip_document(row.get("nit"))
            name = clean_name(row.get("nombre_sujeto"))
            radicado = clean_text(row.get("radicado"))
            if not document_id or not name or not radicado:
                continue

            finding_id = stable_id(
                "hallazgo",
                document_id,
                radicado,
                row.get("vigencia_auditada"),
                row.get("fecha_tr_mite"),
            )

            company_map[document_id] = build_company_row(
                document_id=document_id,
                name=name,
                source="fiscal_findings",
            )
            findings.append({
                "finding_id": finding_id,
                "name": clean_text(row.get("proceso_o_asunto_evaluado")) or f"Hallazgo {radicado}",
                "type": "CO_FISCAL_FINDING",
                "radicado": radicado,
                "process_name": clean_text(row.get("proceso_o_asunto_evaluado")),
                "audited_year": clean_text(row.get("vigencia_auditada")),
                "report_date": parse_iso_date(
                    value(
                        row,
                        "fecha_comunicaci_n_informe",
                        "fecha_comunicacion_informe_final",
                    )
                ),
                "transfer_date": parse_iso_date(row.get("fecha_recibo_traslado")),
                "date": parse_iso_date(
                    value(
                        row,
                        "fecha_tr_mite",
                        "fecha_tramite",
                    )
                ),
                "status": clean_text(
                    value(
                        row,
                        "tr_mite_a_corte_de_la_fecha",
                        "tramite_a_corte_de_la_fecha_final_de_la_rendicion",
                    )
                ),
                "description": clean_text(row.get("hechos")),
                "observations": clean_text(row.get("observaciones")),
                "amount": parse_amount(value(row, "cuant_a", "cuantia")),
                "source": "fiscal_findings",
                "country": "CO",
            })
            rels.append({
                "source_key": document_id,
                "target_key": finding_id,
                "source": "fiscal_findings",
                "radicado": radicado,
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.findings = deduplicate_rows(findings, ["finding_id"])
        self.company_rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.findings:
            loaded += loader.load_nodes("Finding", self.findings, key_field="finding_id")
        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {document_id: row.source_key}) "
                "MATCH (f:Finding {finding_id: row.target_key}) "
                "MERGE (c)-[r:TIENE_HALLAZGO]->(f) "
                "SET r.source = row.source, "
                "    r.radicado = row.radicado"
            )
            loaded += loader.run_query(query, self.company_rels)

        self.rows_loaded = loaded
