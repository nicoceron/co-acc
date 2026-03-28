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


class FiscalResponsibilityPipeline(Pipeline):
    """Load Contraloría fiscal-responsibility records as company sanctions."""

    name = "fiscal_responsibility"
    source_id = "fiscal_responsibility"

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
        self.sanctions: list[dict[str, Any]] = []
        self.company_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        path = Path(self.data_dir) / "fiscal_responsibility" / "fiscal_responsibility.csv"
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
        sanctions: list[dict[str, Any]] = []
        rels: list[dict[str, Any]] = []

        def value(row: pd.Series, *keys: str) -> object:
            for key in keys:
                if key in row and clean_text(row.get(key)):
                    return row.get(key)
            return ""

        for _, row in self.raw.iterrows():
            document_id = strip_document(
                value(
                    row,
                    "n_mero_de_identificaci_n",
                    "numero_de_identificacion",
                )
            )
            name = clean_name(
                value(
                    row,
                    "raz_n_social_de_la_entidad",
                    "razon_social_de_la_entidad",
                )
            )
            if not document_id or not name:
                continue

            sanction_id = stable_id(
                "fiscal_resp",
                document_id,
                value(
                    row,
                    "n_mero_de_resoluci_n_de_la",
                    "numero_de_resolucion_de_la_sancion_acto_administrativo",
                ),
                value(
                    row,
                    "fecha_de_firmeza_de_la_decisi",
                    "fecha_de_firmeza_de_la_decision_ejecutoria_del_acto_administrativo",
                ),
                value(
                    row,
                    "monto_de_la_multa_o_sanci",
                    "monto_de_la_multa_o_sancion",
                ),
            )

            company_map[document_id] = build_company_row(
                document_id=document_id,
                name=name,
                source="fiscal_responsibility",
            )
            sanctions.append({
                "sanction_id": sanction_id,
                "name": clean_text(row.get("tipo_de_sanci_n_multa")) or sanction_id,
                "type": "CO_FISCAL_RESPONSIBILITY",
                "sanction_domain": "FISCAL",
                "penalty_type": clean_text(
                    value(
                        row,
                        "tipo_de_sanci_n_multa",
                        "tipo_de_sancion_multa_amonestacion_etc_antecedentes",
                    )
                ),
                "topic": clean_text(
                    value(
                        row,
                        "tema_clasificaci_n_o_motivo",
                        "tema_clasificacion_o_motivo",
                    )
                ),
                "document_type": clean_text(value(row, "identificaci_n", "identificacion")),
                "resolution_number": clean_text(
                    value(
                        row,
                        "n_mero_de_resoluci_n_de_la",
                        "numero_de_resolucion_de_la_sancion_acto_administrativo",
                    )
                ),
                "decision_number": clean_text(
                    value(
                        row,
                        "n_mero_de_resoluci_n_que",
                        "numero_de_resolucion_que_resuelve_el_recurso_interpuesto",
                    )
                ),
                "decision_date": parse_iso_date(
                    value(
                        row,
                        "fecha_de_resoluci_n_de_la",
                        "fecha_de_resolucion_de_la_sancion_o_decision_acto_administrativo",
                    )
                ),
                "date_start": parse_iso_date(
                    value(
                        row,
                        "fecha_de_firmeza_de_la_decisi",
                        "fecha_de_firmeza_de_la_decision_ejecutoria_del_acto_administrativo",
                    )
                ),
                "appeal_info": clean_text(
                    value(
                        row,
                        "informaci_n_de_recursos",
                        "informacion_de_recursos_interpuestos",
                    )
                ),
                "description": clean_text(
                    value(
                        row,
                        "descripci_n_o_detalle_resumen",
                        "descripcion_o_detalle_resumen_del_caso",
                    )
                ),
                "issuing_entity": clean_text(row.get("fuente")) or "Contraloria General de la Republica",
                "value": parse_amount(
                    value(
                        row,
                        "monto_de_la_multa_o_sanci",
                        "monto_de_la_multa_o_sancion",
                    )
                ),
                "source": "fiscal_responsibility",
                "country": "CO",
            })
            rels.append({
                "source_key": document_id,
                "target_key": sanction_id,
                "source": "fiscal_responsibility",
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.sanctions = deduplicate_rows(sanctions, ["sanction_id"])
        self.company_rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.sanctions:
            loaded += loader.load_nodes("Sanction", self.sanctions, key_field="sanction_id")
        if self.company_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (c:Company {document_id: row.source_key}) "
                "MATCH (s:Sanction {sanction_id: row.target_key}) "
                "MERGE (c)-[r:SANCIONADA]->(s) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.company_rels)

        self.rows_loaded = loaded
