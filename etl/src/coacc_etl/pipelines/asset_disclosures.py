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
    parse_flag,
    parse_iso_date,
    read_csv_normalized,
)
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _pick_value(row: pd.Series, *keys: str) -> object:
    for key in keys:
        value = row.get(key)
        if clean_text(value):
            return value
    return ""


class AssetDisclosuresPipeline(Pipeline):
    """Load Ley 2013 asset disclosure summaries as DeclaredAsset nodes."""

    name = "asset_disclosures"
    source_id = "asset_disclosures"

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
        self.assets: list[dict[str, Any]] = []
        self.asset_rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "asset_disclosures" / "asset_disclosures.csv"
        if not csv_path.exists():
            logger.warning("[asset_disclosures] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        person_map: dict[str, dict[str, Any]] = {}
        asset_map: dict[str, dict[str, Any]] = {}
        asset_rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            document_id = strip_document(clean_text(row.get("numero_documento")))
            form_id = clean_text(row.get("numero_formulario"))
            if not document_id or not form_id:
                continue

            person_name = build_person_name(
                _pick_value(row, "primer_nombre_declarante_pn", "primer_nombre_declarante"),
                _pick_value(row, "segundo_nombre_declarante_pn", "segundo_nombre_declarante"),
                _pick_value(row, "primer_apellido_declarante_pn", "primer_apellido_declarante"),
                _pick_value(row, "segundo_apellido_declarante_pn", "segundo_apellido_declarante"),
            ) or document_id
            asset_id = f"ley2013_asset_{form_id}"

            person_map[document_id] = {
                "document_id": document_id,
                "cedula": document_id,
                "name": person_name,
                "document_type": clean_text(row.get("tipo_documento")),
                "source": "asset_disclosures",
                "country": "CO",
            }

            asset_map[asset_id] = {
                "asset_id": asset_id,
                "name": f"Asset disclosure {form_id}",
                "form_id": form_id,
                "declaration_type": clean_text(row.get("tipo_declaracion")),
                "declaration_status": clean_text(row.get("estado_declaracion")),
                "publication_date": parse_iso_date(row.get("fecha_publicac_declarac")),
                "entity_name": clean_text(row.get("nombre_entidad")),
                "declarant_role": clean_text(row.get("cargo_declarante")),
                "tax_year": clean_text(row.get("ano_gravable_declarac_iyr")),
                "is_contractor": parse_flag(row.get("declarante_es_contratista")),
                "reported_income": parse_flag(row.get("reporto_ingresos")),
                "has_bank_accounts": parse_flag(row.get("ctas_bancarias_pn")),
                "has_assets": parse_flag(row.get("bienes_patrimoniales_pn")),
                "has_liabilities": parse_flag(row.get("acreencias_obligaciones_pn")),
                "has_board_roles": parse_flag(
                    _pick_value(
                        row,
                        "particip_juntas_consejos_direct",
                        "particip_juntas_consejos",
                    )
                ),
                "has_corporate_interests": parse_flag(row.get("particip_corp_socied_asoc")),
                "has_private_activities": parse_flag(row.get("activ_econom_privadas")),
                "source": "asset_disclosures",
                "country": "CO",
            }

            asset_rels.append({
                "source_key": document_id,
                "target_key": asset_id,
                "source": "asset_disclosures",
            })

        self.people = deduplicate_rows(list(person_map.values()), ["document_id"])
        self.assets = deduplicate_rows(list(asset_map.values()), ["asset_id"])
        self.asset_rels = deduplicate_rows(asset_rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.people:
            loaded += loader.load_nodes("Person", self.people, key_field="document_id")
        if self.assets:
            loaded += loader.load_nodes("DeclaredAsset", self.assets, key_field="asset_id")
        if self.asset_rels:
            query = (
                "UNWIND $rows AS row "
                "MATCH (p:Person {document_id: row.source_key}) "
                "MATCH (a:DeclaredAsset {asset_id: row.target_key}) "
                "MERGE (p)-[r:DECLARO_BIEN]->(a) "
                "SET r.source = row.source"
            )
            loaded += loader.run_query(query, self.asset_rels)

        self.rows_loaded = loaded
