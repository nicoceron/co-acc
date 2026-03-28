from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, read_csv_normalized
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class HealthProvidersPipeline(Pipeline):
    """Load REPS providers and their service sites."""

    name = "health_providers"
    source_id = "health_providers"

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
        self.health_sites: list[dict[str, Any]] = []
        self.rels: list[dict[str, Any]] = []

    def extract(self) -> None:
        csv_path = Path(self.data_dir) / "health_providers" / "health_providers.csv"
        if not csv_path.exists():
            logger.warning("[health_providers] file not found: %s", csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def transform(self) -> None:
        company_map: dict[str, dict[str, Any]] = {}
        health_map: dict[str, dict[str, Any]] = {}
        rels: list[dict[str, Any]] = []

        for _, row in self._raw.iterrows():
            provider_document = strip_document(clean_text(row.get("numeroidentificacion")))
            site_id = clean_text(row.get("codigohabilitacionsede"))
            if not provider_document or not site_id:
                continue

            provider_name = clean_name(row.get("nombreprestador"))
            company_map[provider_document] = {
                "document_id": provider_document,
                "nit": provider_document,
                "name": provider_name,
                "razon_social": provider_name,
                "nature": clean_text(row.get("naturalezajuridica")),
                "provider_class": clean_text(row.get("claseprestador")),
                "department": clean_text(row.get("departamentoprestadordesc")),
                "municipality": clean_text(row.get("municipioprestadordesc")),
                "address": clean_text(row.get("direccionprestador")),
                "email": clean_text(row.get("email_prestador")),
                "phone": clean_text(row.get("telefonoprestador")),
                "source": "health_providers",
                "country": "CO",
            }

            health_map[site_id] = {
                "reps_code": site_id,
                "name": clean_name(row.get("nombresede")) or site_id,
                "provider_name": provider_name,
                "nature": clean_text(row.get("naturalezajuridica")),
                "claseprestador": clean_text(row.get("claseprestador")),
                "is_ese": clean_text(row.get("ese")),
                "uf": clean_text(row.get("departamentodededesc")),
                "municipio": clean_text(row.get("municipiosededesc")),
                "address": clean_text(row.get("direcci_nsede")),
                "email": clean_text(row.get("email_sede")),
                "phone": clean_text(row.get("t_lefonosede")),
                "source": "health_providers",
                "country": "CO",
            }

            rels.append({
                "source_key": provider_document,
                "target_key": site_id,
                "source": "health_providers",
            })

        self.companies = deduplicate_rows(list(company_map.values()), ["document_id"])
        self.health_sites = deduplicate_rows(list(health_map.values()), ["reps_code"])
        self.rels = deduplicate_rows(rels, ["source_key", "target_key"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0

        if self.companies:
            loaded += loader.load_nodes("Company", self.companies, key_field="document_id")
        if self.health_sites:
            loaded += loader.load_nodes("Health", self.health_sites, key_field="reps_code")
        if self.rels:
            loaded += loader.load_relationships(
                rel_type="OPERA_UNIDAD",
                rows=self.rels,
                source_label="Company",
                source_key="document_id",
                target_label="Health",
                target_key="reps_code",
                properties=["source"],
            )

        self.rows_loaded = loaded
