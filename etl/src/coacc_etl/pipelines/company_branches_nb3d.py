from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_iso_date,
    read_csv_normalized,
    stable_id,
)
from coacc_etl.streaming import iter_csv_chunks
from coacc_etl.transforms import deduplicate_rows, strip_document

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _branch_document_id(row: dict[str, str]) -> str:
    return stable_id(
        "nb3d_branch",
        row.get("codigo_camara"),
        row.get("matricula"),
        row.get("razon_social"),
        row.get("numero_identificacion"),
    )


class CompanyBranchesNb3dPipeline(Pipeline):
    """Load connected commerce-establishment rows and attach them to existing owners."""

    name = "company_branches_nb3d"
    source_id = "company_branches_nb3d"

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
        self.branches: list[dict[str, Any]] = []
        self.owner_links: list[dict[str, Any]] = []

    def _csv_path(self) -> Path:
        return Path(self.data_dir) / "company_branches_nb3d" / "company_branches_nb3d.csv"

    def extract(self) -> None:
        csv_path = self._csv_path()
        if not csv_path.exists():
            logger.warning("[%s] file not found: %s", self.name, csv_path)
            return

        self._raw = read_csv_normalized(str(csv_path), dtype=str, keep_default_na=False)
        if self.limit:
            self._raw = self._raw.head(self.limit)
        self.rows_in = len(self._raw)

    def _transform_frame(
        self,
        frame: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        branch_map: dict[str, dict[str, Any]] = {}
        owner_links: list[dict[str, Any]] = []

        for row in frame.to_dict(orient="records"):
            branch_document = _branch_document_id(row)
            owner_document = strip_document(clean_text(row.get("nit_propietario")))
            fallback_owner = strip_document(clean_text(row.get("numero_identificacion")))
            owner_document = owner_document or fallback_owner
            if not branch_document or not owner_document:
                continue

            branch_name = clean_name(row.get("razon_social")) or branch_document
            branch_map[branch_document] = {
                "document_id": branch_document,
                "name": branch_name,
                "razon_social": branch_name,
                "company_kind": "ESTABLISHMENT",
                "synthetic_document_id": True,
                "registry_source": "nb3d-v3n7",
                "branch_registry_number": clean_text(row.get("matricula")),
                "registry_chamber_code": clean_text(row.get("codigo_camara")),
                "registry_chamber_name": clean_text(row.get("camara_comercio")),
                "registry_status": clean_text(row.get("estado_matricula")),
                "registry_status_code": clean_text(row.get("codigo_estado_matricula")),
                "registry_category": clean_text(row.get("categoria_matricula")),
                "registry_last_renewed_year": clean_text(row.get("ultimo_ano_renovado")),
                "registered_at": parse_iso_date(row.get("fecha_matricula")),
                "renewed_at": parse_iso_date(row.get("fecha_renovacion")),
                "registry_cancelled_at": parse_iso_date(row.get("fecha_cancelacion")),
                "primary_ciiu_code": clean_text(row.get("cod_ciiu_act_econ_pri")),
                "owner_document_id": owner_document,
                "owner_type": clean_text(row.get("tipo_propietario")),
                "owner_chamber_code": clean_text(row.get("codigo_camara_propietario")),
                "owner_registry_number": clean_text(row.get("matr_cula_propietario")),
                "source": self.source_id,
                "country": "CO",
            }

            owner_links.append({
                "owner_document": owner_document,
                "branch_document": branch_document,
                "source": self.source_id,
                "registry_number": clean_text(row.get("matricula")),
                "role": "ESTABLISHMENT_OWNER",
                "owner_type": clean_text(row.get("tipo_propietario")),
            })

        return (
            deduplicate_rows(list(branch_map.values()), ["document_id"]),
            deduplicate_rows(owner_links, ["owner_document", "branch_document"]),
        )

    def transform(self) -> None:
        self.branches, self.owner_links = self._transform_frame(self._raw)

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.branches:
            loaded += loader.load_nodes("Company", self.branches, key_field="document_id")
        if self.owner_links:
            query = """
                UNWIND $rows AS row
                MATCH (branch:Company {document_id: row.branch_document})
                OPTIONAL MATCH (owner_company:Company {document_id: row.owner_document})
                OPTIONAL MATCH (owner_person:Person {document_id: row.owner_document})
                FOREACH (_ IN CASE WHEN owner_person IS NULL THEN [] ELSE [1] END |
                    FOREACH (__ IN CASE WHEN owner_company IS NULL THEN [1] ELSE [] END |
                        MERGE (owner_person)-[rp:OFFICER_OF]->(branch)
                        SET rp.source = row.source,
                            rp.role = row.role,
                            rp.registry_number = row.registry_number,
                            rp.owner_type = row.owner_type
                    )
                )
                FOREACH (_ IN CASE
                    WHEN owner_company IS NULL OR owner_company.document_id = branch.document_id
                    THEN []
                    ELSE [1]
                END |
                    MERGE (owner_company)-[rc:ADMINISTRA]->(branch)
                    SET rc.source = row.source,
                        rc.role = row.role,
                        rc.registry_number = row.registry_number,
                        rc.owner_type = row.owner_type
                )
            """
            loaded += loader.run_query(query, self.owner_links)
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
        link_query = """
            UNWIND $rows AS row
            MATCH (branch:Company {document_id: row.branch_document})
            OPTIONAL MATCH (owner_company:Company {document_id: row.owner_document})
            OPTIONAL MATCH (owner_person:Person {document_id: row.owner_document})
            FOREACH (_ IN CASE WHEN owner_person IS NULL THEN [] ELSE [1] END |
                FOREACH (__ IN CASE WHEN owner_company IS NULL THEN [1] ELSE [] END |
                    MERGE (owner_person)-[rp:OFFICER_OF]->(branch)
                    SET rp.source = row.source,
                        rp.role = row.role,
                        rp.registry_number = row.registry_number,
                        rp.owner_type = row.owner_type
                )
            )
            FOREACH (_ IN CASE
                WHEN owner_company IS NULL OR owner_company.document_id = branch.document_id
                THEN []
                ELSE [1]
            END |
                MERGE (owner_company)-[rc:ADMINISTRA]->(branch)
                SET rc.source = row.source,
                    rc.role = row.role,
                    rc.registry_number = row.registry_number,
                    rc.owner_type = row.owner_type
            )
        """
        processed = 0
        loaded = 0
        next_log_at = self.chunk_size * 10

        for chunk in iter_csv_chunks(
            csv_path,
            chunk_size=self.chunk_size,
            limit=self.limit,
            dtype=str,
            keep_default_na=False,
        ):
            branches, owner_links = self._transform_frame(chunk)
            if branches:
                loaded += loader.load_nodes("Company", branches, key_field="document_id")
            if owner_links:
                loaded += loader.run_query(link_query, owner_links)

            processed += len(chunk)
            self.rows_in = processed
            self.rows_loaded = loaded
            if processed >= next_log_at:
                logger.info("[%s] streamed %s rows", self.name, f"{processed:,}")
                next_log_at += self.chunk_size * 10
