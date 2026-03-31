from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.base import Pipeline
from coacc_etl.loader import Neo4jBatchLoader
from coacc_etl.pipelines.colombia_shared import (
    clean_name,
    clean_text,
    parse_iso_date,
    read_csv_normalized_with_fallback,
    stable_id,
)
from coacc_etl.transforms import deduplicate_rows

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


class EnvironmentalFilesPipeline(Pipeline):
    """Load environmental files and complaints as documentary overlap signals."""

    name = "environmental_files"
    source_id = "environmental_files"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size, **kwargs)
        self._frames: list[pd.DataFrame] = []
        self.files: list[dict[str, Any]] = []
        self.company_file_rows: list[dict[str, Any]] = []

    def extract(self) -> None:
        paths = [
            Path(self.data_dir)
            / "environmental_files"
            / "corpoboyaca_environmental_files.csv",
            Path(self.data_dir)
            / "environmental_files"
            / "corantioquia_licenses.csv",
            Path(self.data_dir)
            / "environmental_files"
            / "car_cundinamarca_complaints.csv",
        ]
        frames: list[pd.DataFrame] = []
        for path in paths:
            if not path.exists():
                continue
            frames.append(
                read_csv_normalized_with_fallback(
                    str(path),
                    dtype=str,
                    keep_default_na=False,
                )
            )
        if not frames:
            logger.warning("[%s] no environmental input files found under data/environmental_files", self.name)
            return

        self._frames = frames
        self.rows_in = sum(len(frame) for frame in frames)
        if self.limit:
            remaining = self.limit
            limited_frames: list[pd.DataFrame] = []
            for frame in frames:
                if remaining <= 0:
                    break
                limited = frame.head(remaining)
                limited_frames.append(limited)
                remaining -= len(limited)
            self._frames = limited_frames
            self.rows_in = sum(len(frame) for frame in self._frames)

    def transform(self) -> None:
        files: list[dict[str, Any]] = []
        company_file_rows: list[dict[str, Any]] = []

        for frame in self._frames:
            rows = frame.to_dict(orient="records")
            if not rows:
                continue
            columns = set(frame.columns)
            for row in rows:
                if "nombre_o_raz_n_social" in columns or "nombre_o_razon_social" in columns:
                    dataset = "corpoboyaca_environmental_files"
                    file_id = stable_id(
                        "env_file",
                        dataset,
                        row.get("expediente"),
                        row.get("no_radicado"),
                        row.get("fecha_de_creaci_n"),
                    )
                    subject_name = clean_name(
                        row.get("nombre_o_raz_n_social") or row.get("nombre_o_razon_social")
                    )
                    title = (
                        clean_text(row.get("nombre_del_proyecto"))
                        or clean_text(row.get("expediente"))
                        or file_id
                    )
                    search_text = " ".join(
                        value
                        for value in (
                            title,
                            subject_name,
                            clean_text(row.get("municipio")),
                            clean_text(row.get("tipo_de_solicitud")),
                        )
                        if value
                    )
                    files.append(
                        {
                            "file_id": file_id,
                            "title": title,
                            "name": title,
                            "summary": clean_text(row.get("tipo_de_solicitud")),
                            "expediente": clean_text(row.get("expediente")),
                            "status": clean_text(row.get("estado")),
                            "subject_name": subject_name,
                            "municipality": clean_text(row.get("municipio")),
                            "territorial": clean_text(row.get("territorial")),
                            "date": parse_iso_date(row.get("fecha_de_creaci_n")),
                            "search_text": search_text,
                            "source_dataset": dataset,
                            "source": self.source_id,
                            "country": "CO",
                        }
                    )
                    if subject_name:
                        company_file_rows.append(
                            {
                                "file_id": file_id,
                                "subject_name": subject_name,
                                "source": self.source_id,
                            }
                        )
                    continue

                if "tipo_empresa" in columns and "tipo_proyecto" in columns:
                    dataset = "corantioquia_licenses"
                    file_id = stable_id(
                        "env_file",
                        dataset,
                        row.get("expediente"),
                        row.get("municipio"),
                        row.get("tipo_proyecto"),
                    )
                    title = clean_text(row.get("tipo_proyecto")) or clean_text(row.get("expediente")) or file_id
                    files.append(
                        {
                            "file_id": file_id,
                            "title": title,
                            "name": title,
                            "summary": clean_text(row.get("naturaleza")),
                            "expediente": clean_text(row.get("expediente")),
                            "status": clean_text(row.get("estado")),
                            "municipality": clean_text(row.get("municipio")),
                            "territorial": clean_text(row.get("territorial")),
                            "search_text": " ".join(
                                value
                                for value in (
                                    title,
                                    clean_text(row.get("naturaleza")),
                                    clean_text(row.get("municipio")),
                                    clean_text(row.get("tipo_empresa")),
                                )
                                if value
                            ),
                            "source_dataset": dataset,
                            "source": self.source_id,
                            "country": "CO",
                        }
                    )
                    continue

                dataset = "car_cundinamarca_complaints"
                file_id = stable_id(
                    "env_file",
                    dataset,
                    row.get("item"),
                    row.get("fecha_radicaci_n"),
                    row.get("usuario"),
                )
                title = clean_text(row.get("tipo_petici_n")) or f"Queja ambiental {clean_text(row.get('item'))}"
                files.append(
                    {
                        "file_id": file_id,
                        "title": title,
                        "name": title,
                        "summary": clean_text(row.get("tipo_respuesta")),
                        "expediente": clean_text(row.get("item")),
                        "status": clean_text(row.get("color_estado")),
                        "subject_name": clean_name(row.get("usuario")),
                        "date": parse_iso_date(row.get("fecha_radicaci_n")),
                        "search_text": " ".join(
                            value
                            for value in (
                                title,
                                clean_text(row.get("dependencia_principal")),
                                clean_text(row.get("usuario")),
                            )
                            if value
                        ),
                        "source_dataset": dataset,
                        "source": self.source_id,
                        "country": "CO",
                    }
                )

        self.files = deduplicate_rows(files, ["file_id"])
        self.company_file_rows = deduplicate_rows(company_file_rows, ["file_id", "subject_name"])

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)
        loaded = 0
        if self.files:
            loaded += loader.load_nodes("EnvironmentalFile", self.files, key_field="file_id")
        if self.company_file_rows:
            relationship_rows: list[dict[str, Any]] = []
            company_ids_by_name: dict[str, list[str]] = {}
            with self.driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
                result = session.run(
                    """
                    MATCH (c:Company)
                    WITH c,
                         coalesce(c.document_id, '') AS document_id,
                         toUpper(coalesce(c.razon_social, c.name, '')) AS subject_name
                    WHERE document_id <> '' AND subject_name <> ''
                    RETURN document_id, subject_name
                    """
                )
                for row in result:
                    company_ids_by_name.setdefault(row["subject_name"], []).append(row["document_id"])

            for row in self.company_file_rows:
                for document_id in company_ids_by_name.get(row["subject_name"], []):
                    relationship_rows.append(
                        {
                            "source_key": document_id,
                            "target_key": row["file_id"],
                            "source": row["source"],
                            "subject_match": "exact_normalized_name",
                        }
                    )

            relationship_rows = deduplicate_rows(
                relationship_rows,
                ["source_key", "target_key"],
            )
            if relationship_rows:
                loaded += loader.load_relationships(
                    rel_type="REGISTRO_AMBIENTAL",
                    rows=relationship_rows,
                    source_label="Company",
                    source_key="document_id",
                    target_label="EnvironmentalFile",
                    target_key="file_id",
                    properties=["source", "subject_match"],
                )
        self.rows_loaded = loaded
