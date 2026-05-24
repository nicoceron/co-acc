from __future__ import annotations

import zipfile
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from coacc_etl.catalog import DatasetSpec
from coacc_etl.ingest.custom.paco_sanctions import PacoFeed, ingest

if TYPE_CHECKING:
    from pathlib import Path


def _file_url(path: Path) -> str:
    return path.resolve().as_uri()


def test_paco_sanctions_adapter_snapshots_multiple_feeds(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path / "lake"))

    zip_path = tmp_path / "siri.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "siri.txt",
            "111,Ana,Lopez,DISCIPLINARIO,2024-01-03\n222,Carlos,Ruiz,FISCAL,2024-01-04\n",
        )

    csv_path = tmp_path / "colusiones.csv"
    csv_path.write_text(
        "Identificacion,Personas Sancionadas,Falta que origina la sancion,"
        "Fecha de Radicacion,Radicado\n"
        "900123456,ACME SAS,Colusion,2023-02-01,R-1\n",
        encoding="utf-8",
    )

    feeds = (
        PacoFeed(
            name="siri_fixture",
            url=_file_url(zip_path),
            zipped=True,
            columns=(
                "subject_document_id",
                "first_name",
                "last_name",
                "sanction_type",
                "sanction_date",
            ),
            canonical={
                "subject_document_id": "subject_document_id",
                "subject_name": "first_name",
                "subject_last_name": "last_name",
                "sanction_type": "sanction_type",
                "sanction_date": "sanction_date",
            },
        ),
        PacoFeed(
            name="colusion_fixture",
            url=_file_url(csv_path),
            zipped=False,
            columns=(),
            canonical={
                "subject_document_id": "Identificacion",
                "subject_name": "Personas Sancionadas",
                "sanction_type": "Falta que origina la sancion",
                "sanction_date": "Fecha de Radicacion",
                "reference": "Radicado",
            },
        ),
    )
    spec = DatasetSpec(
        id="paco_sanctions",
        name="PACO",
        sector="sanctions",
        tier="core",
        adapter="paco_sanctions",
        full_refresh_only=True,
        join_keys={"nit": ["subject_document_id"]},
        columns_map={"subject_document_id": "subject_document_id"},
        required_coverage={"subject_document_id": 1.0, "subject_name": 1.0},
        url="https://portal.paco.gov.co/index.php?pagina=descargarDatos",
    )

    result = ingest(spec, feeds=feeds, chunk_size=2)

    assert result.ingested
    assert result.rows == 3
    assert result.watermark_delta is None
    assert all("source=paco_sanctions" in str(path) for path in result.parquet_paths)

    frame = pq.read_table(result.parquet_paths).to_pandas()
    assert set(frame["paco_feed"]) == {"siri_fixture", "colusion_fixture"}
    assert set(frame["subject_document_id"]) == {"111", "222", "900123456"}
    assert "raw_record_json" in frame.columns
    assert "Identificacion" in frame["raw_record_json"].iloc[-1]
    assert "raw_first_name" in frame.columns


def test_custom_adapter_id_is_valid_without_socrata_4x4() -> None:
    spec = DatasetSpec(
        id="paco_sanctions",
        name="PACO",
        sector="sanctions",
        tier="core",
        adapter="paco_sanctions",
        full_refresh_only=True,
        join_keys={"nit": ["subject_document_id"]},
        columns_map={"subject_document_id": "subject_document_id"},
        url="https://portal.paco.gov.co/index.php?pagina=descargarDatos",
    )

    assert spec.is_ingest_ready()
