#!/usr/bin/env python3
"""Download DNP BPIN companion datasets connected to the live graph BPIN universe."""

from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import click
from neo4j import GraphDatabase

from coacc_etl.bogota_secop import build_in_clauses
from coacc_etl.pipelines.colombia_shared import clean_text

DATASETS = (
    {
        "name": "dnp_project_executors",
        "dataset_id": "epzv-8ck4",
        "output_relpath": "dnp_project_executors/dnp_project_executors.csv",
        "key_fields": ("bpin", "codigoentidadejecutora"),
    },
    {
        "name": "dnp_project_beneficiary_locations",
        "dataset_id": "iuc2-3r6h",
        "output_relpath": "dnp_project_beneficiary_locations/dnp_project_beneficiary_locations.csv",
        "key_fields": ("bpin", "departamento", "municipio", "entidadresponsable"),
    },
    {
        "name": "dnp_project_beneficiary_characterization",
        "dataset_id": "tmmn-mpqc",
        "output_relpath": (
            "dnp_project_beneficiary_characterization/"
            "dnp_project_beneficiary_characterization.csv"
        ),
        "key_fields": ("bpin", "caracteristicademografica", "entidadresponsable"),
    },
    {
        "name": "dnp_project_locations",
        "dataset_id": "xikz-44ja",
        "output_relpath": "dnp_project_locations/dnp_project_locations.csv",
        "key_fields": ("bpin", "codigodepartamento", "codigomunicipio", "entidadresponsable"),
    },
)

MIN_BPIN_LENGTH = 7
MAX_BPIN_LENGTH = 16


def _etl_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _repo_root() -> Path:
    return _etl_root().parent


def _downloader_script() -> Path:
    return Path(__file__).with_name("download_socrata_dataset.py")


def _neo4j_password(repo_root: Path) -> str:
    env_path = repo_root / ".env"
    if not env_path.exists():
        return os.getenv("NEO4J_PASSWORD", "")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("NEO4J_PASSWORD="):
            return line.split("=", 1)[1].strip()
    return os.getenv("NEO4J_PASSWORD", "")


def _graph_bpin_set() -> tuple[str, ...]:
    repo_root = _repo_root()
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=("neo4j", _neo4j_password(repo_root)),
    )
    queries = (
        (
            "MATCH ()-[r:CONTRATOU]->() "
            "WHERE coalesce(r.bpin_code, '') <> '' "
            "RETURN DISTINCT r.bpin_code AS value"
        ),
        (
            "MATCH (c:Convenio) "
            "WHERE coalesce(c.convenio_id, '') <> '' "
            "RETURN DISTINCT c.convenio_id AS value"
        ),
    )
    values: list[str] = []
    seen: set[str] = set()
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        for query in queries:
            for record in session.run(query):
                normalized = _normalize_bpin(record["value"])
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                values.append(normalized)
    driver.close()
    return tuple(values)


def _normalize_bpin(raw: object) -> str:
    value = "".join(ch for ch in clean_text(raw) if ch.isdigit())
    if len(value) < MIN_BPIN_LENGTH or len(value) > MAX_BPIN_LENGTH:
        return ""
    if len(set(value)) == 1:
        return ""
    return value


def _row_key(
    row: dict[str, str],
    key_fields: tuple[str, ...],
    fieldnames: list[str],
) -> tuple[str, ...]:
    keyed_values = tuple(row.get(field, "") for field in key_fields)
    if any(keyed_values):
        return keyed_values
    return tuple(row.get(field, "") for field in fieldnames)


def _run_download(
    *,
    dataset_id: str,
    output_path: Path,
    where: str,
    timeout: int,
    batch_size: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(_downloader_script()),
        "--dataset-id",
        dataset_id,
        "--output",
        str(output_path),
        "--where",
        where,
        "--timeout",
        str(timeout),
        "--batch-size",
        str(batch_size),
        "--mode",
        "paged-json",
        "--skip-count",
    ]
    subprocess.run(command, check=True, cwd=_etl_root())


def _merge_csv_files(
    *,
    temp_paths: list[Path],
    output_path: Path,
    key_fields: tuple[str, ...],
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen_fields: set[str] = set()
    readable_paths: list[Path] = []
    rows_written = 0

    for temp_path in temp_paths:
        if not temp_path.exists() or temp_path.stat().st_size == 0:
            continue
        with temp_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            readable_paths.append(temp_path)
            for fieldname in reader.fieldnames:
                if fieldname in seen_fields:
                    continue
                seen_fields.add(fieldname)
                fieldnames.append(fieldname)

    if not readable_paths:
        output_path.write_text("", encoding="utf-8")
        return 0

    seen_rows: set[tuple[str, ...]] = set()
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for temp_path in readable_paths:
            with temp_path.open("r", encoding="utf-8", newline="") as input_handle:
                reader = csv.DictReader(input_handle)
                if not reader.fieldnames:
                    continue
                for row in reader:
                    key = _row_key(row, key_fields, fieldnames)
                    if key in seen_rows:
                        continue
                    seen_rows.add(key)
                    writer.writerow({field: row.get(field, "") for field in fieldnames})
                    rows_written += 1

    return rows_written


def _download_connected_dataset(
    *,
    dataset: dict[str, object],
    bpins: tuple[str, ...],
    output_path: Path,
    timeout: int,
    batch_size: int,
    max_clause_chars: int,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="dnp-bpin-", dir=output_path.parent) as tmp_dir:
        temp_dir = Path(tmp_dir)
        temp_paths: list[Path] = []
        where_clauses = build_in_clauses(
            column="bpin",
            values=bpins,
            max_clause_chars=max_clause_chars,
        )
        for index, where in enumerate(where_clauses, start=1):
            temp_path = temp_dir / f"{dataset['name']}-{index}.csv"
            _run_download(
                dataset_id=str(dataset["dataset_id"]),
                output_path=temp_path,
                where=where,
                timeout=timeout,
                batch_size=batch_size,
            )
            temp_paths.append(temp_path)
        return _merge_csv_files(
            temp_paths=temp_paths,
            output_path=output_path,
            key_fields=tuple(dataset["key_fields"]),
        )


@click.command()
@click.option("--data-dir", default="../data", show_default=True, help="Data directory root")
@click.option("--timeout", default=120, show_default=True, type=int)
@click.option("--batch-size", default=10_000, show_default=True, type=int)
@click.option("--max-clause-chars", default=4_000, show_default=True, type=int)
def main(
    data_dir: str,
    timeout: int,
    batch_size: int,
    max_clause_chars: int,
) -> None:
    bpins = _graph_bpin_set()
    if not bpins:
        raise click.ClickException("No connected BPIN identifiers found in the live graph.")

    data_root = Path(data_dir)
    for dataset in DATASETS:
        output_path = data_root / str(dataset["output_relpath"])
        rows = _download_connected_dataset(
            dataset=dataset,
            bpins=bpins,
            output_path=output_path,
            timeout=timeout,
            batch_size=batch_size,
            max_clause_chars=max_clause_chars,
        )
        click.echo(
            f"Wrote {rows:,} rows to {output_path} for {dataset['dataset_id']}.",
        )


if __name__ == "__main__":
    main()
