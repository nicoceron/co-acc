#!/usr/bin/env python3
"""Download SECOP I historical rows connected to the currently loaded graph universe."""

from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import click
from neo4j import GraphDatabase

from coacc_etl.bogota_secop import build_in_clauses
from coacc_etl.pipelines.colombia_shared import clean_text
from coacc_etl.transforms import strip_document

HISTORICAL_DATASET_ID = "qddk-cgux"
RESOURCE_DATASET_ID = "3xwx-53wt"
HISTORICAL_OUTPUT_RELPATH = "secop_i_historical_processes/secop_i_historical_processes.csv"
RESOURCE_OUTPUT_RELPATH = "secop_i_resource_origins/secop_i_resource_origins.csv"
DOCUMENT_TOKEN_RE = re.compile(r"^[0-9.\-\s/]+$")
MIN_IDENTIFIER_LENGTH = 6
MAX_IDENTIFIER_LENGTH = 12
MIN_BPIN_LENGTH = 8

HISTORICAL_COLUMN_FAMILY_MAP = {
    "identificacion_del_contratista": "company",
    "identific_representante_legal": "person",
    "codigo_bpin": "bpin",
}
HISTORICAL_KEY_FIELDS = ("uid", "id_adjudicacion", "numero_de_constancia")
RESOURCE_KEY_FIELDS = ("id_adjudicacion", "id_origen_de_los_recursos", "codigo_bpin")


def _etl_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _repo_root() -> Path:
    return _etl_root().parent


def _downloader_script() -> Path:
    return Path(__file__).with_name("download_socrata_dataset.py")


def _neo4j_password(repo_root: Path) -> str:
    env_path = repo_root / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("NEO4J_PASSWORD="):
                return line.split("=", 1)[1].strip()
    return os.getenv("NEO4J_PASSWORD", "")


def _write_empty_csv(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline=""):
        pass


def _graph_values(query: str) -> tuple[str, ...]:
    repo_root = _repo_root()
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=("neo4j", _neo4j_password(repo_root)),
    )
    values: list[str] = []
    seen: set[str] = set()
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        for record in session.run(query):
            normalized = clean_text(record["value"])
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            values.append(normalized)
    driver.close()
    return tuple(values)


def _normalize_identifier(raw: object) -> str:
    value = clean_text(raw)
    if not value or not DOCUMENT_TOKEN_RE.fullmatch(value):
        return ""
    digits = strip_document(value)
    if not digits:
        return ""
    if len(digits) < MIN_IDENTIFIER_LENGTH or len(digits) > MAX_IDENTIFIER_LENGTH:
        return ""
    if len(set(digits)) == 1:
        return ""
    return digits


def _normalize_bpin(raw: object) -> str:
    digits = strip_document(clean_text(raw))
    if not digits or len(digits) < MIN_BPIN_LENGTH:
        return ""
    return digits


def _filter_identifiers(values: tuple[str, ...]) -> tuple[str, ...]:
    filtered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_identifier(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        filtered.append(normalized)
    return tuple(filtered)


def _filter_bpins(values: tuple[str, ...]) -> tuple[str, ...]:
    filtered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_bpin(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        filtered.append(normalized)
    return tuple(filtered)


def _load_connected_ids() -> dict[str, tuple[str, ...]]:
    company_ids = _graph_values(
        "MATCH (n:Company) "
        "WHERE EXISTS { ()-[:CONTRATOU|ADJUDICOU_A|CELEBRO_CONVENIO_INTERADMIN]->(n) } "
        "RETURN DISTINCT coalesce(n.document_id, n.nit) AS value"
    )
    person_ids = _graph_values(
        "MATCH (n:Person) "
        "WHERE EXISTS { (n)-[:RECIBIO_SALARIO|SANCIONADA|DONO_A|CANDIDATO_EM]->() } "
        "RETURN DISTINCT coalesce(n.document_id, n.cedula) AS value"
    )
    bpin_values = _graph_values(
        "MATCH ()-[r:CONTRATOU]->() "
        "WHERE coalesce(trim(r.bpin_code), '') <> '' "
        "RETURN DISTINCT r.bpin_code AS value"
    )
    return {
        "company": _filter_identifiers(company_ids),
        "person": _filter_identifiers(person_ids),
        "bpin": _filter_bpins(bpin_values),
    }


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


def _row_key(
    row: dict[str, str],
    key_fields: tuple[str, ...],
    fieldnames: list[str],
) -> tuple[str, ...]:
    keyed_values = tuple(row.get(field, "") for field in key_fields)
    if any(keyed_values):
        return keyed_values
    return tuple(row.get(field, "") for field in fieldnames)


def _merge_csv_files(
    *,
    temp_paths: list[Path],
    output_path: Path,
    key_fields: tuple[str, ...],
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    fieldnames: list[str] = []
    seen_fields: set[str] = set()
    readable_paths: list[Path] = []

    for temp_path in temp_paths:
        if not temp_path.exists() or temp_path.stat().st_size == 0:
            continue
        with temp_path.open("r", encoding="utf-8", newline="") as input_handle:
            reader = csv.DictReader(input_handle)
            if not reader.fieldnames:
                continue
            readable_paths.append(temp_path)
            for fieldname in reader.fieldnames:
                if fieldname in seen_fields:
                    continue
                seen_fields.add(fieldname)
                fieldnames.append(fieldname)

    if not readable_paths:
        _write_empty_csv(output_path)
        return 0

    seen: set[tuple[str, ...]] = set()
    with output_path.open("w", encoding="utf-8", newline="") as output_handle:
        writer = csv.DictWriter(output_handle, fieldnames=fieldnames)
        writer.writeheader()
        for temp_path in readable_paths:
            with temp_path.open("r", encoding="utf-8", newline="") as input_handle:
                reader = csv.DictReader(input_handle)
                if not reader.fieldnames:
                    continue
                for row in reader:
                    key = _row_key(row, key_fields, fieldnames)
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow({field: row.get(field, "") for field in fieldnames})
                    rows_written += 1

    return rows_written


def _download_connected_dataset(
    *,
    dataset_id: str,
    output_path: Path,
    values_by_column: dict[str, tuple[str, ...]],
    timeout: int,
    batch_size: int,
    max_clause_chars: int,
    key_fields: tuple[str, ...],
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f"{output_path.stem}-", dir=output_path.parent) as tmp_dir:
        temp_dir = Path(tmp_dir)
        temp_paths: list[Path] = []
        for column, identifiers in values_by_column.items():
            if not identifiers:
                continue
            where_clauses = build_in_clauses(
                column=column,
                values=identifiers,
                max_clause_chars=max_clause_chars,
            )
            for index, where in enumerate(where_clauses, start=1):
                temp_path = temp_dir / f"{column}-{index}.csv"
                _run_download(
                    dataset_id=dataset_id,
                    output_path=temp_path,
                    where=where,
                    timeout=timeout,
                    batch_size=batch_size,
                )
                temp_paths.append(temp_path)
        return _merge_csv_files(
            temp_paths=temp_paths,
            output_path=output_path,
            key_fields=key_fields,
        )


def _load_adjudication_ids(historical_csv_path: Path) -> tuple[str, ...]:
    if not historical_csv_path.exists():
        return ()
    values: list[str] = []
    seen: set[str] = set()
    with historical_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            adjudication_id = _normalize_identifier(row.get("id_adjudicacion"))
            if not adjudication_id or adjudication_id in seen:
                continue
            seen.add(adjudication_id)
            values.append(adjudication_id)
    return tuple(values)


@click.command()
@click.option("--data-dir", default="../data", show_default=True, help="Data directory root")
@click.option("--timeout", default=120, show_default=True, type=int)
@click.option("--batch-size", default=10_000, show_default=True, type=int)
@click.option("--max-clause-chars", default=4_000, show_default=True, type=int)
def main(data_dir: str, timeout: int, batch_size: int, max_clause_chars: int) -> None:
    data_root = Path(data_dir)
    data_root.mkdir(parents=True, exist_ok=True)
    connected_ids = _load_connected_ids()
    click.echo(
        "Loaded connected ids: "
        f"{len(connected_ids['company']):,} company docs, "
        f"{len(connected_ids['person']):,} person docs, "
        f"{len(connected_ids['bpin']):,} BPINs."
    )

    historical_output_path = data_root / HISTORICAL_OUTPUT_RELPATH
    historical_rows = _download_connected_dataset(
        dataset_id=HISTORICAL_DATASET_ID,
        output_path=historical_output_path,
        values_by_column={
            column: connected_ids[family]
            for column, family in HISTORICAL_COLUMN_FAMILY_MAP.items()
        },
        timeout=timeout,
        batch_size=batch_size,
        max_clause_chars=max_clause_chars,
        key_fields=HISTORICAL_KEY_FIELDS,
    )
    click.echo(f"Wrote {historical_rows:,} SECOP I historical rows to {historical_output_path}.")

    adjudication_ids = _load_adjudication_ids(historical_output_path)
    resource_output_path = data_root / RESOURCE_OUTPUT_RELPATH
    if not adjudication_ids:
        _write_empty_csv(resource_output_path)
        click.echo("No connected adjudication ids found; wrote empty SECOP I resource-origin file.")
        return

    resource_rows = _download_connected_dataset(
        dataset_id=RESOURCE_DATASET_ID,
        output_path=resource_output_path,
        values_by_column={"id_adjudicacion": adjudication_ids},
        timeout=timeout,
        batch_size=batch_size,
        max_clause_chars=max_clause_chars,
        key_fields=RESOURCE_KEY_FIELDS,
    )
    click.echo(f"Wrote {resource_rows:,} SECOP I resource-origin rows to {resource_output_path}.")


if __name__ == "__main__":
    main()
