#!/usr/bin/env python3
"""Download c82u-588k rows connected to the currently loaded graph universe."""

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

DATASET_ID = "c82u-588k"
OUTPUT_RELPATH = "company_registry_c82u/company_registry_c82u.csv"
KEY_FIELDS = (
    "codigo_camara",
    "matricula",
    "numero_identificacion",
    "num_identificacion_representante_legal",
)
COLUMN_FAMILY_MAP = {
    "numero_identificacion": "company",
    "nit": "company",
    "num_identificacion_representante_legal": "person",
}
DOCUMENT_TOKEN_RE = re.compile(r"^[0-9.\-\s/]+$")
MIN_IDENTIFIER_LENGTH = 6
MAX_IDENTIFIER_LENGTH = 12


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


def _write_empty_csv(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline=""):
        pass


def _graph_identifier_set(*, label: str, properties: tuple[str, ...]) -> tuple[str, ...]:
    repo_root = _repo_root()
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=("neo4j", _neo4j_password(repo_root)),
    )
    property_expr = ", ".join(f"n.{prop}" for prop in properties)
    query = f"MATCH (n:{label}) RETURN DISTINCT coalesce({property_expr}) AS value"
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
    if not value:
        return ""
    if not DOCUMENT_TOKEN_RE.fullmatch(value):
        return ""
    digits = strip_document(value)
    if not digits:
        return ""
    if len(digits) < MIN_IDENTIFIER_LENGTH or len(digits) > MAX_IDENTIFIER_LENGTH:
        return ""
    if len(set(digits)) == 1:
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


def _load_connected_ids() -> dict[str, tuple[str, ...]]:
    raw_company_ids = _graph_identifier_set(label="Company", properties=("document_id", "nit"))
    raw_person_ids = _graph_identifier_set(label="Person", properties=("document_id", "cedula"))
    return {
        "company": _filter_identifiers(raw_company_ids),
        "person": _filter_identifiers(raw_person_ids),
    }


def _load_connected_id_stats() -> tuple[dict[str, tuple[str, ...]], dict[str, int]]:
    raw_company_ids = _graph_identifier_set(label="Company", properties=("document_id", "nit"))
    raw_person_ids = _graph_identifier_set(label="Person", properties=("document_id", "cedula"))
    filtered = {
        "company": _filter_identifiers(raw_company_ids),
        "person": _filter_identifiers(raw_person_ids),
    }
    stats = {
        "raw_company": len(raw_company_ids),
        "raw_person": len(raw_person_ids),
        "filtered_company": len(filtered["company"]),
        "filtered_person": len(filtered["person"]),
    }
    return filtered, stats


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
        DATASET_ID,
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


def _merge_csv_files(*, temp_paths: list[Path], output_path: Path) -> int:
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
                    key = _row_key(row, KEY_FIELDS, fieldnames)
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow({field: row.get(field, "") for field in fieldnames})
                    rows_written += 1

    return rows_written


def _download_connected_dataset(
    *,
    output_path: Path,
    connected_ids: dict[str, tuple[str, ...]],
    timeout: int,
    batch_size: int,
    max_clause_chars: int,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="company-registry-", dir=output_path.parent) as tmp_dir:
        temp_dir = Path(tmp_dir)
        temp_paths: list[Path] = []
        for column, family in COLUMN_FAMILY_MAP.items():
            identifiers = connected_ids[family]
            where_clauses = build_in_clauses(
                column=column,
                values=identifiers,
                max_clause_chars=max_clause_chars,
            )
            for index, where in enumerate(where_clauses, start=1):
                temp_path = temp_dir / f"{column}-{index}.csv"
                _run_download(
                    output_path=temp_path,
                    where=where,
                    timeout=timeout,
                    batch_size=batch_size,
                )
                temp_paths.append(temp_path)
        return _merge_csv_files(temp_paths=temp_paths, output_path=output_path)


@click.command()
@click.option("--data-dir", default="../data", show_default=True, help="Data directory root")
@click.option("--timeout", default=120, show_default=True, type=int)
@click.option("--batch-size", default=10_000, show_default=True, type=int)
@click.option("--max-clause-chars", default=4_000, show_default=True, type=int)
def main(data_dir: str, timeout: int, batch_size: int, max_clause_chars: int) -> None:
    data_root = Path(data_dir)
    data_root.mkdir(parents=True, exist_ok=True)

    connected_ids, stats = _load_connected_id_stats()
    click.echo(
        "Loaded "
        f"{stats['raw_company']:,} company ids and "
        f"{stats['raw_person']:,} person ids from the live graph."
    )
    click.echo(
        "Retained "
        f"{stats['filtered_company']:,} document-shaped company ids and "
        f"{stats['filtered_person']:,} document-shaped person ids after filtering."
    )

    output_path = data_root / OUTPUT_RELPATH
    rows_written = _download_connected_dataset(
        output_path=output_path,
        connected_ids=connected_ids,
        timeout=timeout,
        batch_size=batch_size,
        max_clause_chars=max_clause_chars,
    )
    click.echo(f"Wrote {rows_written:,} rows to {output_path}.")


if __name__ == "__main__":
    main()
