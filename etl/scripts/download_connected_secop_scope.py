#!/usr/bin/env python3
"""Download SECOP related datasets limited to the currently loaded contract scope."""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import click

from coacc_etl.bogota_secop import build_in_clauses
from coacc_etl.pipelines.colombia_shared import clean_text

RELATED_DOWNLOADS = (
    {
        "name": "secop_cdp_requests",
        "dataset_id": "a86w-fh92",
        "output_relpath": "secop_cdp_requests/secop_cdp_requests.csv",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "id_siif", "c_digo_cdp"),
        "label": "SECOP II CDP requests",
    },
    {
        "name": "secop_execution_locations",
        "dataset_id": "gra4-pcp2",
        "output_relpath": "secop_execution_locations/secop_execution_locations.csv",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "proceso_de_compra", "ubicacion"),
        "label": "SECOP II execution locations",
    },
    {
        "name": "secop_additional_locations",
        "dataset_id": "wwhe-4sq8",
        "output_relpath": "secop_additional_locations/secop_additional_locations.csv",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "departamento", "ciudad", "direcci_n_original"),
        "label": "SECOP II additional locations",
    },
    {
        "name": "secop_document_archives",
        "dataset_id": "dmgg-8hin",
        "output_relpath": "secop_document_archives/secop_document_archives.csv",
        "column": "proceso",
        "key_fields": ("proceso", "id_documento"),
        "label": "SECOP II document archives",
        "value_scope": "process",
    },
)

QUARANTINED_DOWNLOADS = (
    {
        "name": "secop_budget_commitments",
        "dataset_id": "skc9-met7",
        "column": "id_contrato",
        "label": "SECOP II budget commitments",
        "reason": "No overlap with the current contract summary map in direct live sampling.",
    },
)


def _etl_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _downloader_script() -> Path:
    return Path(__file__).with_name("download_socrata_dataset.py")


def _write_empty_csv(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline=""):
        pass


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
    ]
    subprocess.run(command, check=True, cwd=_etl_root())


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


def _download_related_dataset(
    *,
    dataset_id: str,
    output_path: Path,
    where_clauses: list[str],
    key_fields: tuple[str, ...],
    timeout: int,
    batch_size: int,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not where_clauses:
        _write_empty_csv(output_path)
        return 0

    with TemporaryDirectory(prefix=f"{output_path.stem}-", dir=output_path.parent) as tmp_dir:
        temp_dir = Path(tmp_dir)
        temp_paths: list[Path] = []
        for index, where in enumerate(where_clauses, start=1):
            temp_path = temp_dir / f"{output_path.stem}-{index}.csv"
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


def _load_contract_ids(data_root: Path) -> tuple[str, ...]:
    summary_map_path = data_root / "secop_ii_contracts" / "contract_summary_map.csv"
    if not summary_map_path.exists():
        raise click.ClickException(f"Contract summary map not found: {summary_map_path}")

    ordered: list[str] = []
    seen: set[str] = set()
    with summary_map_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            contract_id = clean_text(row.get("contract_id"))
            if not contract_id or contract_id in seen:
                continue
            seen.add(contract_id)
            ordered.append(contract_id)
    return tuple(ordered)


def _load_process_ids(data_root: Path) -> tuple[str, ...]:
    contracts_path = data_root / "secop_ii_contracts" / "secop_ii_contracts.csv"
    if not contracts_path.exists():
        raise click.ClickException(f"SECOP II contracts file not found: {contracts_path}")

    ordered: list[str] = []
    seen: set[str] = set()
    with contracts_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            process_id = clean_text(row.get("proceso_de_compra"))
            if not process_id or process_id in seen:
                continue
            seen.add(process_id)
            ordered.append(process_id)
    return tuple(ordered)


@click.command()
@click.option("--data-dir", default="../data", show_default=True, help="Data directory root")
@click.option("--timeout", default=120, show_default=True, type=int)
@click.option("--batch-size", default=10_000, show_default=True, type=int)
@click.option(
    "--max-clause-chars",
    default=12_000,
    show_default=True,
    type=int,
    help="Maximum approximate SoQL WHERE clause size for identifier chunking",
)
def main(data_dir: str, timeout: int, batch_size: int, max_clause_chars: int) -> None:
    data_root = Path(data_dir)
    data_root.mkdir(parents=True, exist_ok=True)
    contract_ids: tuple[str, ...] | None = None
    process_ids: tuple[str, ...] | None = None

    for spec in QUARANTINED_DOWNLOADS:
        click.echo(f"Skipping {spec['label']}: {spec['reason']}")

    for spec in RELATED_DOWNLOADS:
        value_scope = spec.get("value_scope", "contract")
        if value_scope == "process":
            if process_ids is None:
                process_ids = _load_process_ids(data_root)
                click.echo(f"Loaded {len(process_ids):,} process ids from current graph scope.")
            scope_values = process_ids
        else:
            if contract_ids is None:
                contract_ids = _load_contract_ids(data_root)
                click.echo(f"Loaded {len(contract_ids):,} contract ids from current graph scope.")
            scope_values = contract_ids

        output_path = data_root / spec["output_relpath"]
        where_clauses = build_in_clauses(
            column=spec["column"],
            values=scope_values,
            max_clause_chars=max_clause_chars,
        )
        click.echo(
            f"Downloading {spec['label']} with {len(where_clauses):,} connected WHERE clause(s)..."
        )
        rows_written = _download_related_dataset(
            dataset_id=spec["dataset_id"],
            output_path=output_path,
            where_clauses=where_clauses,
            key_fields=spec["key_fields"],
            timeout=timeout,
            batch_size=batch_size,
        )
        click.echo(f"Wrote {rows_written:,} rows to {output_path}.")


if __name__ == "__main__":
    main()
