#!/usr/bin/env python3
"""Download a Bogotá-focused SECOP slice using contract and process identifiers."""

from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import click

from coacc_etl.bogota_secop import (
    build_in_clauses,
    load_bogota_contract_scope,
    load_bogota_secop_scope,
)

PRIMARY_DOWNLOADS = (
    {
        "dataset_id": "jbjy-vk9h",
        "output_relpath": "secop_ii_contracts/secop_ii_contracts.csv",
        "where": (
            "departamento = 'Distrito Capital de Bogotá' "
            "AND fecha_de_firma >= '2025-01-01T00:00:00' "
            "AND orden <> 'Nacional'"
        ),
        "label": "SECOP II contracts",
        "limit": 30_000,
        "order": "fecha_de_firma DESC",
    },
)

PROCESS_DOWNLOAD = {
    "dataset_id": "p6dx-8zbt",
    "output_relpath": "secop_ii_processes/secop_ii_processes.csv",
    "column": "id_del_portafolio",
    "key_fields": ("id_del_portafolio", "id_del_proceso"),
    "label": "SECOP II processes",
}

RELATED_DOWNLOADS = (
    {
        "dataset_id": "wi7w-2nvm",
        "output_relpath": "secop_offers/secop_offers.csv",
        "scope_field": "offer_process_portfolio_ids",
        "column": "id_del_proceso_de_compra",
        "key_fields": ("identificador_de_la_oferta", "id_del_proceso_de_compra"),
        "label": "SECOP II offers",
    },
    {
        "dataset_id": "qmzu-gj57",
        "output_relpath": "secop_suppliers/secop_suppliers.csv",
        "scope_field": "supplier_codes",
        "column": "codigo",
        "extra_scope_field": "supplier_nits",
        "extra_column": "nit",
        "key_fields": ("codigo", "nit"),
        "label": "SECOP II suppliers",
    },
    {
        "dataset_id": "ibyt-yi2f",
        "output_relpath": "secop_invoices/secop_invoices.csv",
        "scope_field": "contract_ids",
        "column": "id_contrato",
        "key_fields": ("id_pago", "numero_de_factura", "id_contrato"),
        "label": "SECOP II invoices",
    },
    {
        "dataset_id": "uymx-8p3j",
        "output_relpath": "secop_payment_plans/secop_payment_plans.csv",
        "scope_field": "contract_ids",
        "column": "id_del_contrato",
        "key_fields": ("id_del_contrato", "id_de_pago", "numero_de_factura"),
        "label": "SECOP II payment plans",
    },
    {
        "dataset_id": "mfmm-jqmq",
        "output_relpath": "secop_contract_execution/secop_contract_execution.csv",
        "scope_field": "contract_ids",
        "column": "identificadorcontrato",
        "key_fields": ("identificadorcontrato", "fechacreacion", "nombreplan"),
        "label": "SECOP II contract execution",
    },
    {
        "dataset_id": "cb9c-h8sn",
        "output_relpath": "secop_contract_additions/secop_contract_additions.csv",
        "scope_field": "contract_ids",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "fecharegistro", "tipo", "descripcion"),
        "label": "SECOP II contract additions",
    },
    {
        "dataset_id": "cwhv-7fnp",
        "output_relpath": "secop_budget_items/secop_budget_items.csv",
        "scope_field": "contract_ids",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "identificador_unico", "identificador_item_compromiso"),
        "label": "SECOP II budget items",
    },
    {
        "dataset_id": "u99c-7mfm",
        "output_relpath": "secop_contract_suspensions/secop_contract_suspensions.csv",
        "scope_field": "contract_ids",
        "column": "id_contrato",
        "key_fields": ("id_contrato", "fecha_de_aprobacion", "tipo"),
        "label": "SECOP II contract suspensions",
    },
    {
        "dataset_id": "d9na-abhe",
        "output_relpath": "secop_process_bpin/secop_process_bpin.csv",
        "scope_field": "process_portfolio_ids",
        "column": "id_portafolio",
        "key_fields": ("id_portafolio", "id_proceso", "codigo_bpin"),
        "label": "SECOP II process BPIN",
    },
)

INTERADMIN_DOWNLOADS = (
    {
        "dataset_id": "s484-c9k3",
        "output_relpath": "secop_interadmin_agreements/secop_interadmin_agreements.csv",
        "where": (
            "("
            "departamento like '%BOGOT%' OR municipio like '%BOGOT%' "
            "OR nombre_entidad like '%BOGOT%' OR contratista like '%BOGOT%' "
            "OR departamento like '%CUNDINAMARCA%' OR municipio like '%CUNDINAMARCA%' "
            "OR nombre_entidad like '%CUNDINAMARCA%' OR contratista like '%CUNDINAMARCA%'"
            ") AND fecha_firma >= '2018-01-01T00:00:00.000'"
        ),
        "label": "SECOP interadministrative agreements",
        "limit": 30_000,
        "order": "fecha_firma DESC",
    },
    {
        "dataset_id": "ityv-bxct",
        "output_relpath": (
            "secop_interadmin_agreements_historical/"
            "secop_interadmin_agreements_historical.csv"
        ),
        "where": (
            "("
            "departamento like '%BOGOT%' OR municipio like '%BOGOT%' "
            "OR nombre_entidad like '%BOGOT%' OR contratista like '%BOGOT%' "
            "OR departamento like '%CUNDINAMARCA%' OR municipio like '%CUNDINAMARCA%' "
            "OR nombre_entidad like '%CUNDINAMARCA%' OR contratista like '%CUNDINAMARCA%'"
            ") AND fecha_firma >= '2018-01-01T00:00:00.000'"
        ),
        "label": "SECOP interadministrative agreements historical",
        "limit": 30_000,
        "order": "fecha_firma DESC",
    },
)


def _etl_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _downloader_script() -> Path:
    return Path(__file__).with_name("download_socrata_dataset.py")


def _row_key(
    row: dict[str, str],
    key_fields: tuple[str, ...],
    fieldnames: list[str],
) -> tuple[str, ...]:
    keyed_values = tuple(row.get(field, "") for field in key_fields)
    if any(keyed_values):
        return keyed_values
    return tuple(row.get(field, "") for field in fieldnames)


def _write_empty_csv(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline=""):
        pass


def _run_download(
    *,
    dataset_id: str,
    output_path: Path,
    where: str,
    timeout: int,
    batch_size: int,
    limit: int | None = None,
    order: str | None = None,
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
    if limit is not None:
        command.extend(["--limit", str(limit)])
    if order:
        command.extend(["--order", order])
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

    for spec in PRIMARY_DOWNLOADS:
        output_path = data_root / spec["output_relpath"]
        click.echo(f"Downloading {spec['label']}...")
        _run_download(
            dataset_id=spec["dataset_id"],
            output_path=output_path,
            where=spec["where"],
            timeout=timeout,
            batch_size=batch_size,
            limit=spec.get("limit"),
            order=spec.get("order"),
        )

    for spec in INTERADMIN_DOWNLOADS:
        output_path = data_root / spec["output_relpath"]
        click.echo(f"Downloading {spec['label']}...")
        _run_download(
            dataset_id=spec["dataset_id"],
            output_path=output_path,
            where=spec["where"],
            timeout=timeout,
            batch_size=batch_size,
            limit=spec.get("limit"),
            order=spec.get("order"),
        )

    initial_scope = load_bogota_contract_scope(data_root)
    process_output_path = data_root / PROCESS_DOWNLOAD["output_relpath"]
    process_where_clauses = build_in_clauses(
        column=PROCESS_DOWNLOAD["column"],
        values=initial_scope.process_portfolio_ids,
        max_clause_chars=max_clause_chars,
    )
    click.echo(
        f"Downloading {PROCESS_DOWNLOAD['label']} with "
        f"{len(process_where_clauses):,} clause chunk(s)..."
    )
    process_rows_written = _download_related_dataset(
        dataset_id=PROCESS_DOWNLOAD["dataset_id"],
        output_path=process_output_path,
        where_clauses=process_where_clauses,
        key_fields=PROCESS_DOWNLOAD["key_fields"],
        timeout=timeout,
        batch_size=batch_size,
    )
    click.echo(f"Wrote {process_rows_written:,} deduplicated rows to {process_output_path}")

    scope = load_bogota_secop_scope(data_root)
    click.echo(
        "Bogotá SECOP scope: "
        f"{len(scope.contract_ids):,} contracts, "
        f"{len(scope.process_portfolio_ids):,} process portfolios, "
        f"{len(scope.process_request_ids):,} process requests, "
        f"{len(scope.offer_process_portfolio_ids):,} offer-linked portfolios, "
        f"{len(scope.supplier_codes):,} supplier codes, "
        f"{len(scope.supplier_nits):,} supplier NITs"
    )

    for spec in RELATED_DOWNLOADS:
        output_path = data_root / spec["output_relpath"]
        primary_values = getattr(scope, spec["scope_field"])
        where_clauses = build_in_clauses(
            column=spec["column"],
            values=primary_values,
            max_clause_chars=max_clause_chars,
        )
        extra_scope_field = spec.get("extra_scope_field")
        extra_column = spec.get("extra_column")
        if extra_scope_field and extra_column:
            where_clauses.extend(
                build_in_clauses(
                    column=extra_column,
                    values=getattr(scope, extra_scope_field),
                    max_clause_chars=max_clause_chars,
                )
            )

        click.echo(
            f"Downloading {spec['label']} with {len(where_clauses):,} clause chunk(s)..."
        )
        rows_written = _download_related_dataset(
            dataset_id=spec["dataset_id"],
            output_path=output_path,
            where_clauses=where_clauses,
            key_fields=spec["key_fields"],
            timeout=timeout,
            batch_size=batch_size,
        )
        click.echo(f"Wrote {rows_written:,} deduplicated rows to {output_path}")


if __name__ == "__main__":
    main()
