#!/usr/bin/env python3
"""Refresh large national Socrata sources in manageable partitions."""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import click


@dataclass(frozen=True)
class PartitionSpec:
    name: str
    where: str


ROOT = Path(__file__).resolve().parents[2]
ETL_ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = Path(__file__).with_name("download_socrata_dataset.py")


def payment_plan_partitions() -> list[PartitionSpec]:
    partitions = [
        PartitionSpec(
            name="legacy_or_null",
            where=(
                "fecha_inicio_contrato IS NULL "
                "OR fecha_inicio_contrato < '2016-01-01T00:00:00'"
            ),
        )
    ]
    for year in range(2016, 2031):
        partitions.append(
            PartitionSpec(
                name=str(year),
                where=(
                    f"fecha_inicio_contrato between '{year}-01-01T00:00:00' "
                    f"and '{year}-12-31T23:59:59'"
                ),
            )
        )
    return partitions


def process_bpin_partitions() -> list[PartitionSpec]:
    return [PartitionSpec(name=str(year), where=f"anno_bpin = '{year}'") for year in range(2020, 2028)]


def _run(command: list[str], *, cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _download_partition(
    *,
    dataset_id: str,
    output_path: Path,
    where: str,
    batch_size: int,
    timeout: int,
) -> None:
    command = [
        sys.executable,
        str(DOWNLOADER),
        "--dataset-id",
        dataset_id,
        "--output",
        str(output_path),
        "--where",
        where,
        "--batch-size",
        str(batch_size),
        "--timeout",
        str(timeout),
        "--mode",
        "paged-json",
    ]
    _run(command, cwd=ETL_ROOT)


def _run_pipeline(
    *,
    source: str,
    data_dir: Path,
    neo4j_password: str,
    chunk_size: int,
) -> None:
    command = [
        "uv",
        "run",
        "coacc-etl",
        "run",
        "--source",
        source,
        "--neo4j-password",
        neo4j_password,
        "--data-dir",
        str(data_dir),
        "--streaming",
        "--chunk-size",
        str(chunk_size),
    ]
    _run(command, cwd=ETL_ROOT)


def _cypher(statement: str) -> None:
    command = [
        "docker",
        "exec",
        "coacc-neo4j",
        "cypher-shell",
        "-u",
        "neo4j",
        "-p",
        "",
        statement,
    ]
    _run(command, cwd=ROOT)


def reset_payment_plan_enrichment() -> None:
    _cypher("MATCH (:Person)-[r:SUPERVISA_PAGO]->() DELETE r")
    _cypher(
        "MATCH ()-[r:CONTRATOU]->() "
        "REMOVE "
        "r.payment_plan_count, "
        "r.payment_actual_count, "
        "r.payment_pending_count, "
        "r.payment_delay_count, "
        "r.payment_total_value, "
        "r.payment_actual_total, "
        "r.latest_payment_event_date, "
        "r.latest_payment_estimate, "
        "r.latest_payment_actual_date, "
        "r.latest_payment_status, "
        "r.latest_payment_approver, "
        "r.latest_payment_invoice_number, "
        "r.latest_payment_supervisor_document, "
        "r.latest_payment_supervisor_name, "
        "r.latest_payment_supervisor_type, "
        "r.latest_payment_radicado, "
        "r.latest_payment_cufe"
    )


def reset_bpin_enrichment() -> None:
    _cypher(
        "MATCH ()-[r:CONTRATOU]->() "
        "REMOVE "
        "r.bpin_link_count, "
        "r.bpin_validated_count, "
        "r.bpin_unvalidated_count, "
        "r.bpin_code, "
        "r.bpin_year, "
        "r.bpin_validation_status"
    )


@click.command()
@click.option(
    "--source",
    type=click.Choice(["payment-plans", "bpin", "both"]),
    default="both",
    show_default=True,
)
@click.option("--neo4j-password", default="", show_default=True)
@click.option("--batch-size", default=50_000, show_default=True, type=int)
@click.option("--chunk-size", default=50_000, show_default=True, type=int)
@click.option("--timeout", default=120, show_default=True, type=int)
def main(
    source: str,
    neo4j_password: str,
    batch_size: int,
    chunk_size: int,
    timeout: int,
) -> None:
    data_dir = ROOT / "data"

    def refresh_payment_plans() -> None:
        click.echo("Resetting payment-plan enrichment...", err=True)
        reset_payment_plan_enrichment()
        output_path = data_dir / "secop_payment_plans" / "secop_payment_plans.csv"
        for partition in payment_plan_partitions():
            click.echo(f"[payment-plans] downloading partition {partition.name}", err=True)
            _download_partition(
                dataset_id="uymx-8p3j",
                output_path=output_path,
                where=partition.where,
                batch_size=batch_size,
                timeout=timeout,
            )
            click.echo(f"[payment-plans] loading partition {partition.name}", err=True)
            _run_pipeline(
                source="secop_payment_plans",
                data_dir=data_dir,
                neo4j_password=neo4j_password,
                chunk_size=chunk_size,
            )

    def refresh_bpin() -> None:
        click.echo("Resetting BPIN enrichment...", err=True)
        reset_bpin_enrichment()
        output_path = data_dir / "secop_process_bpin" / "secop_process_bpin.csv"
        for partition in process_bpin_partitions():
            click.echo(f"[bpin] downloading partition {partition.name}", err=True)
            _download_partition(
                dataset_id="d9na-abhe",
                output_path=output_path,
                where=partition.where,
                batch_size=batch_size,
                timeout=timeout,
            )
            click.echo(f"[bpin] loading partition {partition.name}", err=True)
            _run_pipeline(
                source="secop_process_bpin",
                data_dir=data_dir,
                neo4j_password=neo4j_password,
                chunk_size=chunk_size,
            )

    if source in {"payment-plans", "both"}:
        refresh_payment_plans()
    if source in {"bpin", "both"}:
        refresh_bpin()


if __name__ == "__main__":
    main()
