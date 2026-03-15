#!/usr/bin/env python3
"""Download CSV-friendly report exports from MapaInversiones."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import click
import httpx

PROXY_API_BASE = (
    "https://mapainversiones.dnp.gov.co/proxy.ashx?"
    "https://mapainversionesapp-api.dnp.gov.co/api/"
)

REPORTS: dict[str, dict[str, Any]] = {
    "project-basics": {
        "endpoint": "Reportes/GenerarReporteDatosGenerales",
        "params": {
            "sector": "null",
            "estado": "null",
            "entidadResponsable": "null",
        },
    },
}


def _ordered_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    return fieldnames


@click.command()
@click.option(
    "--report",
    "report_key",
    type=click.Choice(sorted(REPORTS)),
    default="project-basics",
    show_default=True,
)
@click.option("--vigencia", default=date.today().year, show_default=True, type=int)
@click.option("--output", required=True, help="Output CSV path")
@click.option("--timeout", default=240, show_default=True, type=int)
def main(report_key: str, vigencia: int, output: str, timeout: int) -> None:
    report = REPORTS[report_key]
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    params = dict(report["params"])
    params["vigencia"] = str(vigencia)
    url = f"{PROXY_API_BASE}{report['endpoint']}?{urlencode(params)}"

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, list):
        raise click.ClickException("MapaInversiones endpoint did not return a JSON list payload")

    rows = [row for row in payload if isinstance(row, dict)]
    if not rows:
        raise click.ClickException("MapaInversiones report returned no rows")

    fieldnames = _ordered_fieldnames(rows)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    click.echo(f"Wrote {len(rows):,} rows to {output_path}")


if __name__ == "__main__":
    main()
