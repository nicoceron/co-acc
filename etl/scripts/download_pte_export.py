#!/usr/bin/env python3
"""Download CSV exports from Colombia's PTE ASP.NET portal."""

from __future__ import annotations

import html
import re
from pathlib import Path

import click
import httpx

BASE_URL = "https://pte-prueba.azurewebsites.net"
EXPORT_BUTTON_NAME = "ctl00$content$cmdExportarCSV"
EXPORT_BUTTON_VALUE = "Exportar CSV"
HIDDEN_FIELDS = ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION")
PAGE_PATHS = {
    "top-contracts": "ContratosMasGrandes",
    "sector-commitments": "CompromisosVigenciaActualSector",
}


def _page_url(page_path: str) -> str:
    return f"{BASE_URL}/{page_path}"


def _extract_hidden_field(page_html: str, field_name: str) -> str:
    pattern = re.compile(
        rf'name="{re.escape(field_name)}"[^>]*value="([^"]*)"',
        re.IGNORECASE,
    )
    match = pattern.search(page_html)
    if not match:
        raise click.ClickException(f"Could not locate hidden field {field_name}")
    return html.unescape(match.group(1))


@click.command()
@click.option("--page", "page_key", type=click.Choice(sorted(PAGE_PATHS)), required=True)
@click.option("--output", required=True, help="Output CSV path")
@click.option("--timeout", default=120, show_default=True, type=int)
def main(page_key: str, output: str, timeout: int) -> None:
    page_path = PAGE_PATHS[page_key]
    page_url = _page_url(page_path)
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        page_response = client.get(page_url)
        page_response.raise_for_status()
        page_html = page_response.text

        payload = {name: _extract_hidden_field(page_html, name) for name in HIDDEN_FIELDS}
        payload[EXPORT_BUTTON_NAME] = EXPORT_BUTTON_VALUE

        response = client.post(page_url, data=payload)
        response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "csv" not in content_type.lower():
        raise click.ClickException(
            f"PTE export did not return CSV content (content-type={content_type!r})"
        )

    output_path.write_bytes(response.content)
    click.echo(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
