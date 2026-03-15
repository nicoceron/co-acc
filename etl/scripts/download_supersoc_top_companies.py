#!/usr/bin/env python3
"""Download the latest Supersociedades top-company workbook and normalize it to CSV."""

from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path

import click
import httpx
import pandas as pd

DEFAULT_REPORT_PAGE = (
    "https://www.supersociedades.gov.co/web/asuntos-economicos-societarios/"
    "sector-real-de-la-economia/-/asset_publisher/dbnl/content/"
    "informe-1000-empresas-m%C3%A1s-grandes-a%C3%B1o-2024"
)
XLSX_URL_PATTERN = re.compile(
    r"https://www\.supersociedades\.gov\.co/documents/[^\"' ]+\.xlsx[^\"' ]*",
    re.IGNORECASE,
)


def _extract_xlsx_url(page_html: str) -> str:
    match = XLSX_URL_PATTERN.search(page_html)
    if not match:
        raise click.ClickException(
            "Could not find an XLSX download link on the Supersociedades page"
        )
    return match.group(0)


@click.command()
@click.option("--report-page", default=DEFAULT_REPORT_PAGE, show_default=True)
@click.option("--output", required=True, help="Output CSV path")
@click.option("--timeout", default=240, show_default=True, type=int)
@click.option("--header-row", default=4, show_default=True, type=int)
def main(report_page: str, output: str, timeout: int, header_row: int) -> None:
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        page_response = client.get(report_page)
        page_response.raise_for_status()
        workbook_url = _extract_xlsx_url(page_response.text)
        workbook_response = client.get(workbook_url)
        workbook_response.raise_for_status()

    dataframe = pd.read_excel(
        BytesIO(workbook_response.content),
        sheet_name=0,
        header=header_row,
        dtype=str,
    )
    dataframe = dataframe.dropna(how="all")
    dataframe.to_csv(output_path, index=False)

    click.echo(f"Wrote {len(dataframe):,} rows to {output_path}")


if __name__ == "__main__":
    main()
