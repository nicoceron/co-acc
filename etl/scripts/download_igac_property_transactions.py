#!/usr/bin/env python3
"""Download filtered IGAC property-transaction rows from Socrata."""

from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Any

import click
import httpx

DATASET_URL = "https://www.datos.gov.co/resource/7y2j-43cv.json"


def _build_where(years: tuple[str, ...]) -> str:
    quoted = ",".join(f"'{year}'" for year in years)
    return f"year_radica in({quoted})"


def _ordered_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    return fieldnames


def _fetch_json(
    client: httpx.Client,
    *,
    params: dict[str, Any],
    retries: int = 6,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = client.get(DATASET_URL, params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise click.ClickException(
                    "Unexpected IGAC payload: endpoint did not return a list"
                )
            return [row for row in payload if isinstance(row, dict)]
        except (httpx.HTTPError, click.ClickException) as exc:
            last_error = exc
            wait_seconds = min(2 ** attempt, 30)
            click.echo(
                f"Retrying IGAC request after {wait_seconds}s due to {last_error}",
                err=True,
            )
            time.sleep(wait_seconds)

    assert last_error is not None
    raise click.ClickException(f"IGAC request failed: {last_error}")


@click.command()
@click.option(
    "--year",
    "years",
    multiple=True,
    default=("2023",),
    show_default=True,
    help="Year(s) to download from the IGAC transaction dataset.",
)
@click.option("--output", required=True, help="Output CSV path")
@click.option("--batch-size", default=50_000, show_default=True, type=int)
@click.option("--timeout", default=120, show_default=True, type=int)
def main(years: tuple[str, ...], output: str, batch_size: int, timeout: int) -> None:
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    params = {
        "$select": "count(*)",
        "$where": _build_where(years),
    }

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        count_payload = _fetch_json(client, params=params)
        if not count_payload:
            raise click.ClickException("IGAC count query returned no rows")
        total_rows = int(count_payload[0].get("count", "0"))
        if total_rows <= 0:
            raise click.ClickException("IGAC dataset query returned zero rows")

        first_page = _fetch_json(
            client,
            params={
                "$where": _build_where(years),
                "$limit": batch_size,
                "$offset": 0,
                "$order": "year_radica,pk",
            },
        )
        if not first_page:
            raise click.ClickException("IGAC dataset query returned no materialized rows")

        fieldnames = _ordered_fieldnames(first_page)
        fetched = 0

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()

            offset = 0
            while True:
                rows = first_page if offset == 0 else _fetch_json(
                    client,
                    params={
                        "$where": _build_where(years),
                        "$limit": batch_size,
                        "$offset": offset,
                        "$order": "year_radica,pk",
                    },
                )
                if not rows:
                    break
                for row in rows:
                    writer.writerow({key: row.get(key, "") for key in fieldnames})
                fetched += len(rows)
                click.echo(f"Fetched {fetched:,} / {total_rows:,} rows", err=True)
                offset += batch_size
                if len(rows) < batch_size:
                    break

    click.echo(f"Wrote {fetched:,} rows to {output_path}")


if __name__ == "__main__":
    main()
