#!/usr/bin/env python3
"""Download a Socrata dataset to CSV."""

from __future__ import annotations

import csv
import os
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

import click
import httpx
from dotenv import load_dotenv

# Search for .env starting from script parent up to project root
env_path = Path(__file__).resolve().parents[2] / ".env"
click.echo(f"Debug: Loading .env from {env_path}")
load_dotenv(env_path, override=True)

for k, v in os.environ.items():
    if k.startswith("SOCRATA_"):
        click.echo(f"Debug Env: {k}={v}")


def _dataset_url(domain: str, dataset_id: str) -> str:
    return f"https://{domain}/resource/{dataset_id}.json"


def _export_url(domain: str, dataset_id: str) -> str:
    return f"https://{domain}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"


def _tmp_output_path(output_path: Path) -> Path:
    return output_path.with_suffix(f"{output_path.suffix}.part")


def _download_via_export(
    client: httpx.Client,
    *,
    dataset_id: str,
    domain: str,
    output_path: Path,
    report_every_bytes: int = 16 * 1024 * 1024,
) -> int:
    tmp_path = _tmp_output_path(output_path)
    downloaded = 0
    last_reported = 0

    with client.stream("GET", _export_url(domain, dataset_id)) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as output_file:
            for chunk in response.iter_bytes():
                if not chunk:
                    continue
                output_file.write(chunk)
                downloaded += len(chunk)
                if downloaded - last_reported >= report_every_bytes:
                    click.echo(f"Downloaded {downloaded / (1024 * 1024):,.1f} MiB", err=True)
                    last_reported = downloaded

    tmp_path.replace(output_path)
    return downloaded


def _download_via_paged_json(
    client: httpx.Client,
    *,
    dataset_id: str,
    domain: str,
    output_path: Path,
    batch_size: int,
    workers: int,
    limit: int | None = None,
    where: str | None = None,
) -> int:
    tmp_path = _tmp_output_path(output_path)
    fieldnames: list[str] = []

    params = {"$select": "count(*)"}
    if where:
        params["$where"] = where

    count_response = client.get(_dataset_url(domain, dataset_id), params=params)
    count_response.raise_for_status()
    count_payload = count_response.json()
    if not isinstance(count_payload, list) or not count_payload:
        raise click.ClickException("Unexpected Socrata payload: count query returned no rows")
    total_available = int(count_payload[0]["count"])

    if limit is not None:
        total_available = min(total_available, limit)

    def fetch_page(offset: int) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                page_params = {"$limit": batch_size, "$offset": offset}
                if where:
                    page_params["$where"] = where
                response = client.get(
                    _dataset_url(domain, dataset_id),
                    params=page_params,
                )
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list):
                    raise click.ClickException("Unexpected Socrata payload: page is not a list")
                return [row for row in rows if isinstance(row, dict)]
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = exc.response.status_code
                if status_code not in {408, 429, 500, 502, 503, 504}:
                    raise
            except httpx.HTTPError as exc:
                last_error = exc
            wait_seconds = min(2 ** attempt, 30)
            click.echo(
                f"Retrying offset {offset:,} after {wait_seconds}s due to {last_error}",
                err=True,
            )
            time.sleep(wait_seconds)
        assert last_error is not None
        raise click.ClickException(f"Paged JSON download failed at offset {offset:,}: {last_error}")

    first_page = fetch_page(0)
    if not first_page:
        with tmp_path.open("w", encoding="utf-8", newline=""):
            pass
        tmp_path.replace(output_path)
        return 0

    first = first_page[0]
    fieldnames = list(first.keys())
    total_rows = 0

    with tmp_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        def write_rows(rows: list[dict[str, Any]]) -> None:
            nonlocal total_rows
            for row in rows:
                normalized: dict[str, Any] = {key: row.get(key, "") for key in fieldnames}
                writer.writerow(normalized)
            total_rows += len(rows)
            click.echo(f"Fetched {total_rows:,} rows", err=True)

        write_rows(first_page)

        offsets = list(range(len(first_page), total_available, batch_size))
        if workers <= 1:
            for offset in offsets:
                rows = fetch_page(offset)
                if not rows:
                    break
                write_rows(rows)
        else:
            max_workers = max(1, workers)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                active: dict[Future[list[dict[str, Any]]], int] = {}
                next_index = 0

                while next_index < len(offsets) and len(active) < max_workers:
                    offset = offsets[next_index]
                    active[executor.submit(fetch_page, offset)] = offset
                    next_index += 1

                while active:
                    done, _ = wait(active.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        active.pop(future)
                        rows = future.result()
                        if rows:
                            write_rows(rows)
                        while next_index < len(offsets) and len(active) < max_workers:
                            offset = offsets[next_index]
                            active[executor.submit(fetch_page, offset)] = offset
                            next_index += 1

    tmp_path.replace(output_path)
    return total_rows


@click.command()
@click.option("--dataset-id", required=True, help="Socrata dataset id, e.g. rpmr-utcd")
@click.option("--output", required=True, help="Output CSV path")
@click.option("--domain", default="www.datos.gov.co", show_default=True)
@click.option("--batch-size", default=50_000, show_default=True, type=int)
@click.option("--paged-json-workers", default=1, show_default=True, type=int)
@click.option("--timeout", default=60, show_default=True, type=int)
@click.option("--limit", type=int, default=None, help="Limit rows processed")
@click.option("--where", type=str, default=None, help="SoQL WHERE clause")
@click.option(
    "--mode",
    type=click.Choice(["auto", "export", "paged-json"]),
    default="auto",
    show_default=True,
    help="Download strategy. 'auto' tries CSV export first, then falls back to paged JSON.",
)
def main(
    dataset_id: str,
    output: str,
    domain: str,
    batch_size: int,
    paged_json_workers: int,
    timeout: int,
    limit: int | None,
    where: str | None,
    mode: str,
) -> None:
    """Download a Socrata dataset and write it to CSV."""
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers: dict[str, str] = {}
    app_token = os.getenv("SOCRATA_APP_TOKEN", "").strip()
    key_id = os.getenv("SOCRATA_KEY_ID", "").strip()
    key_secret = os.getenv("SOCRATA_KEY_SECRET", "").strip()

    # Socrata often rejects requests that send both Basic Auth and an App Token 
    # if the App Token is the same as the Key ID. 
    if app_token and app_token != key_id:
        headers["X-App-Token"] = app_token

    click.echo(f"Debug: Using Key ID: {key_id}")
    click.echo(f"Debug: Using App Token header: {headers.get('X-App-Token')}")

    auth = None
    if key_id and key_secret:
        auth = (key_id, key_secret)

    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers, auth=auth) as client:
        if mode in {"auto", "export"}:
            try:
                total_bytes = _download_via_export(
                    client,
                    dataset_id=dataset_id,
                    domain=domain,
                    output_path=output_path,
                )
                click.echo(
                    f"Wrote {output_path} ({total_bytes / (1024 * 1024):,.1f} MiB via CSV export)"
                )
                return
            except (httpx.HTTPError, OSError) as exc:
                if mode == "export":
                    raise click.ClickException(f"CSV export download failed: {exc}") from exc
                tmp_path = _tmp_output_path(output_path)
                if tmp_path.exists() and tmp_path.stat().st_size > 0:
                    preserved_path = output_path.with_suffix(f"{output_path.suffix}.export.partial")
                    preserved_path.unlink(missing_ok=True)
                    tmp_path.replace(preserved_path)
                    click.echo(
                        "CSV export interrupted after "
                        f"{preserved_path.stat().st_size / (1024 * 1024):,.1f} MiB; "
                        f"preserved partial download at {preserved_path}",
                        err=True,
                    )
                click.echo(
                    f"CSV export unavailable for {dataset_id}, falling back to paged JSON: {exc}",
                    err=True,
                )

        total_rows = _download_via_paged_json(
            client,
            dataset_id=dataset_id,
            domain=domain,
            output_path=output_path,
            batch_size=batch_size,
            workers=paged_json_workers,
            limit=limit,
            where=where,
        )
        click.echo(f"Wrote {total_rows:,} rows to {output_path} via paged JSON")


if __name__ == "__main__":
    main()
