from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from coacc_etl.lakehouse import append_parquet, watermark
from coacc_etl.lakehouse.watermark import Watermark, WatermarkDelta
from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    import pyarrow as pa


def iter_csv_chunks(
    csv_path: Path,
    *,
    chunk_size: int,
    limit: int | None = None,
    **read_csv_kwargs: Any,
) -> Iterator[pd.DataFrame]:
    """Yield CSV chunks, respecting an optional global row limit."""
    processed = 0
    try:
        chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size, **read_csv_kwargs)
    except pd.errors.EmptyDataError:
        return

    for chunk in chunk_iter:
        if limit is not None:
            remaining = limit - processed
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk.head(remaining).copy()

        if chunk.empty:
            break

        processed += len(chunk)
        yield normalize_dataframe_columns(chunk)

        if limit is not None and processed >= limit:
            break


def socrata_get(
    dataset_id: str,
    *,
    limit: int,
    offset: int,
    order: str = ":id",
    where: str | None = None,
    domain: str | None = None,
    timeout: float = 60.0,
) -> list[dict[str, Any]]:
    base_domain = (domain or os.environ.get("SOCRATA_DOMAIN") or "www.datos.gov.co").strip("/")
    params: dict[str, str | int] = {
        "$limit": limit,
        "$offset": offset,
        "$order": order,
    }
    if where:
        params["$where"] = where
    headers = {}
    app_token = os.environ.get("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    url = f"https://{base_domain}/resource/{dataset_id}.json"
    with httpx.Client(timeout=timeout, headers=headers) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected Socrata payload for {dataset_id}: {type(payload).__name__}")
    return [row for row in payload if isinstance(row, dict)]


def stream_socrata(
    dataset_id: str,
    *,
    chunk_size: int = 10_000,
    order_by: str = ":id",
    where: str | None = None,
) -> Iterator[list[dict[str, Any]]]:
    offset = 0
    while True:
        rows = socrata_get(
            dataset_id,
            limit=chunk_size,
            offset=offset,
            order=order_by,
            where=where,
        )
        if not rows:
            return
        yield rows
        offset += chunk_size
        if len(rows) < chunk_size:
            return


def pipeline_stream_to_lake(
    source: str,
    dataset_id: str,
    *,
    normalizer: Callable[[list[dict[str, Any]]], pa.Table],
    chunk_size: int = 10_000,
    where: str | None = None,
) -> WatermarkDelta:
    rows_written = 0
    batch_id = uuid.uuid4().hex
    for chunk in stream_socrata(dataset_id, chunk_size=chunk_size, where=where):
        table = normalizer(chunk)
        now = datetime.now(tz=UTC)
        append_parquet(table, source=source, year=now.year, month=now.month)
        rows_written += len(chunk)
    advanced_at = datetime.now(tz=UTC)
    watermark.set(Watermark(source, advanced_at, batch_id, rows_written))
    return WatermarkDelta(
        source=source,
        rows=rows_written,
        batch_id=batch_id,
        advanced=True,
        last_seen_ts=advanced_at,
    )
