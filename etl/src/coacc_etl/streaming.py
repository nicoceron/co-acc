from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

from coacc_etl.lakehouse import append_parquet, watermark
from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    import pyarrow as pa

    from coacc_etl.lakehouse.watermark import WatermarkDelta


LOG = logging.getLogger(__name__)

_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
    httpx.PoolTimeout,
    httpx.HTTPStatusError,
)


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


def _socrata_request(
    dataset_id: str,
    *,
    params: dict[str, str | int],
    domain: str | None = None,
    timeout: float = 60.0,
    max_attempts: int = 10,
    initial_backoff: float = 2.0,
    max_backoff: float = 300.0,
    log_label: str = "",
) -> list[dict[str, Any]]:
    base_domain = (domain or os.environ.get("SOCRATA_DOMAIN") or "www.datos.gov.co").strip("/")
    headers: dict[str, str] = {}
    app_token = os.environ.get("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token

    url = f"https://{base_domain}/resource/{dataset_id}.json"
    last_error: Exception | None = None
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            with httpx.Client(timeout=timeout, headers=headers) as client:
                response = client.get(url, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError(
                        f"retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(
                    f"Unexpected Socrata payload for {dataset_id}: {type(payload).__name__}"
                )
            return [row for row in payload if isinstance(row, dict)]
        except _RETRYABLE_EXCEPTIONS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            LOG.warning(
                "socrata_%s %s attempt=%s/%s failed (%s); retrying in %.1fs",
                log_label or "request",
                dataset_id,
                attempt,
                max_attempts,
                exc.__class__.__name__,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    if last_error is None:
        raise RuntimeError(f"socrata request exhausted retries for {dataset_id}")
    raise last_error


def socrata_get(
    dataset_id: str,
    *,
    limit: int,
    offset: int,
    order: str = ":id",
    where: str | None = None,
    domain: str | None = None,
    timeout: float = 60.0,
    max_attempts: int = 10,
    initial_backoff: float = 2.0,
    max_backoff: float = 300.0,
) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {
        "$limit": limit,
        "$offset": offset,
        "$order": order,
    }
    if where:
        params["$where"] = where
    return _socrata_request(
        dataset_id,
        params=params,
        domain=domain,
        timeout=timeout,
        max_attempts=max_attempts,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        log_label=f"get_offset={offset}",
    )


def stream_socrata(
    dataset_id: str,
    *,
    chunk_size: int = 10_000,
    order_by: str = ":id",
    where: str | None = None,
    start_offset: int = 0,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield (offset_after_chunk, rows) pairs so callers can checkpoint progress."""
    offset = start_offset
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
        offset += len(rows)
        yield offset, rows
        if len(rows) < chunk_size:
            return


def pipeline_stream_to_lake(
    source: str,
    dataset_id: str,
    *,
    normalizer: Callable[[list[dict[str, Any]]], pa.Table],
    chunk_size: int = 10_000,
    where: str | None = None,
    full_refresh: bool = False,
) -> WatermarkDelta:
    batch_id = uuid.uuid4().hex
    start_offset = 0
    rows_written = 0

    if not full_refresh:
        existing = watermark.get(source)
        if existing and existing.last_offset is not None:
            start_offset = int(existing.last_offset)
            rows_written = int(existing.row_count)
            LOG.info(
                "[%s] resuming from watermark offset=%s rows=%s",
                source,
                start_offset,
                rows_written,
            )

    for next_offset, chunk in stream_socrata(
        dataset_id,
        chunk_size=chunk_size,
        where=where,
        start_offset=start_offset,
    ):
        table = normalizer(chunk)
        now = datetime.now(tz=UTC)
        append_parquet(table, source=source, year=now.year, month=now.month)
        rows_written += len(chunk)
        watermark.advance(
            source,
            rows=rows_written,
            batch_id=batch_id,
            last_seen_ts=now,
            last_offset=next_offset,
        )

    # Loop exited naturally (final page returned). Clear offset so the next
    # run uses the incremental `$where` filter rather than resuming pagination.
    return watermark.advance(
        source,
        rows=rows_written,
        batch_id=batch_id,
        last_offset=None,
    )
