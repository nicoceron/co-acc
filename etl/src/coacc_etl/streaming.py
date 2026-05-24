"""Low-level Socrata + CSV helpers used outside the generic ingester.

The bulk of this module's prior surface (``stream_socrata`` /
``pipeline_stream_to_lake``) was the legacy resumable-streaming path that
served the bespoke pipelines now retired in Wave 4. What survives here are:

- ``iter_csv_chunks`` — chunked CSV reader still used by the legacy
  ``test_colombia_shared`` regression test and by any future custom adapter
  that consumes CSVs.
- ``_socrata_request`` — single-page HTTP fetch with retry/backoff still
  used by ``coacc_etl.lakehouse.reality`` to do live freshness probes
  against Socrata datasets (one cheap row each).

The supported ingest path is now :mod:`coacc_etl.ingest.socrata` —
``SocrataClient`` paginates with the same retry rules and writes parquet
through ``lakehouse.writer``.
"""
from __future__ import annotations

import logging
import os
import time
from typing import TYPE_CHECKING, Any

import httpx
import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


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
        yield chunk

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
                msg = f"Unexpected Socrata payload for {dataset_id}: {type(payload).__name__}"
                raise ValueError(msg)
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
        msg = f"socrata request exhausted retries for {dataset_id}"
        raise RuntimeError(msg)
    raise last_error
