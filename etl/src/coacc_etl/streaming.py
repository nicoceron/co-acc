from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def iter_csv_chunks(
    csv_path: Path,
    *,
    chunk_size: int,
    limit: int | None = None,
    **read_csv_kwargs: Any,
) -> Iterator[pd.DataFrame]:
    """Yield CSV chunks, respecting an optional global row limit."""
    processed = 0
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, **read_csv_kwargs):
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
