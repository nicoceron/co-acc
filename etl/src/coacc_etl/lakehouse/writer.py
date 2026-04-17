from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from coacc_etl.lakehouse.paths import raw_path

if TYPE_CHECKING:
    from pathlib import Path


def _as_arrow_table(df: pa.Table | object) -> pa.Table:
    if isinstance(df, pa.Table):
        return df
    return pa.Table.from_pandas(df, preserve_index=False)


def append_parquet(
    df: pa.Table | object,
    source: str,
    year: int,
    month: int,
    compression: str = "zstd",
) -> Path:
    table = _as_arrow_table(df)
    out = raw_path(source, year, month)
    out.mkdir(parents=True, exist_ok=True)

    tmp = out / f".inflight-{uuid.uuid4().hex}.parquet"
    final = out / (
        f"{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid.uuid4().hex[:8]}.parquet"
    )
    try:
        pq.write_table(table, tmp, compression=compression)
        tmp.rename(final)
    finally:
        if tmp.exists():
            tmp.unlink()
    return final
