from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from coacc_etl.lakehouse.paths import meta_path

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class Watermark:
    source: str
    last_seen_ts: datetime
    last_batch_id: str
    row_count: int
    advanced_at: datetime | None = None
    last_offset: int | None = None


@dataclass(frozen=True)
class WatermarkDelta:
    source: str
    rows: int
    batch_id: str
    advanced: bool
    last_seen_ts: datetime
    last_offset: int | None = None


def _latest_path() -> Path:
    return meta_path() / "watermarks.parquet"


def _events_path() -> Path:
    return meta_path() / "watermark_events.parquet"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _read_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()


def _write_frame_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".inflight-{uuid.uuid4().hex}-{path.name}")
    table = pa.Table.from_pandas(frame, preserve_index=False)
    try:
        pq.write_table(table, tmp, compression="zstd")
        tmp.rename(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _to_record(watermark: Watermark, advanced_at: datetime | None = None) -> dict[str, object]:
    advanced = _utc(advanced_at or watermark.advanced_at or datetime.now(tz=UTC))
    return {
        "source": watermark.source,
        "last_seen_ts": _utc(watermark.last_seen_ts),
        "last_batch_id": watermark.last_batch_id,
        "row_count": int(watermark.row_count),
        "advanced_at": advanced,
        "last_offset": (
            int(watermark.last_offset) if watermark.last_offset is not None else None
        ),
    }


def _from_row(row: pd.Series) -> Watermark:
    offset_raw = row.get("last_offset") if hasattr(row, "get") else None
    last_offset = int(offset_raw) if offset_raw is not None and pd.notna(offset_raw) else None
    return Watermark(
        source=str(row["source"]),
        last_seen_ts=_utc(pd.Timestamp(row["last_seen_ts"]).to_pydatetime()),
        last_batch_id=str(row["last_batch_id"]),
        row_count=int(row["row_count"]),
        advanced_at=_utc(pd.Timestamp(row["advanced_at"]).to_pydatetime()),
        last_offset=last_offset,
    )


def get(source: str) -> Watermark | None:
    frame = _read_frame(_latest_path())
    if frame.empty or "source" not in frame:
        return None
    rows = frame.loc[frame["source"] == source]
    if rows.empty:
        return None
    rows = rows.sort_values("advanced_at")
    return _from_row(rows.iloc[-1])


def set(watermark: Watermark) -> None:  # noqa: A001
    existing = get(watermark.source)
    next_seen = _utc(watermark.last_seen_ts)
    if existing and next_seen < _utc(existing.last_seen_ts):
        raise ValueError(
            f"Watermark for {watermark.source} would regress from "
            f"{existing.last_seen_ts.isoformat()} to {next_seen.isoformat()}"
        )

    record = _to_record(watermark)
    latest = _read_frame(_latest_path())
    latest = latest.loc[latest["source"] != watermark.source] if not latest.empty else latest
    latest = pd.concat([latest, pd.DataFrame([record])], ignore_index=True)
    _write_frame_atomic(latest, _latest_path())

    events = _read_frame(_events_path())
    events = pd.concat([events, pd.DataFrame([record])], ignore_index=True)
    _write_frame_atomic(events, _events_path())


def advance(
    source: str,
    *,
    rows: int,
    batch_id: str | None = None,
    last_seen_ts: datetime | None = None,
    last_offset: int | None = None,
) -> WatermarkDelta:
    seen = _utc(last_seen_ts or datetime.now(tz=UTC))
    current = get(source)
    if current and seen < _utc(current.last_seen_ts):
        raise ValueError(
            f"Watermark for {source} would regress from "
            f"{current.last_seen_ts.isoformat()} to {seen.isoformat()}"
        )
    batch = batch_id or uuid.uuid4().hex
    set(
        Watermark(
            source=source,
            last_seen_ts=seen,
            last_batch_id=batch,
            row_count=rows,
            last_offset=last_offset,
        )
    )
    return WatermarkDelta(
        source=source,
        rows=rows,
        batch_id=batch,
        advanced=current is None or seen > _utc(current.last_seen_ts) or rows > 0,
        last_seen_ts=seen,
        last_offset=last_offset,
    )
