from __future__ import annotations

import argparse
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from coacc_etl.lakehouse.paths import lake_root

if TYPE_CHECKING:
    from pathlib import Path

_DURATION = re.compile(r"^(?P<count>\d+)(?P<unit>[dhm])$")


def _parse_duration(value: str) -> timedelta:
    match = _DURATION.match(value.strip())
    if not match:
        raise argparse.ArgumentTypeError("duration must look like 30d, 12h, or 90m")
    count = int(match.group("count"))
    unit = match.group("unit")
    if unit == "d":
        return timedelta(days=count)
    if unit == "h":
        return timedelta(hours=count)
    return timedelta(minutes=count)


def _partition_dirs(root: Path) -> list[Path]:
    raw_root = root / "raw"
    if not raw_root.exists():
        return []
    return sorted(path for path in raw_root.glob("source=*/year=*/month=*") if path.is_dir())


def compact_partition(partition: Path, *, older_than: timedelta) -> Path | None:
    cutoff = datetime.now(tz=UTC) - older_than
    files = [
        path
        for path in partition.glob("*.parquet")
        if path.is_file()
        and not path.name.startswith(".inflight-")
        and datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) < cutoff
    ]
    if len(files) < 2:
        return None

    table = pq.read_table(files)
    tmp = partition / f".inflight-compact-{uuid.uuid4().hex}.parquet"
    final = partition / f"compact-{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}.parquet"
    try:
        pq.write_table(table, tmp, compression="zstd")
        tmp.rename(final)
        for path in files:
            if path != final and path.exists():
                path.unlink()
    finally:
        if tmp.exists():
            tmp.unlink()
    return final


def compact_lake(*, older_than: timedelta) -> list[Path]:
    outputs: list[Path] = []
    for partition in _partition_dirs(lake_root()):
        output = compact_partition(partition, older_than=older_than)
        if output:
            outputs.append(output)
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--older-than", type=_parse_duration, default=timedelta(days=30))
    args = parser.parse_args()
    for output in compact_lake(older_than=args.older_than):
        print(output)


if __name__ == "__main__":
    main()
