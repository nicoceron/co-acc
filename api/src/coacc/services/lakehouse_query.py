from __future__ import annotations

import os
import re
from pathlib import Path

import duckdb

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]+")


def lake_root() -> Path:
    return Path(os.environ.get("COACC_LAKE_ROOT", "/var/lib/coacc/lake"))


def source_view_name(source: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", source.strip()).strip("_")
    return f"src_{cleaned or 'unknown'}"


def source_files(source: str) -> list[Path]:
    root = lake_root() / "raw" / f"source={source}"
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.parquet")
        if path.is_file() and not path.name.startswith(".inflight-")
    )


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + value.replace("'", "''") + "'" for value in values) + "]"


def connect(*, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if read_only:
        return duckdb.connect(database=":memory:", read_only=False)
    return duckdb.connect(database=":memory:")


def register_source(con: duckdb.DuckDBPyConnection, source: str) -> str:
    view = source_view_name(source)
    files = [str(path) for path in source_files(source)]
    if not files:
        con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT NULL::VARCHAR AS __empty WHERE false")
        return view
    con.execute(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM read_parquet({_sql_list(files)}, "
        "union_by_name = true, hive_partitioning = true)"
    )
    return view


def watermark_exists(source: str) -> bool:
    path = lake_root() / "meta" / "watermarks.parquet"
    if not path.exists():
        return False
    con = connect(read_only=True)
    try:
        rows = con.execute(
            "SELECT 1 FROM read_parquet(?) WHERE source = ? LIMIT 1",
            [str(path), source],
        ).fetchall()
    finally:
        con.close()
    return bool(rows)


def signal_sql_path(signal_id: str) -> Path:
    return Path(__file__).resolve().parents[4] / "config" / "signals" / "sql" / f"{signal_id}.sql"
