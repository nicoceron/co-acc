from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    import duckdb

from coacc_etl.lakehouse.paths import raw_source_path

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]+")


def source_view_name(source: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", source.strip()).strip("_")
    return f"src_{cleaned or 'unknown'}"


def source_files(source: str) -> list[Path]:
    root = raw_source_path(source)
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.parquet")
        if path.is_file() and not path.name.startswith(".inflight-")
    )


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + value.replace("'", "''") + "'" for value in values) + "]"


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
