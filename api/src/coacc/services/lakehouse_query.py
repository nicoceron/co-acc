from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from pathlib import Path

import duckdb

from coacc.services.runtime_paths import config_file, docs_dataset_file

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]+")
_CATALOG_PATH = docs_dataset_file("catalog.proven.csv")


def lake_root() -> Path:
    return Path(os.environ.get("COACC_LAKE_ROOT", "/var/lib/coacc/lake"))


def source_view_name(source: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", source.strip()).strip("_")
    return f"src_{cleaned or 'unknown'}"


@lru_cache(maxsize=1)
def _source_aliases() -> dict[str, tuple[str, ...]]:
    """Map semantic source ids from registries to raw dataset ids."""
    if not _CATALOG_PATH.exists():
        return {}
    aliases: dict[str, list[str]] = {}
    with _CATALOG_PATH.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            dataset_id = (row.get("dataset_id") or "").strip()
            source_refs = (row.get("source_refs") or "").strip()
            if not dataset_id or not source_refs:
                continue
            for source_ref in source_refs.split("|"):
                source_ref = source_ref.strip()
                if not source_ref:
                    continue
                aliases.setdefault(source_ref, [])
                if dataset_id not in aliases[source_ref]:
                    aliases[source_ref].append(dataset_id)
    return {key: tuple(value) for key, value in aliases.items()}


def resolve_source_ids(source: str) -> tuple[str, ...]:
    """Return raw lake source ids for a semantic or raw source id."""
    direct = lake_root() / "raw" / f"source={source}"
    if direct.exists():
        return (source,)
    return _source_aliases().get(source, (source,))


def source_files(source: str) -> list[Path]:
    files: list[Path] = []
    for source_id in resolve_source_ids(source):
        root = lake_root() / "raw" / f"source={source_id}"
        if not root.exists():
            continue
        files.extend(
            path
            for path in root.rglob("*.parquet")
            if path.is_file() and not path.name.startswith(".inflight-")
        )
    return sorted(files)


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
        return bool(source_files(source))
    source_ids = list(resolve_source_ids(source))
    con = connect(read_only=True)
    try:
        rows = con.execute(
            "SELECT 1 FROM read_parquet(?) WHERE source IN (SELECT unnest(?)) LIMIT 1",
            [str(path), source_ids],
        ).fetchall()
    finally:
        con.close()
    return bool(rows) or bool(source_files(source))


def signal_sql_path(signal_id: str) -> Path:
    return config_file("signals", "sql", f"{signal_id}.sql")
