from __future__ import annotations

import csv
import re
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from coacc_etl.lakehouse.paths import raw_source_path
from coacc_etl.runtime_paths import docs_dataset_file

_SAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]+")


def source_view_name(source: str) -> str:
    cleaned = _SAFE_IDENTIFIER.sub("_", source.strip()).strip("_")
    return f"src_{cleaned or 'unknown'}"


def _catalog_path() -> Path:
    return docs_dataset_file("catalog.proven.csv")


@lru_cache(maxsize=8)
def _source_aliases_for(path_key: str) -> dict[str, tuple[str, ...]]:
    """Map semantic source ids from registries to raw dataset ids."""
    catalog_path = Path(path_key)
    if not catalog_path.exists():
        return {}
    aliases: dict[str, list[str]] = {}
    with catalog_path.open(newline="", encoding="utf-8") as fh:
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


def _source_aliases() -> dict[str, tuple[str, ...]]:
    return _source_aliases_for(str(_catalog_path()))


def resolve_source_ids(source: str) -> tuple[str, ...]:
    """Return raw lake source ids for a semantic or raw source id."""
    direct = raw_source_path(source)
    if direct.exists():
        return (source,)
    return _source_aliases().get(source, (source,))


def source_files(source: str) -> list[Path]:
    files: list[Path] = []
    for source_id in resolve_source_ids(source):
        root = raw_source_path(source_id)
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
