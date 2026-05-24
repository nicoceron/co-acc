from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypedDict

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from coacc_etl.lakehouse.paths import curated_path, lake_root, meta_path
from coacc_etl.lakehouse.reader import register_source, source_files
from coacc_etl.streaming import _socrata_request

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from coacc_etl.catalog import DatasetSpec, JoinKeyClass

logger = logging.getLogger(__name__)
JOIN_KEY_PRIORITY: tuple[JoinKeyClass, ...] = (
    "contract",
    "process",
    "nit",
    "bpin",
    "divipola",
    "entity",
)


class RealityError(RuntimeError):
    """Raised when a dataset cannot be probed from the local lake."""


class CuratedManifestInfo(TypedDict):
    manifest_file_count: int
    manifest_entry_count: int
    path: str | None
    generated_at: datetime | None
    rows: int | None


class ColumnSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    duckdb_type: str


class DatasetHealth(BaseModel):
    """Point-in-time local health metrics for one lake dataset."""

    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    dataset_name: str
    probed_at: datetime
    row_count: int
    parquet_file_count: int
    partition_count: int
    required_coverage: dict[str, float] = Field(default_factory=dict)
    null_rate: dict[str, float] = Field(default_factory=dict)
    coverage_columns: dict[str, str] = Field(default_factory=dict)
    watermark_column: str | None = None
    watermark_min: str | None = None
    watermark_max: str | None = None
    join_key_class: str | None = None
    join_key_columns: list[str] = Field(default_factory=list)
    dup_ratio: float | None = None
    sentinel_fraction: float
    partition_skew: float | None = None
    schema_hash: str
    schema_columns: list[ColumnSchema]
    freshness_seconds: float | None = None
    live_count: int | None = None


class CuratedTableHealth(BaseModel):
    """Point-in-time local health metrics for one curated parquet table."""

    model_config = ConfigDict(extra="forbid")

    table: str
    probed_at: datetime
    row_count: int
    parquet_file_count: int
    schema_hash: str
    schema_columns: list[ColumnSchema]
    freshness_seconds: float | None = None
    manifest_file_count: int
    manifest_entry_count: int
    latest_manifest_path: str | None = None
    latest_manifest_generated_at: datetime | None = None
    latest_manifest_rows: int | None = None
    manifest_row_count_matches: bool | None = None
    evidence_ref_rows: int | None = None
    evidence_ref_count: int | None = None
    evidence_ref_coverage: float | None = None


def socrata_live_count(
    dataset_id: str,
    *,
    where: str | None = None,
    domain: str | None = None,
) -> int:
    params: dict[str, str | int] = {"$select": "count(*)"}
    if where:
        params["$where"] = where
    rows = _socrata_request(
        dataset_id,
        params=params,
        domain=domain,
        timeout=120.0,
        max_attempts=10,
        log_label="live_count",
    )
    if not rows:
        return 0

    count_str = ""
    for row in rows:
        candidate = row.get("count") or row.get("COUNT") or row.get("count_*")
        if candidate is not None:
            count_str = str(candidate)
            break

    if not count_str:
        all_keys: set[str] = set()
        for row in rows:
            all_keys.update(row.keys())
        raise ValueError(
            f"socrata_live_count: no count column in response for {dataset_id}. "
            f"Keys: {sorted(all_keys)}"
        )

    return int(count_str.replace(",", ""))


def compute_dataset_health(
    spec: DatasetSpec,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    with_live: bool = False,
    now: datetime | None = None,
) -> DatasetHealth:
    """Compute local lake health for one dataset without loading parquet into RAM."""
    files = source_files(spec.id)
    if not files:
        raise RealityError(f"{spec.id}: no parquet files under local lake raw path")

    probe_time = now or datetime.now(tz=UTC)
    owned_connection = con is None
    connection = con or duckdb.connect(":memory:")
    try:
        view = register_source(connection, spec.id)
        schema_columns = _schema(connection, view)
        schema_names = {column.name for column in schema_columns}
        row_count = _scalar_int(connection, f"SELECT count(*) FROM {_quote_ident(view)}")

        coverage_columns: dict[str, str] = {}
        null_rate: dict[str, float] = {}
        for required_column, _threshold in spec.required_coverage.items():
            lake_column = _resolve_lake_column(spec, required_column, schema_names)
            if lake_column is None:
                null_rate[required_column] = 1.0
                continue
            coverage_columns[required_column] = lake_column
            nulls = _scalar_int(
                connection,
                f"SELECT count(*) FROM {_quote_ident(view)} "
                f"WHERE NOT ({_present_expr(lake_column)})",
            )
            null_rate[required_column] = _ratio(nulls, row_count)

        watermark_column = _resolve_lake_column(spec, spec.watermark_column, schema_names)
        watermark_min, watermark_max = _watermark_range(connection, view, watermark_column)
        join_key_class, join_key_columns = _select_join_key_columns(spec, schema_names)
        dup_ratio = _dup_ratio(connection, view, row_count, join_key_columns)
        sentinel_fraction = _sentinel_fraction(connection, view, row_count, schema_names)
        partition_skew = _partition_skew(connection, view, schema_names)
        file_count = len(files)
        partition_count = _partition_count(files)
        schema_hash = _schema_hash(schema_columns)
        freshness_seconds = _freshness_seconds(files, probe_time)
        live_count = socrata_live_count(spec.id) if with_live else None

        return DatasetHealth(
            dataset_id=spec.id,
            dataset_name=spec.name,
            probed_at=probe_time,
            row_count=row_count,
            parquet_file_count=file_count,
            partition_count=partition_count,
            required_coverage=spec.required_coverage,
            null_rate=null_rate,
            coverage_columns=coverage_columns,
            watermark_column=watermark_column,
            watermark_min=watermark_min,
            watermark_max=watermark_max,
            join_key_class=join_key_class,
            join_key_columns=join_key_columns,
            dup_ratio=dup_ratio,
            sentinel_fraction=sentinel_fraction,
            partition_skew=partition_skew,
            schema_hash=schema_hash,
            schema_columns=schema_columns,
            freshness_seconds=freshness_seconds,
            live_count=live_count,
        )
    finally:
        if owned_connection:
            connection.close()


def compute_all_health(
    specs: Iterable[DatasetSpec],
    *,
    with_live: bool = False,
    now: datetime | None = None,
) -> list[DatasetHealth]:
    """Compute health for a batch of specs using one DuckDB connection."""
    probe_time = now or datetime.now(tz=UTC)
    con = duckdb.connect(":memory:")
    try:
        results: list[DatasetHealth] = []
        for spec in specs:
            results.append(
                compute_dataset_health(spec, con=con, with_live=with_live, now=probe_time)
            )
        return results
    finally:
        con.close()


def compute_curated_table_health(
    table: str,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    now: datetime | None = None,
) -> CuratedTableHealth:
    """Compute local health for one curated table without loading rows into Python."""
    files = _curated_table_files(table)
    if not files:
        raise RealityError(f"curated:{table}: no parquet files under local curated path")

    probe_time = now or datetime.now(tz=UTC)
    owned_connection = con is None
    connection = con or duckdb.connect(":memory:")
    view = f"curated_probe_{hashlib.sha1(table.encode('utf-8')).hexdigest()[:12]}"
    try:
        connection.execute(
            f"CREATE OR REPLACE TEMP VIEW {_quote_ident(view)} AS "
            f"SELECT * FROM read_parquet({_sql_string_list(files)})"
        )
        schema_columns = _schema(connection, view)
        schema_names = {column.name for column in schema_columns}
        row_count = _scalar_int(connection, f"SELECT count(*) FROM {_quote_ident(view)}")
        evidence_ref_rows: int | None = None
        evidence_ref_count: int | None = None
        evidence_ref_coverage: float | None = None
        evidence_type = next(
            (
                column.duckdb_type
                for column in schema_columns
                if column.name == "evidence_refs"
            ),
            None,
        )
        if "evidence_refs" in schema_names:
            evidence_ref_rows = _evidence_ref_rows(connection, view, evidence_type)
            evidence_ref_count = _evidence_ref_count(connection, view, evidence_type)
            evidence_ref_coverage = _ratio(evidence_ref_rows, row_count)

        manifest = _latest_curated_manifest(table)
        latest_manifest_rows = manifest["rows"]
        return CuratedTableHealth(
            table=table,
            probed_at=probe_time,
            row_count=row_count,
            parquet_file_count=len(files),
            schema_hash=_schema_hash(schema_columns),
            schema_columns=schema_columns,
            freshness_seconds=_freshness_seconds(files, probe_time),
            manifest_file_count=manifest["manifest_file_count"],
            manifest_entry_count=manifest["manifest_entry_count"],
            latest_manifest_path=manifest["path"],
            latest_manifest_generated_at=manifest["generated_at"],
            latest_manifest_rows=latest_manifest_rows,
            manifest_row_count_matches=(
                None if latest_manifest_rows is None else latest_manifest_rows == row_count
            ),
            evidence_ref_rows=evidence_ref_rows,
            evidence_ref_count=evidence_ref_count,
            evidence_ref_coverage=evidence_ref_coverage,
        )
    finally:
        if owned_connection:
            connection.close()


def compute_all_curated_health(
    tables: Iterable[str],
    *,
    now: datetime | None = None,
) -> list[CuratedTableHealth]:
    """Compute health for curated tables using one DuckDB connection."""
    probe_time = now or datetime.now(tz=UTC)
    con = duckdb.connect(":memory:")
    try:
        return [compute_curated_table_health(table, con=con, now=probe_time) for table in tables]
    finally:
        con.close()


def local_curated_tables(root: Path | None = None) -> list[str]:
    """Return curated table names that have local parquet files."""
    curated_root = (root or lake_root()) / "curated"
    if not curated_root.exists():
        return []
    tables: list[str] = []
    for path in sorted(curated_root.glob("table=*")):
        if path.is_dir() and any(path.rglob("*.parquet")):
            tables.append(path.name.split("=", 1)[1])
    return tables


def _schema(con: duckdb.DuckDBPyConnection, view: str) -> list[ColumnSchema]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_quote_ident(view)}").fetchall()
    return [
        ColumnSchema(name=str(row[0]), duckdb_type=str(row[1]))
        for row in rows
        if row and row[0] is not None
    ]


def _schema_hash(schema: list[ColumnSchema]) -> str:
    payload = [(column.name, column.duckdb_type) for column in schema]
    data = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _resolve_lake_column(
    spec: DatasetSpec,
    column: str | None,
    schema_names: set[str],
) -> str | None:
    if not column:
        return None
    if column in schema_names:
        return column

    for canonical, source in spec.columns_map.items():
        if source == column and canonical in schema_names:
            return canonical
    return None


def _select_join_key_columns(
    spec: DatasetSpec, schema_names: set[str]
) -> tuple[str | None, list[str]]:
    ordered_key_classes = [
        key_class for key_class in JOIN_KEY_PRIORITY if key_class in spec.join_keys
    ] + [key_class for key_class in spec.join_keys if key_class not in JOIN_KEY_PRIORITY]
    for key_class in ordered_key_classes:
        columns = spec.join_keys[key_class]
        resolved = [
            column
            for column in (
                _resolve_lake_column(spec, source_column, schema_names) for source_column in columns
            )
            if column is not None
        ]
        if resolved:
            return key_class, resolved
    return None, []


def _dup_ratio(
    con: duckdb.DuckDBPyConnection,
    view: str,
    row_count: int,
    join_key_columns: list[str],
) -> float | None:
    if row_count <= 0 or not join_key_columns:
        return None
    key_expr = _join_key_expr(join_key_columns)
    duplicate_rows = _scalar_int(
        con,
        f"WITH keyed AS ("
        f"SELECT {key_expr} AS __key FROM {_quote_ident(view)} "
        f"WHERE {_all_present_expr(join_key_columns)}"
        f"), counts AS ("
        f"SELECT __key, count(*) AS cnt FROM keyed GROUP BY __key HAVING count(*) > 1"
        f") SELECT coalesce(sum(cnt - 1), 0) FROM counts",
    )
    return _ratio(duplicate_rows, row_count)


def _sentinel_fraction(
    con: duckdb.DuckDBPyConnection,
    view: str,
    row_count: int,
    schema_names: set[str],
) -> float:
    if row_count <= 0:
        return 0.0

    predicates: list[str] = []
    if "year" in schema_names:
        predicates.append("CAST(year AS VARCHAR) IN ('0', '0000')")
    if "month" in schema_names:
        predicates.append("CAST(month AS VARCHAR) IN ('0', '00')")
    if "snapshot" in schema_names:
        predicates.append("CAST(snapshot AS VARCHAR) IN ('', 'unknown')")
    if not predicates:
        return 0.0

    sentinel_rows = _scalar_int(
        con,
        f"SELECT count(*) FROM {_quote_ident(view)} WHERE " + " OR ".join(predicates),
    )
    return _ratio(sentinel_rows, row_count)


def _partition_skew(
    con: duckdb.DuckDBPyConnection,
    view: str,
    schema_names: set[str],
) -> float | None:
    group_columns: list[str]
    if {"year", "month"}.issubset(schema_names):
        group_columns = ["year", "month"]
    elif "snapshot" in schema_names:
        group_columns = ["snapshot"]
    else:
        return None

    quoted = ", ".join(_quote_ident(column) for column in group_columns)
    row = con.execute(
        f"WITH partitions AS ("
        f"SELECT {quoted}, count(*)::DOUBLE AS cnt "
        f"FROM {_quote_ident(view)} GROUP BY {quoted}"
        f") SELECT avg(cnt), stddev_pop(cnt) FROM partitions"
    ).fetchone()
    if not row or row[0] in (None, 0):
        return None
    avg = float(row[0])
    stddev = float(row[1] or 0.0)
    return stddev / avg


def _watermark_range(
    con: duckdb.DuckDBPyConnection,
    view: str,
    watermark_column: str | None,
) -> tuple[str | None, str | None]:
    if watermark_column is None:
        return None, None

    row = con.execute(
        f"SELECT min(CAST({_quote_ident(watermark_column)} AS VARCHAR)), "
        f"max(CAST({_quote_ident(watermark_column)} AS VARCHAR)) "
        f"FROM {_quote_ident(view)} "
        f"WHERE {_present_expr(watermark_column)}"
    ).fetchone()
    if not row:
        return None, None
    min_value = str(row[0]) if row[0] is not None else None
    max_value = str(row[1]) if row[1] is not None else None
    return min_value, max_value


def _partition_count(files: list[Path]) -> int:
    partitions = {file.parent.as_posix() for file in files}
    return len(partitions)


def _freshness_seconds(files: list[Path], now: datetime) -> float | None:
    if not files:
        return None
    latest_mtime = max(file.stat().st_mtime for file in files)
    return max(0.0, now.timestamp() - latest_mtime)


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def _curated_table_files(table: str) -> list[Path]:
    return sorted(path for path in curated_path(table).rglob("*.parquet") if path.is_file())


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_string_list(files: list[Path]) -> str:
    return "[" + ", ".join(_sql_string(str(file)) for file in files) + "]"


def _evidence_ref_rows(
    con: duckdb.DuckDBPyConnection,
    view: str,
    evidence_type: str | None,
) -> int:
    if _is_list_type(evidence_type):
        return _scalar_int(
            con,
            f"SELECT count(*) FROM {_quote_ident(view)} "
            "WHERE evidence_refs IS NOT NULL AND array_length(evidence_refs) > 0",
        )
    return _scalar_int(
        con,
        f"SELECT count(*) FROM {_quote_ident(view)} WHERE {_present_expr('evidence_refs')}",
    )


def _evidence_ref_count(
    con: duckdb.DuckDBPyConnection,
    view: str,
    evidence_type: str | None,
) -> int:
    if _is_list_type(evidence_type):
        return _scalar_int(
            con,
            f"SELECT coalesce(sum(array_length(evidence_refs)), 0) FROM {_quote_ident(view)} "
            "WHERE evidence_refs IS NOT NULL",
        )
    return _evidence_ref_rows(con, view, evidence_type)


def _is_list_type(duckdb_type: str | None) -> bool:
    if duckdb_type is None:
        return False
    normalized = duckdb_type.upper()
    return "[]" in normalized or normalized.startswith("LIST")


def _latest_curated_manifest(table: str) -> CuratedManifestInfo:
    manifest_dir = meta_path() / "curated"
    manifest_paths = sorted(manifest_dir.glob("*.json")) if manifest_dir.exists() else []
    entry_count = 0
    latest_path: Path | None = None
    latest_generated_at: datetime | None = None
    latest_rows: int | None = None
    for path in manifest_paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("Skipping unreadable curated manifest: %s", path)
            continue
        generated_at = _parse_manifest_datetime(raw.get("generated_at"))
        if generated_at is None:
            generated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        tables = raw.get("tables") if isinstance(raw, dict) else None
        if not isinstance(tables, list):
            continue
        for item in tables:
            if not isinstance(item, dict) or item.get("table") != table:
                continue
            entry_count += 1
            if latest_generated_at is None or generated_at > latest_generated_at:
                latest_generated_at = generated_at
                latest_path = path
                latest_rows = _coerce_int(item.get("rows"))
    return {
        "manifest_file_count": len(manifest_paths),
        "manifest_entry_count": entry_count,
        "path": str(latest_path) if latest_path is not None else None,
        "generated_at": latest_generated_at,
        "rows": latest_rows,
    }


def _parse_manifest_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    value = numerator / denominator
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return float(value)


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _present_expr(column: str) -> str:
    quoted = _quote_ident(column)
    return (
        f"{quoted} IS NOT NULL "
        f"AND trim(CAST({quoted} AS VARCHAR)) <> '' "
        f"AND trim(CAST({quoted} AS VARCHAR)) <> 'None'"
    )


def _all_present_expr(columns: list[str]) -> str:
    return " AND ".join(f"({_present_expr(column)})" for column in columns)


def _join_key_expr(columns: list[str]) -> str:
    parts = [f"coalesce(CAST({_quote_ident(column)} AS VARCHAR), '')" for column in columns]
    return "concat_ws('|', " + ", ".join(parts) + ")"


def main() -> None:
    parser = argparse.ArgumentParser(description="Print the live row count of a Socrata dataset.")
    parser.add_argument("dataset_id", help="Socrata dataset ID (e.g. jbjy-vk9h)")
    parser.add_argument("--where", default=None, help="Optional $where clause")
    parser.add_argument(
        "--domain",
        default=None,
        help="Socrata domain (default: SOCRATA_DOMAIN env)",
    )
    args = parser.parse_args()

    count = socrata_live_count(args.dataset_id, where=args.where, domain=args.domain)
    print(f"{args.dataset_id}: {count:,}")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    main()
