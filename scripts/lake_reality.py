#!/usr/bin/env python3
"""Lake-vs-live reality reconciler.

Scans every source in lake/raw/, computes quality metrics, and outputs
a CSV report to lake/meta/reality_report.csv. Compares lake row counts
and column null percentages against the live Socrata source.

If COACC_LAKE_ROOT is not set and ./lake exists relative to CWD, uses that.
Otherwise exits 1 with a clear error.
"""
from __future__ import annotations

import csv
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from coacc_etl.lakehouse.paths import lake_root, meta_path
from coacc_etl.lakehouse.reader import source_files, source_view_name
from coacc_etl.lakehouse.reality import socrata_live_count

logger = logging.getLogger(__name__)

SOCRATA_IDS: dict[str, str] = {
    "secop_integrado": "jbjy-vk9h",
}

CRITICAL_COLUMNS: dict[str, list[str]] = {
    "secop_integrado": [
        "buyer_name",
        "supplier_name",
        "contract_value",
        "supplier_document_id",
        "buyer_document_id",
    ],
}

PK_COLUMNS: dict[str, str] = {
    "secop_integrado": "id_contrato",
}


def _ensure_lake_root() -> Path:
    root = lake_root()
    if root.exists():
        return root
    cwd_lake = Path.cwd() / "lake"
    if cwd_lake.exists() and cwd_lake.is_dir():
        os.environ["COACC_LAKE_ROOT"] = str(cwd_lake.resolve())
        return cwd_lake
    print(
        f"ERROR: No lake found. Set COACC_LAKE_ROOT or run from a directory "
        f"containing ./lake. Checked: {root}, {cwd_lake}",
        file=sys.stderr,
    )
    sys.exit(1)


def _discover_sources(raw_root: Path) -> list[str]:
    if not raw_root.exists():
        return []
    sources = set()
    for path in raw_root.glob("source=*"):
        if path.is_dir():
            name = path.name.split("=", 1)[1]
            sources.add(name)
    return sorted(sources)


def _live_count_for_source(source: str) -> int | None:
    socrata_id = SOCRATA_IDS.get(source)
    if not socrata_id:
        return None
    try:
        return socrata_live_count(socrata_id)
    except Exception as exc:
        logger.warning("live count failed for %s (%s): %s", source, socrata_id, exc)
        return None


def _compute_metrics(con: duckdb.DuckDBPyConnection, source: str) -> dict[str, object]:
    view = source_view_name(source)
    files = [str(p) for p in source_files(source)]
    if not files:
        return {}

    con.execute(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM read_parquet({files}, union_by_name=true, hive_partitioning=true)"
    )

    metrics: dict[str, object] = {"source": source, "dirty": False}

    con.execute(f"SELECT count(*) FROM {view}")
    metrics["lake_rows"] = con.fetchone()[0]

    pk_col = PK_COLUMNS.get(source)
    if pk_col:
        col_norm = pk_col.lower().replace(" ", "_")
        try:
            con.execute(f"SELECT count(DISTINCT {col_norm}) FROM {view}")
            metrics["lake_distinct_pk"] = con.fetchone()[0]
        except Exception as exc:
            logger.warning("[%s] lake_distinct_pk query failed: %s", source, exc)
            metrics["lake_distinct_pk"] = None
            metrics["dirty"] = True

        try:
            con.execute(
                f"SELECT count(*) FROM (SELECT {col_norm}, count(*) AS cnt "
                f"FROM {view} GROUP BY {col_norm} HAVING cnt > 1) AS dupes"
            )
            metrics["lake_distinct_pk_dupes"] = con.fetchone()[0]
        except Exception as exc:
            logger.warning("[%s] lake_distinct_pk_dupes query failed: %s", source, exc)
            metrics["lake_distinct_pk_dupes"] = None
            metrics["dirty"] = True

    critical = CRITICAL_COLUMNS.get(source, [])
    for col in critical:
        col_norm = col.lower().replace(" ", "_")
        col_display = col
        try:
            con.execute(
                f"SELECT count(*) FROM {view} WHERE {col_norm} IS NULL "
                f"OR CAST({col_norm} AS VARCHAR) = '' "
                f"OR CAST({col_norm} AS VARCHAR) = 'None'"
            )
            null_count = con.fetchone()[0]
            pct = round(100.0 * null_count / max(int(metrics["lake_rows"]), 1), 2)
            metrics[f"{col_display}_null_pct"] = pct
        except Exception as exc:
            logger.warning("[%s] %s null_pct query failed: %s", source, col_display, exc)
            metrics[f"{col_display}_null_pct"] = None
            metrics["dirty"] = True

    fecha_col = "fecha_de_firma"
    try:
        con.execute(
            f"SELECT min(CAST({fecha_col} AS VARCHAR)), max(CAST({fecha_col} AS VARCHAR)) "
            f"FROM {view} WHERE {fecha_col} IS NOT NULL AND CAST({fecha_col} AS VARCHAR) <> ''"
        )
        row = con.fetchone()
        metrics["earliest_firma"] = row[0] if row else None
        metrics["latest_firma"] = row[1] if row else None
    except Exception as exc:
        logger.warning("[%s] fecha_de_firma range query failed: %s", source, exc)
        metrics["earliest_firma"] = None
        metrics["latest_firma"] = None
        metrics["dirty"] = True

    if metrics.get("earliest_firma"):
        try:
            con.execute(
                f"SELECT count(*) FROM {view} "
                f"WHERE {fecha_col} IS NULL OR CAST({fecha_col} AS VARCHAR) = ''"
            )
            null_firma = con.fetchone()[0]
            metrics["null_firma_pct"] = round(
                100.0 * null_firma / max(int(metrics["lake_rows"]), 1), 2
            )
        except Exception as exc:
            logger.warning("[%s] null_firma_pct query failed: %s", source, exc)
            metrics["null_firma_pct"] = None
            metrics["dirty"] = True

    return metrics


def run_reality() -> list[dict[str, object]]:
    _ensure_lake_root()
    raw_root = lake_root() / "raw"
    sources = _discover_sources(raw_root)
    if not sources:
        logger.info("No sources found in %s", raw_root)
        return []

    con = duckdb.connect(":memory:")
    results: list[dict[str, object]] = []

    for source in sources:
        logger.info("Scanning source=%s ...", source)
        metrics = _compute_metrics(con, source)
        if not metrics:
            logger.info("  no parquet files found")
            continue

        live = _live_count_for_source(source)
        metrics["live_rows"] = live

        if live is not None:
            lake_rows = int(metrics.get("lake_rows", 0))
            metrics["delta"] = live - lake_rows
            metrics["delta_pct"] = round(
                100.0 * (live - lake_rows) / max(live, 1), 4
            )
        else:
            metrics["delta"] = None
            metrics["delta_pct"] = None

        metrics["probed_at"] = datetime.now(tz=UTC).isoformat()
        results.append(metrics)
        logger.info("  lake_rows=%s live_rows=%s", metrics.get("lake_rows"), live)

    con.close()

    report_path = meta_path() / "reality_report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    if results:
        fieldnames = list(results[0].keys())
        for row in results:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)

        with report_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)

        logger.info("Report written to %s", report_path)
    else:
        logger.info("No results to report")

    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    results = run_reality()

    if not results:
        print("No sources found in lake.")
        sys.exit(0)

    for row in results:
        source = row.get("source", "?")
        lake_rows = row.get("lake_rows", "?")
        live_rows = row.get("live_rows", "?")
        delta = row.get("delta", "?")
        delta_pct = row.get("delta_pct", "?")
        dirty = row.get("dirty", False)
        print(f"\n{source}:")
        print(f"  lake_rows:            {lake_rows}")
        print(f"  live_rows:            {live_rows}")
        print(f"  delta:                {delta}")
        print(f"  delta_pct:            {delta_pct}%")
        if dirty:
            print(f"  ** report dirty: some metrics had query errors **")

        for key, value in sorted(row.items()):
            if key.endswith("_null_pct") and value is not None:
                print(f"  {key}:  {value}%")
            if key in ("lake_distinct_pk", "lake_distinct_pk_dupes", "earliest_firma", "latest_firma", "null_firma_pct"):
                print(f"  {key}:  {value}")

    failing = False
    for row in results:
        for key, value in row.items():
            if key == "buyer_name_null_pct" and value is not None and value > 5:
                print(f"\nFAIL: {row['source']} buyer_name null_pct = {value}% (threshold: 5%)")
                failing = True
            if key == "contract_value_null_pct" and value is not None and value > 30:
                print(f"\nFAIL: {row['source']} contract_value null_pct = {value}% (threshold: 30%)")
                failing = True

    if failing:
        sys.exit(1)


if __name__ == "__main__":
    main()