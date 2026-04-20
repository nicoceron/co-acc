"""Reality-check tests for the SECOP integrado normalizer.

These tests are expected to FAIL before the Phase 1 normalizer fix and
PASS after it.  If any pass before the fix, the test is not testing the
actual bug.

Five required tests from the Phase 0 spec:
  1. test_normalize_populates_buyer_name
  2. test_normalize_populates_contract_value
  3. test_normalize_supplier_document_coverage
  4. test_partition_uses_fecha_de_firma
  5. test_watermark_uses_max_data_timestamp
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from coacc_etl.lakehouse import append_parquet, watermark
from coacc_etl.lakehouse.reader import source_files
from coacc_etl.pipelines.secop_integrado import normalize, normalize_frame_for_lake

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
SAMPLE_JSON = FIXTURES / "secop_integrado" / "jbjy_vk9h_sample.json"

EXPECTED_RAW_COLUMNS = {
    "nombre_entidad",
    "nit_entidad",
    "proveedor_adjudicado",
    "valor_del_contrato",
    "ultima_actualizacion",
    "fecha_de_firma",
}


@pytest.fixture()
def sample_rows() -> list[dict]:
    if not SAMPLE_JSON.exists():
        pytest.skip("Live fixture not available; run scripts/capture_fixture.py first")
    return json.loads(SAMPLE_JSON.read_text(encoding="utf-8"))


@pytest.fixture()
def sample_frame(sample_rows: list[dict]) -> pd.DataFrame:
    from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns

    return normalize_dataframe_columns(pd.DataFrame.from_records(sample_rows)).fillna("")


def test_fixture_has_expected_raw_columns(sample_rows: list[dict]) -> None:
    keys = set()
    for row in sample_rows:
        keys.update(row.keys())
    from coacc_etl.pipelines.colombia_shared import normalize_column_name

    normalized_keys = {normalize_column_name(k) for k in keys}
    missing = EXPECTED_RAW_COLUMNS - normalized_keys
    assert not missing, (
        f"Fixture is missing expected raw columns (after normalization): {missing}. "
        f"Available: {sorted(normalized_keys)}"
    )


def test_normalize_populates_buyer_name(sample_frame: pd.DataFrame) -> None:
    table = normalize_frame_for_lake(sample_frame)
    df = table.to_pandas()
    total = len(df)
    if total == 0:
        pytest.fail("normalize_frame_for_lake returned 0 rows")
    non_empty = (df["buyer_name"].astype(str).str.strip() != "").sum()
    coverage = non_empty / total
    assert coverage >= 0.95, (
        f"buyer_name populated on {non_empty}/{total} rows ({coverage:.1%}), expected >=95%"
    )


def test_normalize_populates_contract_value(sample_frame: pd.DataFrame) -> None:
    table = normalize_frame_for_lake(sample_frame)
    df = table.to_pandas()
    total = len(df)
    if total == 0:
        pytest.fail("normalize_frame_for_lake returned 0 rows")
    non_null = df["contract_value"].notna().sum()
    coverage = non_null / total
    assert coverage >= 0.80, (
        f"contract_value non-NULL on {non_null}/{total} rows ({coverage:.1%}), expected >=80%"
    )


def test_normalize_supplier_document_coverage(sample_frame: pd.DataFrame) -> None:
    table = normalize_frame_for_lake(sample_frame)
    df = table.to_pandas()
    total = len(df)
    if total == 0:
        pytest.fail("normalize_frame_for_lake returned 0 rows")
    non_empty = (df["supplier_document_id"].astype(str).str.strip() != "").sum()
    coverage = non_empty / total
    assert coverage >= 0.95, (
        f"supplier_document_id populated on {non_empty}/{total} rows ({coverage:.1%}), expected >=95%"
    )


def test_partition_uses_fecha_de_firma(sample_rows: list[dict]) -> None:
    table = normalize(sample_rows)
    df = table.to_pandas()
    fecha_col = "fecha_de_firma"
    if fecha_col not in df.columns:
        pytest.fail(
            f"fecha_de_firma column missing from normalized output. "
            f"Columns: {sorted(df.columns)}"
        )

    with TemporaryDirectory() as tmpdir:
        import os

        orig_lake_root = os.environ.get("COACC_LAKE_ROOT")
        os.environ["COACC_LAKE_ROOT"] = tmpdir
        try:
            append_parquet(table, source="secop_integrado", year=2026, month=4)

            files = source_files("secop_integrado")
            assert files, "No parquet files written"

            year_partitions = set()
            for f in files:
                for part in f.parts:
                    if part.startswith("year="):
                        year_partitions.add(part)

            import duckdb

            con = duckdb.connect(":memory:")
            paths = [str(f) for f in files]
            con.execute(
                f"CREATE OR REPLACE VIEW secop_integrado AS "
                f"SELECT * FROM read_parquet({paths}, union_by_name=true, hive_partitioning=true)"
            )

            from coacc_etl.pipelines.colombia_shared import parse_iso_date

            firma_years = set()
            for v in df[fecha_col].dropna().astype(str):
                if v.strip() and v.strip() not in ("None", "nan"):
                    p = parse_iso_date(v.strip())
                    if p:
                        firma_years.add(f"year={p[:4]}")

            data_years = len(firma_years)
            con.close()
        finally:
            if orig_lake_root is not None:
                os.environ["COACC_LAKE_ROOT"] = orig_lake_root
            else:
                os.environ.pop("COACC_LAKE_ROOT", None)

    assert len(year_partitions) > 1, (
        f"Parquet files all landed in a single year partition ({year_partitions}). "
        f"If partitioning used fecha_de_firma (which spans {data_years} years in this fixture), "
        f"there would be multiple year= dirs. Current partitioning uses load-time, not data-time."
    )


def test_watermark_uses_max_data_timestamp(sample_rows: list[dict]) -> None:
    table = normalize(sample_rows[:10])
    ts_col = "ultima_actualizacion"
    if ts_col not in table.column_names:
        ts_col = "load_date"

    with TemporaryDirectory() as tmpdir:
        import os

        orig_lake_root = os.environ.get("COACC_LAKE_ROOT")
        os.environ["COACC_LAKE_ROOT"] = tmpdir
        try:
            append_parquet(table, source="secop_integrado_test_wm", year=2026, month=4)
            delta = watermark.advance(
                "secop_integrado_test_wm",
                rows=len(sample_rows[:10]),
                batch_id="test_batch",
            )
            last_seen = delta.last_seen_ts

            import duckdb

            con = duckdb.connect(":memory:")
            paths = [str(f) for f in source_files("secop_integrado_test_wm")]
            view = "secop_integrado_test_wm"
            con.execute(
                f"CREATE VIEW {view} AS SELECT * FROM read_parquet({paths}, "
                f"union_by_name=true, hive_partitioning=true)"
            )
            con.execute(
                f"SELECT max(CAST({ts_col} AS VARCHAR)) FROM {view} "
                f"WHERE CAST({ts_col} AS VARCHAR) <> ''"
            )
            row = con.fetchone()
            max_ts_str = row[0] if row else None
            con.close()
        finally:
            if orig_lake_root is not None:
                os.environ["COACC_LAKE_ROOT"] = orig_lake_root
            else:
                os.environ.pop("COACC_LAKE_ROOT", None)

    if not max_ts_str:
        pytest.fail(
            f"No non-empty {ts_col} values in written data. "
            f"Normalizer must preserve {ts_col} from raw source."
        )

    from coacc_etl.pipelines.colombia_shared import parse_iso_date

    max_data_ts = parse_iso_date(max_ts_str)
    assert max_data_ts is not None, f"Could not parse max data timestamp: {max_ts_str}"

    from datetime import UTC as _UTC, datetime as dt

    max_data_dt = dt.fromisoformat(max_data_ts) if isinstance(max_data_ts, str) else max_data_ts
    if max_data_dt.tzinfo is None:
        max_data_dt = max_data_dt.replace(tzinfo=_UTC)
    last_seen_aware = last_seen.replace(tzinfo=_UTC) if last_seen.tzinfo is None else last_seen

    assert last_seen_aware == max_data_dt, (
        f"Watermark last_seen_ts ({last_seen_aware}) must equal max data timestamp ({max_data_dt}). "
        f"Watermark should store max({ts_col}) from the data, not datetime.now()."
    )


def test_normalize_populates_supplier_name(sample_frame: pd.DataFrame) -> None:
    table = normalize_frame_for_lake(sample_frame)
    df = table.to_pandas()
    total = len(df)
    if total == 0:
        pytest.fail("normalize_frame_for_lake returned 0 rows")
    non_empty = (df["supplier_name"].astype(str).str.strip() != "").sum()
    coverage = non_empty / total
    assert coverage >= 0.95, (
        f"supplier_name populated on {non_empty}/{total} rows ({coverage:.1%}), expected >=95%"
    )


def test_normalize_produces_signed_date(sample_frame: pd.DataFrame) -> None:
    table = normalize_frame_for_lake(sample_frame)
    df = table.to_pandas()
    total = len(df)
    signed_col = "signed_date"
    if signed_col not in df.columns:
        pytest.fail(
            f"signed_date column missing from normalized output. "
            f"Columns: {sorted(df.columns)}"
        )
    non_empty = (df[signed_col].astype(str).str.strip() != "").sum()
    coverage = non_empty / total if total > 0 else 0
    assert coverage >= 0.80, (
        f"signed_date populated on {non_empty}/{total} rows ({coverage:.1%}), expected >=80%"
    )