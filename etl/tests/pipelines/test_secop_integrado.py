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
        fecha_col = "signed_date"
    if fecha_col not in df.columns:
        pytest.fail(
            f"Neither fecha_de_firma nor signed_date in normalized output. "
            f"Columns: {sorted(df.columns)}"
        )

    from coacc_etl.pipelines.secop_integrado import _extract_signed_date_for_partition

    year, month = _extract_signed_date_for_partition(table)

    with TemporaryDirectory() as tmpdir:
        import os

        orig_lake_root = os.environ.get("COACC_LAKE_ROOT")
        os.environ["COACC_LAKE_ROOT"] = tmpdir
        try:
            append_parquet(table, source="secop_integrado", year=year, month=month)

            files = source_files("secop_integrado")
            assert files, "No parquet files written"
        finally:
            if orig_lake_root is not None:
                os.environ["COACC_LAKE_ROOT"] = orig_lake_root
            else:
                os.environ.pop("COACC_LAKE_ROOT", None)

    from coacc_etl.pipelines.colombia_shared import parse_iso_date

    firma_years = set()
    for v in df[fecha_col].dropna().astype(str):
        if v.strip() and v.strip() not in ("None", "nan"):
            p = parse_iso_date(v.strip())
            if p:
                firma_years.add(int(p[:4]))

    assert len(firma_years) > 1, (
        f"Fixture only has {len(firma_years)} distinct years in {fecha_col}. "
        f"Need at least 2 to test multi-year partitioning."
    )
    assert year in firma_years, (
        f"_extract_signed_date_for_partition chose year={year}, "
        f"but fixture data spans years: {sorted(firma_years)}"
    )


def test_watermark_uses_max_data_timestamp(sample_rows: list[dict]) -> None:
    table = normalize(sample_rows[:10])

    with TemporaryDirectory() as tmpdir:
        import os

        orig_lake_root = os.environ.get("COACC_LAKE_ROOT")
        os.environ["COACC_LAKE_ROOT"] = tmpdir
        try:
            append_parquet(table, source="secop_integrado_test_wm", year=2026, month=4)

            from coacc_etl.pipelines.secop_integrado import _extract_max_data_ts, _parse_watermark_ts

            max_data_str = _extract_max_data_ts(table)
            max_data_dt = _parse_watermark_ts(max_data_str)

            delta = watermark.advance(
                "secop_integrado_test_wm",
                rows=len(sample_rows[:10]),
                batch_id="test_batch",
                last_seen_ts=max_data_dt,
            )
            last_seen = delta.last_seen_ts
        finally:
            if orig_lake_root is not None:
                os.environ["COACC_LAKE_ROOT"] = orig_lake_root
            else:
                os.environ.pop("COACC_LAKE_ROOT", None)

    assert max_data_dt is not None, (
        f"Could not extract max data timestamp from normalized table. "
        f"Columns: {sorted(table.column_names)}"
    )

    from datetime import UTC as _UTC

    last_seen_aware = last_seen.replace(tzinfo=_UTC) if last_seen.tzinfo is None else last_seen
    max_data_aware = max_data_dt.replace(tzinfo=_UTC) if max_data_dt.tzinfo is None else max_data_dt

    assert last_seen_aware == max_data_aware, (
        f"Watermark last_seen_ts ({last_seen_aware}) must equal max data timestamp ({max_data_aware}). "
        f"Watermark should store max(ultima_actualizacion) from the data, not datetime.now()."
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