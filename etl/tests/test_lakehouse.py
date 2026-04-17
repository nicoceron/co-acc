from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from coacc_etl.lakehouse import append_parquet, register_source, watermark
from coacc_etl.lakehouse.paths import raw_path
from coacc_etl.lakehouse.watermark import Watermark


def test_write_read_roundtrip_preserves_schema(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    frame = pd.DataFrame({"id": ["a", "b"], "value": [1, 2]})

    append_parquet(frame, "secop_integrado", 2026, 4)
    con = duckdb.connect()
    view = register_source(con, "secop_integrado")
    result = con.execute(
        f"SELECT id, value, source, year, month FROM {view} ORDER BY id"
    ).fetchall()

    assert result == [
        ("a", 1, "secop_integrado", 2026, "04"),
        ("b", 2, "secop_integrado", 2026, "04"),
    ]


def test_partition_hive_pattern_matches_glob(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    output = append_parquet(pd.DataFrame({"id": ["x"]}), "secop_suppliers", 2026, 3)

    assert output.parent == raw_path("secop_suppliers", 2026, 3)
    assert "source=secop_suppliers/year=2026/month=03" in output.as_posix()


def test_reader_ignores_inflight_files(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    output = append_parquet(pd.DataFrame({"id": ["final"]}), "source_a", 2026, 4)
    inflight = output.parent / ".inflight-manual.parquet"
    pd.DataFrame({"id": ["inflight"]}).to_parquet(inflight)

    con = duckdb.connect()
    view = register_source(con, "source_a")
    result = con.execute(f"SELECT id FROM {view}").fetchall()

    assert result == [("final",)]


def test_watermark_monotonicity(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    now = datetime(2026, 4, 17, tzinfo=UTC)
    watermark.set(Watermark("secop_integrado", now, "batch-1", 10))

    assert watermark.get("secop_integrado") is not None
    with pytest.raises(ValueError, match="would regress"):
        watermark.advance("secop_integrado", rows=2, last_seen_ts=now - timedelta(days=1))
