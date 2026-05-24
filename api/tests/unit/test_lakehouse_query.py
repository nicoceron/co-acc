from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from coacc.services import lakehouse_query

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_register_source_resolves_catalog_alias(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    out = tmp_path / "raw" / "source=jbjy-vk9h" / "year=2026" / "month=05"
    out.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"contract_id": "C-1"}]), out / "fixture.parquet")

    con = duckdb.connect()
    try:
        view = lakehouse_query.register_source(con, "secop_ii_contracts")
        rows = con.execute(f"SELECT contract_id FROM {view}").fetchall()
    finally:
        con.close()

    assert rows == [("C-1",)]


def test_watermark_exists_accepts_snapshot_only_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    out = tmp_path / "raw" / "source=paco_sanctions" / "snapshot=20260524T000000Z"
    out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"subject_document_id": "900123456"}]),
        out / "part.parquet",
    )

    assert lakehouse_query.watermark_exists("paco_sanctions") is True
