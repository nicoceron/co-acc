"""Smoke tests for surviving low-level helpers.

The bulk of ``coacc_etl.pipelines.colombia_shared`` was deleted in Wave 4.B
along with the rest of the legacy Pipeline stack. This file used to cover
``read_csv_normalized`` and ``iter_csv_chunks`` together; only the
``streaming.iter_csv_chunks`` smoke test survives.
"""
from __future__ import annotations

from coacc_etl.streaming import iter_csv_chunks


def test_iter_csv_chunks_handles_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")

    chunks = list(iter_csv_chunks(path, chunk_size=100, dtype=str, keep_default_na=False))

    assert chunks == []
