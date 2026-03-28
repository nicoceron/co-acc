from __future__ import annotations

from coacc_etl.pipelines.colombia_shared import read_csv_normalized
from coacc_etl.streaming import iter_csv_chunks


def test_read_csv_normalized_handles_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")

    frame = read_csv_normalized(str(path), dtype=str, keep_default_na=False)

    assert frame.empty
    assert list(frame.columns) == []


def test_iter_csv_chunks_handles_empty_file(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")

    chunks = list(iter_csv_chunks(path, chunk_size=100, dtype=str, keep_default_na=False))

    assert chunks == []
