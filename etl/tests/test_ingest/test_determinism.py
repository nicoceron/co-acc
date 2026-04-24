"""Determinism: same fixture → identical row-level parquet output.

We don't byte-compare the parquet *files* (the writer stamps timestamp +
random uuid into filenames, and pyarrow's file metadata varies across runs).
We instead verify row-level determinism: two independent ingests against the
same page produce identical ordered data frames in the lake, with identical
watermark deltas.
"""
from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq

from coacc_etl.catalog import DatasetSpec
from coacc_etl.ingest import ingest
from coacc_etl.lakehouse.paths import raw_source_path


def _read_canonical_frame(source: str) -> pd.DataFrame:
    root = raw_source_path(source)
    frames = []
    for path in sorted(root.rglob("*.parquet")):
        if path.name.startswith(".inflight-"):
            continue
        frames.append(pq.read_table(path).to_pandas())
    if not frames:
        return pd.DataFrame()
    # Sort rows so two independent layouts with the same content compare equal
    # regardless of parquet partitioning order.
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(
        by=list(combined.columns), kind="mergesort"
    ).reset_index(drop=True)
    return combined


def test_two_ingests_produce_identical_row_content(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
    tmp_path,
    monkeypatch,
) -> None:
    # Run 1 — separate lake.
    run1_root = tmp_path / "run1"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(run1_root))
    monkeypatch.setattr(
        "coacc_etl.lakehouse.paths.LAKE_ROOT", run1_root, raising=False
    )
    result1 = ingest(hallazgos_spec, client=fake_client_factory([hallazgos_page]))
    frame1 = _read_canonical_frame(hallazgos_spec.id)

    # Run 2 — fresh lake.
    run2_root = tmp_path / "run2"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(run2_root))
    monkeypatch.setattr(
        "coacc_etl.lakehouse.paths.LAKE_ROOT", run2_root, raising=False
    )
    result2 = ingest(hallazgos_spec, client=fake_client_factory([hallazgos_page]))
    frame2 = _read_canonical_frame(hallazgos_spec.id)

    pd.testing.assert_frame_equal(frame1, frame2, check_like=False)
    assert result1.rows == result2.rows
    assert result1.partitions == result2.partitions
    # Watermark ts derives from data, not wall-clock — so it matches exactly.
    assert result1.watermark_delta is not None
    assert result2.watermark_delta is not None
    assert result1.watermark_delta.last_seen_ts == result2.watermark_delta.last_seen_ts
