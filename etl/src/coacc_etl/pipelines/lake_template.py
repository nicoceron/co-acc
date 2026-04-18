from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa

from coacc_etl.base import Pipeline
from coacc_etl.lakehouse import append_parquet, watermark
from coacc_etl.pipelines.colombia_shared import normalize_dataframe_columns
from coacc_etl.streaming import iter_csv_chunks, pipeline_stream_to_lake

if TYPE_CHECKING:
    from coacc_etl.lakehouse.watermark import WatermarkDelta


class LakeCsvPipeline(Pipeline):
    """Lake-first source scaffold for datasets that do not yet need a Neo4j loader."""

    socrata_dataset_id_env: str | None = None

    def extract(self) -> None:
        self.rows_in = 0

    def transform(self) -> None:
        return

    def load(self) -> None:
        return

    def csv_path(self) -> Path:
        return Path(self.data_dir) / self.source_id / f"{self.source_id}.csv"

    def normalize_rows(self, rows: list[dict[str, Any]]) -> pa.Table:
        frame = normalize_dataframe_columns(pd.DataFrame.from_records(rows)).fillna("")
        frame["source"] = self.source_id
        return pa.Table.from_pandas(frame, preserve_index=False)

    def normalize_frame(self, frame: pd.DataFrame) -> pa.Table:
        normalized = normalize_dataframe_columns(frame).fillna("")
        normalized["source"] = self.source_id
        return pa.Table.from_pandas(normalized, preserve_index=False)

    def run_to_lake(self, *, full_refresh: bool = False) -> WatermarkDelta:
        csv_path = self.csv_path()
        batch_id = uuid.uuid4().hex
        rows_total = 0
        if csv_path.exists():
            for chunk in iter_csv_chunks(
                csv_path,
                chunk_size=self.chunk_size,
                limit=self.limit,
                dtype=str,
                keep_default_na=False,
            ):
                table = self.normalize_frame(chunk)
                now = datetime.now(tz=UTC)
                append_parquet(table, self.source_id, year=now.year, month=now.month)
                rows_total += len(chunk)
            return watermark.advance(self.source_id, rows=rows_total, batch_id=batch_id)

        dataset_id = os.environ.get(self.socrata_dataset_id_env or "")
        if dataset_id:
            return pipeline_stream_to_lake(
                self.source_id,
                dataset_id,
                normalizer=self.normalize_rows,
                chunk_size=self.chunk_size,
                full_refresh=full_refresh,
            )

        return watermark.advance(self.source_id, rows=0, batch_id=batch_id)
