from __future__ import annotations

from coacc_etl.lakehouse.paths import LAKE_ROOT, curated_path, lake_root, meta_path, raw_path
from coacc_etl.lakehouse.reader import register_source, source_files
from coacc_etl.lakehouse.watermark import Watermark, WatermarkDelta, advance, get
from coacc_etl.lakehouse.writer import append_parquet

__all__ = [
    "LAKE_ROOT",
    "Watermark",
    "WatermarkDelta",
    "advance",
    "append_parquet",
    "curated_path",
    "get",
    "lake_root",
    "meta_path",
    "raw_path",
    "register_source",
    "source_files",
]
