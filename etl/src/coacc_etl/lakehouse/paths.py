from __future__ import annotations

import os
import re
from pathlib import Path

LAKE_ROOT = Path(os.environ.get("COACC_LAKE_ROOT", "/var/lib/coacc/lake"))
_SAFE_PART = re.compile(r"[^A-Za-z0-9_.=-]+")


def _clean_part(value: str) -> str:
    cleaned = _SAFE_PART.sub("_", value.strip())
    return cleaned.strip("._") or "unknown"


def lake_root() -> Path:
    return Path(os.environ.get("COACC_LAKE_ROOT", str(LAKE_ROOT)))


def raw_path(source: str, year: int, month: int) -> Path:
    return (
        lake_root()
        / "raw"
        / f"source={_clean_part(source)}"
        / f"year={int(year)}"
        / f"month={int(month):02d}"
    )


def raw_snapshot_path(source: str, snapshot: str) -> Path:
    """Snapshot-mode write target for full_refresh_only datasets."""
    return (
        lake_root()
        / "raw"
        / f"source={_clean_part(source)}"
        / f"snapshot={_clean_part(snapshot)}"
    )


def raw_source_path(source: str) -> Path:
    return lake_root() / "raw" / f"source={_clean_part(source)}"


def curated_path(table: str) -> Path:
    return lake_root() / "curated" / f"table={_clean_part(table)}"


def meta_path() -> Path:
    return lake_root() / "meta"
