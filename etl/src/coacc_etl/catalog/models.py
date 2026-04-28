"""Pydantic models for per-dataset ingest contracts.

A ``DatasetSpec`` is the runtime authority for how a single Socrata dataset
flows into the lake. One YAML file per spec lives under ``etl/datasets/``
and is validated on load.

Fields that Wave 2 bootstrap can fill from the signed catalog (id, name,
sector, tier, join_keys, url) are required. Fields that require opening
the dataset's Socrata schema or existing ETL normalizer code
(watermark_column, partition_column, columns_map, required_coverage)
are optional at the YAML level and enforced by the generic ingester in
Wave 3 when ``tier == "core"``.
"""
from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

Tier = Literal["core", "context", "backlog"]
JoinKeyClass = Literal["nit", "contract", "process", "entity", "bpin", "divipola"]

_SOCRATA_ID_RE = re.compile(r"^[a-z0-9]{4}-[a-z0-9]{4}$")


class DatasetSpec(BaseModel):
    """One YAML contract per dataset."""

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    sector: str = ""
    tier: Tier
    join_keys: dict[JoinKeyClass, list[str]] = Field(default_factory=dict)
    watermark_column: str | None = None
    partition_column: str | None = None
    columns_map: dict[str, str] = Field(default_factory=dict)
    required_coverage: dict[str, float] = Field(default_factory=dict)
    freq: str | None = None
    url: str
    notes: str = ""
    # Snapshot mode for sources without a row-level timestamp. The ingester
    # pulls every row, writes to ``lake/raw/source=<id>/snapshot=<iso>/``,
    # and never advances a watermark. Use for child/relationship tables
    # republished wholesale by the upstream system (DNP project links,
    # SECOP extension tables, etc.).
    full_refresh_only: bool = False

    @field_validator("id")
    @classmethod
    def _validate_socrata_id(cls, value: str) -> str:
        if not _SOCRATA_ID_RE.fullmatch(value):
            msg = f"dataset id {value!r} is not a Socrata 4x4 id"
            raise ValueError(msg)
        return value

    @field_validator("required_coverage")
    @classmethod
    def _validate_coverage_range(cls, value: dict[str, float]) -> dict[str, float]:
        for col, threshold in value.items():
            if not 0.0 <= threshold <= 1.0:
                msg = f"required_coverage[{col!r}]={threshold} must be in [0, 1]"
                raise ValueError(msg)
        return value

    def is_ingest_ready(self) -> bool:
        """True when this spec has the bits the generic ingester requires.

        Checks shape only — any tier may be ingest-ready. The CLI's
        ``ingest-all`` is what filters to ``tier=core``; the per-dataset
        ``ingest <id>`` accepts any ingest-ready spec, including
        ``tier=context`` enrichment datasets the operator opts into.

        Two valid shapes:

        - **Incremental** (the default): ``watermark_column`` +
          ``partition_column`` + ``columns_map`` all set. The ingester
          uses ``$where`` against the watermark for incremental pulls.
        - **Snapshot** (``full_refresh_only: true``): ``columns_map`` set;
          watermark/partition columns are not required. The ingester
          fetches every row and writes to a fresh ``snapshot=<iso>/``
          partition. No watermark is advanced.
        """
        if not self.columns_map:
            return False
        if self.full_refresh_only:
            return True
        return bool(self.watermark_column and self.partition_column)
