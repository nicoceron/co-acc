from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

SignalSeverity = Literal["low", "medium", "high", "critical"]


class SignalHitRow(BaseModel):
    """Parquet contract for one materialized signal hit."""

    run_id: str
    signal_id: str
    signal_version: int = Field(ge=1)
    hit_id: str
    entity_uid: str
    entity_key: str | None = None
    entity_label: str | None = None
    scope_key: str
    scope_type: str
    severity: SignalSeverity
    score: float
    title: str
    description: str
    public_safe: bool
    reviewer_only: bool
    identity_confidence: float = Field(ge=0.0, le=1.0)
    identity_match_type: str | None = None
    identity_quality: str | None = None
    evidence_count: int = Field(ge=0)
    evidence_bundle_id: str
    evidence_refs: list[str] = Field(default_factory=list)
    created_at: str
    first_seen_at: str
    last_seen_at: str

    @model_validator(mode="after")
    def _evidence_count_matches_refs(self) -> SignalHitRow:
        if self.evidence_count != len(self.evidence_refs):
            raise ValueError("evidence_count must match evidence_refs length")
        return self


class EvidenceBundleRow(BaseModel):
    """Parquet contract for one evidence item inside a materialized bundle."""

    run_id: str
    bundle_id: str
    hit_id: str
    signal_id: str
    item_index: int = Field(ge=1)
    source_id: str | None = None
    record_id: str | None = None
    parquet_path: str
    row_selector: str
    label: str
    url: str | None = None
    observed_at: str
    public_safe: bool
    identity_match_type: str | None = None
    identity_quality: str | None = None
