from pydantic import BaseModel, Field

from coacc.models.signal import EvidenceItemResponse, SignalHitResponse


class CaseCreate(BaseModel):
    title: str = Field(max_length=200)
    description: str | None = Field(default=None, max_length=2000)


class CaseSummary(BaseModel):
    id: str
    title: str
    description: str | None = None
    status: str = "new"
    created_at: str
    updated_at: str
    entity_ids: list[str] = Field(default_factory=list)
    signal_count: int = 0
    public_signal_count: int = 0
    last_refreshed_at: str | None = None
    last_run_id: str | None = None
    stale: bool = True


class CaseListResponse(BaseModel):
    cases: list[CaseSummary]
    total: int


class CaseEvidenceBundle(BaseModel):
    bundle_id: str
    headline: str
    source_list: list[str] = Field(default_factory=list)
    evidence_items: list[EvidenceItemResponse] = Field(default_factory=list)


class CaseEventResponse(BaseModel):
    id: str
    type: str
    label: str
    date: str
    entity_id: str | None = None
    signal_hit_id: str | None = None
    evidence_bundle_id: str | None = None
    bundle_document_count: int | None = None


class CaseResponse(CaseSummary):
    signals: list[SignalHitResponse] = Field(default_factory=list)
    evidence_bundles: list[CaseEvidenceBundle] = Field(default_factory=list)
    events: list[CaseEventResponse] = Field(default_factory=list)
