from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from coacc.models.entity import SourceAttribution  # noqa: TC001

SignalSeverity = Literal["low", "medium", "high", "critical"]
SignalRunnerKind = Literal["pattern", "cypher", "duckdb"]


class SignalRunner(BaseModel):
    kind: SignalRunnerKind
    ref: str


class SignalPublicPolicy(BaseModel):
    allow_public: bool = False
    require_public_evidence: bool = True
    require_exact_identity: bool = True
    allowed_identity_match_types: list[str] = Field(default_factory=list)
    allow_person_entities: bool = False


class SignalEvidenceMapping(BaseModel):
    item_type: str = "reference"
    label_field: str | None = None
    node_ref_field: str | None = None
    summary_field: str | None = None


class SignalDefinition(BaseModel):
    id: str
    version: int = 1
    title: str
    description: str
    category: str
    severity: SignalSeverity
    entity_types: list[str] = Field(default_factory=list)
    public_safe: bool = False
    reviewer_only: bool = False
    engine: SignalRunnerKind | None = None
    sources: list[str] = Field(default_factory=list)
    public_presentation: str | None = None
    review_only_expansion: str | None = None
    requires_identity: list[str] = Field(default_factory=list)
    sources_required: list[str] = Field(default_factory=list)
    scope_type: str = "entity"
    dedup_fields: list[str] = Field(default_factory=lambda: ["scope_key"])
    runner: SignalRunner
    public_policy: SignalPublicPolicy = Field(default_factory=SignalPublicPolicy)
    evidence_mapping: SignalEvidenceMapping = Field(default_factory=SignalEvidenceMapping)
    pattern_id: str | None = None
    dedup_key_template: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _upgrade_legacy_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        upgraded = dict(data)
        pattern_id = upgraded.get("pattern_id")
        runner = upgraded.get("runner")
        engine = upgraded.get("engine")
        if runner is None and isinstance(pattern_id, str) and pattern_id.strip():
            upgraded["runner"] = {"kind": "pattern", "ref": pattern_id.strip()}
        if runner is None and isinstance(engine, str) and engine.strip():
            upgraded["runner"] = {"kind": engine.strip(), "ref": upgraded["id"]}
        if upgraded.get("engine") is None and isinstance(upgraded.get("runner"), dict):
            upgraded["engine"] = upgraded["runner"].get("kind")

        if not upgraded.get("sources") and upgraded.get("sources_required"):
            upgraded["sources"] = upgraded["sources_required"]

        public_policy = upgraded.get("public_policy")
        requires_identity = [
            str(item).strip()
            for item in (upgraded.get("requires_identity") or [])
            if str(item).strip()
        ]
        if public_policy is None:
            upgraded["public_policy"] = {
                "allow_public": bool(upgraded.get("public_safe", False)),
                "require_public_evidence": bool(upgraded.get("public_safe", False)),
                "require_exact_identity": any(
                    item.startswith("EXACT_") for item in requires_identity
                ),
                "allowed_identity_match_types": [
                    item for item in requires_identity if item.startswith("EXACT_")
                ],
                "allow_person_entities": "Person" in (upgraded.get("entity_types") or [])
                and bool(upgraded.get("public_safe", False)),
            }

        if "dedup_fields" not in upgraded or not upgraded.get("dedup_fields"):
            upgraded["dedup_fields"] = ["scope_key"]

        if "scope_type" not in upgraded or not upgraded.get("scope_type"):
            upgraded["scope_type"] = "entity"

        if "evidence_mapping" not in upgraded or upgraded.get("evidence_mapping") is None:
            upgraded["evidence_mapping"] = {"item_type": "reference"}

        return upgraded


class SignalRegistry(BaseModel):
    registry_version: int = 1
    default: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    aliases: dict[str, str] = Field(default_factory=dict)
    signals: list[SignalDefinition] = Field(default_factory=list)


class EvidenceItemResponse(BaseModel):
    item_id: str
    source_id: str | None = None
    record_id: str | None = None
    url: str | None = None
    label: str | None = None
    item_type: str = "reference"
    node_ref: str | None = None
    observed_at: str | None = None
    public_safe: bool = True
    identity_match_type: str | None = None
    identity_quality: str | None = None


class SignalHitResponse(BaseModel):
    hit_id: str
    run_id: str | None = None
    signal_id: str
    signal_version: int
    title: str
    description: str
    category: str
    severity: SignalSeverity
    public_safe: bool
    reviewer_only: bool
    entity_id: str
    entity_key: str
    entity_label: str | None = None
    scope_key: str | None = None
    scope_type: str = "entity"
    dedup_key: str
    score: float
    identity_confidence: float
    identity_match_type: str | None = None
    identity_quality: str | None = None
    evidence_count: int
    evidence_bundle_id: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    data: dict[str, str | float | int | bool | list[str] | None] = Field(default_factory=dict)
    sources: list[SourceAttribution] = Field(default_factory=list)
    evidence_items: list[EvidenceItemResponse] = Field(default_factory=list)
    created_at: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None


class SignalListItem(SignalDefinition):
    hit_count: int = 0
    last_seen_at: str | None = None


class SignalListResponse(BaseModel):
    registry_version: int
    last_run_id: str | None = None
    last_refreshed_at: str | None = None
    signals: list[SignalListItem]


class SignalDetailResponse(BaseModel):
    definition: SignalDefinition
    sample_hits: list[SignalHitResponse] = Field(default_factory=list)


class EntitySignalsResponse(BaseModel):
    entity_id: str
    entity_key: str
    total: int
    last_run_id: str | None = None
    last_refreshed_at: str | None = None
    stale: bool = True
    signals: list[SignalHitResponse] = Field(default_factory=list)
