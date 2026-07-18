from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, SerializationInfo, field_serializer, model_validator

from coacc.models.entity import SourceAttribution  # noqa: TC001

SignalSeverity = Literal["low", "medium", "high", "critical"]
SignalRunnerKind = Literal["pattern", "cypher", "duckdb"]

CONFIDENCE_IDENTITY_WEIGHT = 0.55
CONFIDENCE_EVIDENCE_WEIGHT = 0.25
CONFIDENCE_CORROBORATION_WEIGHT = 0.20


def visible_signal_id(signal_id: str) -> str:
    """Hide the legacy visibility suffix without breaking lake table identifiers."""
    return signal_id.removesuffix("_review_only")


def evidence_confidence_component(evidence_count: int) -> float:
    if evidence_count <= 0:
        return 0.0
    if evidence_count == 1:
        return 0.65
    if evidence_count == 2:
        return 0.85
    return 1.0


def corroboration_confidence_component(source_count: int) -> float:
    if source_count <= 0:
        return 0.0
    if source_count == 1:
        return 0.70
    if source_count == 2:
        return 0.90
    return 1.0


def combine_confidence_components(
    *,
    identity: float,
    evidence: float,
    corroboration: float,
) -> float:
    bounded_identity = min(1.0, max(0.0, identity))
    bounded_evidence = min(1.0, max(0.0, evidence))
    bounded_corroboration = min(1.0, max(0.0, corroboration))
    return round(
        100
        * (
            CONFIDENCE_IDENTITY_WEIGHT * bounded_identity
            + CONFIDENCE_EVIDENCE_WEIGHT * bounded_evidence
            + CONFIDENCE_CORROBORATION_WEIGHT * bounded_corroboration
        ),
        1,
    )


class SignalConfidenceComponents(BaseModel):
    identity: float = Field(ge=0.0, le=1.0)
    evidence_traceability: float = Field(ge=0.0, le=1.0)
    source_corroboration: float = Field(ge=0.0, le=1.0)


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

    @property
    def display_id(self) -> str:
        return visible_signal_id(self.id)

    @field_serializer("id")
    def _serialize_visible_id(self, value: str, info: SerializationInfo) -> str:
        if info.context and info.context.get("canonical_signal_ids"):
            return value
        return visible_signal_id(value)

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
    row_selector: str | None = None
    file_selector: str | None = None
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
    entity_id: str
    entity_key: str
    entity_label: str | None = None
    scope_key: str | None = None
    scope_type: str = "entity"
    dedup_key: str
    score: float
    identity_confidence: float
    confidence_index: float | None = Field(default=None, ge=0.0, le=100.0)
    confidence_components: SignalConfidenceComponents | None = None
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

    @field_serializer("signal_id")
    def _serialize_visible_signal_id(self, value: str) -> str:
        return visible_signal_id(value)

    @model_validator(mode="after")
    def _derive_confidence_index(self) -> SignalHitResponse:
        source_ids = {
            source.database.strip()
            for source in self.sources
            if source.database.strip()
        }
        source_ids.update(
            item.source_id.strip()
            for item in self.evidence_items
            if item.source_id and item.source_id.strip()
        )
        evidence_count = max(
            self.evidence_count,
            len(self.evidence_refs),
            len(self.evidence_items),
        )
        components = self.confidence_components or SignalConfidenceComponents(
            identity=min(1.0, max(0.0, self.identity_confidence)),
            evidence_traceability=evidence_confidence_component(evidence_count),
            source_corroboration=corroboration_confidence_component(len(source_ids)),
        )
        self.confidence_components = components
        if self.confidence_index is None:
            self.confidence_index = combine_confidence_components(
                identity=components.identity,
                evidence=components.evidence_traceability,
                corroboration=components.source_corroboration,
            )
        return self


class SignalListItem(SignalDefinition):
    hit_count: int = 0
    last_seen_at: str | None = None
    materialized: bool = False
    materialization_state: Literal["materialized", "registered_only"] = "registered_only"
    confidence_index: float | None = Field(default=None, ge=0.0, le=100.0)


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
