from __future__ import annotations

from coacc.config import settings
from coacc.models.pattern import PATTERN_METADATA, PatternInfo, PatternResult
from coacc.models.signal import SignalConfidenceComponents, visible_signal_id
from coacc.services.lakehouse_entity_service import get_lake_entity
from coacc.services.lakehouse_signal_service import materialized_entity_signals
from coacc.services.signal_registry import (
    list_signal_definitions,
    load_signal_registry,
    resolve_signal_id,
)

_SIGNAL_TO_PATTERN = {
    "procurement_single_bidder_high_value": "low_competition_bidding",
    "procurement_offers_competition_drop": "low_competition_bidding",
    "procurement_sanctioned_supplier_awarded": "sanctioned_supplier_record",
    "procurement_supplier_concentration_across_entities": "contract_concentration",
    "procurement_repeat_awards_same_supplier": "split_contracts_below_threshold",
    "procurement_short_bidding_window": "procurement_short_bidding_window",
    "procurement_politically_exposed_position_supplier_overlap": (
        "sensitive_public_official_supplier_overlap"
    ),
    "procurement_related_companies_shared_officer": "shared_officer_supplier_network",
}

_SEVERITY_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}


def _localized(meta: dict[str, str], key: str, lang: str, fallback: str) -> str:
    localized_key = f"{key}_{lang}"
    if localized_key in meta and meta[localized_key]:
        return meta[localized_key]
    english_key = f"{key}_en"
    if english_key in meta and meta[english_key]:
        return meta[english_key]
    return fallback


def signal_to_pattern_id(signal_id: str) -> str:
    canonical_signal_id = resolve_signal_id(signal_id)
    mapped = _SIGNAL_TO_PATTERN.get(canonical_signal_id)
    if mapped:
        return mapped
    registry = load_signal_registry()
    for alias, target in registry.aliases.items():
        if target == canonical_signal_id and (
            alias in PATTERN_METADATA or alias in _SIGNAL_TO_PATTERN.values()
        ):
            return alias
    if canonical_signal_id in PATTERN_METADATA:
        return canonical_signal_id
    return visible_signal_id(canonical_signal_id)


def _pattern_row_from_meta(pattern_id: str) -> PatternInfo:
    meta = PATTERN_METADATA.get(pattern_id, {})
    return PatternInfo(
        id=pattern_id,
        name_es=meta.get("name_es", pattern_id),
        name_en=meta.get("name_en", pattern_id),
        description_es=meta.get("desc_es", ""),
        description_en=meta.get("desc_en", ""),
    )


def _better_severity(current: str | None, candidate: str | None) -> str | None:
    if candidate is None:
        return current
    if current is None:
        return candidate
    return (
        candidate
        if _SEVERITY_RANK.get(candidate, 0) > _SEVERITY_RANK.get(current, 0)
        else current
    )


def _merge_unique(left: list[str], right: list[str]) -> list[str]:
    merged: list[str] = []
    for value in [*left, *right]:
        if value and value not in merged:
            merged.append(value)
    return merged


def lake_pattern_summaries(
    provider_patterns: list[dict[str, str]],
) -> list[PatternInfo]:
    from coacc.services import lakehouse_signal_service

    rows: dict[str, PatternInfo] = {}
    for raw in provider_patterns:
        pattern_id = raw["id"]
        rows[pattern_id] = PatternInfo(
            id=pattern_id,
            name_es=raw.get("name_es", pattern_id),
            name_en=raw.get("name_en", pattern_id),
            description_es=raw.get("description_es", ""),
            description_en=raw.get("description_en", ""),
        )

    definitions = {definition.id: definition for definition in list_signal_definitions()}
    lake_counts = lakehouse_signal_service.materialized_signal_counts()
    lake_severities = lakehouse_signal_service.materialized_signal_severities()
    lake_confidence = lakehouse_signal_service.materialized_signal_confidence_indices()

    for signal_id, definition in definitions.items():
        pattern_id = signal_to_pattern_id(signal_id)
        row = rows.get(pattern_id) or _pattern_row_from_meta(pattern_id)
        if row.name_es == pattern_id:
            row.name_es = definition.title
        if row.name_en == pattern_id:
            row.name_en = definition.title
        if not row.description_es:
            row.description_es = definition.description
        if not row.description_en:
            row.description_en = definition.description
        row.category = row.category or definition.category
        row.severity = _better_severity(
            row.severity,
            lake_severities.get(signal_id) or definition.severity,
        )
        row.signal_ids = _merge_unique(row.signal_ids, [visible_signal_id(signal_id)])
        row.sources_required = _merge_unique(
            row.sources_required,
            definition.sources or definition.sources_required,
        )
        rows[pattern_id] = row

    for signal_id, (hit_count, observed_at) in lake_counts.items():
        canonical_signal_id = resolve_signal_id(signal_id)
        definition = definitions.get(canonical_signal_id)
        pattern_id = signal_to_pattern_id(canonical_signal_id)
        row = rows.get(pattern_id) or _pattern_row_from_meta(pattern_id)
        if definition is not None:
            if row.name_es == pattern_id:
                row.name_es = definition.title
            if row.name_en == pattern_id:
                row.name_en = definition.title
            if not row.description_es:
                row.description_es = definition.description
            if not row.description_en:
                row.description_en = definition.description
            row.category = row.category or definition.category
            row.sources_required = _merge_unique(
                row.sources_required,
                definition.sources or definition.sources_required,
            )
        previous_hit_count = row.hit_count
        candidate_confidence = lake_confidence.get(canonical_signal_id)
        if candidate_confidence is not None:
            if row.confidence_index is None or previous_hit_count <= 0:
                row.confidence_index = candidate_confidence
            else:
                row.confidence_index = round(
                    (
                        row.confidence_index * previous_hit_count
                        + candidate_confidence * hit_count
                    )
                    / (previous_hit_count + hit_count),
                    1,
                )
        row.hit_count += hit_count
        row.last_seen_at = max(
            [value for value in [row.last_seen_at, observed_at] if value],
            default=None,
        )
        row.severity = _better_severity(
            row.severity,
            lake_severities.get(canonical_signal_id)
            or (definition.severity if definition else None),
        )
        row.materialized = True
        row.materialization_state = "materialized"
        row.signal_ids = _merge_unique(
            row.signal_ids,
            [visible_signal_id(canonical_signal_id)],
        )
        rows[pattern_id] = row

    return sorted(
        rows.values(),
        key=lambda row: (
            -int(row.hit_count),
            row.name_es.lower(),
            row.id,
        ),
    )


def materialized_pattern_ids() -> set[str]:
    from coacc.services import lakehouse_signal_service

    return {
        signal_to_pattern_id(signal_id)
        for signal_id in lakehouse_signal_service.materialized_signal_counts()
    }


def lake_patterns_for_entity(
    entity_id: str,
    *,
    lang: str = "es",
    pattern_id: str = "__all__",
    public_only: bool = True,
) -> list[PatternResult] | None:
    entity = get_lake_entity(entity_id, include_person=not public_only)
    if entity is None:
        return None

    response = materialized_entity_signals(
        entity.id,
        public_only=public_only,
        limit=100,
    )
    requested = pattern_id.strip()
    results: list[PatternResult] = []
    for hit in response.signals:
        mapped_pattern_id = signal_to_pattern_id(hit.signal_id)
        if requested != "__all__" and mapped_pattern_id != requested:
            continue
        meta = PATTERN_METADATA.get(mapped_pattern_id, {})
        evidence_refs = [
            ref for ref in hit.evidence_refs[: settings.pattern_max_evidence_refs] if ref
        ]
        if not evidence_refs:
            evidence_refs = [
                item.url or item.record_id or item.label or item.item_id
                for item in hit.evidence_items[: settings.pattern_max_evidence_refs]
                if item.url or item.record_id or item.label or item.item_id
            ]
        results.append(PatternResult(
            pattern_id=mapped_pattern_id,
            pattern_name=_localized(meta, "name", lang, hit.title),
            description=_localized(meta, "desc", lang, hit.description),
            data={
                "signal_id": visible_signal_id(hit.signal_id),
                "hit_id": hit.hit_id,
                "severity": hit.severity,
                "scope_key": hit.scope_key,
                "scope_type": hit.scope_type,
                "risk_signal": hit.score,
                "confidence_index": hit.confidence_index,
                "evidence_count": hit.evidence_count,
                "evidence_refs": evidence_refs,
                "identity_quality": hit.identity_quality,
            },
            entity_ids=[entity.id],
            sources=hit.sources,
            confidence_index=hit.confidence_index or 0.0,
            confidence_components=hit.confidence_components
            or SignalConfidenceComponents(
                identity=0.0,
                evidence_traceability=0.0,
                source_corroboration=0.0,
            ),
            exposure_tier="confidence_indexed",
            intelligence_tier="community",
        ))
    return results
