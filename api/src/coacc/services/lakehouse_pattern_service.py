from __future__ import annotations

from coacc.config import settings
from coacc.models.pattern import PATTERN_METADATA, PatternResult
from coacc.services.lakehouse_entity_service import get_lake_entity
from coacc.services.lakehouse_signal_service import materialized_entity_signals

_SIGNAL_TO_PATTERN = {
    "procurement_sanctioned_supplier_awarded": "sanctioned_supplier_record",
    "procurement_supplier_concentration_across_entities": "contract_concentration",
    "procurement_repeat_awards_same_supplier": "split_contracts_below_threshold",
}


def _localized(meta: dict[str, str], key: str, lang: str, fallback: str) -> str:
    localized_key = f"{key}_{lang}"
    if localized_key in meta and meta[localized_key]:
        return meta[localized_key]
    english_key = f"{key}_en"
    if english_key in meta and meta[english_key]:
        return meta[english_key]
    return fallback


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
        mapped_pattern_id = _SIGNAL_TO_PATTERN.get(hit.signal_id)
        if mapped_pattern_id is None:
            continue
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
        if not evidence_refs:
            continue
        results.append(PatternResult(
            pattern_id=mapped_pattern_id,
            pattern_name=_localized(meta, "name", lang, hit.title),
            description=_localized(meta, "desc", lang, hit.description),
            data={
                "signal_id": hit.signal_id,
                "hit_id": hit.hit_id,
                "severity": hit.severity,
                "scope_key": hit.scope_key,
                "scope_type": hit.scope_type,
                "risk_signal": hit.score,
                "evidence_count": hit.evidence_count,
                "evidence_refs": evidence_refs,
                "identity_quality": hit.identity_quality,
            },
            entity_ids=[entity.id],
            sources=hit.sources,
            exposure_tier="public_safe",
            intelligence_tier="community",
        ))
    return results
