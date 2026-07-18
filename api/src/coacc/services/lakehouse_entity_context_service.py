from __future__ import annotations

from typing import TYPE_CHECKING

from coacc.models.entity import (
    EntityEvidenceTrailResponse,
    EvidenceTrailBundle,
    EvidenceTrailDocument,
    EvidenceTrailParty,
    ExposureFactor,
    ExposureResponse,
    SourceAttribution,
    TimelineEvent,
    TimelineResponse,
)
from coacc.models.graph import GraphEdge, GraphNode, GraphResponse
from coacc.services.lakehouse_anomaly_service import entity_anomaly_scores
from coacc.services.lakehouse_entity_service import get_lake_entity
from coacc.services.lakehouse_signal_service import materialized_entity_signals
from coacc.services.public_guard import should_hide_person_entities

_DEFAULT_EVENT_DATE = "1970-01-01T00:00:00+00:00"

if TYPE_CHECKING:
    from collections.abc import Iterable

    from coacc.models.anomaly import CaseAnomalyScore
    from coacc.models.signal import EvidenceItemResponse, SignalHitResponse


def _dedupe_sources(sources: Iterable[SourceAttribution]) -> list[SourceAttribution]:
    seen: set[tuple[str, str | None, str | None]] = set()
    result: list[SourceAttribution] = []
    for source in sources:
        key = (source.database, source.record_id, source.extracted_at)
        if key in seen:
            continue
        seen.add(key)
        result.append(source)
    return result


def _source_from_id(source_id: str | None) -> list[SourceAttribution]:
    if not source_id:
        return []
    return [SourceAttribution(database=source_id)]


def _context_entity_id(entity_id: str, entity_id_from_dimension: str | None) -> str:
    return entity_id_from_dimension or entity_id


def _signal_date(hit: SignalHitResponse) -> str:
    return hit.last_seen_at or hit.first_seen_at or hit.created_at or _DEFAULT_EVENT_DATE


def _evidence_document(
    item: EvidenceItemResponse,
    *,
    fallback_index: int,
) -> EvidenceTrailDocument | None:
    item_id = item.item_id or item.record_id or item.url or f"evidence-{fallback_index}"
    title = item.label or item.url or item.record_id or item.item_id or item.source_id
    if not title:
        return None
    return EvidenceTrailDocument(
        id=str(item_id),
        title=str(title),
        url=item.url,
        kind=item.item_type,
        uploaded_at=item.observed_at,
        source=item.source_id,
        record_id=item.record_id,
        identity_match_type=item.identity_match_type,
        row_selector=item.row_selector or item.node_ref,
        file_selector=item.file_selector,
    )


def _documents_from_refs(hit: SignalHitResponse) -> list[EvidenceTrailDocument]:
    documents: list[EvidenceTrailDocument] = []
    for index, ref in enumerate(hit.evidence_refs, start=1):
        if not ref:
            continue
        documents.append(
            EvidenceTrailDocument(
                id=f"{hit.hit_id}:ref:{index}",
                title=ref,
                url=ref if ref.startswith(("http://", "https://")) else None,
                kind="reference",
                source=hit.sources[0].database if hit.sources else None,
            )
        )
    return documents


def lake_evidence_trail(entity_id: str, *, limit: int = 12) -> EntityEvidenceTrailResponse | None:
    entity = get_lake_entity(entity_id, include_person=not should_hide_person_entities())
    context_entity_id = _context_entity_id(entity_id, entity.id if entity else None)
    signals = materialized_entity_signals(context_entity_id, public_only=False, limit=limit)
    if signals.total == 0:
        return None

    bundles: list[EvidenceTrailBundle] = []
    for hit in signals.signals[:limit]:
        documents = [
            document
            for index, item in enumerate(hit.evidence_items, start=1)
            if (document := _evidence_document(item, fallback_index=index)) is not None
        ]
        if not documents:
            documents = _documents_from_refs(hit)
        document_kinds = list(dict.fromkeys(document.kind or "reference" for document in documents))
        bundles.append(
            EvidenceTrailBundle(
                id=hit.evidence_bundle_id or hit.hit_id,
                bundle_type="signal_evidence",
                title=hit.title,
                reference=hit.scope_key,
                description=hit.description,
                relation_summary=f"{hit.signal_id} ({hit.severity})",
                via_entity_name=entity.properties.get("name") if entity else None,
                via_entity_ref=entity.id if entity else hit.entity_id,
                document_count=len(documents),
                document_kinds=document_kinds,
                documents=documents,
                parties=[
                    EvidenceTrailParty(
                        role="subject",
                        name=str(
                            (entity.properties.get("name") if entity else None)
                            or hit.entity_key
                            or entity_id
                        ),
                        document_id=str(entity.properties.get("document_id"))
                        if entity and entity.properties.get("document_id")
                        else None,
                        entity_id=entity.id if entity else hit.entity_id,
                    )
                ],
                source=hit.sources[0].database if hit.sources else None,
            )
        )

    return EntityEvidenceTrailResponse(
        entity_id=context_entity_id,
        bundles=bundles,
        total_bundles=len(bundles),
        total_documents=sum(bundle.document_count for bundle in bundles),
    )


def _center_node(entity_id: str) -> GraphNode | None:
    entity = get_lake_entity(entity_id, include_person=not should_hide_person_entities())
    if entity is None:
        return None
    document_id = entity.properties.get("document_id") or entity.properties.get("nit")
    label = (
        entity.properties.get("name")
        or entity.properties.get("razon_social")
        or entity.properties.get("document_id")
        or entity.id
    )
    return GraphNode(
        id=entity.id,
        label=str(label),
        type=entity.type,
        document_id=str(document_id) if document_id else None,
        properties=entity.properties,
        sources=entity.sources,
        is_pep=entity.is_pep,
        exposure_tier=entity.exposure_tier,
    )


def _signal_node(hit: SignalHitResponse) -> GraphNode:
    return GraphNode(
        id=f"signal:{hit.hit_id}",
        label=hit.title,
        type="signal",
        properties={
            "signal_id": hit.signal_id,
            "severity": hit.severity,
            "score": hit.score,
            "scope_key": hit.scope_key,
            "scope_type": hit.scope_type,
        },
        sources=hit.sources,
        exposure_tier="public_safe" if hit.public_safe else "restricted",
    )


def _evidence_node(hit: SignalHitResponse, item: EvidenceItemResponse, index: int) -> GraphNode:
    node_id = item.item_id or item.record_id or item.url or f"{hit.hit_id}:evidence:{index}"
    label = item.label or item.url or item.record_id or item.source_id or str(node_id)
    return GraphNode(
        id=f"evidence:{node_id}",
        label=str(label),
        type="sourceDocument",
        document_id=item.record_id,
        properties={
            "source_id": item.source_id,
            "url": item.url,
            "record_id": item.record_id,
            "observed_at": item.observed_at,
        },
        sources=_source_from_id(item.source_id),
        exposure_tier="public_safe" if item.public_safe else "restricted",
    )


def _anomaly_node(score: CaseAnomalyScore) -> GraphNode:
    return GraphNode(
        id=f"anomaly:{score.contract_id}",
        label=f"Anomaly score {score.score:.2f}",
        type="contract",
        document_id=score.contract_id,
        properties={
            "contract_id": score.contract_id,
            "score": score.score,
            "score_confidence": score.score_confidence,
            "prior_sanction_supplier": score.prior_sanction_supplier,
            "process_url": score.process_url,
        },
        sources=[SourceAttribution(database="lake_anomaly_scores")],
        exposure_tier="public_safe",
    )


def lake_graph(entity_id: str, *, depth: int = 1) -> GraphResponse | None:
    center = _center_node(entity_id)
    if center is None:
        return None

    signals = materialized_entity_signals(center.id, public_only=False, limit=25)
    anomaly_scores, _ = entity_anomaly_scores(center.id, limit=5)
    if signals.total == 0 and not anomaly_scores:
        return None

    nodes_by_id: dict[str, GraphNode] = {center.id: center}
    edges: list[GraphEdge] = []

    for hit in signals.signals:
        signal_node = _signal_node(hit)
        nodes_by_id[signal_node.id] = signal_node
        edges.append(
            GraphEdge(
                id=f"{center.id}->{signal_node.id}",
                source=center.id,
                target=signal_node.id,
                type="HAS_SIGNAL",
                properties={"severity": hit.severity, "score": hit.score},
                confidence=hit.identity_confidence,
                sources=hit.sources,
            )
        )
        if depth <= 1:
            continue
        for index, item in enumerate(hit.evidence_items[:3], start=1):
            evidence_node = _evidence_node(hit, item, index)
            nodes_by_id[evidence_node.id] = evidence_node
            edges.append(
                GraphEdge(
                    id=f"{signal_node.id}->{evidence_node.id}",
                    source=signal_node.id,
                    target=evidence_node.id,
                    type="SUPPORTED_BY",
                    properties={"source_id": item.source_id},
                    confidence=hit.identity_confidence,
                    sources=evidence_node.sources,
                )
            )

    for score in anomaly_scores:
        anomaly_node = _anomaly_node(score)
        nodes_by_id[anomaly_node.id] = anomaly_node
        edges.append(
            GraphEdge(
                id=f"{center.id}->{anomaly_node.id}",
                source=center.id,
                target=anomaly_node.id,
                type="HAS_ANOMALY_SCORE",
                properties={"score": score.score},
                confidence=1.0,
                sources=anomaly_node.sources,
            )
        )

    return GraphResponse(nodes=list(nodes_by_id.values()), edges=edges, center_id=center.id)


def lake_exposure(entity_id: str) -> ExposureResponse | None:
    entity = get_lake_entity(entity_id, include_person=not should_hide_person_entities())
    context_entity_id = _context_entity_id(entity_id, entity.id if entity else None)
    signals = materialized_entity_signals(context_entity_id, public_only=False, limit=100)
    anomaly_scores, anomaly_total = entity_anomaly_scores(context_entity_id, limit=25)
    if signals.total == 0 and anomaly_total == 0:
        return None

    critical_count = sum(1 for hit in signals.signals if hit.severity == "critical")
    max_signal_score = max((hit.score for hit in signals.signals), default=0.0)
    max_anomaly_score = max((score.score for score in anomaly_scores), default=0.0)
    prior_sanction_count = sum(1 for score in anomaly_scores if score.prior_sanction_supplier)
    factors = [
        ExposureFactor(
            name="Materialized signal hits",
            value=float(signals.total),
            percentile=min(99.0, float(signals.total)),
            weight=0.30,
            sources=["lake_signal_hits"],
        ),
        ExposureFactor(
            name="Critical signal hits",
            value=float(critical_count),
            percentile=min(99.0, critical_count * 10.0),
            weight=0.25,
            sources=["lake_signal_hits"],
        ),
        ExposureFactor(
            name="Maximum signal score",
            value=max_signal_score,
            percentile=max_signal_score * 100.0,
            weight=0.20,
            sources=["lake_signal_hits"],
        ),
        ExposureFactor(
            name="Maximum anomaly score",
            value=max_anomaly_score,
            percentile=max_anomaly_score * 100.0,
            weight=0.20,
            sources=["lake_anomaly_scores"],
        ),
        ExposureFactor(
            name="Prior sanction anomaly flags",
            value=float(prior_sanction_count),
            percentile=100.0 if prior_sanction_count else 0.0,
            weight=0.05,
            sources=["lake_anomaly_scores"],
        ),
    ]
    exposure_index = sum(factor.percentile * factor.weight for factor in factors)
    sources = _dedupe_sources([
        *[source for hit in signals.signals for source in hit.sources],
        SourceAttribution(database="lake_anomaly_scores"),
    ])
    return ExposureResponse(
        entity_id=context_entity_id,
        exposure_index=round(exposure_index, 2),
        factors=factors,
        peer_group=entity.type if entity else "lake_entity",
        peer_count=max(1, anomaly_total or signals.total),
        sources=sources,
    )


def lake_timeline(
    entity_id: str,
    *,
    cursor: str | None = None,
    limit: int = 50,
) -> TimelineResponse | None:
    entity = get_lake_entity(entity_id, include_person=not should_hide_person_entities())
    context_entity_id = _context_entity_id(entity_id, entity.id if entity else None)
    signals = materialized_entity_signals(context_entity_id, public_only=False, limit=100)
    anomaly_scores, _ = entity_anomaly_scores(context_entity_id, limit=50)
    if signals.total == 0 and not anomaly_scores:
        return None

    events: list[TimelineEvent] = []
    for hit in signals.signals:
        events.append(
            TimelineEvent(
                id=f"signal:{hit.hit_id}",
                date=_signal_date(hit),
                label=hit.title,
                entity_type="Signal",
                properties={
                    "signal_id": hit.signal_id,
                    "severity": hit.severity,
                    "score": hit.score,
                    "scope_key": hit.scope_key,
                },
                sources=hit.sources,
            )
        )
    for score in anomaly_scores:
        events.append(
            TimelineEvent(
                id=f"anomaly:{score.contract_id}",
                date=score.scored_at or _DEFAULT_EVENT_DATE,
                label=f"Anomaly score {score.score:.2f} for {score.contract_id}",
                entity_type="AnomalyScore",
                properties={
                    "contract_id": score.contract_id,
                    "score": score.score,
                    "prior_sanction_supplier": score.prior_sanction_supplier,
                },
                sources=[SourceAttribution(database="lake_anomaly_scores")],
            )
        )
    events.sort(key=lambda event: (event.date, event.id), reverse=True)
    if cursor:
        events = [event for event in events if event.date < cursor]
    selected = events[:limit]
    next_cursor = selected[-1].date if len(selected) == limit and len(events) > limit else None
    return TimelineResponse(
        entity_id=context_entity_id,
        events=selected,
        total=len(selected),
        next_cursor=next_cursor,
    )
