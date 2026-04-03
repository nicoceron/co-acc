from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast

from neo4j import AsyncSession

from coacc.models.case import (
    CaseCreate,
    CaseEvidenceBundle,
    CaseEventResponse,
    CaseListResponse,
    CaseResponse,
    CaseSummary,
)
from coacc.models.investigation import InvestigationResponse
from coacc.models.signal import SignalHitResponse
from coacc.services import investigation_service
from coacc.services.intelligence_provider import IntelligenceProvider
from coacc.services.neo4j_service import execute_query, execute_query_single
from coacc.services.signal_materializer import (
    _record_to_signal_hit,
    get_stored_entity_signals,
    refresh_entity_signals,
)


def _case_from_investigation(
    investigation: InvestigationResponse,
    *,
    signal_count: int = 0,
    public_signal_count: int = 0,
    last_refreshed_at: str | None = None,
    last_run_id: str | None = None,
    stale: bool = True,
) -> CaseSummary:
    return CaseSummary(
        id=investigation.id,
        title=investigation.title,
        description=investigation.description,
        status=getattr(investigation, "status", "new") or "new",
        created_at=investigation.created_at,
        updated_at=investigation.updated_at,
        entity_ids=investigation.entity_ids,
        signal_count=signal_count,
        public_signal_count=public_signal_count,
        last_refreshed_at=last_refreshed_at,
        last_run_id=last_run_id,
        stale=stale,
    )


def _severity_rank(severity: str) -> int:
    return {"critical": 4, "high": 3, "medium": 2, "low": 1}.get(severity, 0)


async def _case_summary_meta(
    session: AsyncSession,
    case_id: str,
    user_id: str,
) -> tuple[int, int, str | None, str | None, bool]:
    record = await execute_query_single(
        session,
        "case_summary_meta",
        {"case_id": case_id, "user_id": user_id},
    )
    if record is None:
        return 0, 0, None, None, True
    return (
        int(record["signal_count"] or 0),
        int(record["public_signal_count"] or 0),
        record.get("last_refreshed_at"),
        record.get("last_run_id"),
        bool(record.get("stale", True)),
    )


async def list_cases(
    session: AsyncSession,
    page: int,
    size: int,
    user_id: str,
) -> CaseListResponse:
    investigations, total = await investigation_service.list_investigations(
        session,
        page,
        size,
        user_id,
    )
    cases: list[CaseSummary] = []
    for investigation in investigations:
        signal_count, public_signal_count, last_refreshed_at, last_run_id, stale = (
            await _case_summary_meta(session, investigation.id, user_id)
        )
        cases.append(_case_from_investigation(
            investigation,
            signal_count=signal_count,
            public_signal_count=public_signal_count,
            last_refreshed_at=last_refreshed_at,
            last_run_id=last_run_id,
            stale=stale,
        ))
    return CaseListResponse(cases=cases, total=total)


async def create_case(
    session: AsyncSession,
    body: CaseCreate,
    user_id: str,
) -> CaseSummary:
    investigation = await investigation_service.create_investigation(
        session,
        body.title,
        body.description,
        user_id,
    )
    return _case_from_investigation(investigation)


async def refresh_case(
    session: AsyncSession,
    case_id: str,
    user_id: str,
    provider: IntelligenceProvider,
    *,
    lang: str = "es",
) -> CaseResponse | None:
    investigation = await investigation_service.get_investigation(session, case_id, user_id)
    if investigation is None:
        return None

    case_run_id = str(uuid.uuid4())
    hits_by_id: dict[str, SignalHitResponse] = {}
    for entity_id in investigation.entity_ids:
        response = await refresh_entity_signals(
            session,
            entity_id,
            provider,
            lang=lang,
            trigger="case_refresh",
            created_by=user_id,
        )
        for hit in response.signals:
            hits_by_id[hit.hit_id] = hit

    refreshed_at = datetime.now(UTC).isoformat()
    reset_record = await execute_query_single(
        session,
        "case_refresh_reset",
        {
            "case_id": case_id,
            "user_id": user_id,
            "run_id": case_run_id,
            "refreshed_at": refreshed_at,
        },
    )
    if reset_record is None:
        return None

    hits = sorted(
        hits_by_id.values(),
        key=lambda hit: (-_severity_rank(hit.severity), -hit.score, -hit.evidence_count, hit.title),
    )
    if hits:
        await execute_query(
            session,
            "case_refresh_attach",
            {
                "case_id": case_id,
                "user_id": user_id,
                "hits": [
                    {
                        "hit_id": hit.hit_id,
                        "event_id": f"{case_id}:{hit.hit_id}",
                        "type": "signal_hit",
                        "label": hit.title,
                        "date": cast("str", hit.last_seen_at or hit.created_at or refreshed_at),
                        "entity_id": hit.entity_id,
                        "evidence_bundle_id": hit.evidence_bundle_id,
                    }
                    for hit in hits
                ],
            },
        )

    return await get_case(session, case_id, user_id)


async def get_case(
    session: AsyncSession,
    case_id: str,
    user_id: str,
) -> CaseResponse | None:
    investigation = await investigation_service.get_investigation(session, case_id, user_id)
    if investigation is None:
        return None

    signal_count, public_signal_count, last_refreshed_at, last_run_id, stale = (
        await _case_summary_meta(session, case_id, user_id)
    )
    signal_records = await execute_query(
        session,
        "case_signal_hits",
        {"case_id": case_id, "user_id": user_id},
    )
    hits = [
        _record_to_signal_hit(record)
        for record in signal_records
    ]
    hits = sorted(
        hits,
        key=lambda hit: (-_severity_rank(hit.severity), -hit.score, -hit.evidence_count, hit.title),
    )

    bundles = [
        CaseEvidenceBundle(
            bundle_id=cast("str", hit.evidence_bundle_id or f"bundle:{hit.hit_id}"),
            headline=hit.title,
            source_list=[source.database for source in hit.sources],
            evidence_items=hit.evidence_items,
        )
        for hit in hits
    ]
    event_records = await execute_query(
        session,
        "case_events",
        {"case_id": case_id, "user_id": user_id},
    )
    events = [
        CaseEventResponse(
            id=str(record["id"]),
            type=str(record["type"]),
            label=str(record["label"]),
            date=str(record["date"]),
            entity_id=record.get("entity_id"),
            signal_hit_id=record.get("signal_hit_id"),
            evidence_bundle_id=record.get("evidence_bundle_id"),
            bundle_document_count=int(record["bundle_document_count"] or 0),
        )
        for record in event_records
    ]

    return CaseResponse(
        **_case_from_investigation(
            investigation,
            signal_count=signal_count,
            public_signal_count=public_signal_count,
            last_refreshed_at=last_refreshed_at,
            last_run_id=last_run_id,
            stale=stale,
        ).model_dump(),
        signals=hits,
        evidence_bundles=bundles,
        events=events,
    )


async def get_case_preview_for_entity(
    session: AsyncSession,
    entity_id: str,
) -> tuple[int, str | None] | None:
    stored = await get_stored_entity_signals(session, entity_id)
    if stored.total == 0:
        return None
    return stored.total, stored.last_refreshed_at
