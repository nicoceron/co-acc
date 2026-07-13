from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncSession

from coacc.dependencies import get_optional_session
from coacc.models.signal import SignalDetailResponse, SignalListResponse
from coacc.services import lakehouse_signal_service
from coacc.services.signal_materializer import (
    get_latest_materializer_run,
    get_signal_samples,
    list_signal_summaries,
)
from coacc.services.signal_registry import get_signal_definition, load_signal_registry

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


@router.get("/", response_model=SignalListResponse)
async def list_signals(
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
) -> SignalListResponse:
    registry = load_signal_registry()
    signals = await list_signal_summaries(session)
    last_run_id, last_refreshed_at = await get_latest_materializer_run(session)
    return SignalListResponse(
        registry_version=registry.registry_version,
        last_run_id=last_run_id,
        last_refreshed_at=last_refreshed_at,
        signals=signals,
    )


@router.get("/{signal_id}", response_model=SignalDetailResponse)
async def get_signal(
    signal_id: str,
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
    limit: Annotated[int, Query(ge=1, le=25)] = 10,
) -> SignalDetailResponse:
    definition = get_signal_definition(signal_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Signal not found")
    sample_hits = await get_signal_samples(session, signal_id, limit)
    materialized_severity = lakehouse_signal_service.materialized_signal_severities().get(
        definition.id
    )
    if materialized_severity:
        definition = definition.model_copy(update={"severity": materialized_severity})
    return SignalDetailResponse(definition=definition, sample_hits=sample_hits)
