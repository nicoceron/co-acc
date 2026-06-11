from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncDriver, AsyncSession
from starlette.requests import Request

from coacc.config import settings
from coacc.dependencies import get_intelligence_provider, get_optional_session
from coacc.middleware.rate_limit import limiter
from coacc.models.pattern import PatternListResponse, PatternResponse, PatternResult
from coacc.services.intelligence_provider import IntelligenceProvider
from coacc.services.lakehouse_pattern_service import (
    lake_pattern_summaries,
    lake_patterns_for_entity,
    materialized_pattern_ids,
)
from coacc.services.public_guard import enforce_entity_lookup_enabled

router = APIRouter(prefix="/api/v1/patterns", tags=["patterns"])

_PATTERN_ENGINE_DISABLED_DETAIL = (
    "Pattern engine temporarily unavailable pending validation."
)


def _enforce_patterns_enabled() -> None:
    if not settings.patterns_enabled:
        raise HTTPException(status_code=503, detail=_PATTERN_ENGINE_DISABLED_DETAIL)


async def run_all_patterns(
    driver: AsyncDriver,
    entity_id: str | None = None,
    lang: str = "es",
    include_probable: bool = False,
    provider: IntelligenceProvider | None = None,
) -> list[PatternResult]:
    intelligence = provider or get_intelligence_provider()
    return await intelligence.run_all_patterns(
        driver,
        entity_id=entity_id,
        lang=lang,
        include_probable=include_probable,
    )


async def run_pattern(
    session: AsyncSession,
    pattern_id: str,
    entity_id: str | None = None,
    lang: str = "es",
    include_probable: bool = False,
    provider: IntelligenceProvider | None = None,
) -> list[PatternResult]:
    intelligence = provider or get_intelligence_provider()
    return await intelligence.run_pattern(
        session,
        pattern_id=pattern_id,
        entity_id=entity_id,
        lang=lang,
        include_probable=include_probable,
    )


@router.get("/{entity_id}", response_model=PatternResponse)
@limiter.limit("30/minute")
async def get_patterns_for_entity(
    request: Request,
    entity_id: str,
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
    provider: Annotated[IntelligenceProvider, Depends(get_intelligence_provider)],
    lang: Annotated[str, Query()] = "es",
    include_probable: Annotated[bool, Query()] = False,
) -> PatternResponse:
    _enforce_patterns_enabled()
    if settings.public_mode:
        enforce_entity_lookup_enabled()
    driver: AsyncDriver | None = getattr(request.app.state, "neo4j_driver", None)
    lake_results = lake_patterns_for_entity(entity_id, lang=lang, public_only=True)
    if lake_results is not None and (lake_results or session is None or driver is None):
        results = lake_results
    elif session is None or driver is None:
        if lake_results is None:
            raise HTTPException(status_code=404, detail="Entity not found")
        results = []
    else:
        results = await run_all_patterns(
            driver,
            entity_id,
            lang,
            include_probable=include_probable,
            provider=provider,
        )
    return PatternResponse(
        entity_id=entity_id,
        patterns=results,
        total=len(results),
    )


@router.get("/{entity_id}/{pattern_name}", response_model=PatternResponse)
@limiter.limit("30/minute")
async def get_specific_pattern(
    request: Request,
    entity_id: str,
    pattern_name: str,
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
    provider: Annotated[IntelligenceProvider, Depends(get_intelligence_provider)],
    lang: Annotated[str, Query()] = "es",
    include_probable: Annotated[bool, Query()] = False,
) -> PatternResponse:
    _enforce_patterns_enabled()
    if settings.public_mode:
        enforce_entity_lookup_enabled()
    provider_available = {row["id"] for row in provider.list_patterns()}
    lake_available = materialized_pattern_ids()
    available = provider_available | lake_available
    if pattern_name not in available:
        app_env = settings.app_env.strip().lower()
        detail = (
            "Pattern not found"
            if app_env in ("prod", "production")
            else f"Pattern not found: {pattern_name}. Available: {sorted(available)}"
        )
        raise HTTPException(status_code=404, detail=detail)
    lake_results = lake_patterns_for_entity(
        entity_id,
        lang=lang,
        pattern_id=pattern_name,
        public_only=True,
    )
    if lake_results is not None and (
        lake_results or session is None or pattern_name not in provider_available
    ):
        results = lake_results
    elif session is None or pattern_name not in provider_available:
        if lake_results is None:
            raise HTTPException(status_code=404, detail="Entity not found")
        results = []
    else:
        results = await run_pattern(
            session,
            pattern_name,
            entity_id,
            lang,
            include_probable=include_probable,
            provider=provider,
        )
    return PatternResponse(
        entity_id=entity_id,
        patterns=results,
        total=len(results),
    )


@router.get("/", response_model=PatternListResponse)
async def list_patterns(
    provider: Annotated[IntelligenceProvider, Depends(get_intelligence_provider)],
) -> PatternListResponse:
    _enforce_patterns_enabled()
    return PatternListResponse(patterns=lake_pattern_summaries(provider.list_patterns()))
