from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncSession

from coacc.dependencies import CurrentReviewer, get_intelligence_provider, get_session
from coacc.models.case import CaseCreate, CaseListResponse, CaseResponse, CaseSummary
from coacc.services.case_service import create_case, get_case, list_cases, refresh_case
from coacc.services.intelligence_provider import IntelligenceProvider

router = APIRouter(prefix="/api/v1/cases", tags=["cases"])


@router.get("/", response_model=CaseListResponse)
async def get_cases(
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentReviewer,
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> CaseListResponse:
    return await list_cases(session, page, size, user.id)


@router.post("/", response_model=CaseSummary, status_code=201)
async def post_case(
    body: CaseCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentReviewer,
) -> CaseSummary:
    return await create_case(session, body, user.id)


@router.get("/{case_id}", response_model=CaseResponse)
async def get_case_detail(
    case_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentReviewer,
) -> CaseResponse:
    case = await get_case(session, case_id, user.id)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")
    return case


@router.post("/{case_id}/refresh", response_model=CaseResponse)
async def refresh_case_detail(
    case_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    user: CurrentReviewer,
    provider: Annotated[IntelligenceProvider, Depends(get_intelligence_provider)],
    lang: Annotated[str, Query()] = "es",
) -> CaseResponse:
    case = await refresh_case(session, case_id, user.id, provider, lang=lang)
    if case is None:
        raise HTTPException(status_code=404, detail="Case not found")
    return case
