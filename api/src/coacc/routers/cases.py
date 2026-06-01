from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncSession

from coacc.dependencies import (
    CurrentReviewer,
    can_access_reviewer_content,
    get_intelligence_provider,
    get_optional_session,
    get_optional_user_without_database_required,
    get_session,
)
from coacc.models.case import CaseCreate, CaseListResponse, CaseResponse, CaseSummary
from coacc.models.user import UserResponse
from coacc.services.case_service import create_case, get_case, list_cases, refresh_case
from coacc.services.intelligence_provider import IntelligenceProvider
from coacc.services.lakehouse_signal_service import get_lake_case, list_lake_cases

router = APIRouter(prefix="/api/v1/cases", tags=["cases"])


def _require_reviewer_when_graph_is_available(
    session: AsyncSession | None,
    user: UserResponse | None,
) -> None:
    if session is None:
        return
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not can_access_reviewer_content(user):
        raise HTTPException(status_code=403, detail="Reviewer access required")


@router.get("/", response_model=CaseListResponse)
async def get_cases(
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
    user: Annotated[
        UserResponse | None,
        Depends(get_optional_user_without_database_required),
    ],
    page: Annotated[int, Query(ge=1)] = 1,
    size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> CaseListResponse:
    if session is None:
        return list_lake_cases(page, size)
    _require_reviewer_when_graph_is_available(session, user)
    assert user is not None
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
    session: Annotated[AsyncSession | None, Depends(get_optional_session)],
    user: Annotated[
        UserResponse | None,
        Depends(get_optional_user_without_database_required),
    ],
) -> CaseResponse:
    if session is None:
        lake_case = get_lake_case(case_id)
        if lake_case is None:
            raise HTTPException(status_code=404, detail="Case not found")
        return lake_case
    _require_reviewer_when_graph_is_available(session, user)
    assert user is not None
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
