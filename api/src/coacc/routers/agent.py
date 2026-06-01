from fastapi import APIRouter

from coacc.models.agent import AgentQueryRequest, AgentQueryResponse
from coacc.services.lakehouse_agent_service import answer_agent_query

router = APIRouter(tags=["agent"])


@router.post("/api/v1/agent/query", response_model=AgentQueryResponse)
@router.post("/agent/query", response_model=AgentQueryResponse, include_in_schema=False)
async def post_agent_query(body: AgentQueryRequest) -> AgentQueryResponse:
    return answer_agent_query(body)
