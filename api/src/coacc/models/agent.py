from __future__ import annotations

from pydantic import BaseModel, Field


class AgentQueryRequest(BaseModel):
    question: str = Field(min_length=1, max_length=1000)
    case_id: str | None = Field(default=None, max_length=200)


class AgentCitation(BaseModel):
    dataset_id: str | None = None
    source_id: str | None = None
    row_key: str | None = None
    url: str | None = None
    label: str | None = None
    case_id: str | None = None
    evidence_item_id: str | None = None


class AgentSubgraphNode(BaseModel):
    id: str
    label: str
    type: str


class AgentSubgraphEdge(BaseModel):
    source: str
    target: str
    label: str


class AgentSubgraph(BaseModel):
    case_id: str
    title: str
    nodes: list[AgentSubgraphNode] = Field(default_factory=list)
    edges: list[AgentSubgraphEdge] = Field(default_factory=list)


class AgentQueryResponse(BaseModel):
    answer: str
    citations: list[AgentCitation] = Field(default_factory=list)
    subgraphs: list[AgentSubgraph] = Field(default_factory=list)
    related_case_ids: list[str] = Field(default_factory=list)
