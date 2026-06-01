from pydantic import BaseModel, Field


class CaseAnomalyScore(BaseModel):
    contract_id: str
    entity_uid: str
    score: float
    score_confidence: str = "unknown"
    top_features: list[str] = Field(default_factory=list)
    prior_sanction_supplier: bool = False
    process_url: str | None = None
    score_run_id: str
    model_run_id: str | None = None
    feature_run_id: str | None = None
    scored_at: str | None = None


class EntityAnomalyScoresResponse(BaseModel):
    entity_id: str
    total: int
    scores: list[CaseAnomalyScore] = Field(default_factory=list)
