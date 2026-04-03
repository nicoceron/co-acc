from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MatchResult:
    match_type: str
    confidence: float
    identity_quality: str


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def exact_company_match() -> MatchResult:
    return MatchResult(
        match_type="EXACT_COMPANY_NIT",
        confidence=1.0,
        identity_quality="exact",
    )


def exact_person_match() -> MatchResult:
    return MatchResult(
        match_type="EXACT_PERSON_DOCUMENT",
        confidence=1.0,
        identity_quality="exact",
    )


def exact_project_match() -> MatchResult:
    return MatchResult(
        match_type="EXACT_BPIN",
        confidence=1.0,
        identity_quality="exact",
    )


def exact_contract_match() -> MatchResult:
    return MatchResult(
        match_type="EXACT_CONTRACT_KEY",
        confidence=1.0,
        identity_quality="exact",
    )


def scored_match(
    *,
    has_exact_numeric_identifier: bool,
    strong_name_similarity: bool = False,
    shared_municipality: bool = False,
    high_collision_risk: bool = False,
    deterministic_match_type: str = "PROBABLE_NAME_MATCH",
) -> MatchResult:
    score = 0.0
    if has_exact_numeric_identifier:
        score += 0.70
    if strong_name_similarity:
        score += 0.20
    if shared_municipality:
        score += 0.05
    if high_collision_risk:
        score -= 0.20

    confidence = _clamp(score)
    if confidence >= 0.99:
        quality = "exact"
    elif confidence >= 0.85:
        quality = "high"
    elif confidence >= 0.60:
        quality = "probable"
    else:
        quality = "name_only"

    match_type = deterministic_match_type
    if has_exact_numeric_identifier and deterministic_match_type == "PROBABLE_NAME_MATCH":
        match_type = "HIGH_CONFIDENCE_IDENTIFIER_MATCH"

    return MatchResult(
        match_type=match_type,
        confidence=confidence,
        identity_quality=quality,
    )
