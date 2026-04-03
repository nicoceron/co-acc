from coacc_etl.entity_resolution.aliases import build_alias_row
from coacc_etl.entity_resolution.canonical import (
    canonical_company_key,
    canonical_contract_key,
    canonical_person_key,
    canonical_project_key,
    canonical_public_entity_key,
    normalize_document_id,
    normalize_nit,
)
from coacc_etl.entity_resolution.matchers import (
    MatchResult,
    exact_company_match,
    exact_contract_match,
    exact_person_match,
    exact_project_match,
    scored_match,
)

__all__ = [
    "build_alias_row",
    "canonical_company_key",
    "canonical_contract_key",
    "canonical_person_key",
    "canonical_project_key",
    "canonical_public_entity_key",
    "normalize_document_id",
    "normalize_nit",
    "MatchResult",
    "exact_company_match",
    "exact_contract_match",
    "exact_person_match",
    "exact_project_match",
    "scored_match",
]
