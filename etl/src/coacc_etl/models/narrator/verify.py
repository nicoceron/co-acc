from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from coacc_etl.models.narrator.subgraph import CaseSubgraph

REQUIRED_SECTIONS = ("# Lead", "## Evidencia", "## Señales", "## Fuentes")
FORBIDDEN_TERMS = (
    "corrupto",
    "criminal",
    "delincuente",
    "defraudo",
    "defraudó",
    "cometio fraude",
    "cometió fraude",
    "culpable",
)
ALLOWED_CAPITALIZED_TERMS = {
    "Lead",
    "Evidencia",
    "Señales",
    "Fuentes",
    "SECOP II",
    "PACO",
    "Isolation Forest",
}
_WORD_RE = re.compile(r"\b[\wÁÉÍÓÚÑáéíóúñ]+\b", re.UNICODE)
_CITATION_RE = re.compile(r"\(([A-Za-z0-9_-]+),\s*([^)]+)\)")
_ENTITY_RE = re.compile(
    r"\b(?:[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.-]+|[A-ZÁÉÍÓÚÑ]{2,})"
    r"(?:[ \t]+(?:[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.-]+|[A-ZÁÉÍÓÚÑ]{2,})){1,5}\b"
)
_REPO_ROOT = Path(__file__).resolve().parents[5]


@dataclass(frozen=True)
class NarrativeVerificationResult:
    valid: bool
    errors: list[str]
    word_count: int


def _word_count(text: str) -> int:
    return len(_WORD_RE.findall(text))


@lru_cache(maxsize=1)
def known_dataset_ids() -> set[str]:
    ids: set[str] = set()
    catalog = _REPO_ROOT / "docs" / "datasets" / "catalog.signed.csv"
    if catalog.exists():
        with catalog.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                dataset_id = (row.get("dataset_id") or "").strip()
                if dataset_id:
                    ids.add(dataset_id)
    datasets_dir = _REPO_ROOT / "etl" / "datasets"
    if datasets_dir.exists():
        for path in datasets_dir.glob("*.yml"):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("id:"):
                    ids.add(line.split(":", 1)[1].strip())
                    break
    return ids


def _candidate_entities(text: str) -> set[str]:
    return {
        match.group(0).strip()
        for match in _ENTITY_RE.finditer(text)
        if match.group(0).strip() not in ALLOWED_CAPITALIZED_TERMS
    }


def _entity_allowed(candidate: str, allowed_names: set[str]) -> bool:
    if candidate in allowed_names:
        return True
    return any(candidate in name or name in candidate for name in allowed_names)


def check(
    narrative: str,
    subgraph: CaseSubgraph,
    *,
    min_words: int = 250,
    max_words: int = 400,
) -> NarrativeVerificationResult:
    errors: list[str] = []
    for section in REQUIRED_SECTIONS:
        if not re.search(rf"^{re.escape(section)}\s*$", narrative, flags=re.MULTILINE):
            errors.append(f"missing section: {section}")

    words = _word_count(narrative)
    if words < min_words:
        errors.append(f"word count below minimum: {words} < {min_words}")
    if words > max_words:
        errors.append(f"word count above maximum: {words} > {max_words}")

    lowered = narrative.lower()
    for term in FORBIDDEN_TERMS:
        if term in lowered:
            errors.append(f"forbidden ethics term: {term}")

    allowed_names = {node.name for node in subgraph.nodes if node.name}
    allowed_names.add(subgraph.contract_id)
    for candidate in _candidate_entities(narrative):
        if not _entity_allowed(candidate, allowed_names):
            errors.append(f"unknown entity mention: {candidate}")

    allowed_citations = {
        (citation.dataset_id, citation.row_key)
        for citation in subgraph.evidence
    }
    citations = [
        (dataset_id.strip(), row_key.strip())
        for dataset_id, row_key in _CITATION_RE.findall(narrative)
    ]
    if not citations:
        errors.append("no citations found")
    catalog_ids = known_dataset_ids()
    for dataset_id, row_key in citations:
        if dataset_id not in catalog_ids:
            errors.append(f"unknown dataset citation: {dataset_id}")
        if (dataset_id, row_key) not in allowed_citations:
            errors.append(f"unresolved citation: ({dataset_id}, {row_key})")

    return NarrativeVerificationResult(valid=not errors, errors=errors, word_count=words)
