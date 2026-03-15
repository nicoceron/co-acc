from __future__ import annotations

import re
from typing import Any

from coacc_etl.pipelines.colombia_shared import clean_name, clean_text
from coacc_etl.transforms import strip_document

_MAX_REFERENCE_VALUES = 5

_DOCUMENT_PATTERN = re.compile(
    r"\b(?:NIT[:\s-]*)?(\d[\d.\-]{5,16}\d)\b",
    flags=re.IGNORECASE,
)
_PROCESS_PATTERN = re.compile(
    r"\b("
    r"CO1\.[A-Z0-9]+\.[A-Z0-9.\-]+"
    r"|[A-Z]{2,}(?:[-_/][A-Z0-9.]+){1,5}"
    r"|(?:CONTRATO|CONTRAT0|PROCESO|RADICADO|RESOLUCION|RESOLUCI[OÓ]N)\s+[A-Z0-9.\-_/]{3,30}"
    r")\b",
    flags=re.IGNORECASE,
)
_PROCESS_PREFIX_PATTERN = re.compile(
    r"^(?:CONTRATO|CONTRAT0|PROCESO|RADICADO|RESOLUCION|RESOLUCION)\s+",
)
_ENTITY_PREFIX_PATTERN = re.compile(
    r"\b(?:EMPRESA|SOCIEDAD|FUNDACI[OÓ]N|CORPORACI[OÓ]N|CONSORCIO|UNION TEMPORAL|"
    r"UNIÓN TEMPORAL|GRUPO|COOPERATIVA)\s+([A-Z0-9][A-Z0-9 .,&()'/\-]{3,80})",
    flags=re.IGNORECASE,
)
_ENTITY_SUFFIX_PATTERN = re.compile(
    r"\b([A-Z0-9][A-Z0-9 .,&()'/\-]{3,80}\b"
    r"(?:S\.?\s*A\.?\s*S\.?|S\.?\s*A\.?|LTDA\.?|LIMITADA|E\.?\s*S\.?\s*P\.?|IPS|ESE|S EN C))\b",
    flags=re.IGNORECASE,
)

_LEGAL_ROLE_TERMS = (
    "REPRESENTANTE LEGAL",
    "REPRESENTANTE LEGAL SUPLENTE",
    "APODERADO JUDICIAL",
    "APODERADO",
    "ASESOR",
    "MIEMBRO DE JUNTA",
    "JUNTA DIRECTIVA",
    "SOCIO",
    "SOCIA",
)
_FAMILY_TERMS = (
    "CONYUGE",
    "CÓNYUGE",
    "COMPANERO PERMANENTE",
    "COMPAÑERO PERMANENTE",
    "ESPOSO",
    "ESPOSA",
    "HIJO",
    "HIJA",
    "PADRE",
    "MADRE",
    "HERMANO",
    "HERMANA",
    "PRIMO",
    "PRIMA",
    "TIO",
    "TIA",
    "SOBRINO",
    "SOBRINA",
    "CUÑADO",
    "CUÑADA",
    "PARIENTE",
    "FAMILIAR",
)
_LITIGATION_TERMS = (
    "LITIGIO",
    "PROCESO JUDICIAL",
    "DEMANDA",
    "ACCION POPULAR",
    "ACCIÓN POPULAR",
    "CONSEJO DE ESTADO",
    "TRIBUNAL",
    "JUZGADO",
    "LICENCIA AMBIENTAL",
)
_ENTITY_STOPWORDS = {
    "ACTIVO",
    "ACTIVA",
    "ACTIVIDADES",
    "AL",
    "ANTE",
    "CON",
    "CONSEJO",
    "DE",
    "DEL",
    "EL",
    "EN",
    "INFO",
    "JUDICIAL",
    "LA",
    "LAS",
    "LOS",
    "NO",
    "POR",
    "PROCESO",
    "QUE",
    "SECCION",
    "SECCIÓN",
    "SIN",
    "UNA",
    "Y",
}


def _append_unique(
    values: list[str],
    candidate: object,
    *,
    limit: int = _MAX_REFERENCE_VALUES,
) -> None:
    value = clean_text(candidate)
    if not value or value in values:
        return
    values.append(value)
    if len(values) > limit:
        del values[limit:]


def _normalize_entity_candidate(raw: object) -> str:
    text = clean_text(raw)
    if not text:
        return ""
    text = re.sub(
        r"^(?:REPRESENTANTE LEGAL(?: SUPLENTE)?|APODERADO JUDICIAL|APODERADO|ASESOR|"
        r"SOCIO(?: DE)?|MIEMBRO DE JUNTA(?: DIRECTIVA)?)\s+DE\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\b(?:NIT|CONTRATO|PROCESO|RADICADO|RESOLUCION|RESOLUCIÓN|LITIGIO|DEMANDA)\b.*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"^[,;:.\- ]+|[,;:.\- ]+$", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    normalized = clean_name(text)
    if not normalized:
        return ""
    parts = [part for part in normalized.split() if part]
    if len(parts) == 1 and parts[0] in _ENTITY_STOPWORDS:
        return ""
    if parts and parts[0] in _ENTITY_STOPWORDS:
        parts = parts[1:]
    if len(parts) < 2 and not any(
        marker in normalized for marker in ("SAS", "LTDA", "S.A", "IPS", "ESE")
    ):
        return ""
    normalized = " ".join(parts[:12]).strip()
    return normalized


def _extract_keyword_terms(text: str, terms: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    upper = clean_name(text)
    for term in sorted(terms, key=len, reverse=True):
        if term in upper:
            if any(term in existing for existing in found):
                continue
            _append_unique(found, term)
    return found


def extract_disclosure_references(raw_text: object) -> dict[str, Any]:
    text = clean_text(raw_text)
    if not text:
        return {
            "mentioned_document_ids": [],
            "mentioned_process_refs": [],
            "mentioned_company_names": [],
            "legal_role_terms": [],
            "family_terms": [],
            "litigation_terms": [],
            "company_document_mention_count": 0,
            "process_reference_count": 0,
            "company_name_mention_count": 0,
            "legal_role_term_count": 0,
            "family_term_count": 0,
            "litigation_term_count": 0,
        }

    document_ids: list[str] = []
    process_refs: list[str] = []
    company_names: list[str] = []

    for match in _DOCUMENT_PATTERN.finditer(text):
        start = match.start(1)
        end = match.end(1)
        prev_char = text[start - 1] if start > 0 else ""
        next_char = text[end] if end < len(text) else ""
        if prev_char.isalpha() or prev_char == "-" or next_char.isalpha():
            continue
        digits = strip_document(match.group(1))
        if len(digits) < 7 or len(digits) > 14:
            continue
        _append_unique(document_ids, digits)

    upper_text = clean_name(text)
    for match in _PROCESS_PATTERN.finditer(upper_text):
        candidate = clean_text(match.group(1))
        if not candidate:
            continue
        if candidate.isdigit():
            continue
        _append_unique(process_refs, candidate)
        stripped_candidate = clean_text(_PROCESS_PREFIX_PATTERN.sub("", candidate))
        if stripped_candidate and stripped_candidate != candidate:
            _append_unique(process_refs, stripped_candidate)

    for match in _ENTITY_PREFIX_PATTERN.finditer(upper_text):
        candidate = _normalize_entity_candidate(match.group(0))
        if not candidate:
            continue
        _append_unique(company_names, candidate)

    for match in _ENTITY_SUFFIX_PATTERN.finditer(upper_text):
        candidate = _normalize_entity_candidate(match.group(1))
        if not candidate:
            continue
        _append_unique(company_names, candidate)

    legal_role_terms = _extract_keyword_terms(text, _LEGAL_ROLE_TERMS)
    family_terms = _extract_keyword_terms(text, _FAMILY_TERMS)
    litigation_terms = _extract_keyword_terms(text, _LITIGATION_TERMS)

    return {
        "mentioned_document_ids": document_ids[:_MAX_REFERENCE_VALUES],
        "mentioned_process_refs": process_refs[:_MAX_REFERENCE_VALUES],
        "mentioned_company_names": company_names[:_MAX_REFERENCE_VALUES],
        "legal_role_terms": legal_role_terms,
        "family_terms": family_terms,
        "litigation_terms": litigation_terms,
        "company_document_mention_count": len(document_ids[:_MAX_REFERENCE_VALUES]),
        "process_reference_count": len(process_refs[:_MAX_REFERENCE_VALUES]),
        "company_name_mention_count": len(company_names[:_MAX_REFERENCE_VALUES]),
        "legal_role_term_count": len(legal_role_terms),
        "family_term_count": len(family_terms),
        "litigation_term_count": len(litigation_terms),
    }
