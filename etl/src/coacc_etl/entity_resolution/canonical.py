from __future__ import annotations

import re


def _digits(value: object) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def normalize_nit(value: object) -> str:
    return _digits(value)


def normalize_document_id(value: object) -> str:
    return _digits(value)


def canonical_person_key(document_type: str | None, document_number: object) -> str:
    doc_type = (document_type or "CC").strip().upper() or "CC"
    return f"CO:{doc_type}:{normalize_document_id(document_number)}"


def canonical_company_key(nit: object) -> str:
    return f"CO:NIT:{normalize_nit(nit)}"


def canonical_public_entity_key(identifier: object) -> str:
    normalized = _digits(identifier)
    if normalized:
        return f"CO:ENTITY:{normalized}"
    return f"CO:ENTITY:{str(identifier or '').strip()}"


def canonical_contract_key(source_system: str | None, contract_id: object) -> str:
    source = (source_system or "coacc").strip().lower() or "coacc"
    return f"{source}:{str(contract_id or '').strip()}"


def canonical_project_key(bpin: object) -> str:
    return _digits(bpin)
