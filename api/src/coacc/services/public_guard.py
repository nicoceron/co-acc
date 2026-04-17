from __future__ import annotations

import re

from fastapi import HTTPException, status

from coacc.config import settings

PERSON_LABELS = {"Person", "Partner"}
INTERNAL_LABELS = {"User", "Investigation", "Annotation", "Tag"}
SENSITIVE_PROP_KEYS = {
    "cedula",
    "numero_documento",
    "doc_partial",
    "doc_raw",
    "masked_doc",
}
PUBLIC_SIGNAL_DENYLIST = {
    "name",
    "full_name",
    "supplier_name",
    "vendor_name",
    "person_name",
    "beneficiary_name",
    "owner_name",
    "document_id",
    "supplier_document_id",
    "person_document_id",
    "nit",
}

IDENTIFIER_PATTERN = re.compile(r"^\d{5,14}$")


def _clean_identifier(value: str) -> str:
    return re.sub(r"[.\-/]", "", value or "")


def is_public_mode() -> bool:
    return settings.public_mode


def should_hide_person_entities() -> bool:
    return settings.public_mode and not settings.public_allow_person


def has_person_labels(labels: list[str]) -> bool:
    return any(label in PERSON_LABELS for label in labels)


def infer_exposure_tier(labels: list[str]) -> str:
    label_set = set(labels)
    if label_set & INTERNAL_LABELS:
        return "internal_only"
    if label_set & PERSON_LABELS:
        return "restricted"
    return "public_safe"


def sanitize_public_properties(
    props: dict[str, str | float | int | bool | None],
) -> dict[str, str | float | int | bool | None]:
    if not is_public_mode():
        return props
    return {
        key: value
        for key, value in props.items()
        if key not in SENSITIVE_PROP_KEYS
        and "cedula" not in key.lower()
    }


def enforce_entity_lookup_policy(raw_identifier: str) -> None:
    if not is_public_mode():
        return
    enforce_entity_lookup_enabled()
    clean = _clean_identifier(raw_identifier)
    if not IDENTIFIER_PATTERN.match(clean):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid identifier format",
        )


def enforce_entity_lookup_enabled() -> None:
    if settings.public_mode and not settings.public_allow_entity_lookup:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Entity lookup endpoint disabled in public mode",
        )


def enforce_person_access_policy(labels: list[str]) -> None:
    if not is_public_mode():
        return
    if has_person_labels(labels) and should_hide_person_entities():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Person-level entities disabled in public mode",
        )


def ensure_investigations_enabled() -> None:
    if settings.public_mode and not settings.public_allow_investigations:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Investigation endpoints disabled in public mode",
        )


def public_signal_payload_leaks_identity(data: dict[str, object]) -> bool:
    for key, value in data.items():
        lowered = key.lower()
        if lowered in PUBLIC_SIGNAL_DENYLIST or "document" in lowered or "cedula" in lowered:
            return True
        if lowered.endswith("_name") and value:
            return True
    return False
