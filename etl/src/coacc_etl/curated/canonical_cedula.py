from __future__ import annotations

import re


def canonicalize_cedula(value: object, *, document_type: object | None = None) -> str | None:
    """Canonicalize Colombian personal identifiers for person dimensions."""
    doc_type = str(document_type or "").upper()
    if "NIT" in doc_type:
        return None
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if not digits or len(digits) < 5 or len(digits) > 12:
        return None
    return digits.lstrip("0") or None
