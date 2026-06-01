from __future__ import annotations

import re

_NIT_DV_WEIGHTS = (71, 67, 59, 53, 47, 43, 41, 37, 29, 23, 19, 17, 13, 7, 3)


def document_digits(value: object) -> str | None:
    """Return digits-only document text, or None when no digits are present."""
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits or None


def calculate_nit_dv(body: str) -> str:
    """Calculate the Colombian NIT verification digit for a digits-only body."""
    digits = document_digits(body)
    if digits is None:
        raise ValueError("NIT body has no digits")
    if len(digits) > len(_NIT_DV_WEIGHTS):
        raise ValueError("NIT body is longer than the supported DIAN weight table")

    weights = _NIT_DV_WEIGHTS[-len(digits) :]
    total = sum(int(digit) * weight for digit, weight in zip(digits, weights, strict=True))
    remainder = total % 11
    return str(remainder if remainder < 2 else 11 - remainder)


def canonicalize_nit(value: object, *, document_type: object | None = None) -> str | None:
    """Canonicalize a Colombian NIT as digits-only body plus verification digit.

    A 9-digit value is treated as a NIT body and receives a computed DV. A
    10-digit value is accepted only when the provided DV matches the DIAN
    MOD-11 calculation. Document types that clearly describe personal IDs are
    intentionally rejected so cedulas do not leak into company dimensions.
    """
    doc_type = str(document_type or "").upper()
    if "CEDULA" in doc_type or "CÉDULA" in doc_type or doc_type in {"CC", "C.C."}:
        return None

    digits = document_digits(value)
    if digits is None:
        return None
    if len(digits) == 9:
        return digits + calculate_nit_dv(digits)
    if len(digits) == 10:
        body, provided_dv = digits[:-1], digits[-1]
        return digits if calculate_nit_dv(body) == provided_dv else None
    return None
