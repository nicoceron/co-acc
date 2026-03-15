import re


def strip_document(doc: str | None) -> str:
    if not doc:
        return ""
    # Only keep digits
    return re.sub(r"[^0-9]", "", doc)


def format_nit(nit: str | None) -> str:
    digits = strip_document(nit)
    if not digits:
        return ""
    if len(digits) < 2:
        return digits
    # Common Colombian NIT format: 123.456.789-0
    # But many sources just give digits.
    # We return the digits or a simple format if it matches standard length.
    if len(digits) == 9:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:]}"
    if len(digits) == 10:
        return f"{digits[:9]}-{digits[9:]}"
    return digits


def classify_document(doc: str | None) -> str:
    """Classify a Colombian document string for identity handling.

    Returns one of:
    - nit: Likely a NIT (9-10 digits)
    - cedula: Likely a Cédula (5-10 digits)
    - invalid: anything else
    """
    digits = strip_document(doc)
    if not digits:
        return "invalid"
    
    length = len(digits)
    if 5 <= length <= 10:
        # NIT and Cédula overlap in length. 
        # In this project we often treat them generically as document_id.
        return "valid_id"
    
    return "invalid"
