from coacc_etl.curated.canonical_cedula import canonicalize_cedula
from coacc_etl.curated.canonical_nit import calculate_nit_dv, canonicalize_nit


def test_calculate_nit_dv_uses_dian_mod11_weights() -> None:
    assert calculate_nit_dv("900123456") == "8"
    assert calculate_nit_dv("900765432") == "6"
    assert calculate_nit_dv("901000111") == "8"


def test_canonicalize_nit_formats_and_validates_verification_digit() -> None:
    assert canonicalize_nit("900.123.456", document_type="NIT") == "9001234568"
    assert canonicalize_nit("900.123.456-8", document_type="NIT") == "9001234568"
    assert canonicalize_nit("900.123.456-7", document_type="NIT") is None
    assert canonicalize_nit("1234567890", document_type="CC") is None


def test_canonicalize_cedula_rejects_company_documents() -> None:
    assert canonicalize_cedula("00123.456.789", document_type="CC") == "123456789"
    assert canonicalize_cedula("9001234568", document_type="NIT") is None
