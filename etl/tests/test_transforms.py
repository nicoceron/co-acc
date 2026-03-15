from coacc_etl.transforms import (
    classify_document,
    deduplicate_rows,
    format_nit,
    normalize_name,
    strip_document,
)


class TestNameNormalization:
    def test_basic(self) -> None:
        assert normalize_name("Juan Pérez") == "JUAN PEREZ"

    def test_accents_removed(self) -> None:
        assert normalize_name("Andrés Muñoz") == "ANDRES MUNOZ"

    def test_extra_whitespace(self) -> None:
        assert normalize_name("  Maria   Helena  ") == "MARIA HELENA"

    def test_empty(self) -> None:
        assert normalize_name("") == ""
        assert normalize_name(None) == ""


class TestDocumentFormatting:
    def test_strip_document(self) -> None:
        assert strip_document("123.456.789-0") == "1234567890"
        assert strip_document("12.345.678") == "12345678"
        assert strip_document(None) == ""

    def test_format_nit(self) -> None:
        assert format_nit("9001234567") == "900123456-7"
        assert format_nit("123456789") == "123.456.789"

    def test_classify_document(self) -> None:
        assert classify_document("1234567890") == "valid_id"
        assert classify_document("123.456.789-0") == "valid_id"
        assert classify_document("123") == "invalid"


class TestDeduplication:
    def test_basic(self) -> None:
        rows = [
            {"id": "111", "name": "A"},
            {"id": "222", "name": "B"},
            {"id": "111", "name": "A-dup"},
        ]
        result = deduplicate_rows(rows, ["id"])
        assert len(result) == 2
        assert result[0]["name"] == "A"

    def test_composite_key(self) -> None:
        rows = [
            {"id": "111", "year": 2020},
            {"id": "111", "year": 2024},
            {"id": "111", "year": 2020},
        ]
        result = deduplicate_rows(rows, ["id", "year"])
        assert len(result) == 2

    def test_empty(self) -> None:
        assert deduplicate_rows([], ["id"]) == []
