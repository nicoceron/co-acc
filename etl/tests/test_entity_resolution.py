from coacc_etl.entity_resolution.aliases import build_alias_row
from coacc_etl.entity_resolution.canonical import (
    canonical_company_key,
    canonical_contract_key,
    canonical_person_key,
    canonical_project_key,
    normalize_document_id,
    normalize_nit,
)
from coacc_etl.entity_resolution.confidence import classify_confidence, normalize_score
from coacc_etl.entity_resolution.config import get_person_settings


class TestNormalizeScore:
    def test_clamp_above(self) -> None:
        assert normalize_score(1.5) == 1.0

    def test_clamp_below(self) -> None:
        assert normalize_score(-0.3) == 0.0

    def test_passthrough(self) -> None:
        assert normalize_score(0.85) == 0.85

    def test_boundaries(self) -> None:
        assert normalize_score(0.0) == 0.0
        assert normalize_score(1.0) == 1.0


class TestClassifyConfidence:
    def test_high(self) -> None:
        assert classify_confidence(0.95) == "high"
        assert classify_confidence(0.9) == "high"

    def test_medium(self) -> None:
        assert classify_confidence(0.85) == "medium"
        assert classify_confidence(0.7) == "medium"

    def test_low(self) -> None:
        assert classify_confidence(0.69) == "low"
        assert classify_confidence(0.0) == "low"


class TestGetPersonSettings:
    def test_returns_dict(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            # splink not installed — skip
            return
        assert isinstance(settings, dict)

    def test_has_comparisons(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            return
        assert "comparisons" in settings

    def test_has_blocking_rules(self) -> None:
        try:
            settings = get_person_settings()
        except ImportError:
            return
        assert "blocking_rules_to_generate_predictions" in settings


class TestCanonicalKeys:
    def test_normalize_nit_strips_formatting(self) -> None:
        assert normalize_nit("900.123.456-7") == "9001234567"

    def test_normalize_document_id_strips_formatting(self) -> None:
        assert normalize_document_id("1.234.567") == "1234567"

    def test_canonical_person_key_defaults_to_cc(self) -> None:
        assert canonical_person_key(None, "1.234.567") == "CO:CC:1234567"

    def test_canonical_company_key_uses_nit(self) -> None:
        assert canonical_company_key("900.123.456-7") == "CO:NIT:9001234567"

    def test_canonical_contract_key_uses_source_namespace(self) -> None:
        assert canonical_contract_key("SECOP_II", "CNT-123") == "secop_ii:CNT-123"

    def test_canonical_project_key_normalizes_bpin(self) -> None:
        assert canonical_project_key("BPIN 2024-001") == "2024001"

    def test_build_alias_row_captures_match_metadata(self) -> None:
        row = build_alias_row(
            alias_id="nit:9001234567",
            kind="nit",
            value="900.123.456-7",
            normalized="9001234567",
            target_key="9001234567",
            source_id="secop_ii_contracts",
            confidence=1.0,
            match_type="EXACT_COMPANY_NIT",
        )

        assert row["alias_id"] == "nit:9001234567"
        assert row["kind"] == "nit"
        assert row["match_type"] == "EXACT_COMPANY_NIT"
