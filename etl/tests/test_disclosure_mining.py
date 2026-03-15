from __future__ import annotations

from coacc_etl.pipelines.disclosure_mining import extract_disclosure_references


def test_extract_disclosure_references_finds_documents_entities_and_keywords() -> None:
    payload = extract_disclosure_references(
        "Representante legal suplente de CONSORCIO ANDINO S.A.S. "
        "NIT 900123456 en contrato ANI-001-2024 y proceso judicial activo con un familiar."
    )

    assert payload["mentioned_document_ids"] == ["900123456"]
    assert "ANI-001-2024" in payload["mentioned_process_refs"]
    assert "CONSORCIO ANDINO S.A.S" in payload["mentioned_company_names"]
    assert "REPRESENTANTE LEGAL SUPLENTE" in payload["legal_role_terms"]
    assert "FAMILIAR" in payload["family_terms"]
    assert "PROCESO JUDICIAL" in payload["litigation_terms"]
