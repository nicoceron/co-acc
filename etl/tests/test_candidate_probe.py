from __future__ import annotations

from typing import TYPE_CHECKING

from coacc_etl.candidate_probe import (
    ColumnProbe,
    DatasetProbeResult,
    classify_probe_result,
    extract_candidate_dataset_ids,
    infer_key_families,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_infer_key_families_uses_column_names_not_dataset_title() -> None:
    families = infer_key_families(
        [
            "dato_raro",
            "numero_de_identificacion",
            "id_contrato",
            "nombre_representante_legal",
            "codigo_bpin",
        ]
    )

    assert families["company_id"] == ["numero_de_identificacion"]
    assert families["contract_id"] == ["id_contrato"]
    assert families["person_name"] == ["nombre_representante_legal"]
    assert families["bpin"] == ["codigo_bpin"]


def test_infer_key_families_avoids_substring_false_positives() -> None:
    columns = [
        "ESTU_VALORMATRICULAUNIVERSIDAD",
        "fecha_resoluci_n_seccional",
        "seccional",
        "numero_de_identificacion",
        "id_contrato",
    ]
    families = infer_key_families(columns)
    matched_columns = {column for values in families.values() for column in values}

    assert "ESTU_VALORMATRICULAUNIVERSIDAD" not in matched_columns
    assert "fecha_resoluci_n_seccional" not in matched_columns
    assert "seccional" not in matched_columns
    assert families["company_id"] == ["numero_de_identificacion"]
    assert families["contract_id"] == ["id_contrato"]
    assert families["person_id"] == ["numero_de_identificacion"]


def test_extract_candidate_dataset_ids_excludes_implemented_and_false_positives(
    tmp_path: Path,
) -> None:
    docs_path = tmp_path / "research.md"
    docs_path.write_text("keep 3xwx-53wt ignore open-data and 2jzx-383z", encoding="utf-8")

    registry_path = tmp_path / "registry.csv"
    registry_path.write_text(
        "source_id,primary_url\nsigep,https://www.datos.gov.co/d/2jzx-383z\n",
        encoding="utf-8",
    )

    ids = extract_candidate_dataset_ids(doc_paths=(docs_path,), registry_path=registry_path)
    assert ids == ["3xwx-53wt"]


def test_classify_probe_result_requires_real_overlap() -> None:
    result = DatasetProbeResult(
        dataset_id="demo-1234",
        title="Demo",
        description="",
        row_count=100,
        updated_at=None,
        columns=["id_contrato"],
        key_families={"contract_id": ["id_contrato"]},
        probes={
            "contract_id": [
                ColumnProbe(
                    actual_name="id_contrato",
                    normalized_name="id_contrato",
                    non_empty_sample_values=15,
                    sample_overlap=0,
                    live_probe_hits=12,
                )
            ]
        },
        recommendation="drop",
        reason="",
        errors=[],
    )

    recommendation, reason = classify_probe_result(result)
    assert recommendation == "implement"
    assert "live SoQL probes" in reason
