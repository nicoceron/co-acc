from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.models.narrator import (
    AnomalyContext,
    CaseSubgraph,
    EvidenceCitation,
    SubgraphNode,
    build_prompt,
    build_templated_narrative,
    check,
)

if TYPE_CHECKING:
    from pathlib import Path


def _fixture_subgraph() -> CaseSubgraph:
    return CaseSubgraph(
        case_id="anomaly:Qy0x",
        contract_id="C-1",
        contract_reference="REF-1",
        process_url="https://secop.example/C-1",
        buyer_name="Entidad Uno",
        supplier_name="Proveedor Sancionado SAS",
        procurement_modality="Contratacion directa",
        contract_value=1_250_000_000.0,
        signing_date="2026-06-01",
        anomaly=AnomalyContext(
            score=0.91,
            score_confidence="low",
            top_features=["log_value_z_buyer", "timing_anomaly_score"],
            score_run_id="score-run",
            model_run_id="model-run",
            scored_at="2026-06-01T02:00:00+00:00",
        ),
        nodes=[
            SubgraphNode(node_id="contract:C-1", name="C-1", node_type="contract"),
            SubgraphNode(node_id="buyer", name="Entidad Uno", node_type="buyer"),
            SubgraphNode(
                node_id="supplier",
                name="Proveedor Sancionado SAS",
                node_type="supplier",
            ),
        ],
        evidence=[
            EvidenceCitation(
                dataset_id="jbjy-vk9h",
                row_key="C-1",
                label="SECOP II contract",
                url="https://secop.example/C-1",
            )
        ],
    )


def _write_narrator_inputs(root: Path) -> None:
    scores_out = root / "curated" / "anomaly_scores" / "run_id=score-run"
    scores_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": "score-run",
                "model_run_id": "model-run",
                "feature_run_id": "feature-run",
                "scored_at": "2026-06-01T02:00:00+00:00",
                "contract_id": "C-1",
                "entity_uid": "company:9001234568",
                "score": 0.91,
                "score_confidence": "low",
                "top_features": ["log_value_z_buyer", "timing_anomaly_score"],
                "prior_sanction_supplier": True,
                "process_url": "https://secop.example/C-1",
            }
        ]),
        scores_out / "part-00000.parquet",
    )
    current_out = root / "models" / "anomaly"
    current_out.mkdir(parents=True)
    (current_out / "current.json").write_text(
        json.dumps({
            "run_id": "model-run",
            "score_run_id": "score-run",
            "feature_run_id": "feature-run",
        }),
        encoding="utf-8",
    )
    contracts_out = root / "curated" / "table=fct_procurement_contract_awards"
    contracts_out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "contract_id": "C-1",
                "contract_reference": "REF-1",
                "process_url": "https://secop.example/C-1",
                "buyer_name": "Entidad Uno",
                "supplier_name": "Proveedor Sancionado SAS",
                "procurement_modality": "Contratacion directa",
                "contract_value": 1_250_000_000.0,
                "signing_date": "2026-06-01",
            }
        ]),
        contracts_out / "part-00000.parquet",
    )


def test_prompt_deterministic() -> None:
    subgraph = _fixture_subgraph()

    assert build_prompt(subgraph) == build_prompt(subgraph)


def test_verify_rejects_hallucinated_entity() -> None:
    subgraph = _fixture_subgraph()
    narrative = build_templated_narrative(subgraph).replace(
        "Proveedor Sancionado SAS",
        "Empresa Falsa SAS",
        1,
    )

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("unknown entity mention: Empresa Falsa SAS" in error for error in result.errors)


def test_verify_rejects_fake_dataset_id() -> None:
    subgraph = _fixture_subgraph()
    narrative = build_templated_narrative(subgraph).replace(
        "(jbjy-vk9h, C-1)",
        "(fake-dataset, C-1)",
        1,
    )

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("unknown dataset citation: fake-dataset" in error for error in result.errors)


def test_verify_word_count() -> None:
    subgraph = _fixture_subgraph()
    narrative = (
        "# Lead\nCorto.\n## Evidencia\nCorto.\n## Señales\nCorto.\n"
        "## Fuentes\n(jbjy-vk9h, C-1)"
    )

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("word count below minimum" in error for error in result.errors)


def test_template_narrative_verifies() -> None:
    subgraph = _fixture_subgraph()

    result = check(build_templated_narrative(subgraph), subgraph)

    assert result.valid, result.errors
    assert 250 <= result.word_count <= 400


def test_narrator_cli_generate_uses_template_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_narrator_inputs(tmp_path)

    result = CliRunner().invoke(cli, ["narrator", "generate", "C-1", "--provider", "template"])

    assert result.exit_code == 0, result.output
    assert "provider=template" in result.output
    assert (tmp_path / "curated" / "narratives" / "C-1.md").exists()
