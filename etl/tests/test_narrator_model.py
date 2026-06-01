from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.models.narrator import (
    AnomalyContext,
    CaseSubgraph,
    EvidenceCitation,
    SubgraphNode,
    SubgraphSignal,
    build_prompt,
    build_templated_narrative,
    check,
    generate_narrative,
    top_scored_case_ids,
)

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "subgraphs" / "recorded_narrator_cases.json"


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


def _load_recorded_cases() -> list[dict[str, Any]]:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def _subgraph_from_dict(payload: dict[str, Any]) -> CaseSubgraph:
    return CaseSubgraph(
        case_id=str(payload["case_id"]),
        contract_id=str(payload["contract_id"]),
        contract_reference=payload.get("contract_reference"),
        process_url=payload.get("process_url"),
        buyer_name=payload.get("buyer_name"),
        supplier_name=payload.get("supplier_name"),
        procurement_modality=payload.get("procurement_modality"),
        contract_value=payload.get("contract_value"),
        signing_date=payload.get("signing_date"),
        anomaly=AnomalyContext(**payload["anomaly"]),
        nodes=[SubgraphNode(**node) for node in payload.get("nodes", [])],
        signals=[SubgraphSignal(**signal) for signal in payload.get("signals", [])],
        evidence=[EvidenceCitation(**citation) for citation in payload.get("evidence", [])],
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
            },
            {
                "run_id": "score-run",
                "model_run_id": "model-run",
                "feature_run_id": "feature-run",
                "scored_at": "2026-06-01T02:00:00+00:00",
                "contract_id": "C-2",
                "entity_uid": "company:800000000",
                "score": 0.42,
                "score_confidence": "standard",
                "top_features": ["buyer_supplier_concentration"],
                "prior_sanction_supplier": False,
                "process_url": "https://secop.example/C-2",
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
            },
            {
                "contract_id": "C-2",
                "contract_reference": "REF-2",
                "process_url": "https://secop.example/C-2",
                "buyer_name": "Entidad Dos",
                "supplier_name": "Proveedor Dos SAS",
                "procurement_modality": "Licitacion publica",
                "contract_value": 220_000_000.0,
                "signing_date": "2026-06-02",
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


def test_verify_rejects_unresolved_citation() -> None:
    subgraph = _fixture_subgraph()
    narrative = build_templated_narrative(subgraph).replace(
        "(jbjy-vk9h, C-1)",
        "(jbjy-vk9h, C-404)",
        1,
    )

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("unresolved citation: (jbjy-vk9h, C-404)" in error for error in result.errors)


def test_verify_rejects_forbidden_ethics_term() -> None:
    subgraph = _fixture_subgraph()
    narrative = build_templated_narrative(subgraph).replace(
        "La lectura no afirma una irregularidad",
        "Proveedor Sancionado SAS es corrupto. La lectura no afirma una irregularidad",
        1,
    )

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("forbidden ethics term: corrupto" in error for error in result.errors)


def test_verify_rejects_missing_section() -> None:
    subgraph = _fixture_subgraph()
    narrative = build_templated_narrative(subgraph).replace("## Fuentes", "## Fuente", 1)

    result = check(narrative, subgraph)

    assert not result.valid
    assert any("missing section: ## Fuentes" in error for error in result.errors)


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


@pytest.mark.parametrize(
    "case_payload",
    _load_recorded_cases(),
    ids=lambda case_payload: str(case_payload["case_id"]),
)
def test_full_pipeline_against_recorded_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    case_payload: dict[str, Any],
) -> None:
    subgraph = _subgraph_from_dict(case_payload["subgraph"])
    recorded_response = "\n".join(case_payload["recorded_response"])
    calls: list[str] = []

    def fake_extract(case_id: str) -> CaseSubgraph:
        assert case_id == subgraph.case_id
        return subgraph

    def fake_call_llm(
        prompt: str,
        *,
        provider: str,
        model: str | None = None,
        api_key: str | None = None,
        max_tokens: int = 600,
    ) -> str:
        assert "Subgraph YAML:" in prompt
        assert subgraph.contract_id in prompt
        assert provider == "gemini"
        assert model is None
        assert api_key is None
        assert max_tokens == 600
        calls.append(prompt)
        return recorded_response

    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setenv("GEMINI_API_KEY", "fixture-key")
    monkeypatch.setattr("coacc_etl.models.narrator.generate.extract", fake_extract)
    monkeypatch.setattr("coacc_etl.models.narrator.generate.call_llm", fake_call_llm)

    result = generate_narrative(subgraph.case_id, provider="gemini")

    assert result.provider == "gemini"
    assert result.valid
    assert 250 <= result.word_count <= 400
    assert len(calls) == 1
    assert Path(result.narrative_path).read_text(encoding="utf-8") == recorded_response


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


def test_top_scored_case_ids_respects_limit_and_min_score(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_narrator_inputs(tmp_path)

    assert top_scored_case_ids(limit=5, min_score=0.5) == ["C-1"]


def test_narrator_cli_generate_batch_writes_top_cases(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_narrator_inputs(tmp_path)

    result = CliRunner().invoke(
        cli,
        ["narrator", "generate-batch", "--provider", "template", "--limit", "2"],
    )

    assert result.exit_code == 0, result.output
    assert "requested=2 generated=2 skipped=0" in result.output
    assert (tmp_path / "curated" / "narratives" / "C-1.md").exists()
    assert (tmp_path / "curated" / "narratives" / "C-2.md").exists()

    second = CliRunner().invoke(
        cli,
        ["narrator", "generate-batch", "--provider", "template", "--limit", "2"],
    )

    assert second.exit_code == 0, second.output
    assert "requested=2 generated=0 skipped=2" in second.output
