from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc.main import app

if TYPE_CHECKING:
    from pathlib import Path

    from httpx import AsyncClient


def _write_agent_lake(root: Path) -> None:
    awards_out = root / "curated" / "table=fct_procurement_contract_awards"
    scores_out = root / "curated" / "anomaly_scores" / "run_id=agent-run"
    awards_out.mkdir(parents=True)
    scores_out.mkdir(parents=True)
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
        awards_out / "part-00000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([
            {
                "run_id": "agent-run",
                "model_run_id": "model-run",
                "feature_run_id": "feature-run",
                "scored_at": "2026-06-01T02:00:00+00:00",
                "contract_id": "C-1",
                "entity_uid": "company:9001234568",
                "score": 0.92,
                "score_confidence": "low",
                "top_features": ["single_bidder", "prior_sanction_supplier"],
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
            "score_run_id": "agent-run",
            "feature_run_id": "feature-run",
            "trained_at": "2026-06-01T01:55:00+00:00",
        }),
        encoding="utf-8",
    )


@pytest.mark.anyio
async def test_agent_query_returns_lake_answer_with_citations(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_agent_lake(tmp_path)

    response = await client.post(
        "/api/v1/agent/query",
        json={"question": "Que muestra el contrato C-1?"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "C-1" in payload["answer"]
    assert "Proveedor Sancionado SAS" in payload["answer"]
    assert payload["citations"][0]["dataset_id"] == "jbjy-vk9h"
    assert payload["citations"][0]["row_key"] == "C-1"
    assert payload["subgraphs"][0]["case_id"].startswith("anomaly:")
    assert {node["type"] for node in payload["subgraphs"][0]["nodes"]} >= {
        "contract",
        "buyer",
        "supplier",
    }


@pytest.mark.anyio
async def test_agent_query_legacy_route_alias(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None
    _write_agent_lake(tmp_path)

    response = await client.post("/agent/query", json={"question": "C-1"})

    assert response.status_code == 200
    assert response.json()["citations"][0]["row_key"] == "C-1"


@pytest.mark.anyio
async def test_agent_query_without_lake_data_returns_no_data_answer(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    app.state.neo4j_driver = None

    response = await client.post(
        "/api/v1/agent/query",
        json={"question": "Hay algun caso?"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["citations"] == []
    assert payload["subgraphs"] == []
    assert "No encontre" in payload["answer"]
