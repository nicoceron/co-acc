from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING, Any

from coacc_etl import source_qualification as triage

if TYPE_CHECKING:
    from pathlib import Path


def test_manual_aliases_detect_latent_government_join_keys() -> None:
    columns = [
        "identificadorcontrato",
        "nit_entidad_compradora",
        "nit_del_proveedor",
        "id_del_proceso_de_compra",
        "c_digo_entidad",
        "codigobpin",
        "nit_participante",
        "n_mero_de_identificaci_n",
        "identificacion_funcionario",
        "idmunicipioentidad",
        "cod_institucion",
    ]

    found_keys, found_classes = triage._find_join_keys(
        [triage._normalize_col(column) for column in columns]
    )

    assert set(found_keys) >= {"bpin", "contract", "entity", "nit", "process"}
    assert "contract" in found_classes
    assert "identificadorcontrato" in found_classes["contract"]
    assert "bpin" in found_classes
    assert "codigobpin" in found_classes["bpin"]
    assert "n_mero_de_identificaci_n" in found_classes["nit"]
    assert "idmunicipioentidad" in found_classes["divipola"]
    assert "cod_institucion" in found_classes["entity"]


def test_all_known_loader_merges_audit_registry_and_signal_refs(tmp_path: Path) -> None:
    appendix = tmp_path / "appendix.csv"
    with appendix.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "dataset_id",
                "name",
                "sector_or_category",
                "scope",
                "recommendation",
                "relevance",
                "audit_status",
                "url",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "dataset_id": "abcd-1234",
                "name": "Appendix name",
                "sector_or_category": "contracts",
                "scope": "colombia_open_data_audit",
                "recommendation": "candidate",
                "relevance": "high",
                "audit_status": "valid",
                "url": "https://www.datos.gov.co/d/abcd-1234",
            }
        )

    audit = tmp_path / "audit.json"
    audit.write_text(
        json.dumps(
            {
                "summary": {},
                "valid_datasets_flat": [
                    {
                        "id": "abcd-1234",
                        "name": "Audit name",
                        "sector": "National Planning",
                        "url": "https://www.datos.gov.co/d/abcd-1234",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    registry = tmp_path / "source_registry.csv"
    with registry.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "source_id",
                "name",
                "category",
                "primary_url",
                "last_seen_url",
                "notes",
                "signal_promotion_state",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source_id": "secop_test",
                "name": "Registry name",
                "category": "contracts",
                "primary_url": "https://www.datos.gov.co/d/abcd-1234",
                "last_seen_url": "",
                "notes": "",
                "signal_promotion_state": "promoted",
            }
        )

    signal_deps = tmp_path / "signal_source_deps.yml"
    signal_deps.write_text(
        "signals:\n  test_signal:\n    sources: [secop_test]\n    required: [secop_test]\n",
        encoding="utf-8",
    )
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()

    rows = triage.load_known_dataset_entries(
        appendix_path=appendix,
        audit_json_path=audit,
        source_registry_path=registry,
        signal_deps_path=signal_deps,
        pipelines_dir=pipelines_dir,
        include_current=False,
        include_appendix=True,
        include_audit_json=True,
        include_source_registry=True,
        include_pipeline_env=True,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["dataset_id"] == "abcd-1234"
    assert row["source_refs"] == "secop_test"
    assert row["signal_refs"] == "secop_test"
    assert set(row["origin_refs"].split("|")) == {
        "appendix",
        "audit_json",
        "signal_deps",
        "source_registry",
    }


def test_gemini_call_extracts_candidate_text(monkeypatch: Any) -> None:
    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '[{"column":"codigobpin",'
                                        '"join_class":"bpin","confidence":0.9}]'
                                    )
                                }
                            ]
                        }
                    }
                ]
            }

    calls: list[dict[str, Any]] = []

    def fake_post(*args: Any, **kwargs: Any) -> FakeResponse:
        calls.append({"args": args, "kwargs": kwargs})
        return FakeResponse()

    monkeypatch.setattr(triage.httpx, "post", fake_post)

    text = triage._call_gemini("prompt", model="gemini-test", api_key="secret")

    assert "codigobpin" in text
    assert calls
    assert calls[0]["kwargs"]["headers"]["x-goog-api-key"] == "secret"


def test_gemini_call_retries_transient_errors(monkeypatch: Any) -> None:
    class FakeRequest:
        pass

    class FakeResponse:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code
            self.request = FakeRequest()

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {"candidates": [{"content": {"parts": [{"text": "[]"}]}}]}

    responses = [FakeResponse(503), FakeResponse(200)]

    def fake_post(*args: Any, **kwargs: Any) -> FakeResponse:
        return responses.pop(0)

    monkeypatch.setattr(triage.httpx, "post", fake_post)
    monkeypatch.setattr(triage.time, "sleep", lambda _: None)

    assert triage._call_gemini("prompt", model="gemini-test", api_key="secret") == "[]"
    assert responses == []


def test_local_env_loader_reads_values_and_respects_override(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\nSOCRATA_APP_TOKEN=from_file\nGEMINI_API_KEY='gemini_file'\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("SOCRATA_APP_TOKEN", "from_process")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    triage._load_env_file(env_file, override=False)

    assert triage.os.environ["SOCRATA_APP_TOKEN"] == "from_process"
    assert triage.os.environ["GEMINI_API_KEY"] == "gemini_file"

    triage._load_env_file(env_file, override=True)

    assert triage.os.environ["SOCRATA_APP_TOKEN"] == "from_file"


def test_catalog_round_trip_preserves_provenance(tmp_path: Path) -> None:
    out = tmp_path / "catalog.csv"
    rows = [
        triage.TriageCatalogRow(
            dataset_id="abcd-1234",
            name="Dataset",
            sector="contracts",
            scope="source_registry",
            recommendation="keep",
            relevance="promoted",
            audit_status="valid",
            rows=10,
            n_columns=2,
            n_meaningful_columns=2,
            columns_all="nit_entidad|id_contrato",
            join_keys_found=2,
            join_key_classes="contract|nit",
            join_key_columns="contract:id_contrato|nit:nit_entidad",
            source_refs="secop_test",
            origin_refs="source_registry",
            signal_refs="secop_test",
            probe_notes=["metadata_only"],
            url="https://www.datos.gov.co/d/abcd-1234",
        )
    ]

    triage.write_catalog(rows, out)
    reloaded = triage.read_catalog(out)

    assert len(reloaded) == 1
    assert reloaded[0].dataset_id == "abcd-1234"
    assert reloaded[0].source_refs == "secop_test"
    assert reloaded[0].origin_refs == "source_registry"
    assert reloaded[0].signal_refs == "secop_test"
    assert reloaded[0].probe_notes == ["metadata_only"]


def test_llm_findings_require_existing_stable_high_confidence_column() -> None:
    row = triage.TriageCatalogRow(
        dataset_id="abcd-1234",
        name="Dataset",
        sector="contracts",
        scope="source_registry",
        recommendation="keep",
        relevance="promoted",
        audit_status="valid",
        rows=-1,
        columns_all="nombre_entidad|c_digo_serie|tipo_de_proceso|identificadorcontrato",
        url="https://www.datos.gov.co/d/abcd-1234",
    )
    llm_results = {
        "abcd-1234": {
            "join_key_findings": [
                {
                    "column": "nombre_entidad",
                    "join_class": "entity",
                    "confidence": 0.99,
                },
                {
                    "column": "codigo_bpin",
                    "join_class": "bpin",
                    "confidence": 0.99,
                },
                {
                    "column": "c_digo_serie",
                    "join_class": "entity",
                    "confidence": 0.99,
                },
                {
                    "column": "tipo_de_proceso",
                    "join_class": "process",
                    "confidence": 0.99,
                },
                {
                    "column": "identificadorcontrato",
                    "join_class": "contract",
                    "confidence": 0.74,
                },
                {
                    "column": "identificadorcontrato",
                    "join_class": "contract",
                    "confidence": 0.9,
                },
            ],
            "relevance": {"relevance_tier": "ingest"},
        }
    }

    with triage.httpx.Client() as client:
        rows = triage._apply_llm_findings(
            [row],
            llm_results,
            client=client,
            domain="www.datos.gov.co",
            probe_sample=0,
            min_confidence=0.75,
        )

    assert rows[0].join_key_classes == "contract"
    assert rows[0].join_key_columns == "contract:identificadorcontrato"
