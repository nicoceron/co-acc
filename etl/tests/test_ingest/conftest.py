"""Shared fixtures for ingest contract and determinism tests.

These tests never hit live Socrata. ``FakeSocrataClient`` satisfies the same
``fetch(dataset_id, where, order)`` interface as :class:`SocrataClient`.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from coacc_etl.catalog import DatasetSpec


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Point the lake root at a tmp dir for every test in this package."""
    root = tmp_path / "lake"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(root))
    # paths.py caches LAKE_ROOT at import time from a different env read path;
    # clear anything cached at the module level if it snuck in.
    monkeypatch.setattr(
        "coacc_etl.lakehouse.paths.LAKE_ROOT",
        root,
        raising=False,
    )
    yield root


@pytest.fixture
def hallazgos_spec() -> DatasetSpec:
    """Minimal filled-in spec modeled on Hallazgos Fiscales (8qxx-ubmq).

    Uses placeholder ``hallazgos-tst1`` to avoid colliding with a real
    Socrata 4x4 id if any other test probes the registry.
    """
    return DatasetSpec(
        id="hllz-tst1",
        name="Hallazgos Fiscales (test)",
        sector="sanctions",
        tier="core",
        join_keys={"nit": ["nit"]},
        watermark_column="fecha_recibo_traslado",
        partition_column="fecha_recibo_traslado",
        columns_map={
            "nit": "nit",
            "entity_name": "nombre_sujeto",
            "process": "proceso_o_asunto_evaluado",
            "received_date": "fecha_recibo_traslado",
            "radicado": "radicado",
            "amount": "cuant_a",
        },
        required_coverage={
            "nit": 0.95,
            "nombre_sujeto": 0.95,
            "fecha_recibo_traslado": 0.80,
        },
        freq="monthly",
        url="https://www.datos.gov.co/d/hllz-tst1",
        notes="",
    )


@pytest.fixture
def hallazgos_page() -> list[dict[str, object]]:
    return [
        {
            "nit": "891580016",
            "nombre_sujeto": "GOBERNACION DEL CAUCA",
            "proceso_o_asunto_evaluado": "AUDITORIA FINANCIERA Y DE GESTIÓN",
            "vigencia_auditada": "2023",
            "fecha_comunicaci_n_informe": "2024-10-30T00:00:00.000",
            "hechos": "PRESUNTAS IRREGULARIDADES",
            "cuant_a": "6366709",
            "fecha_recibo_traslado": "2024-12-26T00:00:00.000",
            "radicado": "202501200252533",
        },
        {
            "nit": "899999061",
            "nombre_sujeto": "MINISTERIO DE HACIENDA",
            "proceso_o_asunto_evaluado": "AUDITORIA GESTIÓN",
            "vigencia_auditada": "2023",
            "fecha_comunicaci_n_informe": "2024-11-15T00:00:00.000",
            "hechos": "DESVIACIÓN PRESUPUESTAL",
            "cuant_a": "12000000",
            "fecha_recibo_traslado": "2025-01-03T00:00:00.000",
            "radicado": "202501200252534",
        },
        {
            "nit": "800197268",
            "nombre_sujeto": "ALCALDIA DE MEDELLIN",
            "proceso_o_asunto_evaluado": "AUDITORIA CONTRATACIÓN",
            "vigencia_auditada": "2022",
            "fecha_comunicaci_n_informe": "2024-12-01T00:00:00.000",
            "hechos": "IRREGULARIDADES CONTRACTUALES",
            "cuant_a": "8500000",
            "fecha_recibo_traslado": "2025-01-15T00:00:00.000",
            "radicado": "202501200252535",
        },
    ]


class FakeSocrataClient:
    """Drop-in replacement for :class:`SocrataClient` that serves a canned page."""

    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self._pages = pages
        self.requests: list[dict[str, object]] = []

    def fetch(
        self,
        dataset_id: str,
        *,
        where: str | None,
        order: str,
    ) -> Iterator[list[dict[str, object]]]:
        self.requests.append({"dataset_id": dataset_id, "where": where, "order": order})
        yield from self._pages

    def close(self) -> None:
        return None


@pytest.fixture
def fake_client_factory():
    def _factory(pages: list[list[dict[str, object]]]) -> FakeSocrataClient:
        return FakeSocrataClient(pages)

    return _factory


@pytest.fixture(autouse=True)
def _no_socrata_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip Socrata auth env to prove we never touch the real HTTP client."""
    for key in ("SOCRATA_APP_TOKEN", "SOCRATA_KEY_ID", "SOCRATA_KEY_SECRET"):
        monkeypatch.delenv(key, raising=False)
    # Guard: if any test ever accidentally uses from_env, it should be obvious.
    os.environ["COACC_TEST_RUNNING"] = "1"
