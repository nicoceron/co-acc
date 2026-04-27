from __future__ import annotations

from coacc_etl.pipeline_registry import get_pipeline_spec, list_pipeline_names, resolve_pipeline
from coacc_etl.pipelines.administrative_acts import AdministrativeActsPipeline


def test_registry_discovers_known_graph_and_lake_pipelines() -> None:
    names = list_pipeline_names()

    assert "administrative_acts" in names
    assert "colombia_shared" not in names
    assert "lake_template" not in names


def test_registry_uses_pipeline_name_and_source_id_separately() -> None:
    spec = get_pipeline_spec("administrative_acts")

    assert spec is not None
    assert spec.name == "administrative_acts"
    assert spec.source_id == "actos_administrativos"


def test_registry_resolves_pipeline_classes() -> None:
    pipeline_cls = resolve_pipeline("administrative_acts")

    assert pipeline_cls is AdministrativeActsPipeline
