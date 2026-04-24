from __future__ import annotations

import importlib
import inspect
import pkgutil
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from coacc_etl import pipelines as pipeline_package
from coacc_etl.base import Pipeline

if TYPE_CHECKING:
    from types import ModuleType

_EXCLUDED_MODULES = {
    "colombia_procurement",
    "colombia_shared",
    "disclosure_mining",
    "lake_template",
    "project_graph",
}


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    source_id: str
    module_name: str
    class_name: str
    pipeline_cls: type[Pipeline]

    @property
    def status_key(self) -> str:
        return self.source_id or self.name


def _iter_pipeline_modules() -> list[str]:
    return sorted(
        module.name
        for module in pkgutil.iter_modules(pipeline_package.__path__)
        if not module.name.startswith("_") and module.name not in _EXCLUDED_MODULES
    )


def _pipeline_candidates(module: ModuleType) -> list[type[Pipeline]]:
    seen: set[type[Pipeline]] = set()
    candidates: list[type[Pipeline]] = []
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if obj is Pipeline or not issubclass(obj, Pipeline):
            continue
        if obj.__module__ != module.__name__ or obj in seen:
            continue
        seen.add(obj)
        candidates.append(obj)
    return candidates


def _resolve_module_pipeline(module: ModuleType) -> type[Pipeline]:
    explicit = getattr(module, "PIPELINE_CLASS", None)
    if explicit is not None:
        if not inspect.isclass(explicit) or not issubclass(explicit, Pipeline):
            msg = f"{module.__name__}.PIPELINE_CLASS is not a Pipeline subclass"
            raise TypeError(msg)
        return explicit

    candidates = _pipeline_candidates(module)
    if len(candidates) == 1:
        return candidates[0]

    msg = (
        f"Unable to resolve pipeline class for {module.__name__}: "
        f"found {len(candidates)} candidates"
    )
    raise LookupError(msg)


@lru_cache(maxsize=1)
def load_pipeline_specs() -> dict[str, PipelineSpec]:
    specs: dict[str, PipelineSpec] = {}
    for module_name in _iter_pipeline_modules():
        module = importlib.import_module(f"{pipeline_package.__name__}.{module_name}")
        pipeline_cls = _resolve_module_pipeline(module)
        pipeline_name = getattr(pipeline_cls, "name", "").strip()
        source_id = getattr(pipeline_cls, "source_id", "").strip()
        if not pipeline_name:
            msg = f"{pipeline_cls.__module__}.{pipeline_cls.__name__} is missing name"
            raise ValueError(msg)
        if pipeline_name in specs:
            existing = specs[pipeline_name]
            msg = (
                f"Duplicate pipeline name '{pipeline_name}' discovered in "
                f"{existing.module_name} and {module.__name__}"
            )
            raise ValueError(msg)
        specs[pipeline_name] = PipelineSpec(
            name=pipeline_name,
            source_id=source_id or pipeline_name,
            module_name=module.__name__,
            class_name=pipeline_cls.__name__,
            pipeline_cls=pipeline_cls,
        )
    return specs


def clear_pipeline_specs_cache() -> None:
    load_pipeline_specs.cache_clear()


def list_pipeline_names() -> list[str]:
    return sorted(load_pipeline_specs())


def resolve_pipeline(name: str) -> type[Pipeline] | None:
    spec = load_pipeline_specs().get(name)
    return spec.pipeline_cls if spec else None


def get_pipeline_spec(name: str) -> PipelineSpec | None:
    return load_pipeline_specs().get(name)
