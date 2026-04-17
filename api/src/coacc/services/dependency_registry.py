from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from coacc.services import lakehouse_query

_DEPS_PATH = Path(__file__).resolve().parents[4] / "config" / "signal_source_deps.yml"
_SEVERITY_ORDER = ["low", "medium", "high", "critical"]


@dataclass(frozen=True)
class SignalDependency:
    signal_id: str
    sources: tuple[str, ...]
    required: tuple[str, ...]
    optional: tuple[str, ...] = field(default_factory=tuple)
    severity_on_partial: str | None = None


def _as_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item).strip() for item in (value or []) if str(item).strip())


@lru_cache(maxsize=1)
def load_deps() -> dict[str, SignalDependency]:
    payload = yaml.safe_load(_DEPS_PATH.read_text(encoding="utf-8")) or {}
    signals = payload.get("signals") or {}
    deps: dict[str, SignalDependency] = {}
    for signal_id, cfg in signals.items():
        raw = cfg or {}
        sources = _as_tuple(raw.get("sources"))
        required = _as_tuple(raw.get("required"))
        optional = _as_tuple(raw.get("optional"))
        deps[str(signal_id)] = SignalDependency(
            signal_id=str(signal_id),
            sources=sources or required + optional,
            required=required,
            optional=optional,
            severity_on_partial=raw.get("severity_on_partial"),
        )
    return deps


def clear_dependency_registry_cache() -> None:
    load_deps.cache_clear()


def signals_to_rerun(advanced_sources: set[str]) -> list[str]:
    if not advanced_sources:
        return []
    deps = load_deps()
    return [
        sig_id
        for sig_id, cfg in deps.items()
        if advanced_sources & set(cfg.sources)
    ]


def can_materialize(sig_id: str) -> tuple[bool, list[str]]:
    cfg = load_deps()[sig_id]
    missing_required = [
        source
        for source in cfg.required
        if not lakehouse_query.watermark_exists(source)
    ]
    return not missing_required, missing_required


def severity_for_sources(sig_id: str, base_severity: str) -> str:
    cfg = load_deps()[sig_id]
    if not cfg.optional or not cfg.severity_on_partial:
        return base_severity
    missing_optional = [
        source
        for source in cfg.optional
        if not lakehouse_query.watermark_exists(source)
    ]
    if not missing_optional:
        return base_severity
    if cfg.severity_on_partial not in _SEVERITY_ORDER:
        return base_severity
    base_index = _SEVERITY_ORDER.index(base_severity)
    partial_index = _SEVERITY_ORDER.index(cfg.severity_on_partial)
    return _SEVERITY_ORDER[min(base_index, partial_index)]


def validate() -> list[str]:
    errors: list[str] = []
    for sig_id, cfg in load_deps().items():
        missing = set(cfg.required) - set(cfg.sources)
        if missing:
            errors.append(f"{sig_id}: required not listed in sources: {sorted(missing)}")
        missing_optional = set(cfg.optional) - set(cfg.sources)
        if missing_optional:
            errors.append(f"{sig_id}: optional not listed in sources: {sorted(missing_optional)}")
    return errors
