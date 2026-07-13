from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml  # type: ignore[import-untyped]

from coacc_etl.runtime_paths import config_file

if TYPE_CHECKING:
    from coacc_etl.signals.contracts import SignalSeverity


@dataclass(frozen=True)
class MaterializableSignalDefinition:
    id: str
    version: int
    title: str
    description: str
    category: str
    severity: SignalSeverity
    public_safe: bool
    scope_type: str
    sources: tuple[str, ...]


def _as_bool(value: object) -> bool:
    return bool(value) if value is not None else False


def _as_str_list(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item).strip() for item in value if str(item).strip())


def _severity(value: object) -> SignalSeverity:
    severity = str(value or "").strip().lower()
    if severity in {"low", "medium", "high", "critical"}:
        return cast("SignalSeverity", severity)
    raise ValueError(f"unsupported signal severity: {value!r}")


def _registry_path() -> Path:
    return config_file("signal_registry.yml")


@lru_cache(maxsize=8)
def _registry_payload(path_key: str) -> dict[str, Any]:
    payload = yaml.safe_load(Path(path_key).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("signal registry root must be a mapping")
    return payload


def _signal_rows_for(path_key: str) -> list[dict[str, Any]]:
    payload = _registry_payload(path_key)
    rows = payload.get("signals")
    if not isinstance(rows, list):
        raise ValueError("signal registry does not contain a signals list")
    return [row for row in rows if isinstance(row, dict)]


def _signal_rows() -> list[dict[str, Any]]:
    return _signal_rows_for(str(_registry_path()))


@lru_cache(maxsize=8)
def _aliases_for(path_key: str) -> dict[str, str]:
    payload = _registry_payload(path_key)
    aliases = payload.get("aliases")
    if not isinstance(aliases, dict):
        return {}
    return {str(key): str(value) for key, value in aliases.items()}


def resolve_signal_id(signal_id: str) -> str:
    aliased = _aliases_for(str(_registry_path())).get(signal_id, signal_id)
    if aliased != signal_id:
        return aliased
    for row in _signal_rows():
        candidate = str(row.get("id") or "").strip()
        if candidate.removesuffix("_review_only") == signal_id:
            return candidate
    return signal_id


@lru_cache(maxsize=8)
def _definitions_for(path_key: str) -> dict[str, MaterializableSignalDefinition]:
    definitions: dict[str, MaterializableSignalDefinition] = {}
    for row in _signal_rows_for(path_key):
        signal_id = str(row.get("id") or "").strip()
        if not signal_id:
            continue
        sources = _as_str_list(row.get("sources")) or _as_str_list(row.get("sources_required"))
        definitions[signal_id] = MaterializableSignalDefinition(
            id=signal_id,
            version=int(row.get("version") or 1),
            title=str(row.get("title") or signal_id),
            description=str(row.get("description") or ""),
            category=str(row.get("category") or "unknown"),
            severity=_severity(row.get("severity")),
            public_safe=_as_bool(row.get("public_safe")),
            scope_type=str(row.get("scope_type") or "entity"),
            sources=sources,
        )
    return definitions


def get_signal_definition(signal_id: str) -> MaterializableSignalDefinition | None:
    return _definitions_for(str(_registry_path())).get(resolve_signal_id(signal_id))
