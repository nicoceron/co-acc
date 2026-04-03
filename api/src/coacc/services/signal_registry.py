from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from coacc.models.signal import SignalDefinition, SignalRegistry

_REGISTRY_PATH = (
    Path(__file__).resolve().parents[4] / "config" / "signal_registry_co_v1.yml"
)


@lru_cache(maxsize=1)
def load_signal_registry() -> SignalRegistry:
    raw = _REGISTRY_PATH.read_text(encoding="utf-8")
    payload = yaml.safe_load(raw)
    return SignalRegistry.model_validate(payload)


def validate_signal_registry() -> SignalRegistry:
    clear_signal_registry_cache()
    return load_signal_registry()


def clear_signal_registry_cache() -> None:
    load_signal_registry.cache_clear()


def list_signal_definitions() -> list[SignalDefinition]:
    return load_signal_registry().signals


def resolve_signal_id(signal_id: str) -> str:
    registry = load_signal_registry()
    return registry.aliases.get(signal_id, signal_id)


def get_signal_definition(signal_id: str) -> SignalDefinition | None:
    canonical_id = resolve_signal_id(signal_id)
    for signal in load_signal_registry().signals:
        if signal.id == canonical_id:
            return signal
    return None
