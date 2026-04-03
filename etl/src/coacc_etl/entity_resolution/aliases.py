from __future__ import annotations

from typing import Any


def build_alias_row(
    *,
    alias_id: str,
    kind: str,
    value: str,
    normalized: str,
    target_key: str,
    source_id: str,
    confidence: float = 1.0,
    match_type: str = "EXACT",
) -> dict[str, Any]:
    return {
        "alias_id": alias_id,
        "kind": kind,
        "value": value,
        "normalized": normalized,
        "target_key": target_key,
        "source_id": source_id,
        "confidence": confidence,
        "match_type": match_type,
    }
