#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from coacc.services.neo4j_service import CypherLoader  # noqa: E402
from coacc.services.signal_registry import (  # noqa: E402
    clear_signal_registry_cache,
    load_signal_registry,
)

if TYPE_CHECKING:
    from coacc.models.signal import SignalDefinition


def _validate_signal(definition: SignalDefinition) -> list[str]:
    errors: list[str] = []

    if not definition.dedup_fields:
        errors.append(f"{definition.id}: dedup_fields must not be empty")

    if not definition.scope_type.strip():
        errors.append(f"{definition.id}: scope_type must not be blank")

    if not definition.runner.ref.strip():
        errors.append(f"{definition.id}: runner.ref must not be blank")

    if definition.runner.kind == "pattern":
        if definition.pattern_id and definition.pattern_id != definition.runner.ref:
            errors.append(
                f"{definition.id}: pattern_id must match runner.ref for pattern-backed definitions",
            )
    elif definition.runner.kind == "cypher":
        try:
            cypher = CypherLoader.load(definition.runner.ref)
        except FileNotFoundError:
            errors.append(
                (
                    f"{definition.id}: missing cypher query file for "
                    f"runner.ref={definition.runner.ref}"
                ),
            )
        else:
            normalized = cypher.upper()
            if "RETURN " not in normalized and not normalized.endswith("RETURN"):
                errors.append(
                    (
                        f"{definition.id}: cypher runner "
                        f"'{definition.runner.ref}' must contain a RETURN clause"
                    ),
                )

    if definition.public_policy.allow_public and definition.reviewer_only:
        errors.append(f"{definition.id}: signal cannot be both public and reviewer_only")

    if definition.public_policy.require_exact_identity:
        allowed = definition.public_policy.allowed_identity_match_types
        if not allowed:
            errors.append(
                (
                    f"{definition.id}: public_policy.require_exact_identity "
                    "requires allowed_identity_match_types"
                ),
            )
        elif not all(match_type.startswith("EXACT_") for match_type in allowed):
            errors.append(
                (
                    f"{definition.id}: allowed_identity_match_types must be "
                    "EXACT_* when require_exact_identity=true"
                ),
            )

    return errors


def main() -> int:
    clear_signal_registry_cache()
    registry = load_signal_registry()

    errors: list[str] = []
    seen_ids: set[str] = set()
    seen_runner_refs: set[tuple[str, str]] = set()

    for signal in registry.signals:
        if signal.id in seen_ids:
            errors.append(f"duplicate signal id: {signal.id}")
        seen_ids.add(signal.id)

        runner_key = (signal.runner.kind, signal.runner.ref)
        if runner_key in seen_runner_refs and signal.runner.kind == "cypher":
            # Cypher reuse is allowed, but we keep the output explicit for auditability.
            pass
        seen_runner_refs.add(runner_key)
        errors.extend(_validate_signal(signal))

    if errors:
        for error in errors:
            print(f"[signal-registry] {error}", file=sys.stderr)
        return 1

    print(
        (
            "[signal-registry] ok: "
            f"registry_version={registry.registry_version} "
            f"signals={len(registry.signals)}"
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
