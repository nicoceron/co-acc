from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, cast


def _load_script(repo: Path) -> Any:
    path = repo / "api" / "scripts" / "materialize_signals.py"
    spec = importlib.util.spec_from_file_location("materialize_signals_script", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["materialize_signals_script"] = module
    spec.loader.exec_module(module)
    return cast("Any", module)


def test_global_materializer_payload_allows_empty_candidate_graph() -> None:
    script = _load_script(Path(__file__).resolve().parents[3])

    assert script._payload([], all_mode=True) == {
        "entities": [],
        "mode": "global",
        "entity_count": 0,
    }
