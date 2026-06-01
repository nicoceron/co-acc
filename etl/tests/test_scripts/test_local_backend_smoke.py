from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "local_backend_smoke.py"
SPEC = importlib.util.spec_from_file_location("local_backend_smoke", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
local_backend_smoke: Any = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(local_backend_smoke)


def test_require_raises_smoke_error() -> None:
    with pytest.raises(local_backend_smoke.SmokeError, match="bad state"):
        local_backend_smoke._require(False, "bad state")


def test_find_free_port_returns_connectable_port_number() -> None:
    port = local_backend_smoke._find_free_port()

    assert 0 < port < 65536


def test_parse_args_accepts_lake_root_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["local_backend_smoke.py", "--lake-root", "/tmp/coacc-lake", "--timeout", "7"],
    )

    args = local_backend_smoke.parse_args()

    assert REPO_ROOT == local_backend_smoke.REPO_ROOT
    assert args.lake_root == Path("/tmp/coacc-lake")
    assert args.timeout == 7.0
