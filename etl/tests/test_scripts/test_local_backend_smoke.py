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


def test_api_env_forces_neo4j_offline_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")

    env = local_backend_smoke._api_env(lake_root=tmp_path)

    assert env["NEO4J_REQUIRED"] == "false"
    assert env["NEO4J_URI"] == local_backend_smoke.DEFAULT_OFFLINE_NEO4J_URI
    assert env["COACC_LAKE_ROOT"] == str(tmp_path)


def test_api_env_allows_explicit_smoke_neo4j_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_SMOKE_NEO4J_URI", "bolt://example.invalid:7687")

    env = local_backend_smoke._api_env(lake_root=tmp_path)

    assert env["NEO4J_URI"] == "bolt://example.invalid:7687"


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


def test_smoke_script_covers_frontend_meta_routes() -> None:
    source = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "/api/v1/meta/health" in source
    assert "/api/v1/meta/stats" in source
