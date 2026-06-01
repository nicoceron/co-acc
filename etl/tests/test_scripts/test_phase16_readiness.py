from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, cast

import yaml  # type: ignore[import-untyped]


def _load_script(repo: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, repo / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return cast("Any", module)


def test_repo_publish_audit_flags_publish_blockers(tmp_path: Path) -> None:
    script = _load_script(Path(__file__).resolve().parents[3], "repo_publish_audit")
    fake_secret = "sk-" + "realbadtoken000000000000"
    (tmp_path / ".env").write_text(f"OPENAI_API_KEY={fake_secret}\n", encoding="utf-8")
    (tmp_path / ".env.example").write_text("OPENAI_API_KEY=\n", encoding="utf-8")
    (tmp_path / "lake").mkdir()
    (tmp_path / "lake" / "part.parquet").write_bytes(b"not-public")

    findings = script.audit_paths(
        tmp_path,
        [Path(".env"), Path(".env.example"), Path("lake/part.parquet")],
    )

    assert {finding.path for finding in findings} == {".env", "lake/part.parquet"}


def test_doc_link_checker_validates_crisp_ml_links() -> None:
    repo = Path(__file__).resolve().parents[3]
    script = _load_script(repo, "check_doc_links")

    assert script.check_paths(repo, [Path("docs/crisp_ml.md")]) == []


def test_doc_link_checker_reports_missing_target(tmp_path: Path) -> None:
    script = _load_script(Path(__file__).resolve().parents[3], "check_doc_links")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "sample.md").write_text("[missing](missing.md)\n", encoding="utf-8")

    findings = script.check_paths(tmp_path, [Path("docs/sample.md")])

    assert len(findings) == 1
    assert findings[0].target == "missing.md"
    assert findings[0].reason == "missing target"


def test_api_compose_service_mounts_lake_runtime_inputs() -> None:
    repo = Path(__file__).resolve().parents[3]
    compose = yaml.safe_load((repo / "docker-compose.yml").read_text(encoding="utf-8"))
    api = compose["services"]["api"]

    assert api["environment"]["COACC_SOURCE_REGISTRY_PATH"] == (
        "/app/docs/datasets/catalog.signed.csv"
    )
    assert api["environment"]["COACC_LAKE_ROOT"] == "/app/lake"
    assert set(api["volumes"]) >= {
        "./config:/app/config:ro",
        "./docs/datasets:/app/docs/datasets:ro",
        "./etl/datasets:/app/etl/datasets:ro",
        "./lake:/app/lake:ro",
    }


def test_signal_materializer_compose_command_expands_user_id_in_container() -> None:
    repo = Path(__file__).resolve().parents[3]
    compose = yaml.safe_load((repo / "docker-compose.yml").read_text(encoding="utf-8"))
    materializer = compose["services"]["signal-materializer"]

    assert '--user-id "$${SIGNAL_MATERIALIZER_USER_ID}"' in materializer["command"]
