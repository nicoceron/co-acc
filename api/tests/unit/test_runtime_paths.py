from __future__ import annotations

from typing import TYPE_CHECKING

from coacc.services import runtime_paths

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_find_project_path_supports_source_checkout_layout(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    source_file = repo / "api" / "src" / "coacc" / "services" / "signal_registry.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("", encoding="utf-8")
    expected = repo / "config" / "signal_registry.yml"
    expected.parent.mkdir()
    expected.write_text("signals: []\n", encoding="utf-8")

    resolved = runtime_paths.find_project_path(
        "config",
        "signal_registry.yml",
        start=source_file,
    )

    assert resolved == expected


def test_find_project_path_supports_container_install_layout(tmp_path: Path) -> None:
    app_root = tmp_path / "app"
    source_file = app_root / "src" / "coacc" / "services" / "signal_registry.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("", encoding="utf-8")
    expected = app_root / "config" / "signal_registry.yml"
    expected.parent.mkdir()
    expected.write_text("signals: []\n", encoding="utf-8")

    resolved = runtime_paths.find_project_path(
        "config",
        "signal_registry.yml",
        start=source_file,
    )

    assert resolved == expected


def test_config_file_honors_config_dir_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_CONFIG_DIR", str(tmp_path / "mounted-config"))

    assert runtime_paths.config_file("signals", "sql", "demo.sql") == (
        tmp_path / "mounted-config" / "signals" / "sql" / "demo.sql"
    )


def test_dataset_contract_dir_honors_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_DATASET_CONTRACT_DIR", str(tmp_path / "contracts"))

    assert runtime_paths.dataset_contract_dir() == tmp_path / "contracts"
