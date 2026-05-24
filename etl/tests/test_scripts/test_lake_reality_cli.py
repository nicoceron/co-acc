from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from coacc_etl.lakehouse.writer import append_parquet

if TYPE_CHECKING:
    import pytest


def _load_lake_reality_script(repo: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        "lake_reality_script", repo / "scripts" / "lake_reality.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast("Any", module)


def _write_curated_signal_table(root: Path) -> None:
    table = "signal_feature_demo"
    out = root / "curated" / f"table={table}"
    out.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([
            {
                "signal_id": "demo",
                "entity_key": "9001",
                "scope_key": "scope:1",
                "risk_signal": 0.5,
                "evidence_refs": ["https://example.test/1"],
            }
        ]),
        out / "part-00000.parquet",
    )
    manifest_dir = root / "meta" / "curated"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "20260524T120000Z.json").write_text(
        json.dumps({
            "generated_at": "2026-05-24T12:00:00+00:00",
            "tables": [{"table": table, "rows": 1, "path": str(out)}],
        }),
        encoding="utf-8",
    )


def test_lake_reality_cli_writes_snapshot_and_diff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    lake = tmp_path / "lake"
    output_dir = tmp_path / "reality"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(lake))
    append_parquet(
        pd.DataFrame(
            {
                "nit": ["9001", "9002"],
                "entity_name": ["Alpha SA", "Beta SAS"],
                "received_date": ["2026-04-01", "2026-04-02"],
                "radicado": ["R-1", "R-2"],
            }
        ),
        "8qxx-ubmq",
        2026,
        4,
    )

    env = os.environ.copy()
    env["COACC_LAKE_ROOT"] = str(lake)
    env["PYTHONPATH"] = str(repo / "etl" / "src")
    result = subprocess.run(
        [
            sys.executable,
            str(repo / "scripts" / "lake_reality.py"),
            "--dataset",
            "8qxx-ubmq",
            "--date",
            "2026-05-23",
            "--output-dir",
            str(output_dir),
        ],
        check=False,
        capture_output=True,
        encoding="utf-8",
        env=env,
        cwd=repo,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    snapshot_path = output_dir / "2026-05-23.json"
    diff_path = output_dir / "2026-05-23.diff.md"
    assert snapshot_path.exists()
    assert diff_path.exists()

    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["datasets"][0]["dataset_id"] == "8qxx-ubmq"
    assert snapshot["datasets"][0]["row_count"] == 2
    assert "Lake reality diff: 2026-05-23" in diff_path.read_text(encoding="utf-8")


def test_lake_reality_cli_writes_curated_snapshot_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    lake = tmp_path / "lake"
    output_dir = tmp_path / "reality"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(lake))
    _write_curated_signal_table(lake)

    env = os.environ.copy()
    env["COACC_LAKE_ROOT"] = str(lake)
    env["PYTHONPATH"] = str(repo / "etl" / "src")
    result = subprocess.run(
        [
            sys.executable,
            str(repo / "scripts" / "lake_reality.py"),
            "--date",
            "2026-05-24",
            "--output-dir",
            str(output_dir),
        ],
        check=False,
        capture_output=True,
        encoding="utf-8",
        env=env,
        cwd=repo,
    )

    assert result.returncode == 0, result.stderr + result.stdout
    snapshot = json.loads((output_dir / "2026-05-24.json").read_text(encoding="utf-8"))
    assert snapshot["datasets"] == []
    assert snapshot["curated_tables"][0]["table"] == "signal_feature_demo"
    assert snapshot["curated_tables"][0]["row_count"] == 1
    assert snapshot["curated_tables"][0]["latest_manifest_rows"] == 1
    assert "Curated Table Summary" in (output_dir / "2026-05-24.diff.md").read_text(
        encoding="utf-8"
    )


def test_changed_catalog_dataset_ids_uses_github_base_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    script = _load_lake_reality_script(repo)
    calls: list[list[str]] = []

    def fake_git_names(args: list[str]) -> list[str]:
        calls.append(args)
        if args == ["diff", "--name-only", "origin/main...HEAD"]:
            return ["etl/datasets/8qxx-ubmq.yml", "README.md"]
        return []

    monkeypatch.setenv("GITHUB_BASE_REF", "main")
    monkeypatch.setattr(script, "_git_names", fake_git_names)

    dataset_ids = script._changed_catalog_dataset_ids({"8qxx-ubmq": object()})

    assert dataset_ids == ["8qxx-ubmq"]
    assert ["diff", "--name-only", "--cached"] in calls
    assert ["diff", "--name-only", "origin/main...HEAD"] in calls


def test_changed_paths_can_be_staged_only(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = Path(__file__).resolve().parents[3]
    script = _load_lake_reality_script(repo)
    calls: list[list[str]] = []

    def fake_git_names(args: list[str]) -> list[str]:
        calls.append(args)
        return []

    monkeypatch.setenv("COACC_REALITY_STAGED_ONLY", "1")
    monkeypatch.setattr(script, "_git_names", fake_git_names)

    assert script._changed_paths() == set()
    assert calls == [["diff", "--name-only", "--cached"]]


def test_changed_yamls_only_filters_missing_local_parquet(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    script = _load_lake_reality_script(repo)
    raw_source = tmp_path / "raw" / "source=8qxx-ubmq"
    raw_source.mkdir(parents=True)
    args = script.argparse.Namespace(
        dataset=[],
        datasets=None,
        changed_yamls_only=True,
        all=False,
        curated_table=[],
        curated_tables=None,
        skip_curated=False,
        curated_only=False,
    )

    monkeypatch.setattr(
        script,
        "_changed_catalog_dataset_ids",
        lambda _catalog: ["8qxx-ubmq", "2jzx-383z"],
    )

    dataset_ids = script._select_dataset_ids(
        args,
        {"8qxx-ubmq": object(), "2jzx-383z": object()},
        tmp_path,
    )

    assert dataset_ids == ["8qxx-ubmq"]


def test_select_curated_tables_skips_for_explicit_raw_dataset(
    tmp_path: Path,
) -> None:
    repo = Path(__file__).resolve().parents[3]
    script = _load_lake_reality_script(repo)
    _write_curated_signal_table(tmp_path)
    args = script.argparse.Namespace(
        dataset=["8qxx-ubmq"],
        datasets=None,
        changed_yamls_only=False,
        all=False,
        curated_table=[],
        curated_tables=None,
        skip_curated=False,
        curated_only=False,
    )

    assert script._select_curated_tables(args, tmp_path) == []
