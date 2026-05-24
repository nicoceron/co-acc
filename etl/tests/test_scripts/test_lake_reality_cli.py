from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from coacc_etl.lakehouse.writer import append_parquet

if TYPE_CHECKING:
    import pytest


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
