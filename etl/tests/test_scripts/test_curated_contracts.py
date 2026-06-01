from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_check_curated_contracts_validates_requested_table(tmp_path: Path) -> None:
    table_root = tmp_path / "curated" / "table=dim_company"
    table_root.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "entity_uid": ["company:9001234568"],
                "nit_canonical": ["9001234568"],
                "nit_variants": [["900123456-8"]],
                "name_canonical": ["Proveedor Demo"],
                "source_row_count": [1],
            }
        ),
        table_root / "part.parquet",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "check_curated_contracts.py"),
            "--lake-root",
            str(tmp_path),
            "--table",
            "dim_company",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "PASS 1 curated contract(s)" in result.stdout


def test_check_curated_contracts_reports_missing_columns(tmp_path: Path) -> None:
    table_root = tmp_path / "curated" / "table=dim_company"
    table_root.mkdir(parents=True)
    pq.write_table(pa.table({"entity_uid": ["company:9001234568"]}), table_root / "part.parquet")

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "check_curated_contracts.py"),
            "--lake-root",
            str(tmp_path),
            "--table",
            "dim_company",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "missing column nit_canonical" in result.stdout
