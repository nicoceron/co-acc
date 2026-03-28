from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_connected_dnp_bpin_wave_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "download_connected_dnp_bpin_wave.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_dataset_ids_cover_connected_dnp_wave() -> None:
    module = _load_module()
    dataset_ids = {item["dataset_id"] for item in module.DATASETS}

    assert dataset_ids == {"epzv-8ck4", "iuc2-3r6h", "tmmn-mpqc", "xikz-44ja"}


def test_normalize_bpin_rejects_dirty_tokens() -> None:
    module = _load_module()

    assert module._normalize_bpin("20210214000125") == "20210214000125"
    assert module._normalize_bpin("'2018004251708") == "2018004251708"
    assert module._normalize_bpin("abc") == ""
    assert module._normalize_bpin("0000000") == ""


def test_download_connected_dataset_creates_output_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "dnp_project_locations.csv"

    def fake_run_download(
        *,
        dataset_id: str,
        output_path: Path,
        where: str,
        timeout: int,
        batch_size: int,
    ) -> None:
        del dataset_id, where, timeout, batch_size
        output_path.write_text(
            "bpin,codigodepartamento,codigomunicipio,entidadresponsable\n"
            "20210214000125,86,86001,Entidad Uno\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_connected_dataset(
        dataset=next(item for item in module.DATASETS if item["name"] == "dnp_project_locations"),
        bpins=("20210214000125",),
        output_path=output,
        timeout=30,
        batch_size=1000,
        max_clause_chars=1000,
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows_written == 1
    assert rows == [
        {
            "bpin": "20210214000125",
            "codigodepartamento": "86",
            "codigomunicipio": "86001",
            "entidadresponsable": "Entidad Uno",
        }
    ]
