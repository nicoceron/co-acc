from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_connected_company_branches_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "download_connected_company_branches.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_normalize_identifier_filters_junk() -> None:
    module = _load_module()

    assert module._normalize_identifier("00000900123456") == "900123456"
    assert module._normalize_identifier("abcdef") == ""
    assert module._normalize_identifier("111111") == ""


def test_download_connected_dataset_creates_output_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "company_branches_nb3d.csv"

    def fake_run_download(
        *,
        output_path: Path,
        where: str,
        timeout: int,
        batch_size: int,
    ) -> None:
        del where, timeout, batch_size
        output_path.write_text(
            "codigo_camara,matricula,nit_propietario,numero_identificacion\n"
            "11,100,900123456,900123456\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_connected_dataset(
        output_path=output,
        identifiers=("900123456",),
        timeout=30,
        batch_size=1000,
        max_clause_chars=1000,
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows_written == 1
    assert rows == [
        {
            "codigo_camara": "11",
            "matricula": "100",
            "nit_propietario": "900123456",
            "numero_identificacion": "900123456",
        }
    ]
