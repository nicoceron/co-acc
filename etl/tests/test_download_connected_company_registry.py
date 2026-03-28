from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_connected_company_registry_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "download_connected_company_registry.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_column_family_map_targets_company_and_person_joins() -> None:
    module = _load_module()

    assert module.DATASET_ID == "c82u-588k"
    assert module.COLUMN_FAMILY_MAP["numero_identificacion"] == "company"
    assert module.COLUMN_FAMILY_MAP["nit"] == "company"
    assert module.COLUMN_FAMILY_MAP["num_identificacion_representante_legal"] == "person"


def test_download_connected_dataset_creates_output_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "company_registry.csv"

    def fake_run_download(
        *,
        output_path: Path,
        where: str,
        timeout: int,
        batch_size: int,
    ) -> None:
        del where, timeout, batch_size
        output_path.write_text(
            "codigo_camara,matricula,numero_identificacion,num_identificacion_representante_legal\n"
            "11,123456,900123456,79876543\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_connected_dataset(
        output_path=output,
        connected_ids={"company": ("900123456",), "person": ("79876543",)},
        timeout=30,
        batch_size=1000,
        max_clause_chars=1000,
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert rows_written == 1
    assert rows == [
        {
            "codigo_camara": "11",
            "matricula": "123456",
            "numero_identificacion": "900123456",
            "num_identificacion_representante_legal": "79876543",
        }
    ]


def test_normalize_identifier_rejects_dirty_graph_tokens() -> None:
    module = _load_module()

    assert module._normalize_identifier("900.123.456-7") == "9001234567"
    assert module._normalize_identifier("  79876543  ") == "79876543"
    assert module._normalize_identifier("coanon_4c99162507820be6") == ""
    assert module._normalize_identifier("edu_dir_d4631b14b445e853") == ""
    assert module._normalize_identifier("000000") == ""
    assert module._normalize_identifier("1023036543666") == ""


def test_filter_identifiers_keeps_only_document_shaped_values() -> None:
    module = _load_module()

    values = (
        "900.123.456-7",
        "coanon_4c99162507820be6",
        "79876543",
        "79876543",
        "111111",
        "1023036543666",
    )

    assert module._filter_identifiers(values) == ("9001234567", "79876543")
