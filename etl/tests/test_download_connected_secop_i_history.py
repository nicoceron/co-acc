from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_connected_secop_i_history_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "download_connected_secop_i_history.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_load_adjudication_ids_deduplicates_and_cleans(tmp_path: Path) -> None:
    module = _load_module()
    csv_path = tmp_path / "historical.csv"
    csv_path.write_text(
        "id_adjudicacion,numero_de_constancia\n"
        " 6105142 ,17-11-6629794\n"
        "6105142,17-11-6629794\n"
        "6713364,17-4-7396592\n",
        encoding="utf-8",
    )

    assert module._load_adjudication_ids(csv_path) == ("6105142", "6713364")


def test_filter_bpins_keeps_digits_and_deduplicates() -> None:
    module = _load_module()
    values = ("2017003630001", "2017003630001", "No definido", "0000", "2018-003-002")
    assert module._filter_bpins(values) == ("2017003630001", "2018003002")


def test_download_connected_dataset_creates_output_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "resource.csv"

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
            "id_adjudicacion,id_origen_de_los_recursos,codigo_bpin\n6105142,8,2017003630001\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_connected_dataset(
        dataset_id="fake-dataset",
        output_path=output,
        values_by_column={"id_adjudicacion": ("6105142",)},
        timeout=30,
        batch_size=1000,
        max_clause_chars=100,
        key_fields=("id_adjudicacion", "id_origen_de_los_recursos", "codigo_bpin"),
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert rows_written == 1
    assert rows == [
        {
            "id_adjudicacion": "6105142",
            "id_origen_de_los_recursos": "8",
            "codigo_bpin": "2017003630001",
        }
    ]
