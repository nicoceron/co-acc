from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_bogota_secop_scope_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = Path(__file__).resolve().parents[1] / "scripts" / "download_bogota_secop_scope.py"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_merge_csv_files_unions_chunk_fields(tmp_path: Path) -> None:
    module = _load_module()

    first = tmp_path / "chunk-1.csv"
    second = tmp_path / "chunk-2.csv"
    output = tmp_path / "merged.csv"

    first.write_text("id,alpha\n1,a\n", encoding="utf-8")
    second.write_text("id,beta\n2,b\n", encoding="utf-8")

    rows_written = module._merge_csv_files(
        temp_paths=[first, second],
        output_path=output,
        key_fields=("id",),
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert rows_written == 2
    assert reader.fieldnames == ["id", "alpha", "beta"]
    assert rows == [
        {"id": "1", "alpha": "a", "beta": ""},
        {"id": "2", "alpha": "", "beta": "b"},
    ]


def test_download_related_dataset_creates_missing_parent_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "merged.csv"

    def fake_run_download(
        *,
        dataset_id: str,
        output_path: Path,
        where: str,
        timeout: int,
        batch_size: int,
        limit: int | None = None,
        order: str | None = None,
    ) -> None:
        del dataset_id, where, timeout, batch_size, limit, order
        output_path.write_text("id,value\n1,a\n", encoding="utf-8")

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_related_dataset(
        dataset_id="fake-dataset",
        output_path=output,
        where_clauses=["id in ('1')"],
        key_fields=("id",),
        timeout=30,
        batch_size=1000,
    )

    assert rows_written == 1
    assert output.exists()


def test_related_downloads_include_contract_suspensions() -> None:
    module = _load_module()

    suspension_entry = next(
        item
        for item in module.RELATED_DOWNLOADS
        if item["output_relpath"] == "secop_contract_suspensions/secop_contract_suspensions.csv"
    )

    assert suspension_entry["dataset_id"] == "u99c-7mfm"
    assert suspension_entry["column"] == "id_contrato"


def test_related_downloads_include_payment_plans_and_bpin() -> None:
    module = _load_module()

    payment_entry = next(
        item
        for item in module.RELATED_DOWNLOADS
        if item["output_relpath"] == "secop_payment_plans/secop_payment_plans.csv"
    )
    bpin_entry = next(
        item
        for item in module.RELATED_DOWNLOADS
        if item["output_relpath"] == "secop_process_bpin/secop_process_bpin.csv"
    )

    assert payment_entry["dataset_id"] == "uymx-8p3j"
    assert payment_entry["column"] == "id_del_contrato"
    assert bpin_entry["dataset_id"] == "d9na-abhe"
    assert bpin_entry["column"] == "id_portafolio"
