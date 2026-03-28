from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_module():
    module_name = "download_connected_secop_scope_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "download_connected_secop_scope.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_load_contract_ids_deduplicates_and_cleans(tmp_path: Path) -> None:
    module = _load_module()
    summary_dir = tmp_path / "secop_ii_contracts"
    summary_dir.mkdir(parents=True)
    summary_map = summary_dir / "contract_summary_map.csv"
    summary_map.write_text(
        "contract_id,summary_id\n"
        " CO1.PCCNTR.1 ,a\n"
        "CO1.PCCNTR.1,b\n"
        "CO1.PCCNTR.2,c\n",
        encoding="utf-8",
    )

    assert module._load_contract_ids(tmp_path) == ("CO1.PCCNTR.1", "CO1.PCCNTR.2")


def test_related_downloads_cover_connected_sources() -> None:
    module = _load_module()

    cdp_entry = next(
        item for item in module.RELATED_DOWNLOADS if item["name"] == "secop_cdp_requests"
    )
    location_entry = next(
        item for item in module.RELATED_DOWNLOADS if item["name"] == "secop_execution_locations"
    )
    additional_location_entry = next(
        item for item in module.RELATED_DOWNLOADS if item["name"] == "secop_additional_locations"
    )
    archive_entry = next(
        item for item in module.RELATED_DOWNLOADS if item["name"] == "secop_document_archives"
    )
    quarantined_entry = next(
        item for item in module.QUARANTINED_DOWNLOADS if item["name"] == "secop_budget_commitments"
    )

    assert cdp_entry["dataset_id"] == "a86w-fh92"
    assert cdp_entry["column"] == "id_contrato"
    assert location_entry["dataset_id"] == "gra4-pcp2"
    assert location_entry["column"] == "id_contrato"
    assert additional_location_entry["dataset_id"] == "wwhe-4sq8"
    assert additional_location_entry["column"] == "id_contrato"
    assert archive_entry["dataset_id"] == "dmgg-8hin"
    assert archive_entry["column"] == "proceso"
    assert archive_entry["value_scope"] == "process"
    assert quarantined_entry["dataset_id"] == "skc9-met7"
    assert "No overlap" in quarantined_entry["reason"]


def test_load_process_ids_deduplicates_and_cleans(tmp_path: Path) -> None:
    module = _load_module()
    contracts_dir = tmp_path / "secop_ii_contracts"
    contracts_dir.mkdir(parents=True)
    contracts_csv = contracts_dir / "secop_ii_contracts.csv"
    contracts_csv.write_text(
        "proceso_de_compra,id_contrato\n"
        " CO1.BDOS.1 ,CO1.PCCNTR.1\n"
        "CO1.BDOS.1,CO1.PCCNTR.2\n"
        "CO1.BDOS.2,CO1.PCCNTR.3\n",
        encoding="utf-8",
    )

    assert module._load_process_ids(tmp_path) == ("CO1.BDOS.1", "CO1.BDOS.2")


def test_download_related_dataset_creates_output_directory(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    output = tmp_path / "nested" / "connected.csv"

    def fake_run_download(
        *,
        dataset_id: str,
        output_path: Path,
        where: str,
        timeout: int,
        batch_size: int,
    ) -> None:
        del dataset_id, where, timeout, batch_size
        output_path.write_text("id_contrato,ubicacion\nCO1.PCCNTR.1,Bogota\n", encoding="utf-8")

    monkeypatch.setattr(module, "_run_download", fake_run_download)

    rows_written = module._download_related_dataset(
        dataset_id="fake-dataset",
        output_path=output,
        where_clauses=["id_contrato in ('CO1.PCCNTR.1')"],
        key_fields=("id_contrato", "ubicacion"),
        timeout=30,
        batch_size=1000,
    )

    with output.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert rows_written == 1
    assert rows == [{"id_contrato": "CO1.PCCNTR.1", "ubicacion": "Bogota"}]
