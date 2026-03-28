from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    module_name = "refresh_large_national_sources_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_path = (
        Path(__file__).resolve().parents[1] / "scripts" / "refresh_large_national_sources.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Unable to load module from {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_payment_plan_partitions_cover_legacy_and_recent_years() -> None:
    module = _load_module()

    partitions = module.payment_plan_partitions()

    assert partitions[0].name == "legacy_or_null"
    assert "fecha_inicio_contrato IS NULL" in partitions[0].where
    assert partitions[1].name == "2016"
    assert partitions[-1].name == "2030"
    assert len(partitions) == 16


def test_bpin_partitions_cover_known_annual_range() -> None:
    module = _load_module()

    partitions = module.process_bpin_partitions()

    assert [partition.name for partition in partitions] == [
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
        "2027",
    ]
