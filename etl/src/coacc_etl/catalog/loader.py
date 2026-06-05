"""Load and validate all DatasetSpec YAML contracts."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml  # type: ignore[import-untyped]

from coacc_etl.catalog.models import DatasetSpec
from coacc_etl.runtime_paths import dataset_contract_dir


def datasets_dir() -> Path:
    return dataset_contract_dir()


def _iter_yaml_paths(root: Path) -> list[Path]:
    return sorted(p for p in root.glob("*.yml") if not p.name.startswith("_"))


@lru_cache(maxsize=8)
def _load_catalog_cached(root_key: str) -> dict[str, DatasetSpec]:
    """Read every ``<id>.yml`` under ``etl/datasets/`` and validate.

    Raises ``ValueError`` on missing root, duplicate ids, or file/id mismatch.
    """
    base = Path(root_key)
    if not base.is_dir():
        msg = f"datasets directory not found: {base}"
        raise ValueError(msg)

    specs: dict[str, DatasetSpec] = {}
    for path in _iter_yaml_paths(base):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            msg = f"{path} does not contain a YAML mapping"
            raise ValueError(msg)
        spec = DatasetSpec.model_validate(raw)
        stem = path.stem
        if spec.id != stem:
            msg = f"{path} declares id={spec.id!r} but filename stem is {stem!r}"
            raise ValueError(msg)
        if spec.id in specs:
            msg = f"duplicate dataset id {spec.id!r} in {path}"
            raise ValueError(msg)
        specs[spec.id] = spec
    return specs


def load_catalog(root: Path | None = None) -> dict[str, DatasetSpec]:
    base = root or datasets_dir()
    return _load_catalog_cached(str(base))


def clear_cache() -> None:
    _load_catalog_cached.cache_clear()
