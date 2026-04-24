"""Load and validate all DatasetSpec YAML contracts."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from coacc_etl.catalog.models import DatasetSpec

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_DATASETS_DIR = _REPO_ROOT / "etl" / "datasets"


def datasets_dir() -> Path:
    return _DEFAULT_DATASETS_DIR


def _iter_yaml_paths(root: Path) -> list[Path]:
    return sorted(p for p in root.glob("*.yml") if not p.name.startswith("_"))


@lru_cache(maxsize=1)
def load_catalog(root: Path | None = None) -> dict[str, DatasetSpec]:
    """Read every ``<id>.yml`` under ``etl/datasets/`` and validate.

    Raises ``ValueError`` on missing root, duplicate ids, or file/id mismatch.
    """
    base = root or _DEFAULT_DATASETS_DIR
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


def clear_cache() -> None:
    load_catalog.cache_clear()
