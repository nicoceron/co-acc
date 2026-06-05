from __future__ import annotations

import os
from pathlib import Path


def _candidate_roots(start: Path) -> tuple[Path, ...]:
    resolved = start.resolve()
    roots = resolved.parents if resolved.suffix else (resolved, *resolved.parents)
    return tuple(roots)


def find_project_path(*parts: str | Path, start: Path | None = None) -> Path:
    relative = Path(*[str(part) for part in parts])
    roots = _candidate_roots(start or Path(__file__))
    for root in roots:
        candidate = root / relative
        if candidate.exists():
            return candidate
    for root in roots:
        if (
            (root / ".git").exists()
            or (root / "config").exists()
            or (root / "docs" / "datasets").exists()
            or (root / "etl" / "datasets").exists()
            or (root / "src" / "coacc_etl").exists()
        ):
            return root / relative
    return roots[0] / relative


def config_file(*parts: str | Path) -> Path:
    configured = os.getenv("COACC_CONFIG_DIR", "").strip()
    if configured:
        return Path(configured).joinpath(*[str(part) for part in parts])
    return find_project_path("config", *parts)


def docs_dataset_file(name: str) -> Path:
    configured = os.getenv("COACC_DATASET_CATALOG_DIR", "").strip()
    if configured:
        return Path(configured) / name
    return find_project_path("docs", "datasets", name)


def dataset_contract_dir() -> Path:
    configured = os.getenv("COACC_DATASET_CONTRACT_DIR", "").strip()
    if configured:
        return Path(configured)
    return find_project_path("etl", "datasets")


def runbook_file(name: str) -> Path:
    configured = os.getenv("COACC_RUNBOOK_DIR", "").strip()
    if configured:
        return Path(configured) / name
    return find_project_path("docs", "runbooks", name)
