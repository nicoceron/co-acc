import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from coacc.services.runtime_paths import dataset_contract_dir, docs_dataset_file


@dataclass(frozen=True)
class SourceRegistryEntry:
    id: str
    name: str
    category: str
    tier: str
    status: str
    implementation_state: str
    load_state: str
    signal_promotion_state: str
    frequency: str
    in_universe_v1: bool
    primary_url: str
    pipeline_id: str
    owner_agent: str
    access_mode: str
    public_access_mode: str
    discovery_status: str
    last_seen_url: str
    cadence_expected: str
    cadence_observed: str
    quality_status: str
    notes: str

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "category": self.category,
            "tier": self.tier,
            "status": self.status,
            "implementation_state": self.implementation_state,
            "load_state": self.load_state,
            "signal_promotion_state": self.signal_promotion_state,
            "frequency": self.frequency,
            "in_universe_v1": self.in_universe_v1,
            "primary_url": self.primary_url,
            "pipeline_id": self.pipeline_id,
            "owner_agent": self.owner_agent,
            "access_mode": self.access_mode,
            "public_access_mode": self.public_access_mode,
            "discovery_status": self.discovery_status,
            "last_seen_url": self.last_seen_url,
            "cadence_expected": self.cadence_expected,
            "cadence_observed": self.cadence_observed,
            "quality_status": self.quality_status,
            "notes": self.notes,
        }


_SIGNED_CATALOG_PATH: Path | None = None
_DATASET_CONTRACT_DIR: Path | None = None
_SOCRATA_URL_MARKER = "datos.gov.co/d/"
_SIGNAL_STATE_BY_RELEVANCE = {
    "already_used": "promoted",
    "promoted": "promoted",
    "enrichment_only": "enrichment_only",
    "quarantined": "quarantined",
}
_SIGNAL_STATE_RANK = {
    "promoted": 3,
    "enrichment_only": 2,
    "quarantined": 1,
}
_FREQUENCY_RANK = {
    "realtime": 6,
    "daily": 5,
    "weekly": 4,
    "monthly": 3,
    "quarterly": 2,
    "annual": 1,
    "election_cycle": 1,
    "ad_hoc": 1,
}


def _default_registry_path() -> Path:
    # Search for the docs directory starting from the current file's parent
    # up to the root or a reasonable limit. Docker mounts docs/datasets here.
    current = Path(__file__).resolve().parent
    for _ in range(10):
        candidate = current / "docs" / "datasets" / "catalog.signed.csv"
        if candidate.exists():
            return candidate
        if (current / ".git").exists() or current.parent == current:
            # Reached repo root or filesystem root
            break
        current = current.parent

    return _SIGNED_CATALOG_PATH or docs_dataset_file("catalog.signed.csv")


def get_registry_path() -> Path:
    """Return the source registry CSV path from env or default.

    COACC_SOURCE_REGISTRY_PATH must be set only by administrators in a trusted
    environment; do not allow untrusted users or processes to set it.
    """
    configured = os.getenv("COACC_SOURCE_REGISTRY_PATH", "").strip()
    return Path(configured) if configured else _default_registry_path()


def load_source_registry() -> list[SourceRegistryEntry]:
    catalog_path = get_registry_path()
    if not catalog_path.exists():
        return []

    yaml_specs = _load_dataset_contracts()
    entries_by_id: dict[str, SourceRegistryEntry] = {}
    with catalog_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            source_refs = _split_refs(row.get("source_refs") or "")
            if not source_refs:
                continue
            for source_id in source_refs:
                _upsert_entry(
                    entries_by_id,
                    _entry_from_catalog_row(row, source_id, yaml_specs),
                )
    for entry in _custom_adapter_entries(yaml_specs, set(entries_by_id)):
        _upsert_entry(entries_by_id, entry)

    entries = list(entries_by_id.values())
    entries.sort(key=lambda entry: entry.id)
    return entries


def _split_refs(value: str) -> list[str]:
    return [part.strip() for part in value.split("|") if part.strip()]


def _load_dataset_contracts() -> dict[str, dict[str, Any]]:
    contract_dir = _DATASET_CONTRACT_DIR or dataset_contract_dir()
    if not contract_dir.is_dir():
        return {}
    specs: dict[str, dict[str, Any]] = {}
    for path in sorted(contract_dir.glob("*.yml")):
        if path.name.startswith("_"):
            continue
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            dataset_id = str(payload.get("id") or path.stem).strip()
            if dataset_id:
                specs[dataset_id] = payload
    return specs


def _dataset_id_from_url(url: str) -> str:
    if _SOCRATA_URL_MARKER not in url:
        return ""
    return url.rsplit("/", maxsplit=1)[-1].strip()


def _is_loaded(dataset_id: str) -> bool:
    lake_root = Path(os.getenv("COACC_LAKE_ROOT", "/var/lib/coacc/lake"))
    raw_root = lake_root / "raw" / f"source={dataset_id}"
    return raw_root.exists() and any(raw_root.rglob("*.parquet"))


def _is_ingest_ready(spec: dict[str, Any] | None) -> bool:
    if not spec:
        return False
    columns_map = spec.get("columns_map")
    if not isinstance(columns_map, dict) or not columns_map:
        return False
    if bool(spec.get("full_refresh_only")):
        return True
    return bool(spec.get("watermark_column") and spec.get("partition_column"))


def _access_mode(url: str, *, adapter: str = "socrata") -> str:
    if adapter != "socrata":
        return "web"
    return "api" if _SOCRATA_URL_MARKER in url else "web"


def _signal_promotion_state(relevance: str) -> str:
    value = relevance.strip().lower()
    return _SIGNAL_STATE_BY_RELEVANCE.get(value, value or "catalog")


def _entry_rank(entry: SourceRegistryEntry) -> tuple[int, int, int, int, str]:
    return (
        int(entry.load_state == "loaded"),
        int(entry.implementation_state == "implemented"),
        _SIGNAL_STATE_RANK.get(entry.signal_promotion_state, 0),
        _FREQUENCY_RANK.get(entry.frequency.strip().lower(), 0),
        entry.primary_url,
    )


def _upsert_entry(
    entries_by_id: dict[str, SourceRegistryEntry],
    entry: SourceRegistryEntry,
) -> None:
    existing = entries_by_id.get(entry.id)
    if existing is None or _entry_rank(entry) > _entry_rank(existing):
        entries_by_id[entry.id] = entry


def _entry_from_catalog_row(
    row: dict[str, str],
    source_id: str,
    yaml_specs: dict[str, dict[str, Any]],
) -> SourceRegistryEntry:
    dataset_id = (row.get("dataset_id") or _dataset_id_from_url(row.get("url") or "")).strip()
    spec = yaml_specs.get(dataset_id)
    implemented = _is_ingest_ready(spec)
    loaded = _is_loaded(dataset_id)
    status = "loaded" if loaded else ("partial" if implemented else "discovered_uningested")
    frequency = str((spec or {}).get("freq") or row.get("update_freq") or "").strip()
    url = (row.get("url") or str((spec or {}).get("url") or "")).strip()
    return SourceRegistryEntry(
        id=source_id,
        name=(row.get("name") or str((spec or {}).get("name") or "")).strip(),
        category=(row.get("sector") or str((spec or {}).get("sector") or "")).strip(),
        tier=str((spec or {}).get("tier") or "catalog").strip(),
        status=status,
        implementation_state="implemented" if implemented else "not_implemented",
        load_state="loaded" if loaded else "not_loaded",
        frequency=frequency,
        in_universe_v1=True,
        primary_url=url,
        pipeline_id=source_id,
        owner_agent="catalog",
        access_mode=_access_mode(url),
        public_access_mode=_access_mode(url),
        signal_promotion_state=_signal_promotion_state(row.get("relevance") or ""),
        discovery_status="monitored" if implemented else "discovered_uningested",
        last_seen_url=url,
        cadence_expected=frequency,
        cadence_observed=(row.get("last_update") or "").strip(),
        quality_status=(row.get("audit_status") or status).strip(),
        notes=(row.get("probe_notes") or str((spec or {}).get("notes") or "")).strip(),
    )


def _custom_adapter_entries(
    yaml_specs: dict[str, dict[str, Any]],
    existing_ids: set[str],
) -> list[SourceRegistryEntry]:
    entries: list[SourceRegistryEntry] = []
    for dataset_id, spec in sorted(yaml_specs.items()):
        adapter = str(spec.get("adapter") or "socrata")
        if adapter == "socrata" or dataset_id in existing_ids:
            continue
        implemented = _is_ingest_ready(spec)
        loaded = _is_loaded(dataset_id)
        status = "loaded" if loaded else ("partial" if implemented else "discovered_uningested")
        url = str(spec.get("url") or "").strip()
        frequency = str(spec.get("freq") or "").strip()
        entries.append(
            SourceRegistryEntry(
                id=dataset_id,
                name=str(spec.get("name") or dataset_id).strip(),
                category=str(spec.get("sector") or "").strip(),
                tier=str(spec.get("tier") or "custom").strip(),
                status=status,
                implementation_state="implemented" if implemented else "not_implemented",
                load_state="loaded" if loaded else "not_loaded",
                signal_promotion_state="promoted",
                frequency=frequency,
                in_universe_v1=True,
                primary_url=url,
                pipeline_id=dataset_id,
                owner_agent="catalog",
                access_mode=_access_mode(url, adapter=adapter),
                public_access_mode="download",
                discovery_status="monitored" if implemented else "discovered_uningested",
                last_seen_url=url,
                cadence_expected=frequency,
                cadence_observed="",
                quality_status=status,
                notes=str(spec.get("notes") or "").strip(),
            )
        )
    return entries


def source_registry_summary(entries: list[SourceRegistryEntry]) -> dict[str, int]:
    universe_v1 = [entry for entry in entries if entry.in_universe_v1]
    implemented = [
        entry for entry in universe_v1 if entry.implementation_state == "implemented"
    ]
    loaded = [entry for entry in universe_v1 if entry.load_state == "loaded"]
    stale = [entry for entry in universe_v1 if entry.status == "stale"]
    blocked = [entry for entry in universe_v1 if entry.status == "blocked_external"]
    quality_fail = [entry for entry in universe_v1 if entry.status == "quality_fail"]
    healthy = [entry for entry in universe_v1 if entry.status == "loaded"]
    promoted = [
        entry for entry in universe_v1 if entry.signal_promotion_state == "promoted"
    ]
    enrichment_only = [
        entry
        for entry in universe_v1
        if entry.signal_promotion_state == "enrichment_only"
    ]
    quarantined = [
        entry for entry in universe_v1 if entry.signal_promotion_state == "quarantined"
    ]
    discovered_uningested = [
        entry
        for entry in universe_v1
        if entry.discovery_status == "discovered_uningested"
        or entry.implementation_state == "not_implemented"
    ]

    return {
        "universe_v1_sources": len(universe_v1),
        "implemented_sources": len(implemented),
        "loaded_sources": len(loaded),
        "healthy_sources": len(healthy),
        "stale_sources": len(stale),
        "blocked_external_sources": len(blocked),
        "quality_fail_sources": len(quality_fail),
        "promoted_sources": len(promoted),
        "enrichment_only_sources": len(enrichment_only),
        "quarantined_sources": len(quarantined),
        "discovered_uningested_sources": len(discovered_uningested),
    }
