#!/usr/bin/env python3
"""Generate the Colombia source registry from curated datos.gov.co datasets."""

from __future__ import annotations

import csv
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

CATALOG_URL = "https://api.us.socrata.com/api/catalog/v1"


@dataclass(frozen=True)
class CuratedSource:
    dataset_id: str
    source_id: str
    category: str
    tier: str
    status: str
    implementation_state: str
    load_state: str
    frequency: str
    in_universe_v1: bool
    pipeline_id: str
    owner_agent: str
    access_mode: str
    notes: str
    public_access_mode: str
    discovery_status: str


CURATED_SOURCES = [
    CuratedSource(
        dataset_id="rpmr-utcd",
        source_id="secop_integrado",
        category="contracts",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="daily",
        in_universe_v1=True,
        pipeline_id="secop_integrado",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Integrated SECOP I/II contract awards with contractor document identifiers.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="it5q-hg94",
        source_id="secop_sanctions",
        category="sanctions",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="weekly",
        in_universe_v1=True,
        pipeline_id="secop_sanctions",
        owner_agent="Agent CO",
        access_mode="api",
        notes="SECOP II fines and sanctions tied to suppliers and contracts.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="p6dx-8zbt",
        source_id="secop_ii_processes",
        category="contracts",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="daily",
        in_universe_v1=True,
        pipeline_id="secop_ii_processes",
        owner_agent="Agent CO",
        access_mode="api",
        notes="SECOP II procurement procedures and award outcomes.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="jbjy-vk9h",
        source_id="secop_ii_contracts",
        category="contracts",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="daily",
        in_universe_v1=True,
        pipeline_id="secop_ii_contracts",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Electronic contracts from SECOP II, including supplier and entity funding fields.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="qmzu-gj57",
        source_id="secop_suppliers",
        category="identity",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="daily",
        in_universe_v1=True,
        pipeline_id="secop_suppliers",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Supplier registry for SECOP II with legal representative metadata.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="mfmm-jqmq",
        source_id="secop_contract_execution",
        category="contracts",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="daily",
        in_universe_v1=True,
        pipeline_id="secop_contract_execution",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Execution progress and delivery performance for SECOP II contracts.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="cb9c-h8sn",
        source_id="secop_contract_additions",
        category="contracts",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="weekly",
        in_universe_v1=True,
        pipeline_id="secop_contract_additions",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Contract additions recorded in SECOP II.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="u8cx-r425",
        source_id="secop_contract_modifications",
        category="contracts",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="weekly",
        in_universe_v1=True,
        pipeline_id="secop_contract_modifications",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Formal contract modifications, extensions, and value changes in SECOP II.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="2jzx-383z",
        source_id="sigep_public_servants",
        category="public_sector",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="sigep_public_servants",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Active public servants registered in SIGEP.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="5u9e-g5w9",
        source_id="sigep_sensitive_positions",
        category="public_sector",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="sigep_sensitive_positions",
        owner_agent="Agent CO",
        access_mode="api",
        notes="SIGEP positions with elevated corruption or budget-control exposure.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="8tz7-h3eu",
        source_id="asset_disclosures",
        category="disclosures",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="asset_disclosures",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Ley 2013 de 2019 asset and income disclosures for public servants and contractors.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="gbry-rnq4",
        source_id="conflict_disclosures",
        category="disclosures",
        tier="P0",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="conflict_disclosures",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Ley 2013 de 2019 conflict-of-interest disclosures.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="qkv4-ek54",
        source_id="sgr_expense_execution",
        category="budget",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="sgr_expense_execution",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Execution of spending for the Sistema General de Regalías.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="mzgh-shtp",
        source_id="sgr_projects",
        category="budget",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="sgr_projects",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Public investment projects financed by the royalty system.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="jgra-rz2t",
        source_id="cuentas_claras_income_2019",
        category="electoral",
        tier="P1",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="election_cycle",
        in_universe_v1=True,
        pipeline_id="cuentas_claras_income_2019",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Campaign income reported to Cuentas Claras for 2019 local elections.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="c36g-9fc2",
        source_id="health_providers",
        category="health",
        tier="P2",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="monthly",
        in_universe_v1=True,
        pipeline_id="health_providers",
        owner_agent="Agent CO",
        access_mode="api",
        notes="REPS health providers and service sites.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
    CuratedSource(
        dataset_id="5wck-szir",
        source_id="higher_ed_enrollment",
        category="education",
        tier="P2",
        status="partial",
        implementation_state="implemented",
        load_state="not_loaded",
        frequency="annual",
        in_universe_v1=True,
        pipeline_id="higher_ed_enrollment",
        owner_agent="Agent CO",
        access_mode="api",
        notes="Higher-education enrollment statistics from MEN.",
        public_access_mode="api",
        discovery_status="monitored",
    ),
]


def fetch_catalog_entries(dataset_ids: list[str]) -> dict[str, dict[str, object]]:
    params = urllib.parse.urlencode({"ids": ",".join(dataset_ids)})
    request = urllib.request.Request(f"{CATALOG_URL}?{params}")
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    return {
        str(item["resource"]["id"]): item
        for item in payload.get("results", [])
    }


def build_rows() -> list[dict[str, str]]:
    metadata = fetch_catalog_entries([source.dataset_id for source in CURATED_SOURCES])
    rows: list[dict[str, str]] = []

    for source in CURATED_SOURCES:
        item = metadata.get(source.dataset_id, {})
        resource = item.get("resource", {})
        permalink = str(item.get("permalink") or f"https://www.datos.gov.co/d/{source.dataset_id}")
        updated_at = str(resource.get("updatedAt") or "")
        rows.append({
            "source_id": source.source_id,
            "name": str(resource.get("name") or source.source_id),
            "category": source.category,
            "tier": source.tier,
            "status": source.status,
            "implementation_state": source.implementation_state,
            "load_state": source.load_state,
            "frequency": source.frequency,
            "in_universe_v1": "true" if source.in_universe_v1 else "false",
            "primary_url": permalink,
            "pipeline_id": source.pipeline_id,
            "owner_agent": source.owner_agent,
            "access_mode": source.access_mode,
            "notes": f"Socrata dataset `{source.dataset_id}`. {source.notes}",
            "public_access_mode": source.public_access_mode,
            "discovery_status": source.discovery_status,
            "last_seen_url": permalink,
            "cadence_expected": source.frequency,
            "cadence_observed": updated_at[:10] if updated_at else "",
            "quality_status": "healthy" if source.status == "loaded" else source.status,
        })

    rows.sort(key=lambda row: row["source_id"])
    return rows


def write_registry(output_path: Path) -> None:
    fieldnames = [
        "source_id",
        "name",
        "category",
        "tier",
        "status",
        "implementation_state",
        "load_state",
        "frequency",
        "in_universe_v1",
        "primary_url",
        "pipeline_id",
        "owner_agent",
        "access_mode",
        "notes",
        "public_access_mode",
        "discovery_status",
        "last_seen_url",
        "cadence_expected",
        "cadence_observed",
        "quality_status",
    ]
    rows = build_rows()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    output_path = Path("docs/source_registry_co_v1.csv")
    write_registry(output_path)
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
