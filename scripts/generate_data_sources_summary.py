#!/usr/bin/env python3
"""Generate the summary block in docs/data-sources.md from source registry CSV."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

START_MARKER = "<!-- SOURCE_SUMMARY_START -->"
END_MARKER = "<!-- SOURCE_SUMMARY_END -->"
IMPLEMENTED_START_MARKER = "<!-- IMPLEMENTED_SOURCES_START -->"
IMPLEMENTED_END_MARKER = "<!-- IMPLEMENTED_SOURCES_END -->"

IMPLEMENTED_SOURCE_MODELS: dict[str, tuple[str, str]] = {
    "asset_disclosures": (
        "`Person`, `DeclaredAsset`, `DECLARO_BIEN`",
        "Ley 2013 asset disclosure summaries for public servants and contractors.",
    ),
    "conflict_disclosures": (
        "`Person`, `Finance`, `DECLARO_FINANZAS`",
        "Ley 2013 conflict-of-interest disclosures normalized as finance records.",
    ),
    "cuentas_claras_income_2019": (
        "`Person`, `Company`, `Election`, `CANDIDATO_EM`, `DONO_A`",
        "Campaign income disclosures for the 2019 territorial election cycle.",
    ),
    "fiscal_findings": (
        "`Company`, `Finding`, `TIENE_HALLAZGO`",
        "Official Contraloría fiscal findings tied to audited entities and radicados.",
    ),
    "fiscal_responsibility": (
        "`Company`, `Sanction`, `SANCIONADA`",
        "Official fiscal-responsibility sanctions from Contraloría / SIREF.",
    ),
    "health_providers": (
        "`Company`, `Health`, `OPERA_UNIDAD`",
        "REPS provider organizations and their health-service sites.",
    ),
    "higher_ed_enrollment": (
        "`Company`, `Education`, `MANTIENE_A`",
        "MEN enrollment aggregates by institution, program, year, and semester.",
    ),
    "igac_property_transactions": (
        "`Company`, `Finance`, `ADMINISTRA`",
        "Municipality-level IGAC property-market activity aggregated from transaction rows.",
    ),
    "mapa_inversiones_projects": (
        "`Company`, `Convenio`, `ADMINISTRA`",
        "MapaInversiones project basics tied to responsible public entities.",
    ),
    "paco_sanctions": (
        "`Company`, `Person`, `Sanction`, `SANCIONADA`",
        "PACO fiscal, disciplinary, procurement-collusion, and SECOP-fine feeds.",
    ),
    "pte_sector_commitments": (
        "`Finance`",
        "Current-year PGN sector commitment aggregates exported from PTE.",
    ),
    "pte_top_contracts": (
        "`Company`, `Finance`, `ADMINISTRA`, `BENEFICIO`, `REFERENTE_A`",
        "Top current-year PGN contracts and beneficiaries exported from PTE.",
    ),
    "registraduria_death_status_checks": (
        "`Person`",
        "Manual-imported Registraduria status checks for identity vigency and death-status screening.",
    ),
    "rues_chambers": (
        "`Company`",
        "Public RUES chamber directory merged from chamber list and detailed chamber metadata endpoints.",
    ),
    "secop_budget_commitments": (
        "`Contract`",
        "Aggregated SECOP II budget commitment balances and SIIF-linked commitment metadata.",
    ),
    "secop_cdp_requests": (
        "`Contract`",
        "CDP request balances, funding-source totals, and SIIF validation metadata merged into contracts.",
    ),
    "sgr_expense_execution": (
        "`Company`, `Finance`, `SUMINISTRO`",
        "SGR expense execution rows linked to registered third parties.",
    ),
    "sgr_projects": (
        "`Company`, `Convenio`, `ADMINISTRA`",
        "Royalty-system investment projects tied to their executing entities.",
    ),
    "siri_antecedents": (
        "`Person`, `Sanction`, `SANCIONADA`",
        "Official SIRI antecedents with person-level disciplinary and related inhabilidad records.",
    ),
    "secop_contract_additions": (
        "`Contract`",
        "Aggregated additions metadata merged back into SECOP II contract nodes.",
    ),
    "secop_contract_execution": (
        "`Contract`",
        "Aggregated execution progress and milestone metrics merged into contracts.",
    ),
    "secop_contract_modifications": (
        "`Contract`",
        "Aggregated modification and value-change metadata merged into contracts.",
    ),
    "secop_execution_locations": (
        "`Contract`",
        "Execution-location points aggregated back into SECOP II contract relationships.",
    ),
    "secop_invoices": (
        "`Contract`",
        "Invoice totals, delivery dates, and payment expectations merged into contracts.",
    ),
    "secop_ii_contracts": (
        "`Company`, `Person`, `Contract`, `GANO`, `OFFICER_OF`, `REFERENTE_A`",
        "Electronic SECOP II contracts linked back to procurement portfolio records.",
    ),
    "secop_ii_processes": (
        "`Company`, `ADJUDICOU_A`",
        "SECOP II procurement procedures normalized into buyer-to-awarded-supplier summaries.",
    ),
    "secop_offers": (
        "`Company`, `Bid`, `LICITO`, `SUMINISTRO_LICITACAO`",
        "Offer-level bidder participation and submitted values for SECOP II processes.",
    ),
    "secop_integrado": (
        "`Company`, `Contract`, `GANO`",
        "Integrated SECOP I/II contract awards using contractor document identifiers.",
    ),
    "secop_sanctions": (
        "`Company`, `Sanction`, `SANCIONADA`",
        "Combined SECOP I and SECOP II sanctions feeds.",
    ),
    "secop_suppliers": (
        "`Company`, `Person`, `OFFICER_OF`",
        "Supplier registry and legal representative metadata from SECOP II.",
    ),
    "sigep_public_servants": (
        "`Person`, `PublicOffice`, `RECIBIO_SALARIO`",
        "Current SIGEP public-servant positions with office and salary metadata.",
    ),
    "sigep_sensitive_positions": (
        "`Person`, `PublicOffice`, `RECIBIO_SALARIO`",
        "SIGEP sensitive-position subset with integrity-risk flags on offices and relationships.",
    ),
    "supersoc_top_companies": (
        "`Company`, `Finance`, `DECLARO_FINANZAS`",
        "Supersociedades top-company filings with revenue, assets, liabilities, and profit metrics.",
    ),
}


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def fetch_runtime_sources(api_base: str) -> dict[str, dict[str, str]]:
    normalized_base = api_base.strip().rstrip("/")
    if not normalized_base:
        return {}
    url = f"{normalized_base}/api/v1/meta/sources"
    try:
        with urlopen(url, timeout=20) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError):
        return {}

    runtime_sources: dict[str, dict[str, str]] = {}
    for source in payload.get("sources") or []:
        source_id = str(source.get("id") or "").strip()
        if source_id:
            runtime_sources[source_id] = source
    return runtime_sources


def overlay_runtime_rows(
    rows: list[dict[str, str]],
    runtime_sources: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    if not runtime_sources:
        return rows

    overlaid: list[dict[str, str]] = []
    for row in rows:
        source_id = (row.get("source_id") or "").strip()
        runtime = runtime_sources.get(source_id)
        if runtime is None:
            overlaid.append(row)
            continue

        updated = dict(row)
        for field in (
            "status",
            "implementation_state",
            "load_state",
            "signal_promotion_state",
            "access_mode",
            "public_access_mode",
            "discovery_status",
            "last_seen_url",
            "cadence_expected",
            "cadence_observed",
            "quality_status",
            "notes",
        ):
            value = runtime.get(field)
            if value not in (None, ""):
                updated[field] = str(value)
        overlaid.append(updated)
    return overlaid


def compute_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    universe = [row for row in rows if parse_bool(row.get("in_universe_v1", ""))]

    status = Counter((row.get("status") or "").strip() for row in universe)
    load_state = Counter((row.get("load_state") or "").strip() for row in universe)
    promotion_state = Counter(
        (row.get("signal_promotion_state") or "promoted").strip() for row in universe
    )
    implemented = [row for row in universe if (row.get("implementation_state") or "").strip() == "implemented"]

    return {
        "universe": len(universe),
        "implemented": len(implemented),
        "loaded": load_state.get("loaded", 0),
        "partial_load": load_state.get("partial", 0),
        "not_loaded": load_state.get("not_loaded", 0),
        "status_loaded": status.get("loaded", 0),
        "status_partial": status.get("partial", 0),
        "status_stale": status.get("stale", 0),
        "status_blocked_external": status.get("blocked_external", 0),
        "status_not_built": status.get("not_built", 0),
        "promoted": promotion_state.get("promoted", 0),
        "enrichment_only": promotion_state.get("enrichment_only", 0),
        "quarantined": promotion_state.get("quarantined", 0),
    }


def load_registry_rows(registry_path: Path) -> list[dict[str, str]]:
    return list(csv.DictReader(registry_path.open(encoding="utf-8", newline="")))


def render_block(counts: dict[str, int], stamp_utc: str, source_descriptor: str) -> str:
    return "\n".join(
        [
            START_MARKER,
            f"**Generated from {source_descriptor} (as-of UTC: {stamp_utc})**",
            "",
            f"- Universe v1 sources: {counts['universe']}",
            f"- Implemented pipelines: {counts['implemented']}",
            f"- Loaded sources (load_state=loaded): {counts['loaded']}",
            f"- Partial sources (load_state=partial): {counts['partial_load']}",
            f"- Not loaded sources (load_state=not_loaded): {counts['not_loaded']}",
            (
                "- Signal roles: "
                f"promoted={counts['promoted']}, "
                f"enrichment_only={counts['enrichment_only']}, "
                f"quarantined={counts['quarantined']}"
            ),
            (
                "- Status counts: "
                f"loaded={counts['status_loaded']}, "
                f"partial={counts['status_partial']}, "
                f"stale={counts['status_stale']}, "
                f"blocked_external={counts['status_blocked_external']}, "
                f"not_built={counts['status_not_built']}"
            ),
            END_MARKER,
        ]
    )


def render_implemented_block(rows: list[dict[str, str]]) -> str:
    implemented_rows = [
        row
        for row in rows
        if (row.get("implementation_state") or "").strip() == "implemented"
    ]
    implemented_rows.sort(key=lambda row: row.get("source_id", ""))

    lines = [
        IMPLEMENTED_START_MARKER,
        "| Source | Pipeline | Signal Role | What it loads | Notes |",
        "|---|---|---|---|---|",
    ]
    for row in implemented_rows:
        source_id = row.get("source_id", "")
        name = row.get("name", source_id)
        pipeline_id = row.get("pipeline_id", "")
        signal_role = (row.get("signal_promotion_state") or "promoted").strip() or "promoted"
        what_it_loads, note = IMPLEMENTED_SOURCE_MODELS.get(
            source_id,
            ("See pipeline implementation.", row.get("notes", "")),
        )
        lines.append(
            f"| {name} | `{pipeline_id}` | `{signal_role}` | {what_it_loads} | {note} |"
        )
    lines.append(IMPLEMENTED_END_MARKER)
    return "\n".join(lines)


def replace_block(doc_text: str, block: str, start_marker: str, end_marker: str) -> str:
    if start_marker in doc_text and end_marker in doc_text:
        start = doc_text.index(start_marker)
        end = doc_text.index(end_marker) + len(end_marker)
        return doc_text[:start] + block + doc_text[end:]

    lines = doc_text.splitlines()
    insertion_idx = 1 if len(lines) > 1 else len(lines)
    out = lines[:insertion_idx] + ["", block, ""] + lines[insertion_idx:]
    return "\n".join(out) + ("\n" if doc_text.endswith("\n") else "")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate summary block in docs/data-sources.md")
    parser.add_argument("--registry-path", default="docs/source_registry_co_v1.csv")
    parser.add_argument("--docs-path", default="docs/data-sources.md")
    parser.add_argument("--api-base", default="")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--stamp-utc", default="")
    args = parser.parse_args()

    registry_path = Path(args.registry_path)
    docs_path = Path(args.docs_path)
    doc_text = docs_path.read_text(encoding="utf-8")
    existing_stamp_match = re.search(r"as-of UTC:\s*([0-9T:\-]+Z)", doc_text)
    existing_stamp = existing_stamp_match.group(1) if existing_stamp_match else ""
    stamp = args.stamp_utc or (existing_stamp if args.check else datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))

    rows = load_registry_rows(registry_path)
    runtime_sources = fetch_runtime_sources(args.api_base)
    rows = overlay_runtime_rows(rows, runtime_sources)
    counts = compute_counts(rows)
    source_descriptor = "`docs/source_registry_co_v1.csv`"
    if runtime_sources and args.api_base.strip():
        source_descriptor += (
            f" + live runtime overlay from `{args.api_base.rstrip('/')}/api/v1/meta/sources`"
        )
    expected_block = render_block(counts, stamp, source_descriptor)
    rendered = replace_block(doc_text, expected_block, START_MARKER, END_MARKER)
    implemented_block = render_implemented_block(rows)
    rendered = replace_block(
        rendered,
        implemented_block,
        IMPLEMENTED_START_MARKER,
        IMPLEMENTED_END_MARKER,
    )

    if args.check:
        if rendered != doc_text:
            print("FAIL")
            print("- docs/data-sources.md summary block is out of date")
            return 1
        print("PASS")
        return 0

    docs_path.write_text(rendered, encoding="utf-8")
    print("UPDATED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
