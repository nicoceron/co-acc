from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "api" / "src" / "coacc" / "data" / "watchlists"

PACO_MULTAS_SECOP = DATA_DIR / "paco_sanctions" / "multas_secop.csv"
MAPA_INVERSIONES = DATA_DIR / "mapa_inversiones_projects" / "mapa_inversiones_projects.csv"

PACO_COLUMNS = [
    "buyer_name",
    "buyer_document_id",
    "territorial_scope",
    "entity_scope",
    "resolution_name",
    "supplier_document_id",
    "supplier_name",
    "contract_id",
    "value",
    "decision_date",
    "source_url",
    "extra_1",
    "extra_2",
    "department",
    "municipality",
]

INVALID_TEXT_VALUES = {"NO DEFINIDO", "NoDefinido", "NO_DEFINIDO", "nan", "None"}


def _clean_text(raw: object) -> str:
    text = str(raw or "").strip()
    if text in INVALID_TEXT_VALUES:
        return ""
    return text


def _looks_like_test_record(text: str) -> bool:
    normalized = text.upper()
    return "PRUEBA" in normalized or "TEST" in normalized or "DEMO" in normalized


def _parse_amount(raw: object) -> float:
    text = str(raw or "").strip().replace(".", "").replace(",", ".")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        cleaned = str(raw or "").strip().replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0


def _parse_percent(raw: object) -> float:
    text = str(raw or "").strip().replace("%", "").replace(",", ".")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _split_responsible_territory(raw: str) -> tuple[str, str] | None:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) < 2:
        return None
    if len(parts) % 2 == 0:
        midpoint = len(parts) // 2
        if parts[:midpoint] == parts[midpoint:]:
            parts = parts[:midpoint]
    municipality = parts[0]
    department = ", ".join(parts[1:])
    if not municipality or not department:
        return None
    return municipality, department


def build_buyer_snapshot() -> list[dict[str, object]]:
    buyers: dict[str, dict[str, object]] = {}
    supplier_values: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    supplier_docs: dict[str, dict[str, str]] = defaultdict(dict)

    with PACO_MULTAS_SECOP.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) < 9:
                continue
            padded = (row + [""] * len(PACO_COLUMNS))[: len(PACO_COLUMNS)]
            record = dict(zip(PACO_COLUMNS, padded, strict=False))
            buyer_document_id = _clean_text(record["buyer_document_id"])
            buyer_name = _clean_text(record["buyer_name"])
            supplier_name = _clean_text(record["supplier_name"])
            supplier_document_id = _clean_text(record["supplier_document_id"])
            contract_id = _clean_text(record["contract_id"])
            value = _parse_amount(record["value"])
            if not buyer_name and not buyer_document_id:
                continue
            if buyer_name and _looks_like_test_record(buyer_name):
                continue

            buyer_id = buyer_document_id or buyer_name
            entry = buyers.setdefault(
                buyer_id,
                {
                    "buyer_id": buyer_id,
                    "buyer_name": buyer_name or buyer_document_id,
                    "buyer_document_id": buyer_document_id or None,
                    "contract_count": 0,
                    "contract_value": 0.0,
                    "supplier_names": set(),
                    "sanctioned_supplier_contract_count": 0,
                    "sanctioned_supplier_value": 0.0,
                },
            )
            entry["contract_count"] = int(entry["contract_count"]) + 1
            entry["contract_value"] = float(entry["contract_value"]) + value
            entry["sanctioned_supplier_contract_count"] = (
                int(entry["sanctioned_supplier_contract_count"]) + 1
            )
            entry["sanctioned_supplier_value"] = float(entry["sanctioned_supplier_value"]) + value
            if supplier_name:
                cast_set = entry["supplier_names"]
                assert isinstance(cast_set, set)
                cast_set.add(supplier_name)
                supplier_values[buyer_id][supplier_name] += value
                if supplier_document_id:
                    supplier_docs[buyer_id][supplier_name] = supplier_document_id
            if contract_id and not supplier_name:
                supplier_values[buyer_id][contract_id] += value

    rows: list[dict[str, object]] = []
    for buyer_id, entry in buyers.items():
        supplier_breakdown = supplier_values.get(buyer_id, {})
        top_supplier_name = None
        top_supplier_value = 0.0
        if supplier_breakdown:
            top_supplier_name, top_supplier_value = max(
                supplier_breakdown.items(),
                key=lambda item: (item[1], item[0]),
            )
        contract_value = float(entry["contract_value"])
        top_supplier_share = top_supplier_value / contract_value if contract_value > 0 else 0.0
        supplier_count = len(entry["supplier_names"]) if entry["supplier_names"] else 0
        signal_types = 1 + (1 if top_supplier_share >= 0.35 else 0)
        suspicion_score = min(
            96,
            42
            + min(int(entry["sanctioned_supplier_contract_count"]) * 4, 28)
            + (12 if top_supplier_share >= 0.35 else 0),
        )
        rows.append(
            {
                "buyer_id": buyer_id,
                "buyer_name": entry["buyer_name"],
                "buyer_document_id": entry["buyer_document_id"],
                "suspicion_score": suspicion_score,
                "signal_types": signal_types,
                "contract_count": int(entry["contract_count"]),
                "contract_value": contract_value,
                "supplier_count": supplier_count,
                "top_supplier_name": top_supplier_name,
                "top_supplier_document_id": supplier_docs.get(buyer_id, {}).get(top_supplier_name or ""),
                "top_supplier_share": top_supplier_share,
                "low_competition_contract_count": 0,
                "direct_invitation_contract_count": 0,
                "sanctioned_supplier_contract_count": int(
                    entry["sanctioned_supplier_contract_count"]
                ),
                "sanctioned_supplier_value": float(entry["sanctioned_supplier_value"]),
                "official_overlap_contract_count": 0,
                "official_overlap_supplier_count": 0,
                "capacity_mismatch_supplier_count": 0,
                "discrepancy_contract_count": 0,
                "discrepancy_value": 0.0,
            }
        )

    rows.sort(
        key=lambda row: (
            -int(row["suspicion_score"]),
            -float(row["sanctioned_supplier_value"]),
            -float(row["top_supplier_share"]),
            str(row["buyer_name"]),
        )
    )
    return rows[:25]


def build_territory_snapshot() -> list[dict[str, object]]:
    territories: dict[str, dict[str, object]] = {}

    with MAPA_INVERSIONES.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            responsible = str(record.get("EntidadResponsable", "")).strip()
            if not responsible or "," not in responsible:
                continue
            territory_parts = _split_responsible_territory(responsible)
            if territory_parts is None:
                continue
            municipality, department = territory_parts
            territory_id = f"{municipality}|{department}"
            total_value = _parse_amount(record.get("ValorTotalProyecto"))
            executed_value = _parse_amount(record.get("ValorEjecutadoProyecto"))
            physical_progress = _parse_percent(record.get("AvanceFisico"))
            financial_progress = _parse_percent(record.get("AvanceFinanciero"))
            progress_gap = max(0.0, financial_progress - physical_progress)
            has_gap = progress_gap >= 30.0

            entry = territories.setdefault(
                territory_id,
                {
                    "territory_id": territory_id,
                    "territory_name": f"{municipality}, {department}",
                    "department": department,
                    "municipality": municipality,
                    "project_count": 0,
                    "project_value": 0.0,
                    "responsible_entities": set(),
                    "gap_count": 0,
                    "gap_value": 0.0,
                },
            )
            entry["project_count"] = int(entry["project_count"]) + 1
            entry["project_value"] = float(entry["project_value"]) + total_value
            cast_set = entry["responsible_entities"]
            assert isinstance(cast_set, set)
            cast_set.add(responsible)
            if has_gap:
                entry["gap_count"] = int(entry["gap_count"]) + 1
                entry["gap_value"] = float(entry["gap_value"]) + executed_value

    rows: list[dict[str, object]] = []
    for territory_id, entry in territories.items():
        gap_count = int(entry["gap_count"])
        if gap_count <= 0:
            continue
        suspicion_score = min(92, 36 + gap_count * 5 + (2 if entry["project_count"] >= 10 else 0))
        rows.append(
            {
                "territory_id": territory_id,
                "territory_name": entry["territory_name"],
                "department": entry["department"],
                "municipality": entry["municipality"],
                "suspicion_score": suspicion_score,
                "signal_types": 1,
                "contract_count": int(entry["project_count"]),
                "contract_value": float(entry["project_value"]),
                "buyer_count": len(entry["responsible_entities"]),
                "supplier_count": 0,
                "top_supplier_name": None,
                "top_supplier_share": 0.0,
                "low_competition_contract_count": 0,
                "direct_invitation_contract_count": 0,
                "sanctioned_supplier_contract_count": 0,
                "sanctioned_supplier_value": 0.0,
                "official_overlap_contract_count": 0,
                "capacity_mismatch_supplier_count": 0,
                "discrepancy_contract_count": gap_count,
                "discrepancy_value": float(entry["gap_value"]),
            }
        )

    rows.sort(
        key=lambda row: (
            -int(row["suspicion_score"]),
            -float(row["discrepancy_value"]),
            -float(row["contract_value"]),
            str(row["territory_name"]),
        )
    )
    return rows[:25]


def build_snapshots() -> dict[str, int]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    buyers = build_buyer_snapshot()
    territories = build_territory_snapshot()
    generated_at = datetime.now(UTC).isoformat()

    (OUTPUT_DIR / "buyers.json").write_text(
        json.dumps({"generated_at": generated_at, "buyers": buyers}, ensure_ascii=False, indent=2)
    )
    (OUTPUT_DIR / "territories.json").write_text(
        json.dumps(
            {"generated_at": generated_at, "territories": territories},
            ensure_ascii=False,
            indent=2,
        )
    )
    return {"buyers": len(buyers), "territories": len(territories)}


if __name__ == "__main__":
    print(json.dumps(build_snapshots(), ensure_ascii=False))
