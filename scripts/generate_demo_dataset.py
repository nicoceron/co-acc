#!/usr/bin/env python3
"""Generate a public-safe synthetic graph dataset for the Colombia demo."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


def build_payload() -> dict[str, object]:
    nodes = [
        {
            "id": "buyer:movilidad",
            "label": "Secretaria Distrital de Movilidad",
            "type": "Company",
            "properties": {
                "document_id": "899999061",
                "nit": "899999061",
                "department": "BOGOTA D.C.",
            },
        },
        {
            "id": "company:capital-urbano",
            "label": "Consorcio Capital Urbano SAS",
            "type": "Company",
            "properties": {
                "document_id": "901234567",
                "nit": "901234567",
                "sector": "infrastructure",
                "city": "BOGOTA D.C.",
            },
        },
        {
            "id": "company:salud-capital",
            "label": "Red Salud Capital SAS",
            "type": "Company",
            "properties": {
                "document_id": "901234569",
                "nit": "901234569",
                "sector": "health",
                "city": "BOGOTA D.C.",
            },
        },
        {
            "id": "person:laura",
            "label": "Laura Catalina Moreno",
            "type": "Person",
            "properties": {
                "document_id": "52100123",
                "cedula": "52100123",
            },
        },
        {
            "id": "office:laura",
            "label": "Directora de Contratacion",
            "type": "PublicOffice",
            "properties": {
                "office_id": "office-laura-contract",
                "sensitive_position": True,
            },
        },
        {
            "id": "election:bogota-2019",
            "label": "Campana Distrital Bogota 2019",
            "type": "Election",
            "properties": {
                "election_id": "cc2019-bogota-001",
                "year": 2019,
            },
        },
        {
            "id": "finance:conflict-laura",
            "label": "Conflict Disclosure Laura",
            "type": "Finance",
            "properties": {
                "finance_id": "conflict-laura-001",
                "type": "CONFLICT_DISCLOSURE",
            },
        },
        {
            "id": "finance:sgr-capital-1",
            "label": "SGR C1 001",
            "type": "Finance",
            "properties": {
                "finance_id": "sgr-c1-001",
                "type": "SGR_EXPENSE_EXECUTION",
                "value": 800000,
            },
        },
        {
            "id": "bid:movilidad-1",
            "label": "Mantenimiento semaforico",
            "type": "Bid",
            "properties": {
                "bid_id": "BID-MOV-001",
                "offer_count": 1,
                "direct_invitation": True,
            },
        },
        {
            "id": "sanction:red-salud",
            "label": "SIRI-001",
            "type": "Sanction",
            "properties": {
                "sanction_id": "SIRI-001",
                "date_start": "2025-01-01",
                "date_end": "2025-12-31",
            },
        },
        {
            "id": "health:kennedy",
            "label": "Hospital Kennedy",
            "type": "Health",
            "properties": {
                "reps_code": "H001",
            },
        },
    ]

    edges = [
        {
            "id": "rel:salary-laura",
            "source": "person:laura",
            "target": "office:laura",
            "type": "RECIBIO_SALARIO",
            "properties": {"source": "synthetic", "sensitive_position": True},
        },
        {
            "id": "rel:officer-laura",
            "source": "person:laura",
            "target": "company:capital-urbano",
            "type": "OFFICER_OF",
            "properties": {"source": "synthetic", "role": "LEGAL_REPRESENTATIVE"},
        },
        {
            "id": "rel:donation-laura",
            "source": "person:laura",
            "target": "election:bogota-2019",
            "type": "DONO_A",
            "properties": {"source": "synthetic", "receipt": "DON-001", "value": 120000000},
        },
        {
            "id": "rel:conflict-laura",
            "source": "person:laura",
            "target": "finance:conflict-laura",
            "type": "DECLARO_FINANZAS",
            "properties": {"source": "synthetic"},
        },
        {
            "id": "rel:award-capital",
            "source": "buyer:movilidad",
            "target": "company:capital-urbano",
            "type": "CONTRATOU",
            "properties": {
                "summary_id": "SECOP-BOG-001",
                "total_value": 280000,
                "contract_count": 4,
                "average_value": 70000,
                "invoice_total_value": 480000,
                "commitment_total_value": 300000,
                "execution_actual_progress_max": 15,
                "buyer_name": "SECRETARIA DISTRITAL DE MOVILIDAD",
                "department": "BOGOTA D.C.",
                "city": "BOGOTA D.C.",
            },
        },
        {
            "id": "rel:offer-capital",
            "source": "company:capital-urbano",
            "target": "bid:movilidad-1",
            "type": "SUMINISTRO_LICITACAO",
            "properties": {"source": "synthetic", "offer_value_total": 280000},
        },
        {
            "id": "rel:sgr-capital",
            "source": "company:capital-urbano",
            "target": "finance:sgr-capital-1",
            "type": "SUMINISTRO",
            "properties": {"source": "synthetic"},
        },
        {
            "id": "rel:sanction-salud",
            "source": "company:salud-capital",
            "target": "sanction:red-salud",
            "type": "SANCIONADA",
            "properties": {"source": "synthetic"},
        },
        {
            "id": "rel:health-salud",
            "source": "company:salud-capital",
            "target": "health:kennedy",
            "type": "OPERA_UNIDAD",
            "properties": {"source": "synthetic"},
        },
    ]

    return {
        "meta": {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "generator_version": "2.0.0",
            "source": "synthetic",
            "theme": "bogota-corruption-practices",
        },
        "nodes": nodes,
        "edges": edges,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic graph demo dataset")
    parser.add_argument(
        "--output",
        default="data/demo/synthetic_graph.json",
        help="Output path for synthetic dataset",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    output_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote synthetic dataset to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
