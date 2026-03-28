#!/usr/bin/env python3
"""Scan material real-pattern coverage from the live API."""

from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

DEFAULT_API_BASE = "http://localhost:8000"
DEFAULT_COMPANY_LIMIT = 200
DEFAULT_PEOPLE_LIMIT = 100
DEFAULT_OUTPUT = "docs/real_pattern_coverage_2026-03-21.md"

PATTERN_LABELS = {
    "contract_suspension_stacking": "Suspensiones repetidas en contratos públicos",
    "interadministrative_channel_stacking": "Convenios interadministrativos apilados con contratación regular",
    "invoice_execution_gap": "Facturación o pagos por delante de la ejecución",
    "low_competition_bidding": "Baja competencia o invitación directa",
    "public_money_channel_stacking": "Canales públicos múltiples sobre el mismo actor",
    "public_official_supplier_overlap": "Proveedor con directivo o vínculo en cargo público",
    "sanctioned_supplier_record": "Proveedor con antecedentes sancionatorios",
    "sensitive_public_official_supplier_overlap": "Proveedor ligado a cargo sensible",
}


def fetch_json(url: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "coacc-pattern-scan/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def fetch_company_patterns(api_base: str, row: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    company_ref = row.get("document_id") or row.get("name")
    url = (
        f"{api_base.rstrip('/')}/api/v1/public/patterns/company/"
        f"{urllib.parse.quote(str(company_ref))}?lang=es"
    )
    payload = fetch_json(url)
    return row, list(payload.get("patterns") or [])


def summarize_data(data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in data.items()
        if isinstance(value, (int, float, str, bool))
        and value not in (0, 0.0, "", False)
    }


def build_markdown(
    *,
    company_limit: int,
    people_limit: int,
    company_counts: Counter[str],
    company_examples: dict[str, list[dict[str, Any]]],
    person_counts: Counter[str],
    person_examples: dict[str, list[dict[str, Any]]],
) -> str:
    lines = [
        "# Real Pattern Coverage Scan",
        "",
        f"- Company queue scanned: `{company_limit}` real company leads",
        f"- People queue scanned: `{people_limit}` real people leads",
        "- Source: live API over the current loaded public graph",
        "",
        "## Company Exact Patterns",
        "",
    ]

    if not company_counts:
        lines.append("No exact company patterns fired in the scanned queue.")
    else:
        for pattern_id, count in company_counts.most_common():
            label = PATTERN_LABELS.get(pattern_id, pattern_id)
            lines.append(f"### {label}")
            lines.append("")
            lines.append(f"- Pattern ID: `{pattern_id}`")
            lines.append(f"- Hits: `{count}`")
            for example in company_examples.get(pattern_id, [])[:5]:
                lines.append(
                    "- Example: "
                    f"`{example['document_id']}` {example['name']} "
                    f"(risk `{example['risk']}`) "
                    f"{json.dumps(example['summary'], ensure_ascii=False)}"
                )
            lines.append("")

    lines.extend([
        "## People Alert Coverage",
        "",
    ])

    if not person_counts:
        lines.append("No people alerts fired in the scanned queue.")
    else:
        for alert_type, count in person_counts.most_common():
            label = PATTERN_LABELS.get(alert_type, alert_type.replace("_", " "))
            lines.append(f"### {label}")
            lines.append("")
            lines.append(f"- Alert ID: `{alert_type}`")
            lines.append(f"- Hits: `{count}`")
            for example in person_examples.get(alert_type, [])[:5]:
                lines.append(
                    "- Example: "
                    f"`{example['document_id']}` {example['name']} "
                    f"(risk `{example['risk']}`) "
                    f"\"{example['reason_text']}\""
                )
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan real pattern coverage from the live API")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--company-limit", type=int, default=DEFAULT_COMPANY_LIMIT)
    parser.add_argument("--people-limit", type=int, default=DEFAULT_PEOPLE_LIMIT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    companies_payload = fetch_json(
        f"{args.api_base.rstrip('/')}/api/v1/meta/watchlist/companies?limit={args.company_limit}"
    )
    people_payload = fetch_json(
        f"{args.api_base.rstrip('/')}/api/v1/meta/watchlist/people?limit={args.people_limit}"
    )
    companies = list(companies_payload.get("companies") or [])
    people = list(people_payload.get("people") or [])

    company_counts: Counter[str] = Counter()
    company_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_company_patterns, args.api_base, row) for row in companies]
        for future in as_completed(futures):
            row, patterns = future.result()
            for pattern in patterns:
                pattern_id = str(pattern.get("pattern_id") or "")
                if not pattern_id:
                    continue
                company_counts[pattern_id] += 1
                if len(company_examples[pattern_id]) < 5:
                    company_examples[pattern_id].append({
                        "name": row.get("name"),
                        "document_id": row.get("document_id"),
                        "risk": row.get("suspicion_score"),
                        "summary": summarize_data(pattern.get("data") or {}),
                    })

    person_counts: Counter[str] = Counter()
    person_examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in people:
        for alert in row.get("alerts") or []:
            alert_type = str(alert.get("alert_type") or "")
            if not alert_type:
                continue
            person_counts[alert_type] += 1
            if len(person_examples[alert_type]) < 5:
                person_examples[alert_type].append({
                    "name": row.get("name"),
                    "document_id": row.get("document_id"),
                    "risk": row.get("suspicion_score"),
                    "reason_text": alert.get("reason_text"),
                })

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_markdown(
            company_limit=len(companies),
            people_limit=len(people),
            company_counts=company_counts,
            company_examples=company_examples,
            person_counts=person_counts,
            person_examples=person_examples,
        ),
        encoding="utf-8",
    )

    print(json.dumps({
        "output": str(output_path),
        "company_patterns": company_counts.most_common(),
        "people_alerts": person_counts.most_common(),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
