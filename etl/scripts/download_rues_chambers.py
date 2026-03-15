#!/usr/bin/env python3
"""Download the public RUES chamber registry into a normalized CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from coacc_etl.pipelines.colombia_shared import clean_text
from coacc_etl.rues import RuesElasticClient


def merge_chamber_rows(
    catalog_rows: list[dict[str, object]],
    detail_rows: list[dict[str, object]],
) -> list[dict[str, str]]:
    by_code: dict[str, dict[str, str]] = {}

    for row in catalog_rows:
        code = clean_text(row.get("CODIGO_CAMARA"))
        if not code:
            continue
        by_code[code] = {
            "camera_code": code,
            "chamber_name": clean_text(row.get("DESC_CAMARA")),
            "chamber_name_full": "",
            "nit": clean_text(row.get("NIT")),
            "phone": "",
            "email": "",
            "address": "",
            "website": "",
            "correspondence_address": "",
            "responsible_contact": "",
            "privacy_policy_url": "",
            "source_url": "https://elasticprd.rues.org.co/api/ListarComboCamaras",
        }

    for row in detail_rows:
        code = clean_text(row.get("CODIGO_CAMARA"))
        if not code:
            continue
        merged = by_code.setdefault(
            code,
            {
                "camera_code": code,
                "chamber_name": clean_text(row.get("DESC_CAMARA")),
                "chamber_name_full": "",
                "nit": "",
                "phone": "",
                "email": "",
                "address": "",
                "website": "",
                "correspondence_address": "",
                "responsible_contact": "",
                "privacy_policy_url": "",
                "source_url": "https://elasticprd.rues.org.co/api/ListaCamara",
            },
        )
        merged.update(
            {
                "chamber_name": merged.get("chamber_name") or clean_text(row.get("DESC_CAMARA")),
                "chamber_name_full": clean_text(row.get("DESC_CAMARA_LARGO")),
                "phone": clean_text(row.get("TELEFONOS")) or clean_text(row.get("HDATA_TELEFONOS")),
                "email": clean_text(row.get("HDATA_EMAIL")),
                "address": clean_text(row.get("DIRECCION")),
                "website": clean_text(row.get("DIRECCION_WEB")),
                "correspondence_address": clean_text(row.get("HDATA_DIR_CORRESP")),
                "responsible_contact": clean_text(row.get("HDATA_PERSONA_RESP")),
                "privacy_policy_url": clean_text(row.get("HDATA_URL_POLITICA")),
                "source_url": "https://elasticprd.rues.org.co/api/ListaCamara",
            }
        )

    return [by_code[key] for key in sorted(by_code)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download public RUES chamber metadata")
    parser.add_argument("--output", default="../data/rues_chambers/rues_chambers.csv")
    args = parser.parse_args()

    with RuesElasticClient() as client:
        catalog = client.list_chamber_catalog()
        details = client.list_chamber_details()

    rows = merge_chamber_rows(
        catalog_rows=list(catalog.get("registros", [])),
        detail_rows=list(details.get("registros", [])),
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False)
    print(f"Wrote {len(rows):,} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
