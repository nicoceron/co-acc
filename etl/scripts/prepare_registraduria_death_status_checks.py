#!/usr/bin/env python3
"""Normalize manually collected Registraduria status-check results to the ETL schema."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from coacc_etl.pipelines.colombia_shared import (
    clean_text,
    parse_flag,
    read_csv_normalized_with_fallback,
)
from coacc_etl.transforms import strip_document

_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "document_id": (
        "document_id",
        "cedula",
        "numero_documento",
        "numero_de_documento",
        "n_documento",
        "documento",
        "identificacion",
    ),
    "status": (
        "status",
        "estado",
        "estado_documento",
        "vigencia",
        "resultado",
    ),
    "status_detail": (
        "status_detail",
        "detalle",
        "detalle_estado",
        "observacion",
        "descripcion",
    ),
    "checked_at": (
        "checked_at",
        "fecha_consulta",
        "fecha_verificacion",
        "consulted_at",
        "fecha",
    ),
    "source_url": (
        "source_url",
        "consulta_url",
        "url",
        "origen",
    ),
    "is_deceased": (
        "is_deceased",
        "fallecido",
        "deceased",
    ),
}


def _pick_value(row: pd.Series, *aliases: str) -> str:
    for alias in aliases:
        value = clean_text(row.get(alias))
        if value:
            return value
    return ""


def _infer_deceased(status: str, detail: str, provided: str) -> bool | None:
    if provided:
        return parse_flag(provided)

    combined = f"{status} {detail}".upper()
    if not combined.strip():
        return None
    if any(token in combined for token in ("MUERTE", "FALLECID", "DEFUNCION")):
        return True
    if any(token in combined for token in ("VIGENTE", "ACTIVA", "VALIDA")):
        return False
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize Registraduria status-check CSVs")
    parser.add_argument("--input", required=True, help="Raw CSV exported or collected from checks")
    parser.add_argument(
        "--output",
        default="../data/registraduria_death_status_checks/registraduria_death_status_checks.csv",
    )
    args = parser.parse_args()

    frame = read_csv_normalized_with_fallback(args.input, dtype=str, keep_default_na=False)
    canonical_rows: list[dict[str, str | bool | None]] = []

    for _, row in frame.iterrows():
        document_id = strip_document(_pick_value(row, *_COLUMN_ALIASES["document_id"]))
        status = _pick_value(row, *_COLUMN_ALIASES["status"])
        status_detail = _pick_value(row, *_COLUMN_ALIASES["status_detail"])
        checked_at = _pick_value(row, *_COLUMN_ALIASES["checked_at"])
        source_url = _pick_value(row, *_COLUMN_ALIASES["source_url"])
        provided_is_deceased = _pick_value(row, *_COLUMN_ALIASES["is_deceased"])

        if not document_id or not status:
            continue

        canonical_rows.append(
            {
                "document_id": document_id,
                "status": status,
                "status_detail": status_detail,
                "checked_at": checked_at,
                "source_url": source_url or "https://www.registraduria.gov.co/Registro-de-defuncion.html",
                "is_deceased": _infer_deceased(status, status_detail, provided_is_deceased),
            }
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(canonical_rows).to_csv(output_path, index=False)
    print(f"Wrote {len(canonical_rows):,} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
