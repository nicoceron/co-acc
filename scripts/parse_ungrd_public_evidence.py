#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from pathlib import Path

from pypdf import PdfReader


REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE_DIR_DEFAULT = REPO_ROOT / "audit-results" / "investigations" / "ungrd-public-evidence-2026-03-27"


KEYWORDS = [
    "impoamericana",
    "ungrd",
    "ratificacion",
    "perfeccionamiento",
    "instruccion",
    "carrotanque",
    "carrotanques",
    "proveeduria",
    "orden",
    "fiduprevisora",
    "la guajira",
    "luis carlos barreto",
    "olmedo",
    "sneyder",
]

REFERENCE_PATTERNS = [
    re.compile(r"\b\d{4}\s*-+\s*PPAL\d*\s*-+\s*\d{3,4}\s*-+\s*\d{4}\b", re.IGNORECASE),
    re.compile(r"\bSMD\s*-+\s*[A-Z-]+\s*-+\s*\d{2,4}\s*-+\s*\d{4}\b", re.IGNORECASE),
    re.compile(r"\b\d{4}\s*-+\s*\d{4}\s*-+\s*\d{4}\b"),
]
NIT_PATTERN = re.compile(r"NIT\.?\s*:?\s*([0-9][0-9.\-]{6,})", re.IGNORECASE)
EMAIL_PATTERN = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}\b")

ORGANIZATION_PATTERNS = {
    "UNGRD": ["unidad nacional para la gestión del riesgo de desastres", "ungrd"],
    "FNGRD": ["fondo nacional de gestión del riesgo de desastres", "fngrd"],
    "FIDUPREVISORA": ["fiduciaria la previsora s.a.", "fiduprevisora s.a.", "fiduprevisora"],
    "IMPOAMERICANA ROGER S.A.S.": ["impoamericana roger s.a.s.", "impoamericana roger sas"],
}

PERSON_PATTERNS = {
    "ROGER ALEXANDER PASTAS FUERTES": ["roger alexander pastas fuertes"],
    "VICTOR ANDRES MEZA GALVAN": ["victor andres meza galvan", "víctor andrés meza galván"],
    "CARLOS FERNANDO LOPEZ PASTRANA": ["carlos fernando lopez pastrana", "carlos fernando lópez pastrana"],
}


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def classify_document(filename: str) -> str:
    lowered = filename.lower()
    if "ratificacion" in lowered:
        return "ratificacion"
    if "perfeccionamiento" in lowered:
        return "perfeccionamiento"
    if "instruccion" in lowered:
        return "instruccion"
    return "documento_contractual"


def resolve_bundle_dir(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def extract_text_with_pypdf(path: Path) -> str:
    reader = PdfReader(str(path))
    pages: list[str] = []
    for page in reader.pages[:8]:
        try:
            pages.append(page.extract_text() or "")
        except Exception:  # noqa: BLE001
            continue
    return normalize_text(" ".join(pages))


def extract_text_with_pdftotext(path: Path) -> str:
    try:
        result = subprocess.run(
            ["/opt/homebrew/bin/pdftotext", str(path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:  # noqa: BLE001
        return ""
    return normalize_text(result.stdout)


def extract_reference_codes(text: str) -> list[str]:
    codes: list[str] = []
    for pattern in REFERENCE_PATTERNS:
        codes.extend(pattern.findall(text))
    normalized = {
        re.sub(r"\s*-\s*", "-", code.strip().upper()).replace("--", "-")
        for code in codes
        if code.strip()
    }
    return sorted(normalized)


def extract_nits(text: str) -> list[str]:
    values: dict[str, str] = {}
    for match in NIT_PATTERN.findall(text):
        digits = re.sub(r"[^0-9\-]", "", match).strip("-")
        if digits:
            key = re.sub(r"[^0-9]", "", digits)
            current = values.get(key)
            if current is None or len(digits) > len(current):
                values[key] = digits
    return sorted(values.values())


def detect_named_patterns(text: str, patterns: dict[str, list[str]]) -> list[str]:
    lowered = text.lower()
    hits: list[str] = []
    for label, candidates in patterns.items():
        if any(candidate in lowered for candidate in candidates):
            hits.append(label)
    return hits


def extract_pdf_record(base_dir: Path, item: dict[str, object]) -> dict[str, object]:
    path = Path(str(item["saved_path"]))
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    reader = PdfReader(str(path))
    joined = extract_text_with_pypdf(path)
    text_source = "pypdf"
    if len(joined) < 120:
        pdftotext_joined = extract_text_with_pdftotext(path)
        if len(pdftotext_joined) > len(joined):
            joined = pdftotext_joined
            text_source = "pdftotext"
    lowered = joined.lower()
    keyword_hits = [keyword for keyword in KEYWORDS if keyword in lowered]
    reference_codes = extract_reference_codes(joined)
    nits = extract_nits(joined)
    emails = sorted(set(EMAIL_PATTERN.findall(joined)))
    organizations = detect_named_patterns(joined, ORGANIZATION_PATTERNS)
    named_people = detect_named_patterns(joined, PERSON_PATTERNS)
    return {
        "filename": path.name,
        "source_url": item["url"],
        "saved_path": str(path),
        "bytes": int(item.get("bytes") or 0),
        "page_count": len(reader.pages),
        "doc_type": classify_document(path.name),
        "text_source": text_source,
        "keyword_hits": keyword_hits,
        "reference_codes": reference_codes,
        "nits": nits,
        "emails": emails,
        "organizations": organizations,
        "named_people": named_people,
        "excerpt": joined[:2000],
    }


def build_summary(manifest: dict[str, object], documents: list[dict[str, object]]) -> dict[str, object]:
    pages = manifest.get("pages") or []
    summary: dict[str, object] = {
        "monitor_pdf_count": len(documents),
        "secop_community_link_count": 0,
        "contratos_gov_link_count": 0,
        "google_drive_link_count": 0,
    }
    for page in pages:
        if not isinstance(page, dict):
            continue
        groups = page.get("link_groups") or {}
        if not isinstance(groups, dict):
            continue
        summary["secop_community_link_count"] = summary.get("secop_community_link_count", 0) + len(
            groups.get("secop_community") or []
        )
        summary["contratos_gov_link_count"] = summary.get("contratos_gov_link_count", 0) + len(
            groups.get("contratos_gov") or []
        )
        summary["google_drive_link_count"] = summary.get("google_drive_link_count", 0) + len(
            groups.get("google_drive") or []
        )
    return summary


def build_network_summary(documents: list[dict[str, object]]) -> dict[str, object]:
    reference_codes: set[str] = set()
    nits: set[str] = set()
    emails: set[str] = set()
    organizations: set[str] = set()
    named_people: set[str] = set()
    for document in documents:
        reference_codes.update(str(value) for value in (document.get("reference_codes") or []))
        nits.update(str(value) for value in (document.get("nits") or []))
        emails.update(str(value) for value in (document.get("emails") or []))
        organizations.update(str(value) for value in (document.get("organizations") or []))
        named_people.update(str(value) for value in (document.get("named_people") or []))
    return {
        "reference_codes": sorted(reference_codes),
        "nits": sorted(nits),
        "emails": sorted(emails),
        "organizations": sorted(organizations),
        "named_people": sorted(named_people),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bundle-dir",
        default=str(BUNDLE_DIR_DEFAULT),
    )
    args = parser.parse_args()

    bundle_dir = resolve_bundle_dir(args.bundle_dir)
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    documents = [
        extract_pdf_record(bundle_dir, item)
        for item in (manifest.get("downloaded_monitor_pdfs") or [])
        if isinstance(item, dict)
    ]
    structured = {
        "generated_at_utc": now_utc(),
        "bundle_dir": str(bundle_dir),
        "source_manifest": str(manifest_path),
        "summary": build_summary(manifest, documents),
        "network_summary": build_network_summary(documents),
        "documents": documents,
        "links": {
            "pages": [
                {
                    "name": page.get("name"),
                    "url": page.get("url"),
                    "title": page.get("title"),
                    "link_groups": page.get("link_groups"),
                }
                for page in (manifest.get("pages") or [])
                if isinstance(page, dict)
            ],
            "graph_evidence_refs": manifest.get("graph_evidence_refs") or {},
        },
    }
    output_path = bundle_dir / "structured-evidence.json"
    output_path.write_text(json.dumps(structured, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(output_path),
                "documents": len(documents),
                "secop_community_links": structured["summary"]["secop_community_link_count"],
                "contratos_gov_links": structured["summary"]["contratos_gov_link_count"],
                "google_drive_links": structured["summary"]["google_drive_link_count"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
