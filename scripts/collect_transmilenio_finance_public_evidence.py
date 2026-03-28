#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUNDLE_DIR = REPO_ROOT / "audit-results" / "investigations" / "transmilenio-finance-2026-03-28"

USER_AGENT = "co-acc-research/1.0 (+https://github.com/nicoceron/co-acc)"

PRIMARY_PAGES: list[dict[str, str]] = [
    {
        "label": "Informe de Gestión 2024",
        "url": "https://www.transmilenio.gov.co/publicaciones/154438/informe-de-gestion-2024/",
    },
    {
        "label": "Informe de Gestión 2025",
        "url": "https://www.transmilenio.gov.co/comunicaciones/publicaciones/2024/informes-de-gestion/informe-de-gestion-2025",
    },
    {
        "label": "Estados Financieros 2025",
        "url": "https://www.transmilenio.gov.co/nuestra-entidad/documentacion/informacion-financiera/"
        "informacion-financiera-de-transmilenio-s-a/estados-financieros-de-transmilenio/2025",
    },
]

DIRECT_PDFS: list[dict[str, str]] = [
    {
        "label": "Auditoría OCI 2024-053 gestión financiera y contable",
        "url": "https://www.transmilenio.gov.co/files/bfa4af5a-c943-42a4-85fe-704999c59c72/"
        "ed169828-fa42-4664-a649-3688981f27a5/Informe%20OCI-2024-053%20Auditor%C3%ADa%20Gesti%C3%B3n%20"
        "de%20Informaci%C3%B3n%20Financiera%20y%20Contable.pdf",
    },
    {
        "label": "Seguimiento PTEP 2025 OCI-2025-020",
        "url": "https://www.transmilenio.gov.co/files/d5484852-b481-4c34-9706-11c4b88ccb76/"
        "797bd1bf-ba6b-4ac0-90ca-7044d186f810/Informe%20OCI-2025-020%20Seguimiento%20Programa%20de%20"
        "Transparencia%20y%20%C3%89tica%20P%C3%BAblica%20-%20PTEP.pdf",
    },
    {
        "label": "Anexo 14 Gastos de TRANSMILENIO a diciembre 2024",
        "url": "https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/"
        "1fd55b96-418d-4f99-a34e-49ad4f109644/Anexo%2014%20Gastos%20de%20TRANSMILENIO%20a%20"
        "diciembre%202024.pdf",
    },
    {
        "label": "Anexo 16 Informe de Tesorería de TRANSMILENIO 2024",
        "url": "https://www.transmilenio.gov.co/files/f208bef4-74dc-4e0d-9ee9-f5114b4800a2/"
        "ddc4317d-451e-436f-a9fa-8d48c5e4c0b4/Anexo%2016%20Informe%20de%20Tesorer%C3%ADa%20de%20"
        "TRANSMILENIO%202024.pdf",
    },
]

INTERESTING_LABELS = (
    "Anexo 14",
    "Anexo 16",
    "Informe de gestion y sostenibilidad 2025 de TRANSMILENIO",
    "Anexo 4",
    "Anexo 5",
    "Anexo 6",
    "Notas de Estados Financieros",
    "Estado de Flujos de Efectivo",
    "Estados de resultados",
    "Estado del Resultado Integral",
)


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def slugify(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "-", value.lower())
    return lowered.strip("-") or "document"


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def extract_links(html: str, base_url: str) -> list[dict[str, str]]:
    matches = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.IGNORECASE | re.DOTALL)
    links: list[dict[str, str]] = []
    for href, label in matches:
        clean_label = re.sub(r"<[^>]+>", " ", label)
        clean_label = re.sub(r"\s+", " ", clean_label).strip()
        absolute_url = urllib.parse.urljoin(base_url, href)
        if not clean_label or not absolute_url.startswith("http"):
            continue
        links.append({"label": clean_label, "url": absolute_url})
    return links


def should_keep(label: str, url: str) -> bool:
    if url.lower().endswith(".pdf"):
        return any(token.lower() in label.lower() for token in INTERESTING_LABELS)
    return False


def safe_name_from_url(url: str, fallback_label: str) -> str:
    parsed = urllib.parse.urlparse(url)
    filename = Path(parsed.path).name
    if filename:
        return filename
    return f"{slugify(fallback_label)}-{hashlib.sha1(url.encode()).hexdigest()[:10]}.pdf"


def download_file(output_dir: Path, label: str, url: str) -> dict[str, object]:
    try:
        payload = fetch_bytes(url)
        error = None
    except urllib.error.URLError as exc:  # noqa: PERF203
        payload = b""
        error = str(exc)
    filename = safe_name_from_url(url, label)
    saved_path = output_dir / filename
    if payload:
        saved_path.write_bytes(payload)
    return {
        "label": label,
        "url": url,
        "saved_path": str(saved_path.relative_to(output_dir)),
        "bytes": len(payload),
        "downloaded": bool(payload),
        "error": error,
    }


def collect_bundle(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pages: list[dict[str, object]] = []
    discovered_docs: list[dict[str, str]] = []

    for page in PRIMARY_PAGES:
        html = fetch_text(page["url"])
        links = extract_links(html, page["url"])
        kept = [item for item in links if should_keep(item["label"], item["url"])]
        pages.append({
            "label": page["label"],
            "url": page["url"],
            "discovered_links": kept,
        })
        discovered_docs.extend(kept)

    unique_docs: dict[str, dict[str, str]] = {}
    for item in [*DIRECT_PDFS, *discovered_docs]:
        unique_docs[item["url"]] = item

    downloads = [
        download_file(output_dir, item["label"], item["url"])
        for item in unique_docs.values()
    ]

    manifest = {
        "generated_at_utc": now_utc(),
        "primary_pages": pages,
        "downloaded_documents": downloads,
        "summary": {
            "documents_discovered": len(unique_docs),
            "documents_downloaded": sum(1 for item in downloads if item["downloaded"]),
            "documents_failed": sum(1 for item in downloads if not item["downloaded"]),
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect official TransMilenio finance/audit PDFs.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_BUNDLE_DIR),
        help="Directory where the evidence bundle should be written.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    manifest = collect_bundle(output_dir)
    print(json.dumps(manifest["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
