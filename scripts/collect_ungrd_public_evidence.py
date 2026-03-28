#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; CO-ACC UNGRD Collector/1.0)"
TIMEOUT_SECONDS = 30
MAX_BYTES = 20_000_000
SLEEP_SECONDS = 0.15


@dataclass(frozen=True)
class Target:
    name: str
    url: str


TARGETS = [
    Target(
        "monitor-ungrd-case-page",
        "https://www.monitorciudadano.co/accion-publica-anticorrupcion/contratos-publicos-caso-ungrd/",
    ),
    Target(
        "ungrd-transparency-contracts",
        "https://portal.gestiondelriesgo.gov.co/Paginas/Transparencia-Contratos.aspx",
    ),
]


GRAPH_EVIDENCE_REFS = {
    "maria_alejandra_benavides_soto": [
        "CO1.PCCNTR.1451509",
        "3.151-2020",
        "CO1.PCCNTR.2163423",
        "3.006-2021",
        "CO1.PCCNTR.3304969",
    ],
    "luis_carlos_barreto_gantiva": [
        "19-12-9687480",
        "MG-CPS-089-2019",
        "17-12-7224406",
        "CPS No.117-2017",
        "21-13-12284857",
        "MC/012/2021",
    ],
    "sneyder_augusto_pinilla_alvarez": [
        "CO1.PCCNTR.2391534",
        "819-2021",
        "CO1.PCCNTR.3454219",
        "0756-2022",
        "12-12-1309810",
        "OPS-232-2012",
        "12-12-838469",
        "OPS-007-2012",
        "12-12-988357",
        "607-2022",
    ],
}


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def slugify_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    if path.endswith("/"):
        path += "index"
    suffix = Path(path).suffix or ".html"
    base = re.sub(r"[^a-zA-Z0-9._-]+", "_", f"{parsed.netloc}{path}")
    if parsed.query:
        base += "_" + re.sub(r"[^a-zA-Z0-9._-]+", "_", parsed.query)
    if not base.endswith(suffix):
        base += suffix
    return base


def fetch(url: str) -> tuple[int | None, dict[str, str], bytes, str | None]:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = response.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                data = data[:MAX_BYTES]
            return response.status, dict(response.headers.items()), data, None
    except Exception as exc:  # noqa: BLE001
        return None, {}, b"", repr(exc)


def normalize_link(base_url: str, raw: str) -> str | None:
    raw = unescape(raw.strip())
    if not raw or raw.startswith(("javascript:", "mailto:", "tel:", "#", "data:")):
        return None
    joined = urljoin(base_url, raw)
    parsed = urlparse(joined)
    if parsed.scheme not in {"http", "https"}:
        return None
    return joined


def extract_links(body: str, base_url: str) -> list[str]:
    links: list[str] = []
    for match in re.finditer(r"""(?:href|src|action)\s*=\s*["']([^"']+)["']""", body, re.I):
        link = normalize_link(base_url, match.group(1))
        if link and link not in links:
            links.append(link)
    return links


def extract_title(body: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
    if not match:
        return None
    return re.sub(r"\s+", " ", unescape(match.group(1))).strip()


def excerpt_text(body: str, limit: int = 1500) -> str:
    body = re.sub(r"<script[\s\S]*?</script>", " ", body, flags=re.I)
    body = re.sub(r"<style[\s\S]*?</style>", " ", body, flags=re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    body = re.sub(r"\s+", " ", unescape(body)).strip()
    return body[:limit]


def save_bytes(root: Path, url: str, data: bytes) -> Path:
    path = root / slugify_url(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def filtered_links(links: list[str]) -> dict[str, list[str]]:
    grouped = {
        "monitor_pdfs": [],
        "ungrd_links": [],
        "secop_community": [],
        "contratos_gov": [],
        "google_drive": [],
        "other_relevant": [],
    }
    for link in links:
        lowered = link.lower()
        if "/documentos/ungrd/" in lowered and lowered.endswith(".pdf"):
            grouped["monitor_pdfs"].append(link)
        elif "portal.gestiondelriesgo.gov.co" in lowered:
            grouped["ungrd_links"].append(link)
        elif "community.secop.gov.co" in lowered:
            grouped["secop_community"].append(link)
        elif "contratos.gov.co" in lowered:
            grouped["contratos_gov"].append(link)
        elif "drive.google.com" in lowered or "docs.google.com" in lowered:
            grouped["google_drive"].append(link)
        elif any(
            token in lowered
            for token in ("secop", "contrato", "carrotanque", "proveed", "ratificacion")
        ):
            grouped["other_relevant"].append(link)
    for key, values in grouped.items():
        deduped: list[str] = []
        for value in values:
            if value not in deduped:
                deduped.append(value)
        grouped[key] = deduped
    return grouped


def collect_pages(output_dir: Path) -> list[dict[str, object]]:
    pages: list[dict[str, object]] = []
    for target in TARGETS:
        status, headers, data, error = fetch(target.url)
        text = ""
        links: list[str] = []
        if data:
            try:
                text = data.decode("utf-8", "ignore")
                links = extract_links(text, target.url)
            except Exception:  # noqa: BLE001
                text = ""
                links = []
        saved_path = save_bytes(output_dir, target.url, data or b"")
        grouped = filtered_links(links)
        pages.append(
            {
                "name": target.name,
                "url": target.url,
                "status": status,
                "error": error,
                "saved_path": str(saved_path),
                "sha256": sha256_bytes(data or b""),
                "content_type": headers.get("Content-Type"),
                "title": extract_title(text) if text else None,
                "excerpt": excerpt_text(text) if text else None,
                "link_groups": grouped,
                "link_count": sum(len(values) for values in grouped.values()),
            }
        )
        time.sleep(SLEEP_SECONDS)
    return pages


def download_monitor_pdfs(output_dir: Path, links: list[str]) -> list[dict[str, object]]:
    saved: list[dict[str, object]] = []
    for link in links:
        status, headers, data, error = fetch(link)
        path = save_bytes(output_dir, link, data or b"")
        saved.append(
            {
                "url": link,
                "status": status,
                "error": error,
                "content_type": headers.get("Content-Type"),
                "sha256": sha256_bytes(data or b""),
                "saved_path": str(path),
                "bytes": len(data or b""),
            }
        )
        time.sleep(SLEEP_SECONDS)
    return saved


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="audit-results/investigations/ungrd-public-evidence-2026-03-27",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    pages = collect_pages(output_dir)
    monitor_links = []
    for page in pages:
        monitor_links.extend(page["link_groups"]["monitor_pdfs"])
    pdfs = download_monitor_pdfs(output_dir, list(dict.fromkeys(monitor_links)))

    manifest = {
        "generated_at_utc": now_utc(),
        "pages": pages,
        "downloaded_monitor_pdfs": pdfs,
        "graph_evidence_refs": GRAPH_EVIDENCE_REFS,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "pages": len(pages),
                "downloaded_monitor_pdfs": len(pdfs),
                "monitor_pdf_links": len(monitor_links),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
