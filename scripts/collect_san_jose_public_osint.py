#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import ssl
import sys
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; CO-ACC Public OSINT Collector/1.0)"
TIMEOUT_SECONDS = 30
MAX_BYTES = 5_000_000
SLEEP_SECONDS = 0.15
TEXTUAL_EXTENSIONS = {
    ".html",
    ".htm",
    ".json",
    ".txt",
    ".xml",
    ".js",
    ".css",
    ".svg",
    ".map",
}
ASSET_EXTENSIONS = {
    ".html",
    ".htm",
    ".json",
    ".txt",
    ".xml",
    ".js",
    ".css",
    ".svg",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".gif",
    ".pdf",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
}


@dataclass(frozen=True)
class Target:
    name: str
    url: str
    kind: str


TARGETS = [
    Target("hazte-home", "https://hazteprofesional.com/", "html"),
    Target("hazte-wp-json", "https://hazteprofesional.com/wp-json/", "json"),
    Target("hazte-page", "https://hazteprofesional.com/wp-json/wp/v2/pages/17714", "json"),
    Target("hazte-robots", "https://hazteprofesional.com/robots.txt", "text"),
    Target("hazte-sitemap", "https://hazteprofesional.com/sitemap.xml", "xml"),
    Target("staging-home", "https://staging.hazteprofesional.com/", "html"),
    Target("staging-wp-json", "https://staging.hazteprofesional.com/wp-json/", "json"),
    Target("staging-page", "https://staging.hazteprofesional.com/wp-json/wp/v2/pages/39", "json"),
    Target("staging-robots", "https://staging.hazteprofesional.com/robots.txt", "text"),
    Target("staging-sitemap", "https://staging.hazteprofesional.com/sitemap.xml", "xml"),
    Target("inscripcion-home", "https://i.hazteprofesional.com/", "html"),
    Target("inscripcion-robots", "https://i.hazteprofesional.com/robots.txt", "text"),
    Target("alianzas-home", "https://alianzas.hazteprofesional.com/", "html"),
    Target("alianzas-robots", "https://alianzas.hazteprofesional.com/robots.txt", "text"),
    Target("bluhartmann-home", "https://bluhartmann.com/", "html"),
    Target("bluhartmann-wp-json", "https://bluhartmann.com/wp-json/", "json"),
    Target("bluhartmann-robots", "https://bluhartmann.com/robots.txt", "text"),
    Target("bluhartmann-sitemap", "https://bluhartmann.com/sitemap.xml", "xml"),
    Target("convenios-home", "https://convenios.bluhartmann.com/", "html"),
    Target("convenios-wp-json", "https://convenios.bluhartmann.com/wp-json/", "json"),
    Target("convenios-robots", "https://convenios.bluhartmann.com/robots.txt", "text"),
    Target("pagos-home", "https://pagos.bluhartmann.com/", "html"),
    Target("pagos-robots", "https://pagos.bluhartmann.com/robots.txt", "text"),
    Target("hazte-crtsh", "https://crt.sh/?q=hazteprofesional.com&output=json", "json"),
    Target("bluhartmann-crtsh", "https://crt.sh/?q=bluhartmann.com&output=json", "json"),
]


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


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    patterns = [
        r"""(?:href|src|action)\s*=\s*["']([^"']+)["']""",
        r"""url\(["']?([^"'()]+)["']?\)""",
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, body, re.IGNORECASE):
            link = normalize_link(base_url, match.group(1))
            if link and link not in links:
                links.append(link)
    return links


def extract_title(body: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", body, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return re.sub(r"\s+", " ", unescape(match.group(1))).strip()


def excerpt_text(body: str, limit: int = 1800) -> str:
    body = re.sub(r"<script[\s\S]*?</script>", " ", body, flags=re.IGNORECASE)
    body = re.sub(r"<style[\s\S]*?</style>", " ", body, flags=re.IGNORECASE)
    body = re.sub(r"<[^>]+>", " ", body)
    body = re.sub(r"\s+", " ", unescape(body)).strip()
    return body[:limit]


def same_host(url: str, host: str) -> bool:
    return urlparse(url).netloc == host


def allowed_asset(url: str) -> bool:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if not suffix and parsed.path.endswith("/"):
        return True
    return suffix in ASSET_EXTENSIONS


def fetch(url: str) -> tuple[int | None, dict[str, str], bytes, str | None]:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = response.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                data = data[:MAX_BYTES]
            return response.status, dict(response.headers.items()), data, None
    except HTTPError as exc:
        body = exc.read(MAX_BYTES) if exc.fp else b""
        return exc.code, dict(exc.headers.items()), body, repr(exc)
    except URLError as exc:
        return None, {}, b"", repr(exc)
    except Exception as exc:  # noqa: BLE001
        return None, {}, b"", repr(exc)


def host_ip(host: str) -> str | None:
    try:
        return socket.gethostbyname(host)
    except OSError:
        return None


def tls_certificate(host: str) -> dict[str, str | None]:
    try:
        context = ssl.create_default_context()
        with socket.create_connection((host, 443), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=host) as tls_sock:
                cert = tls_sock.getpeercert()
        subject = ", ".join("=".join(item) for group in cert.get("subject", []) for item in group)
        issuer = ", ".join("=".join(item) for group in cert.get("issuer", []) for item in group)
        return {
            "subject": subject or None,
            "issuer": issuer or None,
            "not_before": cert.get("notBefore"),
            "not_after": cert.get("notAfter"),
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": repr(exc)}


def save_bytes(root: Path, url: str, data: bytes) -> Path:
    path = root / slugify_url(url)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def collect_assets(root: Path, base_url: str, links: Iterable[str], max_assets: int) -> list[dict[str, object]]:
    host = urlparse(base_url).netloc
    saved: list[dict[str, object]] = []
    seen: set[str] = set()
    for link in links:
        if len(saved) >= max_assets:
            break
        if link in seen or not same_host(link, host) or not allowed_asset(link):
            continue
        seen.add(link)
        status, headers, data, error = fetch(link)
        record: dict[str, object] = {
            "url": link,
            "status": status,
            "content_type": headers.get("Content-Type"),
            "content_length": len(data),
            "error": error,
        }
        if data:
            saved_path = save_bytes(root / "assets", link, data)
            record["path"] = str(saved_path)
            record["sha256"] = sha256_bytes(data)
        saved.append(record)
        time.sleep(SLEEP_SECONDS)
    return saved


def collect_target(root: Path, target: Target, max_assets: int) -> dict[str, object]:
    status, headers, data, error = fetch(target.url)
    host = urlparse(target.url).netloc
    record: dict[str, object] = {
        "name": target.name,
        "url": target.url,
        "kind": target.kind,
        "status": status,
        "host": host,
        "host_ip": host_ip(host),
        "headers": headers,
        "error": error,
        "fetched_at_utc": now_utc(),
        "certificate": tls_certificate(host) if urlparse(target.url).scheme == "https" else None,
    }
    if data:
        saved_path = save_bytes(root / "responses", target.url, data)
        record["path"] = str(saved_path)
        record["sha256"] = sha256_bytes(data)
        record["content_length"] = len(data)
        content_type = headers.get("Content-Type", "")
        if any(kind in content_type for kind in ("text", "json", "javascript", "xml")):
            text = data.decode("utf-8", "ignore")
            record["title"] = extract_title(text)
            links = extract_links(text, target.url)
            record["links"] = links[:200]
            record["excerpt"] = excerpt_text(text)
            record["assets"] = collect_assets(root, target.url, links, max_assets=max_assets)
    return record


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect public San Jose / BluHartmann OSINT bundle")
    parser.add_argument(
        "--output-dir",
        default="audit-results/investigations/san-jose-public-osint-2026-03-23",
        help="Destination directory for downloaded evidence",
    )
    parser.add_argument(
        "--max-assets-per-target",
        type=int,
        default=40,
        help="Maximum number of same-host assets to save per target",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.output_dir)
    root.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generated_at_utc": now_utc(),
        "targets": [],
    }

    for target in TARGETS:
        record = collect_target(root, target, max_assets=args.max_assets_per_target)
        manifest["targets"].append(record)
        time.sleep(SLEEP_SECONDS)

    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output_dir": str(root), "manifest": str(manifest_path), "targets": len(TARGETS)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
