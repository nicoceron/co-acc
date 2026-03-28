from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "api" / "src" / "coacc" / "data" / "watchlists"
DEFAULT_SNAPSHOT_LIMIT = 200
COMPANY_SNAPSHOT_LIMIT = 1000
PEOPLE_SNAPSHOT_LIMIT = 200
BUYER_SNAPSHOT_LIMIT = 200
TERRITORY_SNAPSHOT_LIMIT = 100
API_BASE = os.getenv("COACC_API_BASE", "http://localhost:8000").rstrip("/")
WATCHLIST_ENDPOINT_MAP = {
    "companies": "/api/v1/meta/watchlist/companies",
    "people": "/api/v1/meta/watchlist/people",
    "buyers": "/api/v1/meta/watchlist/buyers",
    "territories": "/api/v1/meta/watchlist/territories",
}


def _load_existing_snapshot_rows(name: str) -> list[dict[str, object]]:
    path = OUTPUT_DIR / f"{name}.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    rows = payload.get(name)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _fetch_watchlist_snapshot(
    name: str,
    limit: int = DEFAULT_SNAPSHOT_LIMIT,
) -> list[dict[str, object]]:
    endpoint = WATCHLIST_ENDPOINT_MAP.get(name)
    if endpoint is None:
        raise ValueError(f"Unsupported watchlist snapshot: {name}")

    payload_key = name
    params = urlencode({"limit": limit})
    request = Request(
        f"{API_BASE}{endpoint}?{params}",
        headers={
            "Accept": "application/json",
            "User-Agent": "coacc-watchlist-snapshots/1.0",
        },
    )

    last_error: Exception | None = None
    for attempt in range(6):
        try:
            with urlopen(request, timeout=180) as response:  # noqa: S310
                payload = json.loads(response.read().decode("utf-8"))
            rows = payload.get(payload_key)
            if not isinstance(rows, list):
                raise RuntimeError(f"Snapshot payload for {name} missing '{payload_key}' list")
            return [row for row in rows if isinstance(row, dict)]
        except (HTTPError, URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(min(2**attempt, 15))

    raise RuntimeError(f"Unable to fetch {name} snapshot from {API_BASE}: {last_error}")


def _build_graph_snapshot(
    name: str,
    limit: int = DEFAULT_SNAPSHOT_LIMIT,
) -> list[dict[str, object]]:
    try:
        return _fetch_watchlist_snapshot(name, limit=limit)
    except Exception:
        existing = _load_existing_snapshot_rows(name)
        if existing:
            return existing[:limit]
        raise


def build_people_snapshot(limit: int = PEOPLE_SNAPSHOT_LIMIT) -> list[dict[str, object]]:
    return _build_graph_snapshot("people", limit=limit)


def build_company_snapshot(limit: int = COMPANY_SNAPSHOT_LIMIT) -> list[dict[str, object]]:
    return _build_graph_snapshot("companies", limit=limit)


def build_buyer_snapshot(limit: int = BUYER_SNAPSHOT_LIMIT) -> list[dict[str, object]]:
    return _build_graph_snapshot("buyers", limit=limit)


def build_territory_snapshot(limit: int = TERRITORY_SNAPSHOT_LIMIT) -> list[dict[str, object]]:
    return _build_graph_snapshot("territories", limit=limit)


def build_snapshots() -> dict[str, int]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    companies = build_company_snapshot()
    people = build_people_snapshot()
    buyers = build_buyer_snapshot()
    territories = build_territory_snapshot()
    generated_at = datetime.now(UTC).isoformat()

    (OUTPUT_DIR / "companies.json").write_text(
        json.dumps(
            {"generated_at": generated_at, "companies": companies},
            ensure_ascii=False,
            indent=2,
        )
    )
    (OUTPUT_DIR / "people.json").write_text(
        json.dumps({"generated_at": generated_at, "people": people}, ensure_ascii=False, indent=2)
    )
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
    return {
        "companies": len(companies),
        "people": len(people),
        "buyers": len(buyers),
        "territories": len(territories),
    }


if __name__ == "__main__":
    print(json.dumps(build_snapshots(), ensure_ascii=False))
