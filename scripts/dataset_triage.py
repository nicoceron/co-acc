#!/usr/bin/env python3
"""Phase 2 dataset triage.

Reads docs/datasets/colombia_open_data_audit.json (the 285 validated Socrata
dataset IDs surfaced from the 25 sector roadmap PDFs) and probes each one
against the live Socrata metadata + count-query endpoints on datos.gov.co.
Classifies every dataset into Tier A / B / C and writes
docs/datasets/catalog.csv plus a human-readable markdown report.

Sanity checks at every boundary:
  1. Input JSON schema: required keys and per-row fields asserted before any
     HTTP call happens.
  2. Per-row metadata fetch: timeout + bounded exponential backoff; HTTP
     errors are recorded in the `notes` column but never abort the run.
  3. Row-count fetch: response must be a list of one dict with a parseable
     integer count; anything else is recorded as `rows=-1` + note.
  4. rowsUpdatedAt must parse as a Socrata epoch or ISO timestamp, else the
     dataset is treated as stale.
  5. Column-name gibberish heuristic: the `n_meaningful_columns` counter only
     counts columns whose fieldName contains a letter, is at least 3 chars,
     and is not purely digits.
  6. Output CSV uses DictWriter with a fixed fieldnames contract so new
     fields cannot silently appear or disappear.

Usage:
    python3 scripts/dataset_triage.py
        [--audit docs/datasets/colombia_open_data_audit.json]
        [--out docs/datasets/catalog.csv]
        [--report docs/datasets/triage_report.md]
        [--limit N]           # probe only first N (for smoke runs)
        [--sleep 0.2]         # seconds between requests (default 0.2 => ~5 rps)
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx

LOG = logging.getLogger("dataset_triage")

DEFAULT_DOMAIN = "www.datos.gov.co"

# --- Classification thresholds ----------------------------------------------

TIER_A_MIN_ROWS = 10_000
TIER_A_MAX_STALE_DAYS = 365
TIER_A_MIN_MEANINGFUL_COLUMNS = 5

TIER_C_MAX_ROWS = 1_000  # anything below this is skipped regardless of keyword

# Sectors that are load-bearing for the anti-corruption thesis. Datasets in
# other sectors can still earn Tier A via keyword matches on name.
ANTICORRUPTION_SECTORS = {
    "Justice and Law Sector Roadmap",
    "National Planning Sector Roadmap",
    "National Planning",
    "Finance and Public Credit Sector Roadmap",
    "Finance",
    "Sustainable Development Sector Roadmap",  # SGR projects
    "MINCIT Sectoral Roadmap 2026",             # registries, sanctions
    "MINCIT",
    "Sectoral Roadmaps 2025-2026",
}

# Sectors that rarely yield anti-corruption signal on their own.
LOW_PRIORITY_SECTORS = {
    "Sports Sector Roadmap",
    "Culture Sector Roadmap",
    "Foreign Affairs Sector Roadmap",
    "Statistics Sector Roadmap",
}

# Name-level keywords that upgrade a dataset to anti-corruption relevance.
TIER_A_KEYWORDS = [
    r"contrat",
    r"secop",
    r"sanci",
    r"inhabil",
    r"disciplinar",
    r"responsabilidad\s+fiscal",
    r"hallazg",
    r"presupuest",
    r"ejecuci[oó]n",
    r"compromiso",
    r"factur",
    r"adicion",
    r"modificaci",
    r"suspensi",
    r"pago",
    r"sgr\b",
    r"sigep",
    r"bien(?:es)?\b",
    r"activos",
    r"conflicto\s+de\s+inter",
    r"servidor\s+p[uú]blico",
    r"cargo",
    r"subsidi",
    r"beneficiari",
    r"antecedent",
    r"procurador",
    r"contralor",
    r"transparenci",
    r"anticorrup",
    r"registro\s+[uú]nico",
    r"rues",
    r"proyecto",
]

KEYWORD_RE = re.compile("|".join(TIER_A_KEYWORDS), re.IGNORECASE)

MEANINGFUL_NAME_RE = re.compile(r"^(?=.*[a-z])[a-z0-9_]{3,}$")


# --- Data structures --------------------------------------------------------

CATALOG_FIELDS = [
    "id",
    "name",
    "sector",
    "tier",
    "rows",
    "last_update",
    "last_update_days",
    "update_freq",
    "n_columns",
    "n_meaningful_columns",
    "columns_sample",
    "category",
    "url",
    "notes",
]


@dataclass
class TriageRow:
    id: str
    name: str
    sector: str
    tier: str = "?"
    rows: int = -1
    last_update: str = ""
    last_update_days: int = -1
    update_freq: str = ""
    n_columns: int = -1
    n_meaningful_columns: int = -1
    columns_sample: str = ""
    category: str = ""
    url: str = ""
    notes: list[str] = field(default_factory=list)

    def as_csv_row(self) -> dict[str, object]:
        d = asdict(self)
        d["notes"] = "; ".join(self.notes)
        return d


# --- HTTP helpers -----------------------------------------------------------


def _build_client(timeout: float) -> httpx.Client:
    headers: dict[str, str] = {"User-Agent": "coacc-dataset-triage/1.0"}
    app_token = os.environ.get("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token
    return httpx.Client(timeout=timeout, headers=headers, follow_redirects=True)


def _get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str] | None = None,
    max_attempts: int = 4,
    initial_backoff: float = 1.0,
    max_backoff: float = 30.0,
) -> httpx.Response:
    backoff = initial_backoff
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, params=params)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise httpx.HTTPStatusError(
                    f"retryable {response.status_code}",
                    request=response.request,
                    response=response,
                )
            return response
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            LOG.debug("retry %s attempt=%s/%s (%s)", url, attempt, max_attempts, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
    raise RuntimeError(f"exhausted retries for {url}: {last_error}") from last_error


# --- Parsers ----------------------------------------------------------------


def _parse_socrata_ts(value: object) -> datetime | None:
    """Socrata returns rowsUpdatedAt as an epoch int (seconds)."""
    if value is None:
        return None
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            try:
                return datetime.fromtimestamp(int(s), tz=UTC)
            except (OSError, ValueError, OverflowError):
                return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    return None


def _meaningful_column_count(columns: list[dict[str, object]]) -> int:
    count = 0
    for col in columns:
        field_name = str(col.get("fieldName", "")).lower()
        if MEANINGFUL_NAME_RE.match(field_name):
            count += 1
    return count


# --- Per-dataset probe ------------------------------------------------------


def probe_dataset(
    client: httpx.Client,
    entry: dict[str, str],
    *,
    domain: str,
) -> TriageRow:
    row = TriageRow(
        id=entry["id"],
        name=entry.get("name", ""),
        sector=entry.get("sector", ""),
        url=entry.get("url", f"https://{domain}/d/{entry['id']}"),
    )

    meta_url = f"https://{domain}/api/views/{row.id}.json"
    try:
        meta_resp = _get_with_retry(client, meta_url)
        if meta_resp.status_code != 200:
            row.notes.append(f"metadata http {meta_resp.status_code}")
            row.tier = "dead"
            return row
        meta = meta_resp.json()
    except Exception as exc:  # noqa: BLE001 — record failure, don't crash run
        row.notes.append(f"metadata fetch failed: {exc}")
        row.tier = "dead"
        return row

    columns = [c for c in meta.get("columns", []) if isinstance(c, dict)]
    row.n_columns = len(columns)
    row.n_meaningful_columns = _meaningful_column_count(columns)
    sample = [str(c.get("fieldName", "")) for c in columns[:8] if c.get("fieldName")]
    row.columns_sample = "|".join(sample)
    row.category = str(meta.get("category") or "")
    row.update_freq = str(
        (meta.get("metadata") or {}).get("custom_fields", {}).get("Periodicidad", "") or ""
    )

    rows_updated = _parse_socrata_ts(meta.get("rowsUpdatedAt"))
    if rows_updated is None:
        row.notes.append("rowsUpdatedAt missing or unparseable")
        row.last_update_days = -1
    else:
        row.last_update = rows_updated.date().isoformat()
        row.last_update_days = (datetime.now(tz=UTC).date() - rows_updated.date()).days

    count_url = f"https://{domain}/resource/{row.id}.json"
    try:
        count_resp = _get_with_retry(client, count_url, params={"$select": "count(*)"})
        if count_resp.status_code != 200:
            row.notes.append(f"count http {count_resp.status_code}")
        else:
            payload = count_resp.json()
            if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                row.notes.append(f"count payload shape invalid: {type(payload).__name__}")
            else:
                # Socrata returns {"count_xxx": "1234"} or {"count": "1234"}.
                count_val = next(iter(payload[0].values()))
                try:
                    row.rows = int(str(count_val))
                except (TypeError, ValueError):
                    row.notes.append(f"count value unparseable: {count_val!r}")
    except Exception as exc:  # noqa: BLE001
        row.notes.append(f"count fetch failed: {exc}")

    return row


# --- Classification ---------------------------------------------------------


def classify(row: TriageRow) -> str:
    if row.tier == "dead":
        return "dead"

    # Fail-closed: without usable metadata, default to decide (not Tier A).
    if row.rows < 0 and row.n_columns < 0:
        return "decide"

    if 0 <= row.rows < TIER_C_MAX_ROWS:
        return "C"

    matches_keyword = bool(KEYWORD_RE.search(row.name))
    in_anticorruption_sector = row.sector in ANTICORRUPTION_SECTORS
    in_low_priority_sector = row.sector in LOW_PRIORITY_SECTORS

    relevant = matches_keyword or in_anticorruption_sector

    fresh = 0 <= row.last_update_days <= TIER_A_MAX_STALE_DAYS
    large = row.rows >= TIER_A_MIN_ROWS
    readable = row.n_meaningful_columns >= TIER_A_MIN_MEANINGFUL_COLUMNS

    if relevant and fresh and large and readable and not in_low_priority_sector:
        return "A"

    if relevant:
        return "B"

    if fresh and large and readable:
        return "B"

    return "C"


# --- Driver -----------------------------------------------------------------


def load_audit(path: Path) -> list[dict[str, str]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    for key in ("summary", "valid_datasets_flat"):
        if key not in raw:
            raise ValueError(f"audit JSON missing required key: {key!r}")
    entries = raw["valid_datasets_flat"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("valid_datasets_flat is empty or wrong type")
    for i, entry in enumerate(entries):
        for key in ("id", "name", "sector"):
            if key not in entry:
                raise ValueError(f"entry {i} missing key {key!r}: {entry}")
    return entries


def write_catalog(rows: list[TriageRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def write_report(rows: list[TriageRow], report_path: Path) -> None:
    tier_count: dict[str, int] = {}
    for row in rows:
        tier_count[row.tier] = tier_count.get(row.tier, 0) + 1

    def _sort_key(r: TriageRow) -> tuple[int, int, str]:
        tier_order = {"A": 0, "B": 1, "C": 2, "decide": 3, "dead": 4}
        return (tier_order.get(r.tier, 9), -max(r.rows, 0), r.name.lower())

    rows_sorted = sorted(rows, key=_sort_key)
    lines: list[str] = [
        "# Dataset triage report",
        "",
        f"Generated: {datetime.now(tz=UTC).isoformat(timespec='seconds')}",
        f"Input: docs/datasets/colombia_open_data_audit.json",
        f"Total probed: {len(rows)}",
        "",
        "## Tier counts",
        "",
    ]
    for tier in ("A", "B", "C", "decide", "dead"):
        lines.append(f"- {tier}: {tier_count.get(tier, 0)}")
    lines.append("")
    lines.append("## Tier A (ingest now)")
    lines.append("")
    lines.append("| ID | Rows | Last update | Sector | Name |")
    lines.append("|---|---:|---|---|---|")
    for row in rows_sorted:
        if row.tier != "A":
            continue
        lines.append(
            f"| `{row.id}` | {row.rows:,} | {row.last_update or '—'} | "
            f"{row.sector} | {row.name} |"
        )
    lines.append("")
    lines.append("## Tier dead / probe failed")
    lines.append("")
    for row in rows_sorted:
        if row.tier != "dead":
            continue
        lines.append(f"- `{row.id}` — {row.name} — {'; '.join(row.notes) or 'no detail'}")
    lines.append("")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--audit",
        default="docs/datasets/colombia_open_data_audit.json",
        help="Path to canonical audit JSON",
    )
    parser.add_argument(
        "--out",
        default="docs/datasets/catalog.csv",
        help="Output catalog CSV",
    )
    parser.add_argument(
        "--report",
        default="docs/datasets/triage_report.md",
        help="Output markdown report",
    )
    parser.add_argument(
        "--domain",
        default=DEFAULT_DOMAIN,
        help="Socrata domain (default www.datos.gov.co)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only probe first N (smoke test)")
    parser.add_argument("--sleep", type=float, default=0.2, help="Seconds between requests")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    audit_path = Path(args.audit)
    if not audit_path.is_file():
        LOG.error("audit JSON not found: %s", audit_path)
        return 2

    entries = load_audit(audit_path)
    if args.limit:
        entries = entries[: args.limit]

    LOG.info("probing %d datasets from %s", len(entries), audit_path)

    rows: list[TriageRow] = []
    with _build_client(args.timeout) as client:
        for i, entry in enumerate(entries, start=1):
            row = probe_dataset(client, entry, domain=args.domain)
            row.tier = classify(row)
            rows.append(row)
            LOG.info(
                "[%d/%d] %s %s rows=%s last=%s cols=%s/%s",
                i,
                len(entries),
                row.tier,
                row.id,
                row.rows,
                row.last_update or "?",
                row.n_meaningful_columns,
                row.n_columns,
            )
            if args.sleep > 0:
                time.sleep(args.sleep)

    write_catalog(rows, Path(args.out))
    write_report(rows, Path(args.report))

    tier_count: dict[str, int] = {}
    for r in rows:
        tier_count[r.tier] = tier_count.get(r.tier, 0) + 1
    LOG.info("wrote %s + %s", args.out, args.report)
    LOG.info("tiers: %s", tier_count)

    if tier_count.get("A", 0) == 0:
        LOG.error("sanity check: 0 Tier A datasets — review thresholds before Phase 3")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
