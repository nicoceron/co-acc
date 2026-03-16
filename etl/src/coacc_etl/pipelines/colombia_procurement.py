from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any

from coacc_etl.pipelines.colombia_shared import clean_name, clean_text, parse_iso_date, stable_id
from coacc_etl.transforms import strip_document

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)

EVIDENCE_LIMIT = 5


def make_company_document_id(raw_document: object, raw_name: object, *, kind: str) -> str:
    document_id = strip_document(clean_text(raw_document))
    if document_id:
        return document_id

    name = clean_name(raw_name) or clean_text(raw_name)
    if not name:
        return ""
    return stable_id("coanon", kind, name)


def merge_company(company_map: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
    document_id = clean_text(row.get("document_id"))
    if not document_id:
        return

    target = company_map.setdefault(document_id, {})

    for key, value in row.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if key == "nit" and not str(value).isdigit():
            continue
        target[key] = value


def build_company_row(
    *,
    document_id: str,
    name: str,
    source: str,
    country: str = "CO",
    **extra: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "document_id": document_id,
        "name": name,
        "razon_social": name,
        "source": source,
        "country": country,
    }
    if document_id.isdigit():
        row["nit"] = document_id
    else:
        row["synthetic_document_id"] = True

    for key, value in extra.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        row[key] = value
    return row


def procurement_year(raw_date: object) -> str | None:
    parsed = parse_iso_date(raw_date)
    if not parsed:
        return None
    return parsed[:4]


def procurement_relation_id(
    source_id: str,
    buyer_document_id: str,
    supplier_document_id: str,
    year: str | None = None,
) -> str:
    return stable_id("co_proc", source_id, buyer_document_id, supplier_document_id, year or "")


def merge_limited_unique(
    values: list[str],
    *candidates: object,
    limit: int = EVIDENCE_LIMIT,
) -> list[str]:
    for candidate in candidates:
        value = clean_text(candidate)
        if not value:
            continue
        if value in values:
            continue
        values.append(value)
        if len(values) >= limit:
            break
    return values[:limit]


def update_date_window(summary: dict[str, Any], raw_date: object) -> None:
    parsed = parse_iso_date(raw_date)
    if not parsed:
        return

    first_date = summary.get("first_date")
    last_date = summary.get("last_date")
    if not first_date or parsed < first_date:
        summary["first_date"] = parsed
    if not last_date or parsed > last_date:
        summary["last_date"] = parsed


def summary_map_csv_path(data_dir: str) -> Path:
    return Path(data_dir) / "secop_ii_contracts" / "contract_summary_map.csv"


def reset_summary_map(path: Path) -> None:
    if path.exists():
        path.unlink()
    sqlite_path = path.with_suffix(".sqlite3")
    if sqlite_path.exists():
        sqlite_path.unlink()


def append_summary_map(path: Path, rows: Iterable[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if not file_exists:
            writer.writerow(["contract_id", "summary_id"])
        writer.writerows(rows)


class ContractSummaryLookup:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.sqlite_path = path.with_suffix(".sqlite3")
        self._conn: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.sqlite_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        return self._conn

    def _build_if_needed(self) -> None:
        if not self.path.exists():
            raise FileNotFoundError(self.path)

        needs_rebuild = not self.sqlite_path.exists()
        if not needs_rebuild:
            needs_rebuild = self.sqlite_path.stat().st_mtime < self.path.stat().st_mtime

        conn = self._connect()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS contract_summary_map ("
            "contract_id TEXT PRIMARY KEY, "
            "summary_id TEXT NOT NULL)"
        )
        if not needs_rebuild:
            return

        logger.info("Rebuilding contract summary lookup: %s", self.sqlite_path)
        conn.execute("DELETE FROM contract_summary_map")
        batch: list[tuple[str, str]] = []
        with self.path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                contract_id = clean_text(row.get("contract_id"))
                summary_id = clean_text(row.get("summary_id"))
                if not contract_id or not summary_id:
                    continue
                batch.append((contract_id, summary_id))
                if len(batch) >= 50_000:
                    conn.executemany(
                        "INSERT OR REPLACE INTO contract_summary_map(contract_id, summary_id) "
                        "VALUES (?, ?)",
                        batch,
                    )
                    conn.commit()
                    batch.clear()
            if batch:
                conn.executemany(
                    "INSERT OR REPLACE INTO contract_summary_map(contract_id, summary_id) "
                    "VALUES (?, ?)",
                    batch,
                )
                conn.commit()

    def lookup_many(self, contract_ids: Iterable[str]) -> dict[str, str]:
        contract_ids = [
            clean_text(contract_id)
            for contract_id in contract_ids
            if clean_text(contract_id)
        ]
        if not contract_ids:
            return {}

        self._build_if_needed()
        conn = self._connect()
        found: dict[str, str] = {}
        for start in range(0, len(contract_ids), 500):
            batch = contract_ids[start : start + 500]
            placeholders = ", ".join("?" for _ in batch)
            cursor = conn.execute(
                f"SELECT contract_id, summary_id FROM contract_summary_map "
                f"WHERE contract_id IN ({placeholders})",
                batch,
            )
            found.update({str(contract_id): str(summary_id) for contract_id, summary_id in cursor})
        return found

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
