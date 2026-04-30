"""Socrata metadata + schema probing.

Hits the Socrata Discovery API and parses the metadata payload into a
:class:`~coacc_etl.qualification.promotion.TriageCatalogRow`. Optional
sample probe verifies that the proven join keys carry data, not just
exist on the schema.
"""
# ruff: noqa: E501
from __future__ import annotations

import logging
import os
import re
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from coacc_etl.qualification.promotion import (
    TriageCatalogRow,
    _find_join_keys,
    _normalize_col,
)

if TYPE_CHECKING:
    pass

LOG = logging.getLogger("source_qualification")

DEFAULT_DOMAIN = "www.datos.gov.co"


def _build_client(timeout: float) -> httpx.Client:
    headers: dict[str, str] = {"User-Agent": "coacc-joinkey-triage/1.0"}
    app_token = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
    key_id = os.environ.get("SOCRATA_KEY_ID", "").strip()
    key_secret = os.environ.get("SOCRATA_KEY_SECRET", "").strip()

    if app_token and app_token != key_id:
        headers["X-App-Token"] = app_token

    auth = None
    if key_id and key_secret:
        auth = (key_id, key_secret)

    return httpx.Client(
        timeout=timeout,
        headers=headers,
        follow_redirects=True,
        auth=auth,
    )


def _get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str | int] | None = None,
    max_attempts: int = 5,
    initial_backoff: float = 1.0,
    max_backoff: float = 60.0,
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


def _parse_socrata_ts(value: object) -> datetime | None:
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
    meaningful_re = re.compile(r"^(?=.*[a-z])[a-z0-9_]{3,}$")
    for col in columns:
        field_name = str(col.get("fieldName", "")).lower()
        if meaningful_re.match(field_name):
            count += 1
    return count


def probe_dataset(
    client: httpx.Client,
    entry: dict[str, str],
    *,
    domain: str,
    probe_sample: int = 5,
    skip_count: bool = False,
    count_client: httpx.Client | None = None,
) -> TriageCatalogRow:
    row = TriageCatalogRow(
        dataset_id=entry.get("dataset_id", ""),
        name=entry.get("name", ""),
        sector=entry.get("sector_or_category", ""),
        scope=entry.get("scope", ""),
        recommendation=entry.get("recommendation", ""),
        relevance=entry.get("relevance", ""),
        audit_status=entry.get("audit_status", ""),
        source_refs=entry.get("source_refs", ""),
        origin_refs=entry.get("origin_refs", ""),
        signal_refs=entry.get("signal_refs", ""),
        url=entry.get("url", ""),
    )

    if not row.dataset_id.strip():
        row.probe_notes.append("missing dataset_id")
        return row

    meta_url = f"https://{domain}/api/views/{row.dataset_id}.json"
    try:
        meta_resp = _get_with_retry(client, meta_url)
        if meta_resp.status_code == 403:
            row.probe_notes.append("metadata forbidden 403")
            row.audit_status = "forbidden_403"
            return row
        if meta_resp.status_code == 404:
            row.probe_notes.append("metadata not found 404")
            row.audit_status = "dead_404"
            return row
        if meta_resp.status_code != 200:
            row.probe_notes.append(f"metadata http {meta_resp.status_code}")
            return row
        meta = meta_resp.json()
    except Exception as exc:
        row.probe_notes.append(f"metadata fetch failed: {exc}")
        return row

    columns_raw = [c for c in meta.get("columns", []) if isinstance(c, dict)]
    row.n_columns = len(columns_raw)
    row.n_meaningful_columns = _meaningful_column_count(columns_raw)

    field_names = [str(c.get("fieldName", "")) for c in columns_raw if c.get("fieldName")]
    row.columns_all = "|".join(field_names)

    normalized_cols = [_normalize_col(fn) for fn in field_names]
    found_keys, found_classes = _find_join_keys(normalized_cols)
    row.join_keys_found = len(found_keys)
    row.join_key_classes = "|".join(sorted(found_keys)) if found_keys else ""
    join_key_col_list: list[str] = []
    for cls_name in sorted(found_classes):
        for col_name in found_classes[cls_name]:
            join_key_col_list.append(f"{cls_name}:{col_name}")
    row.join_key_columns = "|".join(join_key_col_list) if join_key_col_list else ""

    row.update_freq = str(
        (meta.get("metadata") or {}).get("custom_fields", {}).get("Periodicidad", "") or ""
    )

    rows_updated = _parse_socrata_ts(meta.get("rowsUpdatedAt"))
    if rows_updated is None:
        row.probe_notes.append("rowsUpdatedAt missing or unparseable")
        row.last_update_days = -1
    else:
        row.last_update = rows_updated.date().isoformat()
        row.last_update_days = (datetime.now(tz=UTC).date() - rows_updated.date()).days

    meta_row_count = meta.get("rows")
    if isinstance(meta_row_count, int) and meta_row_count > 0:
        row.rows = meta_row_count
        row.probe_notes.append("rows_from_metadata")

    if not skip_count:
        count_url = f"https://{domain}/resource/{row.dataset_id}.json"
        try:
            effective_client = count_client or client
            count_resp = _get_with_retry(
                effective_client,
                count_url,
                params={"$select": "count(*)"},
                max_attempts=2,
                initial_backoff=1.0,
                max_backoff=10.0,
            )
            if count_resp.status_code != 200:
                row.probe_notes.append(f"count http {count_resp.status_code}")
            else:
                payload = count_resp.json()
                if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                    row.probe_notes.append(f"count payload shape invalid: {type(payload).__name__}")
                else:
                    count_val = next(iter(payload[0].values()))
                    try:
                        row.rows = int(str(count_val))
                        if "rows_from_metadata" in row.probe_notes:
                            row.probe_notes.remove("rows_from_metadata")
                    except (TypeError, ValueError):
                        row.probe_notes.append(f"count value unparseable: {count_val!r}")
        except Exception as exc:
            row.probe_notes.append(f"count fetch failed: {exc}")

    if found_keys and probe_sample > 0 and row.rows > 0:
        _probe_join_density(client, row, domain=domain, sample_size=probe_sample)

    return row


def _probe_join_density(
    client: httpx.Client,
    row: TriageCatalogRow,
    *,
    domain: str,
    sample_size: int,
) -> None:
    density_parts: list[str] = []
    for key_class_spec in row.join_key_columns.split("|"):
        if not key_class_spec:
            continue
        parts = key_class_spec.split(":")
        if len(parts) != 2:
            continue
        cls_name, col_name = parts
        api_col_name = col_name
        try:
            url = f"https://{domain}/resource/{row.dataset_id}.json"
            params: dict[str, str | int] = {
                "$select": f"{api_col_name}",
                "$limit": sample_size,
            }
            resp = _get_with_retry(client, url, params=params)
            if resp.status_code != 200:
                density_parts.append(f"{cls_name}:http_{resp.status_code}")
                continue
            payload = resp.json()
            if not isinstance(payload, list):
                density_parts.append(f"{cls_name}:not_list")
                continue
            non_empty = sum(
                1
                for r in payload
                if isinstance(r, dict) and str(r.get(api_col_name, "")).strip()
            )
            density_parts.append(f"{cls_name}:{non_empty}/{len(payload)}")
        except Exception as exc:
            density_parts.append(f"{cls_name}:err({exc})")

    row.sample_join_density = "|".join(density_parts) if density_parts else ""
