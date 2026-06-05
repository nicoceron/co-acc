from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from coacc.services import lakehouse_query

if TYPE_CHECKING:
    from pathlib import Path


def operations_status_path() -> Path:
    return lakehouse_query.lake_root() / "meta" / "operations" / "latest.json"


def _parse_dt(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def latest_lake_ops_report(
    *,
    max_age_hours: float | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    path = operations_status_path()
    max_age_seconds = (
        max_age_hours * 3600 if max_age_hours is not None and max_age_hours > 0 else None
    )
    if not path.exists():
        return {
            "status": "missing",
            "healthy": False if max_age_seconds is not None else None,
            "fresh": False if max_age_seconds is not None else None,
            "detail": "no lake operations status manifest found",
            "path": str(path),
            "max_age_seconds": max_age_seconds,
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "invalid",
            "healthy": False,
            "fresh": False,
            "detail": f"failed to read lake operations status manifest: {exc}",
            "path": str(path),
            "max_age_seconds": max_age_seconds,
        }
    if not isinstance(payload, dict):
        return {
            "status": "invalid",
            "healthy": False,
            "fresh": False,
            "detail": "lake operations status manifest is not a JSON object",
            "path": str(path),
            "max_age_seconds": max_age_seconds,
        }

    finished_at = _parse_dt(payload.get("finished_at"))
    status = str(payload.get("status") or "unknown")
    current_time = (now or datetime.now(tz=UTC)).astimezone(UTC)
    age_seconds: float | None = None
    if finished_at is not None:
        age_seconds = max((current_time - finished_at).total_seconds(), 0.0)

    fresh: bool | None = None
    if max_age_seconds is not None:
        fresh = age_seconds is not None and age_seconds <= max_age_seconds
    healthy = status == "succeeded" and (fresh is not False) and finished_at is not None

    if status != "succeeded":
        detail = f"latest lake operations run status is {status}"
    elif finished_at is None:
        detail = "latest lake operations run is missing finished_at"
    elif fresh is False:
        detail = "latest successful lake operations run is stale"
    else:
        detail = "latest lake operations run succeeded"

    return {
        **payload,
        "status": status,
        "healthy": healthy,
        "fresh": fresh,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "detail": detail,
        "path": str(path),
    }
