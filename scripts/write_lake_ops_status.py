#!/usr/bin/env python3
"""Write the latest production lake-ops status manifest."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_RUN_ID_SAFE = re.compile(r"[^A-Za-z0-9_.=-]+")


def _clean_run_id(value: str) -> str:
    cleaned = _RUN_ID_SAFE.sub("_", value.strip()).strip("._")
    if not cleaned:
        raise ValueError("run_id cannot be empty")
    return cleaned


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["check", "curate", "materialize", "refresh", "smoke", "full"],
    )
    parser.add_argument(
        "--status",
        required=True,
        choices=["running", "succeeded", "failed"],
    )
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--error", default="")
    parser.add_argument("--signal-run-id", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_id = _clean_run_id(args.run_id)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    now = datetime.now(tz=UTC)
    started_at = _parse_dt(args.started_at) or now
    finished_at = _parse_dt(args.finished_at)
    if args.status in {"succeeded", "failed"} and finished_at is None:
        finished_at = now

    duration_seconds: float | None = None
    if finished_at is not None:
        duration_seconds = max((finished_at - started_at).total_seconds(), 0.0)

    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "mode": args.mode,
        "status": args.status,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at) if finished_at is not None else None,
        "duration_seconds": duration_seconds,
        "exit_code": args.exit_code,
        "error": args.error or None,
        "signal_run_id": args.signal_run_id or None,
        "lake_root": str(args.lake_root),
        "written_at": _iso(now),
    }

    operations_dir = args.lake_root / "meta" / "operations"
    run_path = operations_dir / f"{run_id}.json"
    latest_path = operations_dir / "latest.json"
    _write_json_atomic(run_path, payload)
    _write_json_atomic(latest_path, {**payload, "manifest_path": str(run_path)})
    return 0


if __name__ == "__main__":
    sys.exit(main())
