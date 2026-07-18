from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from coacc.config import settings
from coacc.services.lake_ops_status import latest_lake_ops_report
from coacc.services.readiness import build_readiness_report

if TYPE_CHECKING:
    from pathlib import Path

    from httpx import AsyncClient


def _write_latest(root: Path, payload: dict[str, object]) -> None:
    out = root / "meta" / "operations"
    out.mkdir(parents=True)
    (out / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _manifest(
    *,
    status: str = "succeeded",
    finished_at: datetime | None = None,
) -> dict[str, object]:
    finished = finished_at or datetime.now(tz=UTC)
    started = finished - timedelta(minutes=5)
    return {
        "schema_version": 1,
        "run_id": "prod-20260604T100000Z",
        "mode": "refresh",
        "status": status,
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": finished.isoformat().replace("+00:00", "Z"),
        "duration_seconds": 300.0,
        "exit_code": 0 if status == "succeeded" else 1,
        "error": None if status == "succeeded" else "failed",
    }


def test_lake_ops_report_missing_when_manifest_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    report = latest_lake_ops_report(max_age_hours=48)

    assert report["status"] == "missing"
    assert report["healthy"] is False
    assert report["fresh"] is False


def test_lake_ops_report_accepts_recent_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=UTC)
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_latest(tmp_path, _manifest(finished_at=now - timedelta(hours=2)))

    report = latest_lake_ops_report(max_age_hours=48, now=now)

    assert report["status"] == "succeeded"
    assert report["healthy"] is True
    assert report["fresh"] is True
    assert report["age_seconds"] == 7200.0


def test_lake_ops_report_marks_stale_success_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=UTC)
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_latest(
        tmp_path,
        _manifest(finished_at=now - timedelta(hours=72)),
    )

    report = latest_lake_ops_report(max_age_hours=48, now=now)

    assert report["status"] == "succeeded"
    assert report["healthy"] is False
    assert report["fresh"] is False
    assert "stale" in report["detail"]


def test_lake_ops_report_marks_failed_run_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 6, 4, 12, 0, tzinfo=UTC)
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_latest(tmp_path, _manifest(status="failed"))

    report = latest_lake_ops_report(max_age_hours=48, now=now)

    assert report["status"] == "failed"
    assert report["healthy"] is False
    assert report["fresh"] is True


def test_readiness_includes_lake_ops_check(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "coacc_ready_max_lake_ops_age_hours", 48.0)
    _write_latest(tmp_path, _manifest())

    report = build_readiness_report(neo4j_connected=False)

    lake_ops_check = next(check for check in report["checks"] if check["name"] == "lake_ops")
    assert lake_ops_check["status"] == "ok"


@pytest.mark.anyio
async def test_meta_operations_endpoint(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(settings, "coacc_ready_max_lake_ops_age_hours", 48.0)
    _write_latest(tmp_path, _manifest())

    response = await client.get("/api/v1/meta/operations")

    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == "prod-20260604T100000Z"
    assert data["status"] == "succeeded"
    assert data["healthy"] is True
