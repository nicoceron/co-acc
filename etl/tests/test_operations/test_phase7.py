from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from coacc_etl.ingest.socrata import IngestResult
from coacc_etl.lakehouse import watermark as wm
from coacc_etl.operations.phase7 import (
    DEFAULT_SMOKE_MAX_PAGES,
    DEFAULT_SMOKE_PAGE_SIZE,
    DiskBudgetError,
    Phase7RunError,
    run_phase7,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _isolated_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lake"
    monkeypatch.setenv("COACC_LAKE_ROOT", str(root))
    monkeypatch.setattr("coacc_etl.lakehouse.paths.LAKE_ROOT", root, raising=False)
    return root


def test_phase7_smoke_seeds_recent_watermark_and_logs(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def fake_ingest(spec, **kwargs) -> IngestResult:
        calls.append({"id": spec.id, **kwargs})
        delta = wm.WatermarkDelta(
            source=spec.id,
            rows=2,
            batch_id="fake-batch",
            advanced=True,
            last_seen_ts=datetime(2026, 5, 1, tzinfo=UTC),
        )
        return IngestResult(
            dataset_id=spec.id,
            rows=2,
            partitions=[(2026, 5)],
            parquet_paths=[],
            watermark_delta=delta,
            coverage=None,
            batch_id="fake-batch",
            started_at=datetime(2026, 5, 15, tzinfo=UTC),
            finished_at=datetime(2026, 5, 15, tzinfo=UTC),
        )

    log_path = tmp_path / "ingest_log.md"
    records = run_phase7(
        mode="smoke",
        dataset_ids=["wi7w-2nvm"],
        log_path=log_path,
        ingest_fn=fake_ingest,
        max_lookup=lambda _spec: "2026-05-01T00:00:00.000",
    )

    assert [record.status for record in records] == ["ok"]
    assert calls == [
        {
            "id": "wi7w-2nvm",
            "full_refresh": False,
            "page_size": DEFAULT_SMOKE_PAGE_SIZE,
            "max_pages": DEFAULT_SMOKE_MAX_PAGES,
        }
    ]
    seeded = wm.get("wi7w-2nvm")
    assert seeded is not None
    assert seeded.last_seen_ts == datetime(2026, 4, 24, tzinfo=UTC)
    text = log_path.read_text(encoding="utf-8")
    assert "`wi7w-2nvm`" in text
    assert "smoke: seeded watermark" in text


def test_phase7_full_uses_full_refresh_and_full_pagination(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def fake_ingest(spec, **kwargs) -> IngestResult:
        calls.append({"id": spec.id, **kwargs})
        return IngestResult(
            dataset_id=spec.id,
            rows=0,
            partitions=[],
            parquet_paths=[],
            watermark_delta=None,
            coverage=None,
            batch_id="fake-batch",
            started_at=datetime(2026, 5, 15, tzinfo=UTC),
            finished_at=datetime(2026, 5, 15, tzinfo=UTC),
            skipped_reason="no_new_rows",
        )

    records = run_phase7(
        mode="full",
        dataset_ids=["jbjy-vk9h"],
        min_free_gb=0,
        page_size=123,
        max_pages=456,
        log_path=tmp_path / "ingest_log.md",
        ingest_fn=fake_ingest,
    )

    assert records[0].status == "skipped"
    assert calls == [
        {
            "id": "jbjy-vk9h",
            "full_refresh": True,
            "page_size": 123,
            "max_pages": 456,
        }
    ]


def test_phase7_disk_budget_blocks_before_ingest() -> None:
    def fake_ingest(_spec, **_kwargs) -> IngestResult:
        raise AssertionError("ingest should not run")

    with pytest.raises(DiskBudgetError):
        run_phase7(
            mode="smoke",
            dataset_ids=["wi7w-2nvm"],
            min_free_gb=10**9,
            ingest_fn=fake_ingest,
            max_lookup=lambda _spec: "2026-05-01T00:00:00.000",
        )


def test_phase7_failure_is_logged_and_raises(tmp_path: Path) -> None:
    def fake_ingest(_spec, **_kwargs) -> IngestResult:
        raise RuntimeError("boom")

    log_path = tmp_path / "ingest_log.md"
    with pytest.raises(Phase7RunError):
        run_phase7(
            mode="smoke",
            dataset_ids=["wi7w-2nvm"],
            log_path=log_path,
            ingest_fn=fake_ingest,
            max_lookup=lambda _spec: "2026-05-01T00:00:00.000",
        )

    text = log_path.read_text(encoding="utf-8")
    assert "failed" in text
    assert "boom" in text
