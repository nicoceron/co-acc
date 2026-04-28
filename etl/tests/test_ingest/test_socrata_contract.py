"""Contract tests for the generic Socrata ingester.

Uses a canned page rather than live HTTP. Covers the sanity gates named in
``docs/cleanup/refactor_plan.md`` Wave 3:

- Coverage gate blocks below threshold.
- Partition gate rejects unpartitionable rows.
- Watermark gate refuses wall-clock-sourced advances.
- Full-refresh bypasses an existing watermark.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pyarrow.parquet as pq
import pytest

from coacc_etl.catalog import DatasetSpec
from coacc_etl.ingest import IngestError, assert_coverage, ingest
from coacc_etl.ingest.coverage import CoverageFailure
from coacc_etl.lakehouse import watermark as wm
from coacc_etl.lakehouse.paths import meta_path, raw_source_path


def _read_all_parquets(source: str) -> pd.DataFrame:
    root = raw_source_path(source)
    frames = [
        pq.read_table(p).to_pandas()
        for p in sorted(root.rglob("*.parquet"))
        if not p.name.startswith(".inflight-")
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def test_ingest_writes_parquet_and_advances_watermark(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    client = fake_client_factory([hallazgos_page])

    result = ingest(hallazgos_spec, client=client)

    assert result.ingested
    assert result.rows == 3
    assert len(result.parquet_paths) == 2  # 2024-12 and 2025-01
    assert {(y, m) for y, m in result.partitions} == {(2024, 12), (2025, 1)}

    df = _read_all_parquets(hallazgos_spec.id)
    assert len(df) == 3
    # Columns were renamed to canonical names.
    assert set(df.columns) >= {"nit", "entity_name", "received_date", "radicado"}
    assert "nombre_sujeto" not in df.columns  # raw source name not leaked
    # Unmapped source columns are still persisted (not silently dropped).
    assert "vigencia_auditada" in df.columns

    # Watermark advanced to the max parsed timestamp — not wall-clock.
    stored = wm.get(hallazgos_spec.id)
    assert stored is not None
    assert stored.last_seen_ts == datetime(2025, 1, 15, tzinfo=UTC)


def test_ingest_incremental_skips_if_no_new_rows(
    hallazgos_spec: DatasetSpec,
    fake_client_factory,
) -> None:
    # Seed a future watermark so nothing qualifies.
    wm.set(
        wm.Watermark(
            source=hallazgos_spec.id,
            last_seen_ts=datetime(2030, 1, 1, tzinfo=UTC),
            last_batch_id="seed",
            row_count=0,
        )
    )
    client = fake_client_factory([[]])  # empty page
    result = ingest(hallazgos_spec, client=client)
    assert not result.ingested
    assert result.skipped_reason == "no_new_rows"
    # Watermark untouched.
    stored = wm.get(hallazgos_spec.id)
    assert stored is not None
    assert stored.last_seen_ts == datetime(2030, 1, 1, tzinfo=UTC)


def test_coverage_failure_blocks_watermark(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    # Clobber the nit on every row — nit coverage drops to 0.
    page = [dict(row, nit="") for row in hallazgos_page]
    client = fake_client_factory([page])

    with pytest.raises(CoverageFailure) as excinfo:
        ingest(hallazgos_spec, client=client)
    assert "nit" in excinfo.value.failures

    # No parquet written, no watermark advance.
    assert list(raw_source_path(hallazgos_spec.id).rglob("*.parquet")) == []
    assert wm.get(hallazgos_spec.id) is None
    # Failure report persisted.
    failures = list((meta_path() / "failures" / hallazgos_spec.id).glob("*.json"))
    assert len(failures) == 1


def test_partition_sentinel_for_unparseable_rows(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    """Unparseable watermark rows survive in the year=0/month=0 sentinel.

    Socrata's ``$where`` on a date column never returns NULL-comparing rows,
    so dropping them on first ingest would lose them forever. The watermark
    advances using only parseable rows.
    """
    page = list(hallazgos_page)
    page[1] = dict(page[1], fecha_recibo_traslado="not-a-date")
    client = fake_client_factory([page])

    result = ingest(hallazgos_spec, client=client)

    assert result.rows == 3
    assert (0, 0) in result.partitions  # sentinel partition created
    # Watermark = max parseable, ignoring the bad row.
    assert result.watermark_delta is not None
    assert result.watermark_delta.last_seen_ts == datetime(2025, 1, 15, tzinfo=UTC)


def test_partition_gate_rejects_when_all_rows_unparseable(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    """Hard fail when the watermark column appears to be misnamed."""
    page = [dict(row, fecha_recibo_traslado="???") for row in hallazgos_page]
    client = fake_client_factory([page])

    with pytest.raises(IngestError, match="parseable"):
        ingest(hallazgos_spec, client=client)


def test_ingest_not_ready_spec_refuses(hallazgos_spec: DatasetSpec) -> None:
    # Strip the bits that make it ingest-ready.
    unready = hallazgos_spec.model_copy(update={"watermark_column": None})
    with pytest.raises(IngestError, match="not ingest-ready"):
        ingest(unready)


def test_full_refresh_ignores_existing_watermark(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    wm.set(
        wm.Watermark(
            source=hallazgos_spec.id,
            last_seen_ts=datetime(2030, 1, 1, tzinfo=UTC),
            last_batch_id="seed",
            row_count=0,
        )
    )
    client = fake_client_factory([hallazgos_page])
    result = ingest(hallazgos_spec, client=client, full_refresh=True)
    assert result.rows == 3
    # Full-refresh MUST NOT issue a $where clause.
    assert client.requests[0]["where"] is None


def test_coverage_report_standalone() -> None:
    frame = pd.DataFrame({"col_a": ["x", "y", ""], "col_b": ["1", "", ""]})
    report = assert_coverage("ds-id", frame, {"col_a": 0.5})
    assert report.coverage["col_a"] == pytest.approx(2 / 3)

    with pytest.raises(CoverageFailure):
        assert_coverage("ds-id", frame, {"col_b": 0.5})


def test_snapshot_mode_writes_to_snapshot_partition(
    fake_client_factory,
) -> None:
    """full_refresh_only datasets land under snapshot=<iso>/, no watermark."""
    spec = DatasetSpec(
        id="snap-tst1",
        name="Snapshot test",
        tier="context",
        columns_map={"contract_id": "id_contrato", "value": "valor"},
        required_coverage={"id_contrato": 0.99},
        full_refresh_only=True,
        url="https://www.datos.gov.co/d/snap-tst1",
    )
    page = [
        {"id_contrato": f"C{i}", "valor": str(i * 1000), ":id": str(i)}
        for i in range(1, 6)
    ]
    client = fake_client_factory([page])

    result = ingest(spec, client=client)

    assert result.ingested
    assert result.rows == 5
    assert result.watermark_delta is None  # snapshot mode never advances
    assert result.partitions == []  # no year/month partitions
    # Snapshot path lives under snapshot=<iso>/, not year=/month=/.
    assert len(result.parquet_paths) == 1
    assert "snapshot=" in str(result.parquet_paths[0])
    # Snapshot-mode call MUST NOT issue a $where clause.
    assert client.requests[0]["where"] is None
    assert client.requests[0]["order"] == ":id ASC"


def test_snapshot_mode_requires_columns_map() -> None:
    """A full_refresh_only spec without columns_map is not ingest-ready."""
    spec = DatasetSpec(
        id="snap-tst2",
        name="Bad snapshot",
        tier="context",
        full_refresh_only=True,
        url="https://www.datos.gov.co/d/snap-tst2",
    )
    assert not spec.is_ingest_ready()
    with pytest.raises(IngestError, match="not ingest-ready"):
        ingest(spec)


def test_ingest_every_core_spec_in_catalog_is_either_placeholder_or_ready() -> None:
    """Wave 3 gate: either the YAML is a placeholder, or it is ingest-ready.

    Wave 4 fills placeholders in one dataset at a time; this test keeps the
    catalog honest so half-filled YAMLs can't merge unnoticed.
    """
    from coacc_etl.catalog import load_catalog

    specs = load_catalog()
    for dataset_id, spec in specs.items():
        if spec.tier != "core":
            continue
        placeholder = (
            spec.watermark_column is None
            and spec.partition_column is None
            and not spec.columns_map
        )
        assert placeholder or spec.is_ingest_ready(), (
            f"{dataset_id}: half-filled YAML — watermark_column="
            f"{spec.watermark_column!r}, partition_column={spec.partition_column!r},"
            f" columns_map_keys={list(spec.columns_map)!r}"
        )
