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

import httpx
import pandas as pd
import pyarrow.parquet as pq
import pytest

from coacc_etl.catalog import DatasetSpec
from coacc_etl.ingest import IngestError, SocrataClient, assert_coverage, ingest
from coacc_etl.ingest.coverage import CoverageFailure
from coacc_etl.ingest.socrata import _iso_for_where
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


def test_ingest_streams_multiple_pages_through_staging(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    pages = [[hallazgos_page[0]], hallazgos_page[1:]]
    client = fake_client_factory(pages)

    result = ingest(hallazgos_spec, client=client)

    assert result.ingested
    assert result.rows == 3
    assert result.coverage is not None
    assert result.coverage.rows == 3
    assert {(y, m) for y, m in result.partitions} == {(2024, 12), (2025, 1)}
    assert len(_read_all_parquets(hallazgos_spec.id)) == 3
    assert not list((meta_path() / "ingest_staging").rglob("*.parquet"))


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


def test_socrata_client_pagination_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("COACC_SOCRATA_PAGE_SIZE", "12345")
    monkeypatch.setenv("COACC_SOCRATA_MAX_PAGES", "678")

    client = SocrataClient.from_env()
    try:
        assert client.page_size == 12345
        assert client.max_pages == 678
    finally:
        client.close()


def test_socrata_client_rejects_invalid_pagination_env(monkeypatch) -> None:
    monkeypatch.setenv("COACC_SOCRATA_PAGE_SIZE", "0")

    with pytest.raises(IngestError, match="COACC_SOCRATA_PAGE_SIZE"):
        SocrataClient.from_env()


def test_socrata_client_timeout_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("COACC_SOCRATA_TIMEOUT_SECONDS", "90.5")
    monkeypatch.setenv("COACC_SOCRATA_MAX_TIMEOUT_SECONDS", "300")

    client = SocrataClient.from_env()
    try:
        assert client.timeout == 90.5
        assert client.max_timeout == 300
    finally:
        client.close()


def test_socrata_client_rejects_invalid_timeout() -> None:
    with pytest.raises(IngestError, match="max_timeout"):
        SocrataClient.from_env(timeout=300, max_timeout=60)


def test_socrata_client_expands_timeout_between_retries() -> None:
    request = httpx.Request("GET", "https://example.test/resource/foo.json")

    class OkResponse:
        status_code = 200

        def __init__(self) -> None:
            self.request = request

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return [{"id": "1"}]

    class FlakyHTTP:
        def __init__(self) -> None:
            self.timeouts: list[float] = []

        def get(
            self,
            _url: str,
            *,
            params: dict[str, str | int],
            timeout: float,
        ) -> OkResponse:
            del params
            self.timeouts.append(timeout)
            if len(self.timeouts) == 1:
                raise httpx.ReadTimeout("slow page", request=request)
            return OkResponse()

        def close(self) -> None:
            return None

    http = FlakyHTTP()
    client = SocrataClient(
        http=http,  # type: ignore[arg-type]
        timeout=60,
        max_timeout=180,
        sleep_fn=lambda _seconds: None,
    )

    assert client._get("https://example.test/resource/foo.json", {}) == [{"id": "1"}]
    assert http.timeouts == [60, 120]


def test_socrata_client_retries_invalid_json_response() -> None:
    request = httpx.Request("GET", "https://example.test/resource/foo.json")

    class Response:
        status_code = 200

        def __init__(self, *, malformed: bool) -> None:
            self.malformed = malformed
            self.request = request

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            if self.malformed:
                raise ValueError("truncated JSON")
            return [{"id": "1"}]

    class FlakyHTTP:
        def __init__(self) -> None:
            self.calls = 0

        def get(
            self,
            _url: str,
            *,
            params: dict[str, str | int],
            timeout: float,
        ) -> Response:
            del params, timeout
            self.calls += 1
            return Response(malformed=self.calls == 1)

        def close(self) -> None:
            return None

    http = FlakyHTTP()
    client = SocrataClient(
        http=http,  # type: ignore[arg-type]
        sleep_fn=lambda _seconds: None,
    )

    assert client._get("https://example.test/resource/foo.json", {}) == [{"id": "1"}]
    assert http.calls == 2


def test_socrata_client_keyset_paginates_without_offset() -> None:
    request = httpx.Request("GET", "https://example.test/resource/foo.json")

    class OkResponse:
        status_code = 200

        def __init__(self, payload: list[dict[str, str]]) -> None:
            self._payload = payload
            self.request = request

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return self._payload

    class FakeHTTP:
        def __init__(self) -> None:
            self.calls: list[dict[str, str | int]] = []

        def get(
            self,
            _url: str,
            *,
            params: dict[str, str | int],
            timeout: float,
        ) -> OkResponse:
            del timeout
            self.calls.append(dict(params))
            if len(self.calls) == 1:
                return OkResponse([])
            if len(self.calls) == 2:
                return OkResponse(
                    [
                        {":id": "row-a", "wm": "2020-01-01T00:00:00.000"},
                        {":id": "row-b", "wm": "2020-01-01T00:00:00.000"},
                    ]
                )
            return OkResponse([])

        def close(self) -> None:
            return None

    http = FakeHTTP()
    client = SocrataClient(http=http, page_size=2, sleep_fn=lambda _seconds: None)  # type: ignore[arg-type]

    pages = list(
        client.fetch(
            "abcd-1234",
            where=None,
            order="wm ASC, :id ASC",
            pagination="keyset",
            keyset_column="wm",
        )
    )

    assert pages == [
        [
            {":id": "row-a", "wm": "2020-01-01T00:00:00.000"},
            {":id": "row-b", "wm": "2020-01-01T00:00:00.000"},
        ]
    ]
    assert all("$offset" not in call for call in http.calls)
    assert http.calls[0]["$where"] == "wm IS NULL"
    assert http.calls[0]["$order"] == ":id ASC"
    assert http.calls[1]["$where"] == "wm IS NOT NULL"
    assert http.calls[2]["$where"] == (
        "(wm IS NOT NULL) AND "
        "(wm = '2020-01-01T00:00:00.000') AND (:id > 'row-b')"
    )
    assert http.calls[2]["$order"] == ":id ASC"
    assert http.calls[3]["$where"] == (
        "(wm IS NOT NULL) AND (wm > '2020-01-01T00:00:00.000')"
    )
    assert http.calls[3]["$order"] == "wm ASC, :id ASC"
    assert all(call["$select"] == "*, :id" for call in http.calls)


def test_socrata_client_keyset_paginates_null_segment_by_id() -> None:
    request = httpx.Request("GET", "https://example.test/resource/foo.json")

    class OkResponse:
        status_code = 200

        def __init__(self, payload: list[dict[str, str]]) -> None:
            self._payload = payload
            self.request = request

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return self._payload

    class FakeHTTP:
        def __init__(self) -> None:
            self.calls: list[dict[str, str | int]] = []

        def get(
            self,
            _url: str,
            *,
            params: dict[str, str | int],
            timeout: float,
        ) -> OkResponse:
            del timeout
            self.calls.append(dict(params))
            if len(self.calls) == 1:
                return OkResponse([{":id": "row-null-a"}])
            if len(self.calls) == 2:
                return OkResponse([])
            if len(self.calls) == 3:
                return OkResponse([])
            raise AssertionError("unexpected extra keyset request")

        def close(self) -> None:
            return None

    http = FakeHTTP()
    client = SocrataClient(http=http, page_size=1, sleep_fn=lambda _seconds: None)  # type: ignore[arg-type]

    pages = list(
        client.fetch(
            "abcd-1234",
            where=None,
            order="wm ASC, :id ASC",
            pagination="keyset",
            keyset_column="wm",
        )
    )

    assert pages == [[{":id": "row-null-a"}]]
    assert http.calls[1]["$where"] == "(wm IS NULL) AND (:id > 'row-null-a')"
    assert http.calls[2]["$where"] == "wm IS NOT NULL"


def test_incremental_where_preserves_fractional_seconds() -> None:
    value = datetime(2022, 12, 13, 14, 57, 31, 146000, tzinfo=UTC)

    assert _iso_for_where(value) == "2022-12-13T14:57:31.146000"


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
    # Staged parquet is cleaned and never appears as final raw data.
    assert not list((meta_path() / "ingest_staging").rglob("*.parquet"))


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


def test_missing_partition_column_page_lands_in_sentinel(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    """Socrata omits keys that are NULL for every row on a page."""
    spec = hallazgos_spec.model_copy(
        update={
            "required_coverage": {
                "nit": 0.95,
                "nombre_sujeto": 0.95,
                "fecha_recibo_traslado": 0.50,
            }
        }
    )
    missing_date_page = [
        {
            "nit": "900000000",
            "nombre_sujeto": "ENTIDAD SIN FECHA",
            "radicado": "missing-date-page",
        }
    ]
    client = fake_client_factory([hallazgos_page, missing_date_page])

    result = ingest(spec, client=client)

    assert result.rows == 4
    assert (0, 0) in result.partitions
    assert result.watermark_delta is not None
    assert result.watermark_delta.last_seen_ts == datetime(2025, 1, 15, tzinfo=UTC)
    df = _read_all_parquets(spec.id)
    assert len(df) == 4
    assert "missing-date-page" in set(df["radicado"].astype(str))


def test_future_watermark_rows_do_not_poison_state(
    hallazgos_spec: DatasetSpec,
    hallazgos_page: list[dict[str, object]],
    fake_client_factory,
) -> None:
    """Implausible future source dates survive without advancing state."""
    page = list(hallazgos_page)
    page[1] = dict(page[1], fecha_recibo_traslado="2099-12-30T00:00:00.000")
    client = fake_client_factory([page])

    result = ingest(hallazgos_spec, client=client)

    assert result.rows == 3
    assert set(result.partitions) == {(0, 0), (2024, 12), (2025, 1)}
    assert result.watermark_delta is not None
    assert result.watermark_delta.last_seen_ts == datetime(2025, 1, 15, tzinfo=UTC)
    assert wm.get(hallazgos_spec.id) is not None
    assert not list(raw_source_path(hallazgos_spec.id).glob("year=2099/**"))


def test_future_only_incremental_page_is_skipped(
    hallazgos_spec: DatasetSpec,
    fake_client_factory,
) -> None:
    wm.set(
        wm.Watermark(
            source=hallazgos_spec.id,
            last_seen_ts=datetime(2025, 1, 15, tzinfo=UTC),
            last_batch_id="seed",
            row_count=3,
        )
    )
    page = [
        {
            "nit": "891580016",
            "nombre_sujeto": "GOBERNACION DEL CAUCA",
            "fecha_recibo_traslado": "2099-12-30T00:00:00.000",
            "radicado": "future-only",
        }
    ]
    client = fake_client_factory([page])

    result = ingest(hallazgos_spec, client=client)

    assert result.rows == 0
    assert result.skipped_reason == "only_future_watermarks"
    stored = wm.get(hallazgos_spec.id)
    assert stored is not None
    assert stored.last_seen_ts == datetime(2025, 1, 15, tzinfo=UTC)
    assert not list(raw_source_path(hallazgos_spec.id).rglob("*.parquet"))
    assert not list((meta_path() / "ingest_staging").rglob("*.parquet"))


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
