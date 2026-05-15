"""coacc-etl CLI — lakehouse-only after Wave 4.B.

The Neo4j-loading ``run`` / ``sources`` subcommands were retired alongside
the bespoke Pipeline stack. Surviving subcommands all flow through the
generic YAML-driven Socrata ingester:

- ``coacc-etl ingest <id>``      — pull one tier=core dataset into the lake
- ``coacc-etl ingest-all``       — pull every ingest-ready tier=core dataset
- ``coacc-etl qualify ...``      — thin wrapper over ``coacc-source-qualification``
"""
import logging
import sys

import click

from coacc_etl.catalog import DatasetSpec, load_catalog
from coacc_etl.ingest import IngestError
from coacc_etl.ingest import ingest as socrata_ingest
from coacc_etl.operations.phase7 import Phase7RunError, run_phase7


@click.group()
def cli() -> None:
    """CO-ACC ETL — config-driven Socrata ingestion into the parquet lake."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _dataset_core_order(specs: dict[str, DatasetSpec]) -> list[str]:
    # Deterministic dep-safe order: contract- / entity- / nit-keyed first
    # (they anchor joins), then the rest. Within tiers, sort by dataset id.
    core = [s for s in specs.values() if s.tier == "core"]
    anchors = {"contract", "entity", "nit"}

    def key(spec: DatasetSpec) -> tuple[int, str]:
        classes = set(spec.join_keys.keys())
        anchor_rank = 0 if classes & anchors else 1
        return (anchor_rank, spec.id)

    return [spec.id for spec in sorted(core, key=key)]


@cli.command(name="ingest")
@click.argument("dataset_id")
@click.option(
    "--full-refresh/--incremental",
    default=False,
    help="Ignore lake watermark and re-pull from the beginning",
)
@click.option(
    "--page-size",
    type=click.IntRange(min=1),
    default=None,
    help="Socrata rows per page (default: COACC_SOCRATA_PAGE_SIZE or 10,000)",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum Socrata pages to fetch (default: COACC_SOCRATA_MAX_PAGES or 10,000)",
)
def ingest_cmd(
    dataset_id: str,
    full_refresh: bool,
    page_size: int | None,
    max_pages: int | None,
) -> None:
    """Ingest one ingest-ready dataset from Socrata into the lake."""
    specs = load_catalog()
    spec = specs.get(dataset_id)
    if spec is None:
        raise click.ClickException(
            f"dataset_id {dataset_id!r} not in signed catalog "
            f"(known: {len(specs)} datasets)"
        )
    try:
        result = socrata_ingest(
            spec,
            full_refresh=full_refresh,
            page_size=page_size,
            max_pages=max_pages,
        )
    except IngestError as exc:
        raise click.ClickException(str(exc)) from exc

    if not result.ingested:
        click.echo(f"{dataset_id}: {result.skipped_reason or 'no-op'}")
        return
    if spec.full_refresh_only:
        snapshot = result.parquet_paths[0].parent.name if result.parquet_paths else "?"
        click.echo(f"{dataset_id}: wrote {result.rows:,} rows to {snapshot}")
    else:
        click.echo(
            f"{dataset_id}: wrote {result.rows:,} rows across "
            f"{len(result.partitions)} partition(s); watermark -> "
            f"{result.watermark_delta.last_seen_ts.isoformat() if result.watermark_delta else '-'}"
        )


@cli.command(name="ingest-all")
@click.option(
    "--full-refresh/--incremental",
    default=False,
    help="Ignore lake watermarks for every dataset",
)
@click.option(
    "--continue-on-error/--stop-on-error",
    default=False,
    help="Keep going if one dataset fails (default stops)",
)
@click.option(
    "--page-size",
    type=click.IntRange(min=1),
    default=None,
    help="Socrata rows per page (default: COACC_SOCRATA_PAGE_SIZE or 10,000)",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum Socrata pages to fetch (default: COACC_SOCRATA_MAX_PAGES or 10,000)",
)
def ingest_all_cmd(
    full_refresh: bool,
    continue_on_error: bool,
    page_size: int | None,
    max_pages: int | None,
) -> None:
    """Ingest every ``tier: core`` dataset in dep-safe order."""
    specs = load_catalog()
    ids = _dataset_core_order(specs)
    if not ids:
        click.echo("no tier=core datasets are ingest-ready")
        return

    ok = 0
    skipped: list[str] = []
    failed: list[tuple[str, str]] = []
    for dataset_id in ids:
        spec = specs[dataset_id]
        if not spec.is_ingest_ready():
            skipped.append(dataset_id)
            continue
        try:
            result = socrata_ingest(
                spec,
                full_refresh=full_refresh,
                page_size=page_size,
                max_pages=max_pages,
            )
            if result.ingested:
                ok += 1
                click.echo(f"  {dataset_id}: {result.rows:,} rows")
            else:
                click.echo(f"  {dataset_id}: {result.skipped_reason}")
        except IngestError as exc:
            failed.append((dataset_id, str(exc)))
            click.echo(f"  {dataset_id}: FAIL — {exc}", err=True)
            if not continue_on_error:
                break

    click.echo(
        f"ingest-all: ok={ok}, skipped={len(skipped)}, failed={len(failed)} "
        f"of {len(ids)} core datasets"
    )
    if failed and not continue_on_error:
        raise click.ClickException(f"stopped on first failure: {failed[0][0]}")


@cli.command(name="ingest-phase7")
@click.option(
    "--mode",
    type=click.Choice(["smoke", "full"]),
    default="smoke",
    show_default=True,
    help="Smoke seeds recent watermarks; full runs initial full-refresh ingests.",
)
@click.option(
    "--dataset",
    "dataset_ids",
    multiple=True,
    help="Run a subset of Phase 7 dataset ids; repeat for multiple ids.",
)
@click.option(
    "--continue-on-error/--stop-on-error",
    default=False,
    help="Keep going if one dataset fails (default stops)",
)
@click.option(
    "--min-free-gb",
    type=click.FloatRange(min=0),
    default=None,
    help="Minimum free disk under COACC_LAKE_ROOT before starting.",
)
@click.option(
    "--page-size",
    type=click.IntRange(min=1),
    default=None,
    help="Socrata rows per page; mode-specific defaults are used when omitted.",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum Socrata pages per dataset; mode-specific defaults are used when omitted.",
)
@click.option(
    "--smoke-days",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="In smoke mode, seed to max(watermark)-N days when no watermark exists.",
)
def ingest_phase7_cmd(
    mode: str,
    dataset_ids: tuple[str, ...],
    continue_on_error: bool,
    min_free_gb: float | None,
    page_size: int | None,
    max_pages: int | None,
    smoke_days: int,
) -> None:
    """Run the Phase 7 ingest sequence with disk checks and run logging."""
    try:
        records = run_phase7(
            mode="full" if mode == "full" else "smoke",
            dataset_ids=dataset_ids or None,
            continue_on_error=continue_on_error,
            min_free_gb=min_free_gb,
            page_size=page_size,
            max_pages=max_pages,
            smoke_days=smoke_days,
        )
    except Phase7RunError as exc:
        raise click.ClickException(str(exc)) from exc

    ok = sum(1 for record in records if record.status == "ok")
    skipped = sum(1 for record in records if record.status == "skipped")
    failed = sum(1 for record in records if record.status == "failed")
    for record in records:
        click.echo(
            f"{record.dataset_id}: {record.status} rows={record.rows:,} "
            f"coverage={record.coverage} watermark={record.watermark}"
        )
    click.echo(f"phase7 {mode}: ok={ok}, skipped={skipped}, failed={failed}")
    if failed:
        raise click.ClickException(f"phase7 {mode}: {failed} dataset(s) failed")


@cli.command(name="qualify", context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def qualify_cmd(args: tuple[str, ...]) -> None:
    """Thin wrapper over ``coacc-source-qualification``."""
    from coacc_etl import source_qualification as sq

    sys.argv = ["coacc-source-qualification", *args]
    raise SystemExit(sq.main())


if __name__ == "__main__":
    cli()
