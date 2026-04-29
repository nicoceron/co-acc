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

from coacc_etl.catalog import load_catalog
from coacc_etl.ingest import IngestError
from coacc_etl.ingest import ingest as socrata_ingest


@click.group()
def cli() -> None:
    """CO-ACC ETL — config-driven Socrata ingestion into the parquet lake."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _dataset_core_order(specs: dict) -> list[str]:
    # Deterministic dep-safe order: contract- / entity- / nit-keyed first
    # (they anchor joins), then the rest. Within tiers, sort by dataset id.
    core = [s for s in specs.values() if s.tier == "core"]
    anchors = {"contract", "entity", "nit"}

    def key(spec) -> tuple[int, str]:
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
def ingest_cmd(dataset_id: str, full_refresh: bool) -> None:
    """Ingest one ingest-ready dataset from Socrata into the lake."""
    specs = load_catalog()
    spec = specs.get(dataset_id)
    if spec is None:
        raise click.ClickException(
            f"dataset_id {dataset_id!r} not in signed catalog "
            f"(known: {len(specs)} datasets)"
        )
    try:
        result = socrata_ingest(spec, full_refresh=full_refresh)
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
def ingest_all_cmd(full_refresh: bool, continue_on_error: bool) -> None:
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
            result = socrata_ingest(spec, full_refresh=full_refresh)
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


@cli.command(name="qualify", context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def qualify_cmd(args: tuple[str, ...]) -> None:
    """Thin wrapper over ``coacc-source-qualification``."""
    from coacc_etl import source_qualification as sq

    sys.argv = ["coacc-source-qualification", *args]
    raise SystemExit(sq.main())


if __name__ == "__main__":
    cli()
