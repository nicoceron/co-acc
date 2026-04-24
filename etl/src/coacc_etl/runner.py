import logging
import os
import sys
from datetime import UTC, datetime

import click
from neo4j import GraphDatabase

from coacc_etl.catalog import load_catalog
from coacc_etl.ingest import IngestError
from coacc_etl.ingest import ingest as socrata_ingest
from coacc_etl.linking_hooks import run_post_load_hooks
from coacc_etl.pipeline_registry import get_pipeline_spec, list_pipeline_names


@click.group()
def cli() -> None:
    """CO-ACC ETL — Data ingestion pipelines for Colombian public data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@cli.command()
@click.option(
    "--source",
    "--pipeline",
    "source",
    required=True,
    help="Pipeline name (see 'sources' command)",
)
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j", help="Neo4j user")
@click.option("--neo4j-password", default=None, help="Neo4j password")
@click.option("--neo4j-database", default="neo4j", help="Neo4j database")
@click.option("--data-dir", default="./data", help="Directory for downloaded data")
@click.option("--limit", type=int, default=None, help="Limit rows processed")
@click.option("--chunk-size", type=int, default=50_000, help="Chunk size for batch processing")
@click.option(
    "--linking-tier",
    type=click.Choice(["community", "full"]),
    default=os.getenv("LINKING_TIER", "full"),
    show_default=True,
    help="Post-load linking strategy tier",
)
@click.option("--streaming/--no-streaming", default=False, help="Streaming mode")
@click.option("--start-phase", type=int, default=1, help="Skip to phase N")
@click.option("--history/--no-history", default=False, help="Enable history mode when supported")
def run(
    source: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    neo4j_database: str,
    data_dir: str,
    limit: int | None,
    chunk_size: int,
    linking_tier: str,
    streaming: bool,
    start_phase: int,
    history: bool,
) -> None:
    """Run a legacy Neo4j pipeline.

    Kept for Wave 4 parity. Use ``coacc-etl ingest`` for lakehouse writes.
    """
    os.environ["NEO4J_DATABASE"] = neo4j_database

    pipeline_spec = get_pipeline_spec(source)
    if pipeline_spec is None:
        available = ", ".join(list_pipeline_names())
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")
    pipeline_cls = pipeline_spec.pipeline_cls

    if not neo4j_password:
        neo4j_password = os.environ.get("NEO4J_PASSWORD")
    if not neo4j_password:
        raise click.ClickException("--neo4j-password or NEO4J_PASSWORD env var required")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        pipeline = pipeline_cls(
            driver=driver,
            data_dir=data_dir,
            limit=limit,
            chunk_size=chunk_size,
            history=history,
        )

        if streaming and hasattr(pipeline, "run_streaming"):
            started_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            pipeline._upsert_ingestion_run(status="running", started_at=started_at)
            try:
                pipeline.run_streaming(start_phase=start_phase)
                finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                pipeline._upsert_ingestion_run(
                    status="loaded",
                    started_at=started_at,
                    finished_at=finished_at,
                )
            except Exception as exc:
                finished_at = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                pipeline._upsert_ingestion_run(
                    status="quality_fail",
                    started_at=started_at,
                    finished_at=finished_at,
                    error=str(exc)[:1000],
                )
                raise
        else:
            pipeline.run()

        run_post_load_hooks(
            driver=driver,
            source=source,
            neo4j_database=neo4j_database,
            linking_tier=linking_tier,
        )
    finally:
        driver.close()


@cli.command()
@click.option("--status", "show_status", is_flag=True, help="Show ingestion status from Neo4j")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j")
@click.option("--neo4j-password", default=None)
def sources(show_status: bool, neo4j_uri: str, neo4j_user: str, neo4j_password: str | None) -> None:
    """List available data sources."""
    specs = {name: get_pipeline_spec(name) for name in list_pipeline_names()}
    if not show_status:
        click.echo("Available pipelines:")
        for name, spec in specs.items():
            if spec is None or spec.source_id == name:
                click.echo(f"  {name}")
                continue
            click.echo(f"  {name} -> {spec.source_id}")
        return

    if not neo4j_password:
        neo4j_password = os.environ.get("NEO4J_PASSWORD", "")
    if not neo4j_password:
        raise click.ClickException(
            "--neo4j-password or NEO4J_PASSWORD env var required for --status"
        )

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session() as session:
            result = session.run(
                "MATCH (r:IngestionRun) "
                "WITH r ORDER BY r.started_at DESC "
                "WITH r.source_id AS sid, collect(r)[0] AS latest "
                "RETURN latest ORDER BY sid"
            )
            runs = {r["latest"]["source_id"]: dict(r["latest"]) for r in result}

        click.echo(
            f"{'Pipeline':<28} {'Ingestion Id':<24} {'Status':<15} {'Rows In':>10} "
            f"{'Loaded':>10} {'Started':<20} {'Finished':<20}"
        )
        click.echo("-" * 132)

        for name, spec in specs.items():
            if spec is None:
                continue
            run = runs.get(spec.status_key, {})
            click.echo(
                f"{name:<28} "
                f"{spec.status_key:<24} "
                f"{run.get('status', '-'):<15} "
                f"{run.get('rows_in', 0):>10,} "
                f"{run.get('rows_loaded', 0):>10,} "
                f"{str(run.get('started_at', '-')):<20} "
                f"{str(run.get('finished_at', '-')):<20}"
            )
    finally:
        driver.close()


def _dataset_core_order(specs: dict) -> list[str]:
    # Deterministic dep-safe order: contract- and entity-keyed first (they
    # anchor joins), then the rest. Within tiers, sort by dataset id.
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
    """Ingest one ``tier: core`` dataset from Socrata into the lake."""
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
