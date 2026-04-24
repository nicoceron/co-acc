import logging
import os
from datetime import UTC, datetime

import click
from neo4j import GraphDatabase

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
@click.option(
    "--to-lake/--to-neo4j",
    default=False,
    help="Write pipeline output to the Parquet lake",
)
@click.option("--full-refresh/--incremental", default=False, help="Ignore lake watermarks")
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
    to_lake: bool,
    full_refresh: bool,
) -> None:
    """Run an ETL pipeline."""
    os.environ["NEO4J_DATABASE"] = neo4j_database

    pipeline_spec = get_pipeline_spec(source)
    if pipeline_spec is None:
        available = ", ".join(list_pipeline_names())
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")
    pipeline_cls = pipeline_spec.pipeline_cls

    if to_lake:
        pipeline = pipeline_cls(
            driver=None,
            data_dir=data_dir,
            limit=limit,
            chunk_size=chunk_size,
            history=history,
        )
        if not hasattr(pipeline, "run_to_lake"):
            raise click.ClickException(f"Pipeline {source} does not implement --to-lake")
        delta = pipeline.run_to_lake(full_refresh=full_refresh)
        click.echo(
            f"{delta.source}: wrote {delta.rows:,} rows "
            f"(batch={delta.batch_id}, advanced={delta.advanced})"
        )
        return

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


if __name__ == "__main__":
    cli()
