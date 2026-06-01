"""coacc-etl CLI — lakehouse-only after Wave 4.B.

The Neo4j-loading ``run`` / ``sources`` subcommands were retired alongside
the bespoke Pipeline stack. Surviving subcommands all flow through
YAML-declared lake ingesters:

- ``coacc-etl ingest <id>``      — pull one ingest-ready dataset into the lake
- ``coacc-etl ingest-all``       — pull every ingest-ready tier=core dataset
- ``coacc-etl curate``           — build curated DuckDB parquet outputs
- ``coacc-etl signals ...``      — materialize signal runs from curated parquet
- ``coacc-etl qualify ...``      — thin wrapper over ``coacc-source-qualification``
"""

import logging
import sys

import click

from coacc_etl.catalog import DatasetSpec, load_catalog
from coacc_etl.curated import CuratedBuildError, build_curated
from coacc_etl.ingest import IngestError
from coacc_etl.ingest import ingest as run_ingest
from coacc_etl.models.anomaly import (
    AnomalyModelError,
    build_anomaly_features,
    evaluate_scores,
    predict_anomaly_scores,
    promote_anomaly_model,
    train_anomaly_model,
)
from coacc_etl.operations.phase7 import Phase7RunError, run_phase7
from coacc_etl.signals import SignalMaterializationError, materialize_signals


@click.group()
def cli() -> None:
    """CO-ACC ETL — config-driven ingestion into the parquet lake."""
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
    help="Rows per Socrata page or custom-adapter chunk",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum Socrata pages to fetch (default: COACC_SOCRATA_MAX_PAGES or 10,000)",
)
@click.option(
    "--timeout-seconds",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help=(
        "Initial Socrata request timeout; retries double up to "
        "COACC_SOCRATA_MAX_TIMEOUT_SECONDS or 240 seconds"
    ),
)
def ingest_cmd(
    dataset_id: str,
    full_refresh: bool,
    page_size: int | None,
    max_pages: int | None,
    timeout_seconds: float | None,
) -> None:
    """Ingest one ingest-ready dataset into the lake."""
    specs = load_catalog()
    spec = specs.get(dataset_id)
    if spec is None:
        raise click.ClickException(
            f"dataset_id {dataset_id!r} not in signed catalog (known: {len(specs)} datasets)"
        )
    try:
        result = run_ingest(
            spec,
            full_refresh=full_refresh,
            page_size=page_size,
            max_pages=max_pages,
            timeout=timeout_seconds,
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
    help="Rows per Socrata page or custom-adapter chunk",
)
@click.option(
    "--max-pages",
    type=click.IntRange(min=1),
    default=None,
    help="Maximum Socrata pages to fetch (default: COACC_SOCRATA_MAX_PAGES or 10,000)",
)
@click.option(
    "--timeout-seconds",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help=(
        "Initial Socrata request timeout; retries double up to "
        "COACC_SOCRATA_MAX_TIMEOUT_SECONDS or 240 seconds"
    ),
)
def ingest_all_cmd(
    full_refresh: bool,
    continue_on_error: bool,
    page_size: int | None,
    max_pages: int | None,
    timeout_seconds: float | None,
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
            result = run_ingest(
                spec,
                full_refresh=full_refresh,
                page_size=page_size,
                max_pages=max_pages,
                timeout=timeout_seconds,
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
    "--timeout-seconds",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help=(
        "Initial Socrata request timeout; retries double up to "
        "COACC_SOCRATA_MAX_TIMEOUT_SECONDS or 240 seconds"
    ),
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
    timeout_seconds: float | None,
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
            timeout_seconds=timeout_seconds,
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


@cli.command(name="curate")
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="Curated table to rebuild; repeat for multiple tables. Defaults to all.",
)
@click.option(
    "--all",
    "build_all",
    is_flag=True,
    help="Explicitly rebuild every shipped curated table (default when --table is omitted).",
)
def curate_cmd(tables: tuple[str, ...], build_all: bool) -> None:
    """Build DuckDB-curated parquet tables from the raw lake."""
    if build_all and tables:
        raise click.ClickException("use either --all or --table, not both")
    try:
        results = build_curated(None if build_all or not tables else tables)
    except CuratedBuildError as exc:
        raise click.ClickException(str(exc)) from exc
    for result in results:
        click.echo(f"{result.table}: wrote {result.rows:,} rows to {result.path}")


@cli.group(name="signals")
def signals_group() -> None:
    """Materialize downstream signal outputs from curated parquet."""


@signals_group.command(name="materialize")
@click.option(
    "--signal",
    "signal_ids",
    multiple=True,
    help="Signal id to materialize; repeat for multiple signals. Defaults to all shipped signals.",
)
@click.option(
    "--all",
    "build_all",
    is_flag=True,
    help="Explicitly materialize every shipped signal (default when --signal is omitted).",
)
@click.option(
    "--run-id",
    default=None,
    help="Optional run id for deterministic reruns and tests.",
)
@click.option(
    "--allow-empty",
    is_flag=True,
    help="Write outputs even if a selected signal currently produces zero hits.",
)
def signals_materialize_cmd(
    signal_ids: tuple[str, ...],
    build_all: bool,
    run_id: str | None,
    allow_empty: bool,
) -> None:
    """Write signal_hits and evidence_bundles parquet from lake/curated."""
    if build_all and signal_ids:
        raise click.ClickException("use either --all or --signal, not both")
    try:
        result = materialize_signals(
            None if build_all or not signal_ids else signal_ids,
            run_id=run_id,
            allow_empty=allow_empty,
        )
    except SignalMaterializationError as exc:
        raise click.ClickException(str(exc)) from exc

    for signal in result.signal_results:
        click.echo(f"{signal.signal_id}: materialized {signal.hit_count:,} hits")
    click.echo(
        f"signal run {result.run_id}: wrote {result.hit_count:,} hits and "
        f"{result.evidence_count:,} evidence rows"
    )
    click.echo(f"  signal_hits: {result.signal_hits_path}")
    click.echo(f"  evidence_bundles: {result.evidence_bundles_path}")
    click.echo(f"  manifest: {result.manifest_path}")


@cli.group(name="model")
def model_group() -> None:
    """Train, score, evaluate, and promote lake-backed models."""


@model_group.command(name="build-features")
@click.argument("model_name", type=click.Choice(["anomaly"]))
@click.option("--run-id", default=None, help="Optional deterministic feature run id.")
def model_build_features_cmd(model_name: str, run_id: str | None) -> None:
    """Build model feature parquet from curated lake tables."""
    del model_name
    try:
        result = build_anomaly_features(run_id=run_id)
    except AnomalyModelError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"anomaly features {result.run_id}: wrote {result.rows:,} rows")
    click.echo(f"  features: {result.feature_path}")
    click.echo(f"  manifest: {result.manifest_path}")


@model_group.command(name="train")
@click.argument("model_name", type=click.Choice(["anomaly"]))
@click.option("--run-id", default=None, help="Optional deterministic model run id.")
@click.option(
    "--max-training-rows",
    type=click.IntRange(min=1),
    default=200_000,
    show_default=True,
    help="Deterministic bounded sample size for model training.",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=100_000,
    show_default=True,
    help="Rows per prediction batch when scoring the feature parquet.",
)
@click.option("--promote/--no-promote", default=True, help="Update current.json on success.")
def model_train_cmd(
    model_name: str,
    run_id: str | None,
    max_training_rows: int,
    batch_size: int,
    promote: bool,
) -> None:
    """Train the requested model and write score parquet."""
    del model_name
    try:
        result = train_anomaly_model(
            run_id=run_id,
            max_training_rows=max_training_rows,
            batch_size=batch_size,
            promote=promote,
        )
    except AnomalyModelError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"anomaly model {result.run_id}: trained on {result.training_rows:,} rows; "
        f"scored {result.scored_rows:,} contracts"
    )
    click.echo(f"  model: {result.model_dir}")
    click.echo(f"  scores: {result.score_path}")
    click.echo(f"  metrics: {result.metrics_path}")
    if result.current_manifest_path:
        click.echo(f"  current: {result.current_manifest_path}")


@model_group.command(name="predict")
@click.argument("model_name", type=click.Choice(["anomaly"]))
@click.option("--model-run-id", default=None, help="Model run id; defaults to current.json.")
@click.option("--feature-run-id", default=None, help="Feature run id; defaults to model metadata.")
@click.option("--run-id", default=None, help="Optional deterministic score run id.")
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=100_000,
    show_default=True,
    help="Rows per prediction batch.",
)
def model_predict_cmd(
    model_name: str,
    model_run_id: str | None,
    feature_run_id: str | None,
    run_id: str | None,
    batch_size: int,
) -> None:
    """Score anomaly features with a trained model."""
    del model_name
    try:
        result = predict_anomaly_scores(
            model_run_id=model_run_id,
            feature_run_id=feature_run_id,
            run_id=run_id,
            batch_size=batch_size,
        )
    except AnomalyModelError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"anomaly scores {result.run_id}: wrote {result.rows:,} rows")
    click.echo(f"  scores: {result.score_path}")


@model_group.command(name="evaluate")
@click.argument("model_name", type=click.Choice(["anomaly"]))
@click.argument("score_run_id")
def model_evaluate_cmd(model_name: str, score_run_id: str) -> None:
    """Evaluate score parquet against sanction-derived labels."""
    del model_name
    try:
        result = evaluate_scores(score_run_id)
    except AnomalyModelError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"anomaly scores {result.score_run_id}: rows={result.scored_rows:,} "
        f"positives={result.positive_labels:,} "
        f"p@100={result.precision_at_100 if result.precision_at_100 is not None else '-'}"
    )


@model_group.command(name="promote")
@click.argument("model_name", type=click.Choice(["anomaly"]))
@click.argument("run_id")
def model_promote_cmd(model_name: str, run_id: str) -> None:
    """Promote a trained model run as current."""
    del model_name
    try:
        path = promote_anomaly_model(run_id)
    except AnomalyModelError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"promoted anomaly model {run_id}: {path}")


@cli.command(name="qualify", context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def qualify_cmd(args: tuple[str, ...]) -> None:
    """Thin wrapper over ``coacc-source-qualification``."""
    from coacc_etl import source_qualification as sq

    sys.argv = ["coacc-source-qualification", *args]
    raise SystemExit(sq.main())


if __name__ == "__main__":
    cli()
