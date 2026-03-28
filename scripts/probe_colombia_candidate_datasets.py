#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

import click

REPO_ROOT = Path(__file__).resolve().parents[1]
ETL_SRC = REPO_ROOT / "etl" / "src"
if str(ETL_SRC) not in sys.path:
    sys.path.insert(0, str(ETL_SRC))

from coacc_etl.candidate_probe import (  # noqa: E402
    DEFAULT_DOC_PATHS,
    DEFAULT_REGISTRY_PATH,
    SocrataProbeClient,
    build_universe_indexes,
    extract_candidate_dataset_ids,
    probe_dataset,
    write_probe_outputs,
)


@click.command()
@click.option(
    "--candidate-id",
    "candidate_ids",
    multiple=True,
    help="Specific dataset id(s) to probe.",
)
@click.option("--sample-limit", default=250, show_default=True, type=int)
@click.option(
    "--json-output",
    default="audit-results/source-probes/latest/candidate-dataset-probe-2026-03-25.json",
    show_default=True,
)
@click.option(
    "--markdown-output",
    default="docs/candidate_dataset_probe_2026-03-25.md",
    show_default=True,
)
def main(
    candidate_ids: tuple[str, ...],
    sample_limit: int,
    json_output: str,
    markdown_output: str,
) -> None:
    repo_root = REPO_ROOT
    selected_ids = (
        list(candidate_ids)
        if candidate_ids
        else extract_candidate_dataset_ids(
            doc_paths=tuple(repo_root / path for path in DEFAULT_DOC_PATHS),
            registry_path=repo_root / DEFAULT_REGISTRY_PATH,
        )
    )
    if not selected_ids:
        raise click.ClickException("No candidate dataset ids found to probe.")

    click.echo(f"Building live-universe indexes for {len(selected_ids)} candidate datasets...")
    universe = build_universe_indexes(repo_root)
    client = SocrataProbeClient()
    try:
        results = []
        for index, dataset_id in enumerate(selected_ids, start=1):
            click.echo(f"[{index}/{len(selected_ids)}] Probing {dataset_id}...")
            results.append(
                probe_dataset(
                    client,
                    dataset_id=dataset_id,
                    universe=universe,
                    sample_limit=sample_limit,
                )
            )
        results.sort(key=lambda item: (item.recommendation, item.dataset_id))
        write_probe_outputs(
            results=results,
            json_path=repo_root / json_output,
            markdown_path=repo_root / markdown_output,
        )
    finally:
        client.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result.recommendation] = counts.get(result.recommendation, 0) + 1
    click.echo(f"Probe complete: {counts}")


if __name__ == "__main__":
    main()
