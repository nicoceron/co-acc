#!/usr/bin/env python3
"""Probe local lake parquet health and write reality snapshots."""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from coacc_etl.catalog import DatasetSpec, load_catalog
from coacc_etl.lakehouse.health_diff import (
    Finding,
    diff_health,
    findings_have_failures,
    load_thresholds,
    render_diff_markdown,
    thresholds_for,
)
from coacc_etl.lakehouse.paths import lake_root, meta_path
from coacc_etl.lakehouse.reality import DatasetHealth, RealityError, compute_all_health

logger = logging.getLogger(__name__)


class RealityCliError(RuntimeError):
    """Operator-facing CLI error."""


class RealitySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime
    snapshot_date: str
    baseline_date: str | None = None
    datasets: list[DatasetHealth] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe local lake parquet health and diff against the last snapshot."
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Scan every catalog dataset present in the local lake. This is the default.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset ID to scan. May be passed more than once.",
    )
    parser.add_argument(
        "--datasets",
        default=None,
        help="Comma-separated dataset IDs to scan.",
    )
    parser.add_argument(
        "--changed-yamls-only",
        action="store_true",
        help="Scan catalog YAML files changed in git, useful for local pre-commit checks.",
    )
    parser.add_argument(
        "--with-live",
        action="store_true",
        help="Also call Socrata live counts. Local parquet metrics are always computed first.",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Baseline date (YYYY-MM-DD) or JSON snapshot path. Defaults to latest older snapshot.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Snapshot date (YYYY-MM-DD). Defaults to today's UTC date.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for JSON and Markdown outputs. Defaults to lake/meta/reality.",
    )
    parser.add_argument(
        "--thresholds",
        type=Path,
        default=None,
        help="Threshold YAML path. Defaults to config/reality_thresholds.yml.",
    )
    return parser.parse_args()


def run_reality(args: argparse.Namespace) -> RealitySnapshot:
    root = _ensure_lake_root()
    catalog = load_catalog()
    selected_ids = _select_dataset_ids(args, catalog, root)
    selected_specs = [catalog[dataset_id] for dataset_id in selected_ids]
    generated_at = datetime.now(tz=UTC)
    snapshot_date = _snapshot_date(args.date, generated_at)
    output_dir = args.output_dir or (meta_path() / "reality")
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_date, previous = _load_baseline(output_dir, snapshot_date, args.baseline)
    previous_by_id = {dataset.dataset_id: dataset for dataset in previous}
    thresholds = load_thresholds(args.thresholds)
    datasets: list[DatasetHealth] = []
    findings: list[Finding] = []

    if selected_specs:
        logger.info("Probing %d dataset(s): %s", len(selected_specs), ", ".join(selected_ids))
        datasets = compute_all_health(selected_specs, with_live=args.with_live, now=generated_at)
        for dataset in datasets:
            findings.extend(
                diff_health(
                    previous_by_id.get(dataset.dataset_id),
                    dataset,
                    thresholds_for(thresholds, dataset.dataset_id),
                )
            )
    else:
        logger.info("No datasets selected for lake reality probe.")

    snapshot = RealitySnapshot(
        generated_at=generated_at,
        snapshot_date=snapshot_date,
        baseline_date=baseline_date,
        datasets=datasets,
        findings=findings,
    )
    _write_snapshot(output_dir, snapshot)
    return snapshot


def _ensure_lake_root() -> Path:
    root = lake_root()
    if root.exists():
        return root
    cwd_lake = Path.cwd() / "lake"
    if cwd_lake.exists() and cwd_lake.is_dir():
        os.environ["COACC_LAKE_ROOT"] = str(cwd_lake.resolve())
        return cwd_lake
    msg = (
        "No lake found. Set COACC_LAKE_ROOT or run from a directory containing ./lake. "
        f"Checked: {root}, {cwd_lake}"
    )
    raise RealityCliError(msg)


def _select_dataset_ids(
    args: argparse.Namespace,
    catalog: dict[str, DatasetSpec],
    root: Path,
) -> list[str]:
    requested = set(args.dataset)
    if args.datasets:
        requested.update(item.strip() for item in args.datasets.split(",") if item.strip())
    explicit_requested = bool(requested)
    if args.changed_yamls_only:
        requested.update(_changed_catalog_dataset_ids(catalog))

    if requested:
        unknown = sorted(dataset_id for dataset_id in requested if dataset_id not in catalog)
        if unknown:
            raise RealityCliError(f"Unknown dataset id(s): {', '.join(unknown)}")
        return sorted(requested)

    if args.changed_yamls_only and not explicit_requested:
        return []

    if args.all or not requested:
        present = _local_catalog_dataset_ids(root, catalog)
        if not present:
            logger.info("No catalog-backed sources found under %s/raw", root)
        return present
    return []


def _local_catalog_dataset_ids(root: Path, catalog: dict[str, DatasetSpec]) -> list[str]:
    raw_root = root / "raw"
    if not raw_root.exists():
        return []
    ids: list[str] = []
    for path in sorted(raw_root.glob("source=*")):
        if path.is_dir():
            dataset_id = path.name.split("=", 1)[1]
            if dataset_id in catalog:
                ids.append(dataset_id)
            else:
                logger.warning("Skipping source=%s because it is not in the catalog", dataset_id)
    return ids


def _changed_catalog_dataset_ids(catalog: dict[str, DatasetSpec]) -> list[str]:
    paths = set(_git_names(["diff", "--name-only", "--cached"]))
    if not paths:
        paths.update(_git_names(["diff", "--name-only", "HEAD~1..HEAD"]))
    dataset_ids: list[str] = []
    for path in paths:
        p = Path(path)
        if (
            len(p.parts) == 3
            and p.parts[0] == "etl"
            and p.parts[1] == "datasets"
            and p.suffix == ".yml"
            and p.stem in catalog
        ):
            dataset_ids.append(p.stem)
    return sorted(set(dataset_ids))


def _git_names(args: list[str]) -> list[str]:
    try:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            encoding="utf-8",
        )
    except OSError:
        return []
    if completed.returncode != 0:
        return []
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _snapshot_date(value: str | None, generated_at: datetime) -> str:
    if value is None:
        return generated_at.date().isoformat()
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise RealityCliError("--date must be YYYY-MM-DD") from exc
    return value


def _load_baseline(
    output_dir: Path,
    snapshot_date: str,
    requested: str | None,
) -> tuple[str | None, list[DatasetHealth]]:
    path: Path | None
    if requested:
        requested_path = Path(requested)
        path = requested_path if requested_path.exists() else output_dir / f"{requested}.json"
        if not path.exists():
            raise RealityCliError(f"Baseline snapshot not found: {path}")
    else:
        older = [
            candidate
            for candidate in output_dir.glob("*.json")
            if candidate.stem < snapshot_date and _looks_like_date(candidate.stem)
        ]
        path = max(older, default=None)

    if path is None:
        return None, []

    raw = json.loads(path.read_text(encoding="utf-8"))
    snapshot = RealitySnapshot.model_validate(raw)
    return snapshot.snapshot_date, snapshot.datasets


def _looks_like_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _write_snapshot(output_dir: Path, snapshot: RealitySnapshot) -> None:
    json_path = output_dir / f"{snapshot.snapshot_date}.json"
    diff_path = output_dir / f"{snapshot.snapshot_date}.diff.md"
    json_path.write_text(
        json.dumps(snapshot.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    diff_path.write_text(
        render_diff_markdown(
            snapshot_date=snapshot.snapshot_date,
            generated_at=snapshot.generated_at,
            baseline_date=snapshot.baseline_date,
            datasets=snapshot.datasets,
            findings=snapshot.findings,
        ),
        encoding="utf-8",
    )
    logger.info("Wrote %s", json_path)
    logger.info("Wrote %s", diff_path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    try:
        snapshot = run_reality(args)
    except RealityCliError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
    except RealityError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)

    failure_count = sum(1 for finding in snapshot.findings if finding.severity == "fail")
    warning_count = sum(1 for finding in snapshot.findings if finding.severity == "warn")
    print(
        f"Lake reality {snapshot.snapshot_date}: "
        f"{len(snapshot.datasets)} dataset(s), "
        f"{failure_count} failure(s), {warning_count} warning(s)"
    )
    for finding in snapshot.findings:
        if finding.severity in {"fail", "warn"}:
            print(
                f"{finding.severity.upper()} {finding.dataset_id} "
                f"{finding.metric}: {finding.message}"
            )

    sys.exit(1 if findings_have_failures(snapshot.findings) else 0)


if __name__ == "__main__":
    main()
