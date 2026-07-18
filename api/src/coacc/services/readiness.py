from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from coacc.config import settings
from coacc.services import (
    lake_ops_status,
    lakehouse_anomaly_service,
    lakehouse_query,
    lakehouse_signal_service,
)
from coacc.services.runtime_paths import config_file, dataset_contract_dir, docs_dataset_file
from coacc.services.source_registry import load_source_registry, source_registry_summary

if TYPE_CHECKING:
    from pathlib import Path

_DEV_ENVS = {"dev", "test", "local"}
CheckStatus = Literal["ok", "warn", "fail"]


@dataclass(frozen=True)
class ReadinessCheck:
    name: str
    status: CheckStatus
    detail: str
    path: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
        }


def strict_readiness_enabled() -> bool:
    app_env = settings.app_env.strip().lower()
    return settings.coacc_require_lake_assets or app_env not in _DEV_ENVS


def _status(ok: bool, *, strict: bool) -> CheckStatus:
    if ok:
        return "ok"
    return "fail" if strict else "warn"


def _path_check(name: str, path: Path, *, strict: bool, kind: str = "file") -> ReadinessCheck:
    exists = path.is_dir() if kind == "dir" else path.exists()
    detail = f"{kind} is present" if exists else f"{kind} is missing"
    return ReadinessCheck(
        name=name,
        status=_status(exists, strict=strict),
        detail=detail,
        path=str(path),
    )


def _has_parquet_files(path: Path) -> bool:
    return path.exists() and any(item.is_file() for item in path.glob("*.parquet"))


def _lake_checks(strict: bool) -> list[ReadinessCheck]:
    lake_root = lakehouse_query.lake_root()
    checks = [
        _path_check("lake_root", lake_root, strict=strict, kind="dir"),
        _path_check("lake_raw_dir", lake_root / "raw", strict=strict, kind="dir"),
        _path_check("lake_curated_dir", lake_root / "curated", strict=strict, kind="dir"),
        _path_check("lake_meta_dir", lake_root / "meta", strict=strict, kind="dir"),
    ]

    run = lakehouse_signal_service.latest_signal_run()
    if run is None:
        checks.append(
            ReadinessCheck(
                name="materialized_signal_run",
                status=_status(False, strict=strict),
                detail="no completed signal run manifest found",
                path=str(lake_root / "meta" / "signal_runs"),
            )
        )
        return checks

    hits_dir = lake_root / "curated" / "signal_hits" / f"run_id={run.run_id}"
    evidence_dir = lake_root / "curated" / "evidence_bundles" / f"run_id={run.run_id}"
    hits_ok = _has_parquet_files(hits_dir)
    evidence_ok = _has_parquet_files(evidence_dir)
    checks.extend([
        ReadinessCheck(
            name="materialized_signal_hits",
            status=_status(hits_ok, strict=strict),
            detail=f"latest completed signal run is {run.run_id}",
            path=str(hits_dir),
        ),
        ReadinessCheck(
            name="materialized_evidence_bundles",
            status=_status(evidence_ok, strict=strict),
            detail=f"latest completed signal run is {run.run_id}",
            path=str(evidence_dir),
        ),
    ])
    return checks


def _anomaly_checks(strict: bool) -> list[ReadinessCheck]:
    lake_root = lakehouse_query.lake_root()
    run = lakehouse_anomaly_service.current_anomaly_run()
    if run is None:
        return [
            ReadinessCheck(
                name="anomaly_model_run",
                status=_status(False, strict=strict),
                detail="no readable promoted anomaly score run found",
            )
        ]

    manifest_path = lake_root / "models" / "anomaly" / "current.json"
    model_kind: str | None = None
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        model_kind = str(payload.get("model_kind") or "").strip() or None
    pure_iforest = model_kind == "iforest"
    scores_ok = _has_parquet_files(
        lake_root / "curated" / "anomaly_scores" / f"run_id={run.score_run_id}"
    )
    model_ok = bool(
        run.model_run_id
        and (lake_root / "models" / "anomaly" / run.model_run_id / "iforest.joblib").exists()
    )
    return [
        ReadinessCheck(
            name="anomaly_model_run",
            status=_status(scores_ok and model_ok and pure_iforest, strict=strict),
            detail=(
                f"promoted run is {run.score_run_id}; model_kind={model_kind or 'unknown'}"
            ),
        )
    ]


def _age_hours(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max((datetime.now(tz=UTC) - parsed.astimezone(UTC)).total_seconds(), 0.0) / 3600


def _registry_checks(strict: bool) -> list[ReadinessCheck]:
    checks = [
        _path_check("signal_registry", config_file("signal_registry.yml"), strict=strict),
        _path_check("signal_source_deps", config_file("signal_source_deps.yml"), strict=strict),
        _path_check("catalog_signed", docs_dataset_file("catalog.signed.csv"), strict=strict),
        _path_check("catalog_proven", docs_dataset_file("catalog.proven.csv"), strict=strict),
        _path_check("dataset_contract_dir", dataset_contract_dir(), strict=strict, kind="dir"),
    ]

    contract_dir = dataset_contract_dir()
    contract_count = len(list(contract_dir.glob("*.yml"))) if contract_dir.is_dir() else 0
    checks.append(
        ReadinessCheck(
            name="dataset_contracts",
            status=_status(contract_count > 0, strict=strict),
            detail=f"{contract_count} dataset contract YAML files found",
            path=str(contract_dir),
        )
    )

    try:
        source_summary = source_registry_summary(load_source_registry())
    except Exception as exc:  # pragma: no cover - defensive readiness guard
        checks.append(
            ReadinessCheck(
                name="source_registry",
                status=_status(False, strict=strict),
                detail=f"failed to load source registry: {exc}",
            )
        )
        return checks

    source_count = source_summary["universe_v1_sources"]
    checks.append(
        ReadinessCheck(
            name="source_registry",
            status=_status(source_count > 0, strict=strict),
            detail=f"{source_count} universe v1 sources registered",
        )
    )

    min_loaded = max(settings.coacc_ready_min_loaded_sources, 0)
    if min_loaded > 0:
        loaded = source_summary["loaded_sources"]
        checks.append(
            ReadinessCheck(
                name="loaded_sources",
                status=_status(loaded >= min_loaded, strict=strict),
                detail=f"{loaded} loaded sources; minimum required is {min_loaded}",
            )
        )
    return checks


def _operations_checks(strict: bool) -> list[ReadinessCheck]:
    max_age_hours = max(settings.coacc_ready_max_lake_ops_age_hours, 0.0)
    if max_age_hours <= 0:
        return []

    report = lake_ops_status.latest_lake_ops_report(max_age_hours=max_age_hours)
    healthy = bool(report.get("healthy"))
    status = _status(healthy, strict=strict)
    age_seconds = report.get("age_seconds")
    age_detail = (
        f"; age={age_seconds / 3600:.2f}h"
        if isinstance(age_seconds, int | float)
        else ""
    )
    return [
        ReadinessCheck(
            name="lake_ops",
            status=status,
            detail=f"{report.get('detail')}{age_detail}; max_age={max_age_hours:.2f}h",
            path=str(report.get("path") or lake_ops_status.operations_status_path()),
        )
    ]


def build_readiness_report(*, neo4j_connected: bool) -> dict[str, Any]:
    strict = strict_readiness_enabled()
    checks = [
        *_registry_checks(strict),
        *_lake_checks(strict),
        *_anomaly_checks(strict),
        *_operations_checks(strict),
    ]

    if settings.neo4j_required:
        checks.append(
            ReadinessCheck(
                name="neo4j",
                status="ok" if neo4j_connected else "fail",
                detail="connected" if neo4j_connected else "required but unavailable",
            )
        )
    else:
        checks.append(
            ReadinessCheck(
                name="neo4j",
                status="ok" if neo4j_connected else "warn",
                detail="connected" if neo4j_connected else "optional and unavailable",
            )
        )

    failed = [check for check in checks if check.status == "fail"]
    signal_run = lakehouse_signal_service.latest_signal_run()
    anomaly_run = lakehouse_anomaly_service.current_anomaly_run()
    operation = lake_ops_status.latest_lake_ops_report()
    materialized_counts = lakehouse_signal_service.materialized_signal_counts()
    evidence_available = bool(
        signal_run
        and _has_parquet_files(
            lakehouse_query.lake_root()
            / "curated"
            / "evidence_bundles"
            / f"run_id={signal_run.run_id}"
        )
    )
    return {
        "status": "ready" if not failed else "not_ready",
        "app_env": settings.app_env,
        "strict": strict,
        "source_run_id": operation.get("run_id"),
        "signal_run_id": signal_run.run_id if signal_run else None,
        "materialized_signal_count": len(materialized_counts),
        "evidence_bundle_available": evidence_available,
        "anomaly_model_run_id": anomaly_run.model_run_id if anomaly_run else None,
        "anomaly_score_run_id": anomaly_run.score_run_id if anomaly_run else None,
        "artifact_age_hours": {
            "signal": _age_hours(signal_run.finished_at if signal_run else None),
            "anomaly": _age_hours(anomaly_run.promoted_at if anomaly_run else None),
        },
        "checks": [check.to_dict() for check in checks],
    }
