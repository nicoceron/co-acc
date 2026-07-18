from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from coacc_etl.lakehouse.paths import lake_root

if TYPE_CHECKING:
    from pathlib import Path

FEATURE_NAMES = (
    "log_value",
    "log_value_z_buyer",
    "log_value_z_modality",
    "buyer_supplier_concentration",
    "n_prior_contracts_12mo_buyer",
    "n_prior_contracts_12mo_supplier",
    "share_of_buyer_total_value_12mo",
    "timing_anomaly_score",
    "modality_value_mismatch",
    "single_bidder",
)
LABEL_COLUMN = "prior_sanction_supplier"
SUPPLIER_HOLDOUT_PREFIXES = ("0", "1", "2")

_RUN_ID_SAFE = re.compile(r"[^A-Za-z0-9_.=-]+")


class AnomalyModelError(RuntimeError):
    """Raised when anomaly feature, model, or score materialization fails."""


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def new_run_id() -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def clean_run_id(run_id: str) -> str:
    cleaned = _RUN_ID_SAFE.sub("_", run_id.strip()).strip("._")
    if not cleaned:
        raise AnomalyModelError("run_id cannot be empty")
    return cleaned


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def curated_partition(name: str, run_id: str) -> Path:
    return lake_root() / "curated" / name / f"run_id={run_id}"


def model_run_dir(run_id: str) -> Path:
    return lake_root() / "models" / "anomaly" / run_id


def current_manifest_path() -> Path:
    return lake_root() / "models" / "anomaly" / "current.json"


def feature_schema_hash() -> str:
    payload = json.dumps(FEATURE_NAMES, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def artifact_tree_hash(path: Path) -> str:
    digest = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file())
    for item in files:
        digest.update(str(item.relative_to(path)).encode("utf-8"))
        with item.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def code_state() -> tuple[str, bool]:
    override = os.environ.get("COACC_CODE_COMMIT", "").strip()
    try:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        commit = override or subprocess.run(
            ["git", "-C", root, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "-C", root, "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return override or "unknown", True
    return commit or "unknown", dirty


def replace_dir(tmp: Path, out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    tmp.rename(out)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
