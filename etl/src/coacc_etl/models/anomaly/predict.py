from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import duckdb
import joblib  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from coacc_etl.models.anomaly.common import (
    FEATURE_NAMES,
    LABEL_COLUMN,
    AnomalyModelError,
    clean_run_id,
    curated_partition,
    current_manifest_path,
    model_run_dir,
    new_run_id,
    now_iso,
    read_json,
    replace_dir,
    sql_string,
)

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class AnomalyPredictionResult:
    run_id: str
    model_run_id: str
    feature_run_id: str
    score_path: str
    rows: int
    scored_at: str


def _feature_path(feature_run_id: str) -> str:
    feature_dir = curated_partition("anomaly_features", feature_run_id)
    if not feature_dir.exists() or not any(feature_dir.glob("*.parquet")):
        raise AnomalyModelError(f"missing anomaly feature parquet for run: {feature_run_id}")
    return str(feature_dir / "*.parquet")


def _model_metadata(model_run_id: str | None) -> tuple[str, dict[str, Any]]:
    if model_run_id is None:
        manifest = current_manifest_path()
        if not manifest.exists():
            raise AnomalyModelError("missing anomaly model current manifest")
        payload = read_json(manifest)
        return str(payload["run_id"]), payload
    run_id = clean_run_id(model_run_id)
    metadata_path = model_run_dir(run_id) / "metrics.json"
    if not metadata_path.exists():
        raise AnomalyModelError(f"missing anomaly model metadata: {metadata_path}")
    return run_id, read_json(metadata_path)


def _normalize_scores(decisions: Any, minimum: float, maximum: float) -> list[float]:
    width = maximum - minimum
    if width <= 0:
        return [0.0 for _ in decisions]
    return [max(0.0, min(1.0, (maximum - float(value)) / width)) for value in decisions]


def _top_features(
    rows: list[list[float]],
    centers: dict[str, float],
    spreads: dict[str, float],
) -> list[list[str]]:
    output: list[list[str]] = []
    for values in rows:
        ranked: list[tuple[float, str]] = []
        for name, value in zip(FEATURE_NAMES, values, strict=True):
            spread = max(float(spreads.get(name, 1.0)), 1e-9)
            distance = abs((float(value) - float(centers.get(name, 0.0))) / spread)
            ranked.append((distance, name))
        output.append([name for _, name in sorted(ranked, reverse=True)[:3]])
    return output


def _write_batch(
    out_dir: Path,
    part_index: int,
    *,
    run_id: str,
    model_run_id: str,
    feature_run_id: str,
    scored_at: str,
    batch: Any,
    scores: list[float],
    top_features: list[list[str]],
) -> None:
    table = pa.Table.from_pydict({
        "run_id": [run_id] * len(scores),
        "model_run_id": [model_run_id] * len(scores),
        "feature_run_id": [feature_run_id] * len(scores),
        "scored_at": [scored_at] * len(scores),
        "contract_id": [str(value) for value in batch["contract_id"].tolist()],
        "entity_uid": [str(value) for value in batch["entity_uid"].tolist()],
        "score": scores,
        "iforest_score": scores,
        "score_confidence": [str(value) for value in batch["score_confidence"].tolist()],
        "top_features": top_features,
        LABEL_COLUMN: [bool(value) for value in batch[LABEL_COLUMN].tolist()],
        "process_url": [
            str(value) if value is not None else None for value in batch["process_url"].tolist()
        ],
    })
    pq.write_table(table, out_dir / f"part-{part_index:05d}.parquet", compression="zstd")


def predict_anomaly_scores(
    *,
    model_run_id: str | None = None,
    feature_run_id: str | None = None,
    run_id: str | None = None,
    batch_size: int = 100_000,
) -> AnomalyPredictionResult:
    if batch_size <= 0:
        raise AnomalyModelError("batch_size must be positive")
    resolved_model_run_id, metadata = _model_metadata(model_run_id)
    resolved_feature_run_id = clean_run_id(
        feature_run_id or str(metadata.get("feature_run_id") or resolved_model_run_id)
    )
    safe_run_id = clean_run_id(run_id or resolved_model_run_id or new_run_id())
    feature_path = _feature_path(resolved_feature_run_id)
    model_path = model_run_dir(resolved_model_run_id) / "iforest.joblib"
    if not model_path.exists():
        raise AnomalyModelError(f"missing isolation forest model: {model_path}")
    model = joblib.load(model_path)
    centers = cast("dict[str, float]", metadata.get("feature_centers") or {})
    spreads = cast("dict[str, float]", metadata.get("feature_spreads") or {})
    minimum = float(metadata.get("iforest_decision_min", -1.0))
    maximum = float(metadata.get("iforest_decision_max", 1.0))
    scored_at = now_iso()

    out = curated_partition("anomaly_scores", safe_run_id)
    tmp = out.parent / f".inflight-anomaly_scores-{safe_run_id}"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=False)
    con = duckdb.connect(database=":memory:")
    rows_written = 0
    part_index = 0
    try:
        total_row = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_string(feature_path)})"
        ).fetchone()
        total = int(total_row[0]) if total_row else 0
        for offset in range(0, total, batch_size):
            batch = con.execute(
                f"""
                SELECT
                    contract_id,
                    entity_uid,
                    score_confidence,
                    {LABEL_COLUMN},
                    process_url,
                    {", ".join(FEATURE_NAMES)}
                FROM read_parquet({sql_string(feature_path)})
                ORDER BY contract_id
                LIMIT {int(batch_size)}
                OFFSET {int(offset)}
                """
            ).fetchdf()
            if batch.empty:
                continue
            feature_frame = batch[list(FEATURE_NAMES)]
            decisions = model.decision_function(feature_frame)
            scores = _normalize_scores(decisions, minimum, maximum)
            feature_rows = [
                [float(value) for value in row]
                for row in feature_frame.itertuples(index=False, name=None)
            ]
            _write_batch(
                tmp,
                part_index,
                run_id=safe_run_id,
                model_run_id=resolved_model_run_id,
                feature_run_id=resolved_feature_run_id,
                scored_at=scored_at,
                batch=batch,
                scores=scores,
                top_features=_top_features(feature_rows, centers, spreads),
            )
            rows_written += len(scores)
            part_index += 1
        if rows_written == 0:
            raise AnomalyModelError("anomaly prediction produced zero rows")
        replace_dir(tmp, out)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    finally:
        con.close()

    return AnomalyPredictionResult(
        run_id=safe_run_id,
        model_run_id=resolved_model_run_id,
        feature_run_id=resolved_feature_run_id,
        score_path=str(out),
        rows=rows_written,
        scored_at=scored_at,
    )
