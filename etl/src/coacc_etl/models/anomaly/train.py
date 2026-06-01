from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import duckdb
import joblib  # type: ignore[import-untyped]
from sklearn.ensemble import IsolationForest  # type: ignore[import-untyped]

from coacc_etl.models.anomaly.common import (
    FEATURE_NAMES,
    AnomalyModelError,
    clean_run_id,
    current_manifest_path,
    feature_schema_hash,
    model_run_dir,
    new_run_id,
    now_iso,
    sql_string,
    write_json,
)
from coacc_etl.models.anomaly.evaluate import evaluate_scores
from coacc_etl.models.anomaly.features import build_anomaly_features
from coacc_etl.models.anomaly.predict import predict_anomaly_scores


@dataclass(frozen=True)
class AnomalyTrainingResult:
    run_id: str
    feature_run_id: str
    score_run_id: str
    model_dir: str
    feature_path: str
    score_path: str
    metrics_path: str
    current_manifest_path: str
    training_rows: int
    scored_rows: int
    precision_at_100: float | None


def _load_training_sample(feature_path: str, max_training_rows: int) -> Any:
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"""
            SELECT {", ".join(FEATURE_NAMES)}
            FROM read_parquet({sql_string(feature_path)})
            ORDER BY sha256(contract_id)
            LIMIT {int(max_training_rows)}
            """
        ).fetchdf()
    finally:
        con.close()


def _sample_label_counts(feature_path: str, max_training_rows: int) -> tuple[int, int]:
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                count(*) AS rows,
                sum(CASE WHEN prior_sanction_supplier THEN 1 ELSE 0 END) AS positives
            FROM (
                SELECT prior_sanction_supplier
                FROM read_parquet({sql_string(feature_path)})
                ORDER BY sha256(contract_id)
                LIMIT {int(max_training_rows)}
            )
            """
        ).fetchone()
        if row is None:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        con.close()


def _feature_stats(frame: Any) -> tuple[dict[str, float], dict[str, float]]:
    centers: dict[str, float] = {}
    spreads: dict[str, float] = {}
    for name in FEATURE_NAMES:
        series = frame[name].astype(float)
        centers[name] = float(series.median())
        spread = float(series.quantile(0.75) - series.quantile(0.25))
        spreads[name] = spread if spread > 0 else max(float(series.std() or 0.0), 1.0)
    return centers, spreads


def promote_anomaly_model(run_id: str) -> str:
    safe_run_id = clean_run_id(run_id)
    metrics_path = model_run_dir(safe_run_id) / "metrics.json"
    if not metrics_path.exists():
        raise AnomalyModelError(f"missing anomaly model metrics: {metrics_path}")
    import json

    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    write_json(current_manifest_path(), payload)
    return str(current_manifest_path())


def train_anomaly_model(
    *,
    run_id: str | None = None,
    max_training_rows: int = 200_000,
    batch_size: int = 100_000,
    random_state: int = 42,
    promote: bool = True,
) -> AnomalyTrainingResult:
    if max_training_rows <= 0:
        raise AnomalyModelError("max_training_rows must be positive")
    safe_run_id = clean_run_id(run_id or new_run_id())
    feature_result = build_anomaly_features(safe_run_id)
    training_frame = _load_training_sample(feature_result.feature_path, max_training_rows)
    if training_frame.empty:
        raise AnomalyModelError("anomaly training sample is empty")
    model = IsolationForest(
        n_estimators=120,
        max_samples="auto",
        contamination="auto",
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(training_frame)
    decisions = model.decision_function(training_frame)
    decision_min = float(decisions.min())
    decision_max = float(decisions.max())
    centers, spreads = _feature_stats(training_frame)
    training_rows, positive_labels = _sample_label_counts(
        feature_result.feature_path,
        max_training_rows,
    )
    trained_at = now_iso()
    out = model_run_dir(safe_run_id)
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out / "iforest.joblib")
    metrics_path = out / "metrics.json"
    base_metrics: dict[str, Any] = {
        "run_id": safe_run_id,
        "model_kind": "iforest",
        "trained_at": trained_at,
        "feature_run_id": safe_run_id,
        "score_run_id": safe_run_id,
        "feature_schema_hash": feature_schema_hash(),
        "training_rows": training_rows,
        "positive_labels": positive_labels,
        "iforest_decision_min": decision_min,
        "iforest_decision_max": decision_max,
        "feature_centers": centers,
        "feature_spreads": spreads,
        "metrics": {},
        "limitations": [
            "Supervised XGBoost top-up is not promoted in this slice.",
            "single_bidder is populated only when SECOP offers are present in the lake.",
        ],
    }
    write_json(metrics_path, base_metrics)
    prediction = predict_anomaly_scores(
        model_run_id=safe_run_id,
        feature_run_id=safe_run_id,
        run_id=safe_run_id,
        batch_size=batch_size,
    )
    evaluation = evaluate_scores(safe_run_id)
    metrics = {
        **base_metrics,
        "metrics": {
            "precision_at_10": evaluation.precision_at_10,
            "precision_at_100": evaluation.precision_at_100,
            "precision_at_1000": evaluation.precision_at_1000,
            "positive_labels": evaluation.positive_labels,
            "scored_rows": evaluation.scored_rows,
        },
    }
    write_json(metrics_path, metrics)
    current_path = ""
    if promote:
        current_path = promote_anomaly_model(safe_run_id)
    return AnomalyTrainingResult(
        run_id=safe_run_id,
        feature_run_id=safe_run_id,
        score_run_id=safe_run_id,
        model_dir=str(out),
        feature_path=feature_result.feature_path,
        score_path=prediction.score_path,
        metrics_path=str(metrics_path),
        current_manifest_path=current_path,
        training_rows=training_rows,
        scored_rows=prediction.rows,
        precision_at_100=evaluation.precision_at_100,
    )


def training_result_payload(result: AnomalyTrainingResult) -> dict[str, object]:
    return asdict(result)
