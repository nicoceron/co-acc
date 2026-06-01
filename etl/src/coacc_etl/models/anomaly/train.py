from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

import duckdb
import joblib  # type: ignore[import-untyped]
import pandas as pd
from sklearn.ensemble import (  # type: ignore[import-untyped]
    HistGradientBoostingClassifier,
    IsolationForest,
)

from coacc_etl.models.anomaly.common import (
    FEATURE_NAMES,
    LABEL_COLUMN,
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

if TYPE_CHECKING:
    from pathlib import Path


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
    holdout_precision_at_100: float | None
    supervised_training_rows: int
    supervised_positive_labels: int


SUPERVISED_HOLDOUT_PREFIXES = ("0", "1", "2")
SUPERVISED_SCORE_WEIGHT = 0.85


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


def _count_labels(feature_path: str) -> tuple[int, int]:
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                sum(CASE WHEN {LABEL_COLUMN} THEN 1 ELSE 0 END) AS positives,
                sum(CASE WHEN NOT {LABEL_COLUMN} THEN 1 ELSE 0 END) AS negatives
            FROM read_parquet({sql_string(feature_path)})
            WHERE substr(sha256(contract_id), 1, 1) NOT IN (
                {", ".join(sql_string(prefix) for prefix in SUPERVISED_HOLDOUT_PREFIXES)}
            )
            """
        ).fetchone()
        if row is None:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        con.close()


def _load_supervised_training_sample(feature_path: str, max_training_rows: int) -> Any:
    if max_training_rows < 4:
        return pd.DataFrame()
    positive_count, negative_count = _count_labels(feature_path)
    if positive_count < 2 or negative_count < 2:
        return pd.DataFrame()
    positive_limit = min(positive_count, max(2, max_training_rows // 2))
    negative_limit = min(negative_count, max_training_rows - positive_limit)
    if negative_limit < 2:
        positive_limit = max(2, max_training_rows - 2)
        negative_limit = 2
    selected_columns = ", ".join((*FEATURE_NAMES, LABEL_COLUMN))
    holdout_prefixes = ", ".join(
        sql_string(prefix) for prefix in SUPERVISED_HOLDOUT_PREFIXES
    )
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(
            f"""
            SELECT {selected_columns}
            FROM (
                SELECT contract_id, {selected_columns}
                FROM read_parquet({sql_string(feature_path)})
                WHERE {LABEL_COLUMN}
                    AND substr(sha256(contract_id), 1, 1) NOT IN ({holdout_prefixes})
                ORDER BY sha256(contract_id)
                LIMIT {int(positive_limit)}
            )
            UNION ALL
            SELECT {selected_columns}
            FROM (
                SELECT contract_id, {selected_columns}
                FROM read_parquet({sql_string(feature_path)})
                WHERE NOT {LABEL_COLUMN}
                    AND substr(sha256(contract_id), 1, 1) NOT IN ({holdout_prefixes})
                ORDER BY sha256(contract_id)
                LIMIT {int(negative_limit)}
            )
            """
        ).fetchdf()
    finally:
        con.close()


def _precision_at_k(labels: Any, scores: Any, k: int) -> float | None:
    if len(labels) == 0:
        return None
    frame = pd.DataFrame({"label": labels, "score": scores})
    top = frame.sort_values("score", ascending=False).head(k)
    if top.empty:
        return None
    return float(top["label"].astype(int).sum()) / float(len(top))


def _train_supervised_topup(
    *,
    feature_path: str,
    max_training_rows: int,
    random_state: int,
    out_dir: Path,
) -> dict[str, Any]:
    frame = _load_supervised_training_sample(feature_path, max_training_rows)
    if frame.empty:
        return {
            "enabled": False,
            "training_rows": 0,
            "positive_labels": 0,
            "negative_labels": 0,
            "reason": "insufficient positive and negative labels outside holdout split",
        }
    positives = int(frame[LABEL_COLUMN].astype(bool).sum())
    negatives = int(len(frame) - positives)
    if positives < 2 or negatives < 2:
        return {
            "enabled": False,
            "training_rows": int(len(frame)),
            "positive_labels": positives,
            "negative_labels": negatives,
            "reason": "insufficient class diversity outside holdout split",
        }
    model = HistGradientBoostingClassifier(
        max_iter=120,
        learning_rate=0.08,
        max_leaf_nodes=31,
        l2_regularization=0.05,
        random_state=random_state,
    )
    x_train = frame[list(FEATURE_NAMES)]
    y_train = frame[LABEL_COLUMN].astype(int)
    model.fit(x_train, y_train)
    joblib.dump(model, out_dir / "supervised_hgb.joblib")
    classes = [int(value) for value in model.classes_.tolist()]
    positive_index = classes.index(1)
    training_scores = model.predict_proba(x_train)[:, positive_index]
    return {
        "enabled": True,
        "model_kind": "hist_gradient_boosting_classifier",
        "artifact": "supervised_hgb.joblib",
        "score_weight": SUPERVISED_SCORE_WEIGHT,
        "holdout_prefixes": list(SUPERVISED_HOLDOUT_PREFIXES),
        "training_rows": int(len(frame)),
        "positive_labels": positives,
        "negative_labels": negatives,
        "training_precision_at_100": _precision_at_k(y_train, training_scores, 100),
    }


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
    supervised_metrics = _train_supervised_topup(
        feature_path=feature_result.feature_path,
        max_training_rows=max_training_rows,
        random_state=random_state,
        out_dir=out,
    )
    metrics_path = out / "metrics.json"
    base_metrics: dict[str, Any] = {
        "run_id": safe_run_id,
        "model_kind": "iforest+hgb" if supervised_metrics["enabled"] else "iforest",
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
        "supervised_topup": supervised_metrics,
        "metrics": {},
        "limitations": [
            "Supervised top-up uses PACO-backed sanctioned-supplier overlaps as weak labels.",
            "single_bidder is populated only when SECOP offers are present in the lake.",
            "precision_at_k metrics are prioritization checks, not findings of misconduct.",
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
            "holdout_precision_at_10": evaluation.holdout_precision_at_10,
            "holdout_precision_at_100": evaluation.holdout_precision_at_100,
            "holdout_precision_at_1000": evaluation.holdout_precision_at_1000,
            "holdout_positive_labels": evaluation.holdout_positive_labels,
            "holdout_scored_rows": evaluation.holdout_scored_rows,
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
        holdout_precision_at_100=evaluation.holdout_precision_at_100,
        supervised_training_rows=int(supervised_metrics["training_rows"]),
        supervised_positive_labels=int(supervised_metrics["positive_labels"]),
    )


def training_result_payload(result: AnomalyTrainingResult) -> dict[str, object]:
    return asdict(result)
