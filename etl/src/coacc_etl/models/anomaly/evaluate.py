from __future__ import annotations

from dataclasses import dataclass

import duckdb

from coacc_etl.models.anomaly.common import AnomalyModelError, curated_partition, sql_string


@dataclass(frozen=True)
class AnomalyEvaluationResult:
    score_run_id: str
    scored_rows: int
    positive_labels: int
    precision_at_10: float | None
    precision_at_100: float | None
    precision_at_1000: float | None


def _precision_at(con: duckdb.DuckDBPyConnection, score_path: str, k: int) -> float | None:
    row = con.execute(
        f"""
        SELECT
            count(*) AS rows_considered,
            sum(CASE WHEN prior_sanction_supplier THEN 1 ELSE 0 END) AS positives
        FROM (
            SELECT prior_sanction_supplier
            FROM read_parquet({sql_string(score_path)})
            ORDER BY score DESC, contract_id
            LIMIT {int(k)}
        )
        """
    ).fetchone()
    if row is None or int(row[0] or 0) == 0:
        return None
    return float(row[1] or 0) / float(row[0])


def evaluate_scores(score_run_id: str) -> AnomalyEvaluationResult:
    score_dir = curated_partition("anomaly_scores", score_run_id)
    if not score_dir.exists() or not any(score_dir.glob("*.parquet")):
        raise AnomalyModelError(f"missing anomaly score parquet for run: {score_run_id}")
    score_path = str(score_dir / "*.parquet")
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                count(*) AS scored_rows,
                sum(CASE WHEN prior_sanction_supplier THEN 1 ELSE 0 END) AS positives
            FROM read_parquet({sql_string(score_path)})
            """
        ).fetchone()
        scored_rows = int(row[0] if row else 0)
        positive_labels = int(row[1] if row and row[1] is not None else 0)
        return AnomalyEvaluationResult(
            score_run_id=score_run_id,
            scored_rows=scored_rows,
            positive_labels=positive_labels,
            precision_at_10=_precision_at(con, score_path, 10),
            precision_at_100=_precision_at(con, score_path, 100),
            precision_at_1000=_precision_at(con, score_path, 1000),
        )
    finally:
        con.close()
