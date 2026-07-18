from __future__ import annotations

from dataclasses import dataclass

import duckdb

from coacc_etl.models.anomaly.common import (
    LABEL_COLUMN,
    SUPPLIER_HOLDOUT_PREFIXES,
    AnomalyModelError,
    curated_partition,
    sql_string,
)


@dataclass(frozen=True)
class AnomalyEvaluationResult:
    score_run_id: str
    scored_rows: int
    positive_labels: int
    precision_at_10: float | None
    precision_at_100: float | None
    precision_at_1000: float | None
    base_rate: float | None
    average_precision: float | None
    roc_auc: float | None
    random_precision_at_100: float | None
    random_precision_at_1000: float | None
    holdout_scored_rows: int
    holdout_positive_labels: int
    holdout_precision_at_10: float | None
    holdout_precision_at_100: float | None
    holdout_precision_at_1000: float | None
    holdout_base_rate: float | None
    holdout_average_precision: float | None
    holdout_roc_auc: float | None


def _holdout_where_sql() -> str:
    prefixes = ", ".join(sql_string(prefix) for prefix in SUPPLIER_HOLDOUT_PREFIXES)
    return (
        "substr(sha256(coalesce(nullif(entity_uid, ''), contract_id)), 1, 1) "
        f"IN ({prefixes})"
    )


def _ranking_metrics(
    con: duckdb.DuckDBPyConnection,
    score_path: str,
    *,
    where_sql: str = "true",
) -> tuple[float | None, float | None, float | None]:
    row = con.execute(
        f"""
        WITH filtered AS (
            SELECT
                contract_id,
                score,
                {LABEL_COLUMN} AS label
            FROM read_parquet({sql_string(score_path)})
            WHERE {where_sql}
        ),
        descending AS (
            SELECT
                label,
                row_number() OVER (ORDER BY score DESC, contract_id) AS row_num,
                sum(CASE WHEN label THEN 1 ELSE 0 END) OVER (
                    ORDER BY score DESC, contract_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS positives_seen
            FROM filtered
        ),
        ascending AS (
            SELECT
                label,
                rank() OVER (ORDER BY score ASC) AS first_rank,
                count(*) OVER (PARTITION BY score) AS tie_count
            FROM filtered
        ),
        totals AS (
            SELECT
                count(*) AS rows,
                sum(CASE WHEN label THEN 1 ELSE 0 END) AS positives
            FROM filtered
        )
        SELECT
            positives::DOUBLE / nullif(rows, 0) AS base_rate,
            (
                SELECT sum(
                    CASE WHEN label THEN positives_seen::DOUBLE / row_num ELSE 0 END
                ) / nullif((SELECT positives FROM totals), 0)
                FROM descending
            ) AS average_precision,
            (
                SELECT (
                    sum(CASE WHEN label THEN first_rank + (tie_count - 1) / 2.0 ELSE 0 END)
                    - positives * (positives + 1) / 2.0
                ) / nullif(positives * (rows - positives), 0)
                FROM ascending, totals
                GROUP BY positives, rows
            ) AS roc_auc
        FROM totals
        """
    ).fetchone()
    if row is None:
        return None, None, None
    return (
        float(row[0]) if row[0] is not None else None,
        float(row[1]) if row[1] is not None else None,
        float(row[2]) if row[2] is not None else None,
    )


def _precision_at(
    con: duckdb.DuckDBPyConnection,
    score_path: str,
    k: int,
    *,
    where_sql: str = "true",
) -> float | None:
    row = con.execute(
        f"""
        SELECT
            count(*) AS rows_considered,
            sum(CASE WHEN {LABEL_COLUMN} THEN 1 ELSE 0 END) AS positives
        FROM (
            SELECT {LABEL_COLUMN}
            FROM read_parquet({sql_string(score_path)})
            WHERE {where_sql}
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
                sum(CASE WHEN {LABEL_COLUMN} THEN 1 ELSE 0 END) AS positives,
                sum(CASE WHEN {_holdout_where_sql()} THEN 1 ELSE 0 END)
                    AS holdout_scored_rows,
                sum(
                    CASE
                        WHEN {_holdout_where_sql()} AND {LABEL_COLUMN} THEN 1
                        ELSE 0
                    END
                ) AS holdout_positive_labels
            FROM read_parquet({sql_string(score_path)})
            """
        ).fetchone()
        scored_rows = int(row[0] if row else 0)
        positive_labels = int(row[1] if row and row[1] is not None else 0)
        holdout_scored_rows = int(row[2] if row and row[2] is not None else 0)
        holdout_positive_labels = int(row[3] if row and row[3] is not None else 0)
        holdout_where = _holdout_where_sql()
        base_rate, average_precision, roc_auc = _ranking_metrics(con, score_path)
        holdout_base_rate, holdout_average_precision, holdout_roc_auc = _ranking_metrics(
            con,
            score_path,
            where_sql=holdout_where,
        )
        return AnomalyEvaluationResult(
            score_run_id=score_run_id,
            scored_rows=scored_rows,
            positive_labels=positive_labels,
            precision_at_10=_precision_at(con, score_path, 10),
            precision_at_100=_precision_at(con, score_path, 100),
            precision_at_1000=_precision_at(con, score_path, 1000),
            base_rate=base_rate,
            average_precision=average_precision,
            roc_auc=roc_auc,
            random_precision_at_100=base_rate,
            random_precision_at_1000=base_rate,
            holdout_scored_rows=holdout_scored_rows,
            holdout_positive_labels=holdout_positive_labels,
            holdout_precision_at_10=_precision_at(
                con,
                score_path,
                10,
                where_sql=holdout_where,
            ),
            holdout_precision_at_100=_precision_at(
                con,
                score_path,
                100,
                where_sql=holdout_where,
            ),
            holdout_precision_at_1000=_precision_at(
                con,
                score_path,
                1000,
                where_sql=holdout_where,
            ),
            holdout_base_rate=holdout_base_rate,
            holdout_average_precision=holdout_average_precision,
            holdout_roc_auc=holdout_roc_auc,
        )
    finally:
        con.close()
