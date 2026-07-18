from __future__ import annotations

import base64
import binascii
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from coacc.models.anomaly import CaseAnomalyScore
from coacc.services import lakehouse_query

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

ANOMALY_CASE_PREFIX = "anomaly:"


@dataclass(frozen=True)
class LakeAnomalyRun:
    score_run_id: str
    model_run_id: str | None
    feature_run_id: str | None
    score_path: str
    manifest_path: str | None
    promoted_at: str | None


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _score_partition(run_id: str) -> Path:
    return lakehouse_query.lake_root() / "curated" / "anomaly_scores" / f"run_id={run_id}"


def _score_glob(run_id: str) -> str:
    return str(_score_partition(run_id) / "*.parquet")


def _feature_glob(run: LakeAnomalyRun) -> str | None:
    if not run.feature_run_id:
        return None
    partition = (
        lakehouse_query.lake_root()
        / "curated"
        / "anomaly_features"
        / f"run_id={run.feature_run_id}"
    )
    if not partition.exists() or not any(partition.glob("*.parquet")):
        return None
    return str(partition / "*.parquet")


def anomaly_case_id(contract_id: str) -> str:
    token = base64.urlsafe_b64encode(contract_id.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{ANOMALY_CASE_PREFIX}{token}"


def contract_id_from_anomaly_case_id(case_id: str) -> str | None:
    if not case_id.startswith(ANOMALY_CASE_PREFIX):
        return None
    token = case_id.removeprefix(ANOMALY_CASE_PREFIX).strip()
    if not token:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None


def _latest_score_partition() -> Path | None:
    root = lakehouse_query.lake_root() / "curated" / "anomaly_scores"
    if not root.exists():
        return None
    partitions = [
        path
        for path in root.glob("run_id=*")
        if path.is_dir() and any(path.glob("*.parquet"))
    ]
    if not partitions:
        return None
    return max(partitions, key=lambda path: path.stat().st_mtime)


def current_anomaly_run() -> LakeAnomalyRun | None:
    manifest_path = lakehouse_query.lake_root() / "models" / "anomaly" / "current.json"
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("Skipping unreadable anomaly current manifest: %s", manifest_path)
        else:
            score_run_id = str(payload.get("score_run_id") or payload.get("run_id") or "").strip()
            if score_run_id:
                partition = _score_partition(score_run_id)
                if partition.exists() and any(partition.glob("*.parquet")):
                    promoted_at = payload.get("trained_at") or payload.get("promoted_at")
                    return LakeAnomalyRun(
                        score_run_id=score_run_id,
                        model_run_id=str(payload["run_id"]) if payload.get("run_id") else None,
                        feature_run_id=(
                            str(payload["feature_run_id"])
                            if payload.get("feature_run_id")
                            else None
                        ),
                        score_path=_score_glob(score_run_id),
                        manifest_path=str(manifest_path),
                        promoted_at=str(promoted_at) if promoted_at is not None else None,
                    )

    latest_partition = _latest_score_partition()
    if latest_partition is None:
        return None
    score_run_id = latest_partition.name.removeprefix("run_id=")
    promoted_at = datetime.fromtimestamp(latest_partition.stat().st_mtime, UTC).isoformat()
    return LakeAnomalyRun(
        score_run_id=score_run_id,
        model_run_id=None,
        feature_run_id=None,
        score_path=_score_glob(score_run_id),
        manifest_path=None,
        promoted_at=promoted_at,
    )


def _deduped_scores_sql(run: LakeAnomalyRun) -> str:
    scores_sql = f"""
        SELECT *
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        scored_at DESC NULLS LAST,
                        score DESC NULLS LAST,
                        contract_id
                ) AS __score_rank
            FROM read_parquet({_sql_string(run.score_path)})
            WHERE contract_id IS NOT NULL
        )
        WHERE __score_rank = 1
    """
    feature_glob = _feature_glob(run)
    if feature_glob is None:
        return scores_sql
    return f"""
        WITH scores AS ({scores_sql}),
        features AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY contract_id
                        ORDER BY built_at DESC NULLS LAST, contract_id
                    ) AS __feature_rank
                FROM read_parquet({_sql_string(feature_glob)})
                WHERE contract_id IS NOT NULL
            )
            WHERE __feature_rank = 1
        )
        SELECT
            scores.*,
            features.contract_reference,
            features.supplier_name,
            features.supplier_nit_base AS supplier_document_id,
            features.buyer_name,
            features.buyer_document_id,
            features.contract_value,
            features.signing_date,
            features.source_id,
            features.process_url AS feature_process_url
        FROM scores
        LEFT JOIN features USING (contract_id)
    """


def _normalize_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    if hasattr(value, "tolist"):
        raw = value.tolist()
        if isinstance(raw, list):
            return [str(item) for item in raw if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _supplier_document_id(row: dict[str, Any]) -> str | None:
    explicit = _optional_str(row.get("supplier_document_id"))
    if explicit:
        return explicit
    entity_uid = _optional_str(row.get("entity_uid"))
    if entity_uid and ":" in entity_uid:
        candidate = entity_uid.split(":", 1)[1].strip()
        if candidate.isdigit():
            return candidate
    return None


def _score_from_row(row: dict[str, Any], run: LakeAnomalyRun) -> CaseAnomalyScore:
    return CaseAnomalyScore(
        contract_id=str(row["contract_id"]),
        contract_reference=_optional_str(row.get("contract_reference")),
        entity_uid=str(row.get("entity_uid") or ""),
        supplier_name=_optional_str(row.get("supplier_name")),
        supplier_document_id=_supplier_document_id(row),
        buyer_name=_optional_str(row.get("buyer_name")),
        buyer_document_id=_optional_str(row.get("buyer_document_id")),
        contract_value=(
            float(row["contract_value"])
            if row.get("contract_value") is not None
            else None
        ),
        signing_date=_optional_str(row.get("signing_date")),
        source_id=_optional_str(row.get("source_id")),
        score=float(row.get("score") or 0.0),
        score_confidence=str(row.get("score_confidence") or "unknown"),
        top_features=_normalize_list(row.get("top_features")),
        prior_sanction_supplier=bool(row.get("prior_sanction_supplier", False)),
        process_url=(
            _optional_str(row.get("process_url"))
            or _optional_str(row.get("feature_process_url"))
        ),
        score_run_id=str(row.get("run_id") or run.score_run_id),
        model_run_id=_optional_str(row.get("model_run_id")) or run.model_run_id,
        feature_run_id=_optional_str(row.get("feature_run_id")) or run.feature_run_id,
        scored_at=_optional_str(row.get("scored_at")) or run.promoted_at,
    )


def top_anomaly_scores(
    *,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[CaseAnomalyScore], int]:
    run = current_anomaly_run()
    if run is None:
        return [], 0
    con = lakehouse_query.connect(read_only=True)
    try:
        total_row = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT count(*)
            FROM scores
            """
        ).fetchone()
        cursor = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT *
            FROM scores
            ORDER BY score DESC NULLS LAST, contract_id
            LIMIT {int(limit)}
            OFFSET {int(offset)}
            """
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()
    return [_score_from_row(row, run) for row in rows], int(total_row[0]) if total_row else 0


def anomaly_score_for_contract(contract_id: str) -> CaseAnomalyScore | None:
    run = current_anomaly_run()
    if run is None:
        return None
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT *
            FROM scores
            WHERE contract_id = ?
            ORDER BY score DESC NULLS LAST
            LIMIT 1
            """,
            [contract_id],
        )
        columns = [item[0] for item in cursor.description or []]
        row = cursor.fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return _score_from_row(dict(zip(columns, row, strict=False)), run)


def anomaly_scores_for_contracts(contract_ids: list[str]) -> dict[str, CaseAnomalyScore]:
    run = current_anomaly_run()
    unique_contract_ids = list(
        dict.fromkeys(contract_id for contract_id in contract_ids if contract_id)
    )
    if run is None or not unique_contract_ids:
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT *
            FROM scores
            WHERE contract_id IN (SELECT unnest(?))
            ORDER BY score DESC NULLS LAST, contract_id
            """,
            [unique_contract_ids],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()
    return {
        score.contract_id: score
        for score in (_score_from_row(row, run) for row in rows)
    }


def _entity_uid_candidates(entity_id: str) -> list[str]:
    clean = "".join(ch for ch in entity_id if ch.isdigit())
    numeric_candidates = [candidate for candidate in (clean, clean[:9]) if candidate]
    return list(dict.fromkeys([
        entity_id,
        *numeric_candidates,
        *(
            f"{prefix}:{candidate}"
            for candidate in numeric_candidates
            for prefix in ("company", "doc", "supplier")
        ),
    ]))


def entity_anomaly_scores(
    entity_id: str,
    *,
    limit: int = 100,
) -> tuple[list[CaseAnomalyScore], int]:
    run = current_anomaly_run()
    if run is None:
        return [], 0
    candidates = _entity_uid_candidates(entity_id)
    con = lakehouse_query.connect(read_only=True)
    try:
        total_row = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT count(*)
            FROM scores
            WHERE entity_uid IN (SELECT unnest(?))
            """,
            [candidates],
        ).fetchone()
        cursor = con.execute(
            f"""
            WITH scores AS ({_deduped_scores_sql(run)})
            SELECT *
            FROM scores
            WHERE entity_uid IN (SELECT unnest(?))
            ORDER BY score DESC NULLS LAST, contract_id
            LIMIT {int(limit)}
            """,
            [candidates],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()
    return [_score_from_row(row, run) for row in rows], int(total_row[0]) if total_row else 0
