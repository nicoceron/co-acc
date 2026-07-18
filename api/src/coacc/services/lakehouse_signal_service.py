from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from coacc.models.case import (
    CaseEventResponse,
    CaseEvidenceBundle,
    CaseListResponse,
    CaseResponse,
    CaseSummary,
)
from coacc.models.entity import SourceAttribution
from coacc.models.signal import (
    EntitySignalsResponse,
    EvidenceItemResponse,
    SignalConfidenceComponents,
    SignalHitResponse,
    combine_confidence_components,
    corroboration_confidence_component,
)
from coacc.services import lakehouse_query
from coacc.services.lakehouse_anomaly_service import (
    anomaly_case_id,
    anomaly_score_for_contract,
    contract_id_from_anomaly_case_id,
    top_anomaly_scores,
)
from coacc.services.lakehouse_narrative_service import read_lake_narrative
from coacc.services.signal_registry import get_signal_definition, resolve_signal_id

if TYPE_CHECKING:
    from pathlib import Path

    from coacc.models.anomaly import CaseAnomalyScore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LakeSignalRun:
    run_id: str
    finished_at: str
    signal_hits_path: str
    evidence_bundles_path: str


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _manifest_dir() -> Path:
    return lakehouse_query.lake_root() / "meta" / "signal_runs"


def _run_partition(name: str, run_id: str) -> Path:
    return lakehouse_query.lake_root() / "curated" / name / f"run_id={run_id}"


def _default_run_paths(run_id: str) -> tuple[str, str]:
    return (
        str(_run_partition("signal_hits", run_id) / "*.parquet"),
        str(_run_partition("evidence_bundles", run_id) / "*.parquet"),
    )


def _manifest_finished_at_sort_key(path: Path, value: object) -> datetime:
    if value is not None:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            logger.warning("Signal run manifest has invalid finished_at: %s", path)
    return datetime.fromtimestamp(path.stat().st_mtime, UTC)


def latest_signal_run() -> LakeSignalRun | None:
    manifest_dir = _manifest_dir()
    if not manifest_dir.exists():
        return None
    manifests = sorted(path for path in manifest_dir.glob("*.json") if path.is_file())
    candidates: list[tuple[datetime, str, LakeSignalRun]] = []
    for path in manifests:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("Skipping unreadable signal run manifest: %s", path)
            continue
        if payload.get("status") != "completed":
            continue
        run_id = str(payload.get("run_id") or path.stem)
        finished_at = payload.get("finished_at") or payload.get("generated_at")
        if finished_at is None:
            finished_at = datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()
        output_paths = payload.get("outputs") if isinstance(payload.get("outputs"), dict) else {}
        default_hits, default_evidence = _default_run_paths(run_id)
        signal_hits_path = str(output_paths.get("signal_hits") or default_hits)
        evidence_bundles_path = str(output_paths.get("evidence_bundles") or default_evidence)
        if not signal_hits_path.endswith("*.parquet"):
            signal_hits_path = str(_run_partition("signal_hits", run_id) / "*.parquet")
        if not evidence_bundles_path.endswith("*.parquet"):
            evidence_bundles_path = str(_run_partition("evidence_bundles", run_id) / "*.parquet")
        candidates.append((
            _manifest_finished_at_sort_key(path, finished_at),
            path.name,
            LakeSignalRun(
                run_id=run_id,
                finished_at=str(finished_at),
                signal_hits_path=signal_hits_path,
                evidence_bundles_path=evidence_bundles_path,
            ),
        ))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1]))[2]


def latest_signal_run_tuple() -> tuple[str | None, str | None]:
    run = latest_signal_run()
    if run is None:
        return None, None
    return run.run_id, run.finished_at


def _run_has_parquet(run: LakeSignalRun) -> bool:
    hits_dir = _run_partition("signal_hits", run.run_id)
    evidence_dir = _run_partition("evidence_bundles", run.run_id)
    return (
        hits_dir.exists()
        and evidence_dir.exists()
        and any(hits_dir.glob("*.parquet"))
        and any(evidence_dir.glob("*.parquet"))
    )


def _deduped_hits_sql(run: LakeSignalRun) -> str:
    return f"""
        SELECT *
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY hit_id
                    ORDER BY
                        last_seen_at DESC NULLS LAST,
                        created_at DESC NULLS LAST,
                        signal_id,
                        scope_key
                ) AS __hit_rank
            FROM read_parquet({_sql_string(run.signal_hits_path)})
        )
        WHERE __hit_rank = 1
    """


def materialized_signal_counts() -> dict[str, tuple[int, str | None]]:
    run = latest_signal_run()
    if run is None or not _run_has_parquet(run):
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        rows = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)})
            SELECT signal_id, count(*) AS hit_count, max(last_seen_at) AS last_seen_at
            FROM hits
            GROUP BY signal_id
            """
        ).fetchall()
    finally:
        con.close()
    return {
        resolve_signal_id(str(signal_id)): (int(hit_count), str(last_seen_at or run.finished_at))
        for signal_id, hit_count, last_seen_at in rows
    }


def materialized_signal_severities() -> dict[str, str]:
    run = latest_signal_run()
    if run is None or not _run_has_parquet(run):
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        rows = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)}),
            ranked AS (
                SELECT
                    signal_id,
                    max(CASE severity
                        WHEN 'critical' THEN 4
                        WHEN 'high' THEN 3
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 1
                        ELSE 0
                    END) AS severity_rank
                FROM hits
                GROUP BY signal_id
            )
            SELECT
                signal_id,
                CASE severity_rank
                    WHEN 4 THEN 'critical'
                    WHEN 3 THEN 'high'
                    WHEN 2 THEN 'medium'
                    WHEN 1 THEN 'low'
                    ELSE NULL
                END AS severity
            FROM ranked
            WHERE severity_rank > 0
            """
        ).fetchall()
    finally:
        con.close()
    return {
        resolve_signal_id(str(signal_id)): str(severity)
        for signal_id, severity in rows
        if severity is not None
    }


def materialized_signal_confidence_indices() -> dict[str, float]:
    """Return average evidence-quality confidence for every materialized signal."""
    run = latest_signal_run()
    if run is None or not _run_has_parquet(run):
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        rows = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)}),
            components AS (
                SELECT
                    hits.signal_id,
                    least(1.0, greatest(0.0, coalesce(hits.identity_confidence, 0.5)))
                        AS identity_component,
                    CASE
                        WHEN coalesce(hits.evidence_count, 0) <= 0 THEN 0.0
                        WHEN hits.evidence_count = 1 THEN 0.65
                        WHEN hits.evidence_count = 2 THEN 0.85
                        ELSE 1.0
                    END AS evidence_component
                FROM hits
            )
            SELECT
                signal_id,
                avg(identity_component) AS identity_component,
                avg(evidence_component) AS evidence_component
            FROM components
            GROUP BY signal_id
            """
        ).fetchall()
    finally:
        con.close()
    indices: dict[str, float] = {}
    for signal_id, identity_component, evidence_component in rows:
        canonical_signal_id = resolve_signal_id(str(signal_id))
        definition = get_signal_definition(canonical_signal_id)
        source_count = (
            len(set(definition.sources or definition.sources_required))
            if definition
            else 0
        )
        indices[canonical_signal_id] = combine_confidence_components(
            identity=float(identity_component),
            evidence=float(evidence_component),
            corroboration=corroboration_confidence_component(source_count),
        )
    return indices


def _normalize_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _identity_quality(identity_match_type: str | None) -> str:
    if identity_match_type is None:
        return "unknown"
    if identity_match_type.startswith("EXACT_"):
        return "exact"
    if identity_match_type.startswith("HIGH_"):
        return "high"
    return "probable"


def _identity_confidence(identity_quality: str) -> float:
    if identity_quality == "exact":
        return 1.0
    if identity_quality == "high":
        return 0.9
    if identity_quality == "probable":
        return 0.75
    return 0.5


def _evidence_items_for_hits(
    run: LakeSignalRun,
    hit_ids: list[str],
) -> dict[str, list[EvidenceItemResponse]]:
    if not hit_ids:
        return {}
    unique_hit_ids = list(dict.fromkeys(hit_ids))
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH evidence AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY hit_id, item_index
                            ORDER BY
                                observed_at DESC NULLS LAST,
                                source_id,
                                url,
                                label
                        ) AS __evidence_rank
                    FROM read_parquet({_sql_string(run.evidence_bundles_path)})
                    WHERE hit_id IN (SELECT unnest(?))
                )
                WHERE __evidence_rank = 1
            )
            SELECT *
            FROM evidence
            ORDER BY hit_id, item_index
            """,
            [unique_hit_ids],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()

    items_by_hit: dict[str, list[EvidenceItemResponse]] = {}
    for row in rows:
        signal_id = resolve_signal_id(str(row.get("signal_id") or ""))
        definition = get_signal_definition(signal_id)
        item_type = definition.evidence_mapping.item_type if definition else "reference"
        hit_id = str(row.get("hit_id") or "")
        item = EvidenceItemResponse(
            item_id=f"{hit_id}:{row.get('item_index')}",
            source_id=str(row["source_id"]) if row.get("source_id") is not None else None,
            record_id=str(row["record_id"]) if row.get("record_id") is not None else None,
            url=str(row["url"]) if row.get("url") is not None else None,
            label=str(row["label"]) if row.get("label") is not None else None,
            item_type=item_type,
            node_ref=str(row["row_selector"]) if row.get("row_selector") is not None else None,
            row_selector=(
                str(row["row_selector"]) if row.get("row_selector") is not None else None
            ),
            file_selector=(
                str(row["parquet_path"]) if row.get("parquet_path") is not None else None
            ),
            observed_at=str(row["observed_at"]) if row.get("observed_at") is not None else None,
            public_safe=bool(row.get("public_safe", True)),
            identity_match_type=(
                str(row["identity_match_type"])
                if row.get("identity_match_type") is not None
                else None
            ),
            identity_quality=(
                str(row["identity_quality"]) if row.get("identity_quality") is not None else None
            ),
        )
        items_by_hit.setdefault(hit_id, []).append(item)
    return items_by_hit


def _hit_from_row(
    row: dict[str, Any],
    evidence_items: list[EvidenceItemResponse],
) -> SignalHitResponse:
    signal_id = resolve_signal_id(str(row.get("signal_id") or ""))
    definition = get_signal_definition(signal_id)
    if definition is None:
        raise ValueError(f"Unknown signal in materialized lake run: {signal_id}")
    evidence_refs = _normalize_list(row.get("evidence_refs"))
    identity_match_type = (
        str(row["identity_match_type"]) if row.get("identity_match_type") is not None else None
    )
    identity_quality = (
        str(row["identity_quality"])
        if row.get("identity_quality") is not None
        else _identity_quality(identity_match_type)
    )
    source_ids = list(definition.sources or definition.sources_required)
    for item in evidence_items:
        if item.source_id and item.source_id not in source_ids:
            source_ids.append(item.source_id)
    hit_id = str(row["hit_id"])
    entity_uid = str(row.get("entity_uid") or row.get("entity_id") or row.get("entity_key") or "")
    entity_key = str(row.get("entity_key") or entity_uid)
    display_entity_id = entity_uid.split(":", 1)[1] if ":" in entity_uid else entity_uid
    scope_key = str(row.get("scope_key") or entity_key)
    confidence_components = None
    if all(
        row.get(key) is not None
        for key in (
            "confidence_identity",
            "confidence_evidence_traceability",
            "confidence_source_corroboration",
        )
    ):
        confidence_components = SignalConfidenceComponents(
            identity=float(row["confidence_identity"]),
            evidence_traceability=float(row["confidence_evidence_traceability"]),
            source_corroboration=float(row["confidence_source_corroboration"]),
        )
    return SignalHitResponse(
        hit_id=hit_id,
        run_id=str(row.get("run_id") or ""),
        signal_id=signal_id,
        signal_version=int(row.get("signal_version") or definition.version),
        title=str(row.get("title") or definition.title),
        description=str(row.get("description") or definition.description),
        category=definition.category,
        severity=str(row.get("severity") or definition.severity),
        public_safe=bool(row.get("public_safe", definition.public_safe)),
        entity_id=display_entity_id,
        entity_key=entity_key,
        entity_label=str(row["entity_label"]) if row.get("entity_label") is not None else None,
        scope_key=scope_key,
        scope_type=str(row.get("scope_type") or definition.scope_type),
        dedup_key=f"signal:{signal_id}:entity:{entity_key}:scope:{scope_key}",
        score=float(row.get("score") or 0.0),
        identity_confidence=float(
            row.get("identity_confidence") or _identity_confidence(identity_quality)
        ),
        confidence_index=(
            float(row["confidence_index"])
            if row.get("confidence_index") is not None
            else None
        ),
        confidence_components=confidence_components,
        identity_match_type=identity_match_type,
        identity_quality=identity_quality,
        evidence_count=int(row.get("evidence_count") or len(evidence_refs)),
        evidence_bundle_id=str(row.get("evidence_bundle_id") or f"bundle:{hit_id}"),
        evidence_refs=evidence_refs,
        data={},
        sources=[SourceAttribution(database=source_id) for source_id in source_ids],
        evidence_items=evidence_items,
        created_at=str(row["created_at"]) if row.get("created_at") is not None else None,
        first_seen_at=str(row["first_seen_at"]) if row.get("first_seen_at") is not None else None,
        last_seen_at=str(row["last_seen_at"]) if row.get("last_seen_at") is not None else None,
    )


def _hit_rows(
    run: LakeSignalRun,
    *,
    signal_id: str | None = None,
    hit_id: str | None = None,
    limit: int = 25,
    offset: int = 0,
    public_only: bool = False,
) -> list[dict[str, Any]]:
    predicates: list[str] = []
    params: list[object] = []
    if signal_id is not None:
        predicates.append("signal_id = ?")
        params.append(signal_id)
    if hit_id is not None:
        predicates.append("hit_id = ?")
        params.append(hit_id)
    if public_only:
        predicates.append("public_safe = true")
    where_sql = "WHERE " + " AND ".join(predicates) if predicates else ""
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)})
            SELECT *
            FROM hits
            {where_sql}
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END DESC,
                score DESC NULLS LAST,
                last_seen_at DESC NULLS LAST,
                hit_id
            LIMIT {int(limit)}
            OFFSET {int(offset)}
            """,
            params,
        )
        columns = [item[0] for item in cursor.description or []]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()


def materialized_signal_samples(signal_id: str, limit: int = 10) -> list[SignalHitResponse]:
    run = latest_signal_run()
    canonical_signal_id = resolve_signal_id(signal_id)
    if run is None or not _run_has_parquet(run):
        return []
    rows = _hit_rows(run, signal_id=canonical_signal_id, limit=limit)
    evidence_by_hit = _evidence_items_for_hits(run, [str(row["hit_id"]) for row in rows])
    return [_hit_from_row(row, evidence_by_hit.get(str(row["hit_id"]), [])) for row in rows]


def materialized_hit(hit_id: str, *, public_only: bool = False) -> SignalHitResponse | None:
    run = latest_signal_run()
    if run is None or not _run_has_parquet(run):
        return None
    rows = _hit_rows(run, hit_id=hit_id, limit=1, public_only=public_only)
    if not rows:
        return None
    evidence_by_hit = _evidence_items_for_hits(run, [str(rows[0]["hit_id"])])
    return _hit_from_row(rows[0], evidence_by_hit.get(str(rows[0]["hit_id"]), []))


def materialized_entity_signals(
    entity_id: str,
    *,
    public_only: bool = False,
    limit: int = 100,
) -> EntitySignalsResponse:
    run = latest_signal_run()
    clean = "".join(ch for ch in entity_id if ch.isdigit())
    display_entity_id = entity_id.split(":", 1)[1] if ":" in entity_id else entity_id
    numeric_candidates = [candidate for candidate in [clean, clean[:9]] if candidate]
    key_candidates = list(dict.fromkeys([entity_id, *numeric_candidates]))
    uid_candidates = list(dict.fromkeys([
        entity_id,
        *(
            f"{prefix}:{candidate}"
            for candidate in numeric_candidates
            for prefix in ("doc", "company", "buyer")
        ),
    ]))
    if run is None or not _run_has_parquet(run):
        return EntitySignalsResponse(
            entity_id=display_entity_id,
            entity_key=clean or entity_id,
            total=0,
            last_run_id=None,
            last_refreshed_at=None,
            stale=True,
            signals=[],
        )
    predicates = ["(entity_uid IN (SELECT unnest(?)) OR entity_key IN (SELECT unnest(?)))"]
    params: list[object] = [uid_candidates, key_candidates]
    if public_only:
        predicates.append("public_safe = true")
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)})
            SELECT *
            FROM hits
            WHERE {" AND ".join(predicates)}
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END DESC,
                score DESC NULLS LAST,
                last_seen_at DESC NULLS LAST,
                hit_id
            LIMIT {int(limit)}
            """,
            params,
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()
    evidence_by_hit = _evidence_items_for_hits(run, [str(row["hit_id"]) for row in rows])
    hits = [_hit_from_row(row, evidence_by_hit.get(str(row["hit_id"]), [])) for row in rows]
    return EntitySignalsResponse(
        entity_id=display_entity_id,
        entity_key=clean or entity_id,
        total=len(hits),
        last_run_id=run.run_id,
        last_refreshed_at=run.finished_at,
        stale=False,
        signals=hits,
    )


def list_lake_cases(page: int = 1, size: int = 20) -> CaseListResponse:
    offset = (page - 1) * size
    anomaly_scores, anomaly_total = top_anomaly_scores(limit=size, offset=offset)
    if anomaly_total > 0:
        run = latest_signal_run()
        signal_rows_by_contract: dict[str, dict[str, Any]] = {}
        if run is not None and _run_has_parquet(run):
            signal_rows_by_contract = _signal_rows_for_contracts(
                run,
                [score.contract_id for score in anomaly_scores],
            )
        cases = [
            _case_summary_from_row(
                signal_rows_by_contract[score.contract_id],
                run,
                anomaly_score=score,
            )
            if run is not None and score.contract_id in signal_rows_by_contract
            else _anomaly_case_summary(score)
            for score in anomaly_scores
        ]
        return CaseListResponse(
            cases=cases,
            total=anomaly_total,
        )

    run = latest_signal_run()
    if run is None or not _run_has_parquet(run):
        return CaseListResponse(cases=[], total=0)
    con = lakehouse_query.connect(read_only=True)
    try:
        total_row = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)})
            SELECT count(*)
            FROM hits
            """
        ).fetchone()
    finally:
        con.close()
    rows = _hit_rows(run, limit=size, offset=offset, public_only=False)
    cases = [_case_summary_from_row(row, run) for row in rows]
    return CaseListResponse(cases=cases, total=int(total_row[0]) if total_row else 0)


def _signal_rows_for_contracts(
    run: LakeSignalRun,
    contract_ids: list[str],
) -> dict[str, dict[str, Any]]:
    unique_contract_ids = list(
        dict.fromkeys(contract_id for contract_id in contract_ids if contract_id)
    )
    if not unique_contract_ids:
        return {}
    con = lakehouse_query.connect(read_only=True)
    try:
        cursor = con.execute(
            f"""
            WITH hits AS ({_deduped_hits_sql(run)}),
            matched AS (
                SELECT
                    *,
                    CASE
                        WHEN scope_key IN (SELECT unnest(?)) THEN scope_key
                        ELSE split_part(coalesce(scope_key, ''), ':', 1)
                    END AS __contract_id
                FROM hits
                WHERE (
                        scope_key IN (SELECT unnest(?))
                        OR split_part(coalesce(scope_key, ''), ':', 1)
                            IN (SELECT unnest(?))
                    )
            ),
            ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY __contract_id
                        ORDER BY
                            CASE severity
                                WHEN 'critical' THEN 4
                                WHEN 'high' THEN 3
                                WHEN 'medium' THEN 2
                                WHEN 'low' THEN 1
                                ELSE 0
                            END DESC,
                            score DESC NULLS LAST,
                            last_seen_at DESC NULLS LAST,
                            hit_id
                    ) AS __contract_rank
                FROM matched
            )
            SELECT *
            FROM ranked
            WHERE __contract_rank = 1
            """,
            [unique_contract_ids, unique_contract_ids, unique_contract_ids],
        )
        columns = [item[0] for item in cursor.description or []]
        rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    finally:
        con.close()
    return {str(row["__contract_id"]): row for row in rows}


def _contract_id_from_scope(scope_key: str | None) -> str | None:
    if scope_key is None:
        return None
    text = scope_key.strip()
    if not text:
        return None
    if ":" in text:
        text = text.split(":", 1)[0].strip()
    return text or None


def _contract_id_from_signal_row(row: dict[str, Any]) -> str | None:
    raw_contract_id = row.get("contract_id")
    if raw_contract_id is not None and str(raw_contract_id).strip():
        return str(raw_contract_id).strip()
    return _contract_id_from_scope(str(row["scope_key"]) if row.get("scope_key") else None)


def _case_summary_from_row(
    row: dict[str, Any],
    run: LakeSignalRun,
    *,
    anomaly_score: CaseAnomalyScore | None = None,
) -> CaseSummary:
    created_at = str(row.get("first_seen_at") or run.finished_at)
    updated_at = str(row.get("last_seen_at") or run.finished_at)
    entity_key = str(row.get("entity_key") or row.get("entity_uid") or "")
    contract_id = _contract_id_from_signal_row(row)
    if anomaly_score is None:
        anomaly_score = anomaly_score_for_contract(contract_id) if contract_id else None
    return CaseSummary(
        id=str(row["hit_id"]),
        title=str(row.get("title") or row.get("signal_id") or "Lake signal case"),
        description=str(row.get("description") or ""),
        status="new",
        created_at=created_at,
        updated_at=updated_at,
        entity_ids=[entity_key] if entity_key else [],
        signal_count=1,
        public_signal_count=1,
        last_refreshed_at=updated_at,
        last_run_id=str(row.get("run_id") or run.run_id),
        stale=False,
        anomaly_score=anomaly_score,
    )


def _anomaly_case_summary(score: CaseAnomalyScore) -> CaseSummary:
    scored_at = score.scored_at or datetime.now(UTC).isoformat()
    features = ", ".join(score.top_features) if score.top_features else "sin variables dominantes"
    reference = score.contract_reference or score.contract_id
    public_entity_id = score.supplier_document_id or (
        score.entity_uid.split(":", 1)[1]
        if ":" in score.entity_uid
        else score.entity_uid
    )
    parties = " · ".join(
        value for value in (score.supplier_name, score.buyer_name) if value
    )
    return CaseSummary(
        id=anomaly_case_id(score.contract_id),
        title=f"Contrato priorizado {reference}",
        description=(
            f"Puntaje de anomalia {score.score:.3f}; variables destacadas: {features}."
            + (f" {parties}." if parties else "")
        ),
        status="new",
        created_at=scored_at,
        updated_at=scored_at,
        entity_ids=[public_entity_id] if public_entity_id else [],
        signal_count=0,
        public_signal_count=0,
        last_refreshed_at=scored_at,
        last_run_id=score.score_run_id,
        stale=False,
        anomaly_score=score,
    )


def _anomaly_case_response(score: CaseAnomalyScore) -> CaseResponse:
    summary = _anomaly_case_summary(score)
    narrative = read_lake_narrative(summary.id, aliases=[score.contract_id])
    bundle_id = f"{summary.id}:evidence"
    evidence_item = EvidenceItemResponse(
        item_id=f"{summary.id}:contract",
        source_id=score.source_id or "secop_ii_contracts",
        record_id=score.contract_id,
        url=score.process_url,
        label=score.process_url or score.contract_id,
        item_type="contract",
        observed_at=score.scored_at,
        public_safe=True,
    )
    bundle = CaseEvidenceBundle(
        bundle_id=bundle_id,
        headline=summary.title,
        source_list=[score.source_id or "secop_ii_contracts"],
        evidence_items=[evidence_item],
    )
    event = CaseEventResponse(
        id=f"{summary.id}:anomaly_score",
        type="anomaly_score",
        label=f"Puntaje de anomalia {score.score:.3f}",
        date=score.scored_at or summary.updated_at,
        entity_id=summary.entity_ids[0] if summary.entity_ids else None,
        signal_hit_id=None,
        evidence_bundle_id=bundle_id,
        bundle_document_count=1,
    )
    return CaseResponse(
        **summary.model_dump(),
        signals=[],
        evidence_bundles=[bundle],
        events=[event],
        narrative_markdown=narrative.markdown if narrative else None,
        narrative_generated_at=narrative.generated_at if narrative else None,
    )


def get_lake_case(case_id: str) -> CaseResponse | None:
    anomaly_contract_id = contract_id_from_anomaly_case_id(case_id)
    if anomaly_contract_id is not None:
        anomaly_score = anomaly_score_for_contract(anomaly_contract_id)
        if anomaly_score is None:
            return None
        return _anomaly_case_response(anomaly_score)

    hit = materialized_hit(case_id, public_only=False)
    if hit is None:
        return None
    anomaly_contract_id = _contract_id_from_scope(hit.scope_key)
    anomaly_score = (
        anomaly_score_for_contract(anomaly_contract_id) if anomaly_contract_id else None
    )
    narrative = read_lake_narrative(
        case_id,
        aliases=[anomaly_contract_id] if anomaly_contract_id else None,
    )
    evidence_items = hit.evidence_items
    source_list = []
    for item in evidence_items:
        if item.source_id and item.source_id not in source_list:
            source_list.append(item.source_id)
    if not source_list:
        source_list = [source.database for source in hit.sources]
    bundle = CaseEvidenceBundle(
        bundle_id=hit.evidence_bundle_id or f"bundle:{hit.hit_id}",
        headline=hit.title,
        source_list=source_list,
        evidence_items=evidence_items,
    )
    event_date = hit.last_seen_at or hit.created_at or datetime.now(UTC).isoformat()
    event = CaseEventResponse(
        id=f"{hit.hit_id}:event",
        type="signal_hit",
        label=hit.title,
        date=event_date,
        entity_id=hit.entity_id,
        signal_hit_id=hit.hit_id,
        evidence_bundle_id=bundle.bundle_id,
        bundle_document_count=len(evidence_items),
    )
    return CaseResponse(
        id=hit.hit_id,
        title=hit.title,
        description=hit.description,
        status="new",
        created_at=hit.first_seen_at or event_date,
        updated_at=event_date,
        entity_ids=[hit.entity_key],
        signal_count=1,
        public_signal_count=1,
        last_refreshed_at=event_date,
        last_run_id=hit.run_id,
        stale=False,
        anomaly_score=anomaly_score,
        signals=[hit],
        evidence_bundles=[bundle],
        events=[event],
        narrative_markdown=narrative.markdown if narrative else None,
        narrative_generated_at=narrative.generated_at if narrative else None,
    )
