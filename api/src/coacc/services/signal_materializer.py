from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from neo4j import AsyncGraphDatabase

from coacc.config import settings
from coacc.models.entity import SourceAttribution
from coacc.models.signal import (
    EntitySignalsResponse,
    EvidenceItemResponse,
    SignalDefinition,
    SignalHitResponse,
    SignalListItem,
)
from coacc.services import dependency_registry, lakehouse_query
from coacc.services.neo4j_service import execute_query, execute_query_single
from coacc.services.signal_registry import (
    get_signal_definition,
    list_signal_definitions,
    load_signal_registry,
    resolve_signal_id,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    import duckdb
    from neo4j import AsyncSession, Record

    from coacc.services.intelligence_provider import IntelligenceProvider

logger = logging.getLogger(__name__)

_IDENTIFIER_KEYS = (
    "document_id",
    "nit",
    "cedula",
    "numero_documento",
    "contract_id",
    "sanction_id",
    "amendment_id",
    "finance_id",
    "embargo_id",
    "school_id",
    "convenio_id",
    "partner_id",
    "bid_id",
    "asset_id",
    "office_id",
    "stats_id",
    "project_id",
    "bpin_code",
    "case_id",
    "act_id",
    "gaceta_id",
    "requirement_id",
    "session_id",
    "order_id",
    "file_id",
    "doc_id",
)
_PERSON_LABELS = {"Person", "Partner"}
_SEVERITY_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}


@dataclass(frozen=True)
class MaterializationResult:
    signal_id: str
    run_id: str
    hit_count: int
    skipped: bool = False
    missing_required: list[str] | None = None


def _clean_identifier(raw: str) -> str:
    return re.sub(r"[.\-/]", "", raw)


def _hash_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@lru_cache(maxsize=1)
def _git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    sha = result.stdout.strip()
    return sha or None


def _coerce_scalar(value: Any) -> str | float | int | bool | list[str] | None:
    if value is None or isinstance(value, (str, float, int, bool)):
        return value
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item).strip()]
    return str(value)


def _normalize_pattern_data(
    data: Mapping[str, Any],
) -> dict[str, str | float | int | bool | list[str] | None]:
    return {key: _coerce_scalar(value) for key, value in data.items()}


def _as_source_rows(items: list[SourceAttribution]) -> list[str]:
    return [item.database for item in items if item.database]


async def _resolve_entity_context(session: AsyncSession, entity_id: str) -> dict[str, str | None]:
    record = await execute_query_single(
        session,
        "entity_by_element_id",
        {"element_id": entity_id},
    )
    if record is None:
        record = await execute_query_single(session, "entity_by_id", {"id": entity_id})
    clean_identifier = _clean_identifier(entity_id)
    if record is None and clean_identifier != entity_id:
        record = await execute_query_single(session, "entity_by_id", {"id": clean_identifier})

    if record is None:
        return {
            "entity_id": entity_id,
            "entity_key": clean_identifier or entity_id,
            "entity_label": None,
        }

    node = record["e"]
    labels = record["entity_labels"]
    props = dict(node)
    entity_key = next(
        (
            str(props[key]).strip()
            for key in _IDENTIFIER_KEYS
            if props.get(key) is not None and str(props.get(key)).strip()
        ),
        clean_identifier or entity_id,
    )
    return {
        "entity_id": entity_id,
        "entity_key": entity_key,
        "entity_label": labels[0] if labels else None,
    }


def _signal_sort_key(hit: SignalHitResponse) -> tuple[int, float, int, str]:
    return (
        -_SEVERITY_RANK.get(hit.severity, 0),
        -hit.score,
        -hit.evidence_count,
        hit.title.lower(),
    )


def _normalize_list(value: str | float | int | bool | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _derive_scope_key(
    data: Mapping[str, str | float | int | bool | list[str] | None],
    entity_context: Mapping[str, str | None],
) -> str:
    for key in (
        "scope_key",
        "case_id",
        "judicial_case_id",
        "contract_id",
        "project_id",
        "bpin_code",
    ):
        value = data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    evidence_refs = _normalize_list(data.get("evidence_refs"))
    if evidence_refs:
        return evidence_refs[0]
    return str(entity_context["entity_key"] or entity_context["entity_id"] or "")


def _identity_match_type(definition: SignalDefinition) -> str | None:
    if definition.public_policy.allowed_identity_match_types:
        return definition.public_policy.allowed_identity_match_types[0]
    if definition.requires_identity:
        return definition.requires_identity[0]
    return None


def _entity_type_matches(
    definition: SignalDefinition,
    entity_label: str | None,
) -> bool:
    if not definition.entity_types or entity_label is None:
        return True
    return entity_label in set(definition.entity_types)


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


def _query_threshold_params() -> dict[str, str | int | float]:
    return {
        "pattern_split_threshold_value": settings.pattern_split_threshold_value,
        "pattern_split_min_average_value": settings.pattern_split_min_average_value,
        "pattern_split_min_total_value": settings.pattern_split_min_total_value,
        "pattern_split_min_count": settings.pattern_split_min_count,
        "pattern_share_threshold": settings.pattern_share_threshold,
        "pattern_srp_min_orgs": settings.pattern_srp_min_orgs,
        "pattern_inexig_min_recurrence": settings.pattern_inexig_min_recurrence,
        "pattern_max_evidence_refs": settings.pattern_max_evidence_refs,
        "pattern_min_contract_value": settings.pattern_min_contract_value,
        "pattern_min_contract_count": settings.pattern_min_contract_count,
        "pattern_min_discrepancy_ratio": settings.pattern_min_discrepancy_ratio,
    }


def _dedup_key(
    definition: SignalDefinition,
    entity_context: Mapping[str, str | None],
    scope_key: str,
    data: Mapping[str, str | float | int | bool | list[str] | None],
) -> str:
    parts = [
        definition.id,
        str(definition.version),
        str(entity_context["entity_key"] or entity_context["entity_id"] or ""),
        scope_key,
    ]
    for field in definition.dedup_fields:
        if field == "scope_key":
            parts.append(scope_key)
            continue
        value = data.get(field)
        if isinstance(value, list):
            parts.append(",".join(sorted(str(item) for item in value)))
        elif value is not None:
            parts.append(str(value))
    return "|".join(parts)


def _build_evidence_items(
    *,
    definition: SignalDefinition,
    hit_id: str,
    evidence_refs: list[str],
    sources: list[SourceAttribution],
    observed_at: str,
    public_safe: bool,
    identity_match_type: str | None,
    identity_quality: str,
    data: Mapping[str, str | float | int | bool | list[str] | None],
) -> list[EvidenceItemResponse]:
    default_source = sources[0].database if sources else None
    label_field = definition.evidence_mapping.label_field
    raw_label = data.get(label_field) if label_field else None
    derived_label = None
    if raw_label is not None and str(raw_label).strip():
        derived_label = str(raw_label).strip()

    node_ref_field = definition.evidence_mapping.node_ref_field
    raw_node_ref = data.get(node_ref_field) if node_ref_field else None
    node_ref = (
        str(raw_node_ref).strip()
        if raw_node_ref is not None and str(raw_node_ref).strip()
        else None
    )

    rows: list[EvidenceItemResponse] = []
    for index, ref in enumerate(evidence_refs, start=1):
        is_url = ref.startswith("http://") or ref.startswith("https://")
        rows.append(EvidenceItemResponse(
            item_id=f"{hit_id}:{index}",
            source_id=default_source,
            record_id=None if is_url else ref,
            url=ref if is_url else None,
            label=derived_label or ref,
            item_type=definition.evidence_mapping.item_type,
            node_ref=node_ref,
            observed_at=observed_at,
            public_safe=public_safe,
            identity_match_type=identity_match_type,
            identity_quality=identity_quality,
        ))
    return rows


def _compute_public_safe(
    *,
    definition: SignalDefinition,
    entity_label: str | None,
    identity_quality: str,
    evidence_items: list[EvidenceItemResponse],
) -> bool:
    policy = definition.public_policy
    if definition.reviewer_only or not policy.allow_public:
        return False
    if entity_label in _PERSON_LABELS and not policy.allow_person_entities:
        return False
    if policy.require_exact_identity and identity_quality != "exact":
        return False
    if policy.require_public_evidence and not evidence_items:
        return False
    return all(item.public_safe for item in evidence_items)


def _build_hit(
    definition: SignalDefinition,
    entity_context: Mapping[str, str | None],
    data: dict[str, str | float | int | bool | list[str] | None],
    sources: list[SourceAttribution],
    description: str,
    run_id: str,
    observed_at: str,
) -> SignalHitResponse:
    evidence_refs = _normalize_list(data.get("evidence_refs"))
    scope_key = _derive_scope_key(data, entity_context)
    dedup_key = _dedup_key(definition, entity_context, scope_key, data)
    hit_id = _hash_token(dedup_key)

    raw_identity_match_type = data.get("identity_match_type")
    identity_match_type = (
        str(raw_identity_match_type).strip()
        if raw_identity_match_type is not None and str(raw_identity_match_type).strip()
        else _identity_match_type(definition)
    )
    raw_identity_quality = data.get("identity_quality")
    identity_quality = (
        str(raw_identity_quality).strip()
        if raw_identity_quality is not None and str(raw_identity_quality).strip()
        else _identity_quality(identity_match_type)
    )
    provisional_items = _build_evidence_items(
        definition=definition,
        hit_id=hit_id,
        evidence_refs=evidence_refs,
        sources=sources,
        observed_at=observed_at,
        public_safe=True,
        identity_match_type=identity_match_type,
        identity_quality=identity_quality,
        data=data,
    )
    public_safe = _compute_public_safe(
        definition=definition,
        entity_label=entity_context["entity_label"],
        identity_quality=identity_quality,
        evidence_items=provisional_items,
    )
    evidence_items = [
        item.model_copy(update={"public_safe": public_safe})
        for item in provisional_items
    ]

    raw_score = data.get("risk_signal")
    try:
        score = float(raw_score) if raw_score is not None else float(len(evidence_refs))
    except (TypeError, ValueError):
        score = float(len(evidence_refs))

    raw_count = data.get("evidence_count")
    try:
        evidence_count = int(raw_count) if raw_count is not None else len(evidence_refs)
    except (TypeError, ValueError):
        evidence_count = len(evidence_refs)

    return SignalHitResponse(
        hit_id=hit_id,
        run_id=run_id,
        signal_id=definition.id,
        signal_version=definition.version,
        title=definition.title,
        description=description or definition.description,
        category=definition.category,
        severity=definition.severity,
        public_safe=public_safe,
        reviewer_only=definition.reviewer_only,
        entity_id=str(entity_context["entity_id"] or ""),
        entity_key=str(entity_context["entity_key"] or entity_context["entity_id"] or ""),
        entity_label=entity_context["entity_label"],
        scope_key=scope_key,
        scope_type=definition.scope_type,
        dedup_key=dedup_key,
        score=score,
        identity_confidence=_identity_confidence(identity_quality),
        identity_match_type=identity_match_type,
        identity_quality=identity_quality,
        evidence_count=evidence_count,
        evidence_bundle_id=f"bundle:{hit_id}",
        evidence_refs=evidence_refs,
        data=data,
        sources=sources,
        evidence_items=evidence_items,
        created_at=observed_at,
        first_seen_at=observed_at,
        last_seen_at=observed_at,
    )


async def _persist_hit(session: AsyncSession, hit: SignalHitResponse, *, engine: str) -> None:
    await execute_query(
        session,
        "signal_upsert",
        {
            "run_id": hit.run_id,
            "entity_id": hit.entity_id,
            "entity_key": hit.entity_key,
            "entity_label": hit.entity_label,
            "hit_id": hit.hit_id,
            "signal_id": hit.signal_id,
            "signal_version": hit.signal_version,
            "engine": engine,
            "title": hit.title,
            "description": hit.description,
            "category": hit.category,
            "severity": hit.severity,
            "public_safe": hit.public_safe,
            "reviewer_only": hit.reviewer_only,
            "scope_key": hit.scope_key,
            "scope_type": hit.scope_type,
            "dedup_key": hit.dedup_key,
            "score": hit.score,
            "identity_confidence": hit.identity_confidence,
            "identity_match_type": hit.identity_match_type,
            "identity_quality": hit.identity_quality,
            "evidence_count": hit.evidence_count,
            "evidence_refs": hit.evidence_refs,
            "data_json": json.dumps(hit.data, ensure_ascii=True, sort_keys=True),
            "sources": _as_source_rows(hit.sources),
            "created_at": hit.created_at,
            "first_seen_at": hit.first_seen_at,
            "last_seen_at": hit.last_seen_at,
            "bundle_id": hit.evidence_bundle_id,
            "bundle_headline": hit.title,
            "bundle_summary": hit.description,
            "evidence_items": [
                {
                    "item_id": item.item_id,
                    "source_id": item.source_id,
                    "record_id": item.record_id,
                    "url": item.url,
                    "label": item.label,
                    "item_type": item.item_type,
                    "node_ref": item.node_ref,
                    "observed_at": item.observed_at,
                    "public_safe": item.public_safe,
                    "identity_match_type": item.identity_match_type,
                    "identity_quality": item.identity_quality,
                }
                for item in hit.evidence_items
            ],
        },
    )


def _duckdb_rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    columns = [item[0] for item in cursor.description or []]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _duckdb_entity_context(row: Mapping[str, Any]) -> dict[str, str | None]:
    entity_id = str(row.get("entity_id") or row.get("entity_key") or row.get("scope_key") or "")
    entity_key = str(row.get("entity_key") or entity_id)
    entity_label = row.get("entity_label")
    return {
        "entity_id": entity_id,
        "entity_key": entity_key,
        "entity_label": str(entity_label) if entity_label is not None else None,
    }


async def materialize_via_duckdb(
    session: AsyncSession,
    sig_id: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> MaterializationResult:
    definition = get_signal_definition(sig_id)
    if definition is None:
        raise ValueError(f"Unknown signal: {sig_id}")
    if definition.runner.kind != "duckdb":
        raise ValueError(f"Signal {sig_id} is not a DuckDB signal")

    can_run, missing = dependency_registry.can_materialize(definition.id)
    run_id = str(uuid.uuid4())
    observed_at = _now_iso()
    registry = load_signal_registry()
    git_sha = _git_sha()
    severity = dependency_registry.severity_for_sources(definition.id, definition.severity)
    active_definition = definition.model_copy(update={"severity": severity})

    await execute_query(
        session,
        "signal_run_upsert",
        {
            "run_id": run_id,
            "registry_version": registry.registry_version,
            "entity_id": None,
            "entity_key": None,
            "scope_type": "signal",
            "scope_ref": definition.id,
            "lang": "es",
            "trigger": "dependency",
            "created_by": None,
            "git_sha": git_sha,
            "data_snapshot_id": None,
            "started_at": observed_at,
            "finished_at": None,
            "status": "running" if can_run else "skipped",
            "hit_count": 0,
        },
    )
    if not can_run:
        await execute_query(
            session,
            "signal_run_upsert",
            {
                "run_id": run_id,
                "registry_version": registry.registry_version,
                "entity_id": None,
                "entity_key": None,
                "scope_type": "signal",
                "scope_ref": definition.id,
                "lang": "es",
                "trigger": "dependency",
                "created_by": None,
                "git_sha": git_sha,
                "data_snapshot_id": None,
                "started_at": observed_at,
                "finished_at": _now_iso(),
                "status": "skipped",
                "hit_count": 0,
            },
        )
        return MaterializationResult(
            definition.id,
            run_id,
            0,
            skipped=True,
            missing_required=missing,
        )

    owns_connection = con is None
    if con is None:
        con = lakehouse_query.connect(read_only=True)
    try:
        for source in active_definition.sources or active_definition.sources_required:
            lakehouse_query.register_source(con, source)
        sql_path = lakehouse_query.signal_sql_path(active_definition.id)
        rows = _duckdb_rows(con, sql_path.read_text(encoding="utf-8"))
    finally:
        if owns_connection:
            con.close()

    sources = [
        SourceAttribution(database=source)
        for source in (active_definition.sources or active_definition.sources_required)
    ]
    hits_by_id: dict[str, SignalHitResponse] = {}
    for row in rows:
        normalized = _normalize_pattern_data(row)
        hit = _build_hit(
            active_definition,
            _duckdb_entity_context(row),
            normalized,
            sources,
            active_definition.description,
            run_id,
            observed_at,
        )
        await _persist_hit(session, hit, engine="duckdb")
        hits_by_id[hit.hit_id] = hit

    await execute_query(
        session,
        "signal_run_upsert",
        {
            "run_id": run_id,
            "registry_version": registry.registry_version,
            "entity_id": None,
            "entity_key": None,
            "scope_type": "signal",
            "scope_ref": definition.id,
            "lang": "es",
            "trigger": "dependency",
            "created_by": None,
            "git_sha": git_sha,
            "data_snapshot_id": None,
            "started_at": observed_at,
            "finished_at": _now_iso(),
            "status": "completed",
            "hit_count": len(hits_by_id),
        },
    )
    return MaterializationResult(definition.id, run_id, len(hits_by_id))


async def materialize_dependency_signals(
    session: AsyncSession,
    *,
    advanced_sources: set[str] | None = None,
    all_signals: bool = False,
) -> list[MaterializationResult]:
    definitions = list_signal_definitions()
    if all_signals:
        selected = [definition for definition in definitions if definition.runner.kind == "duckdb"]
    else:
        signal_ids = dependency_registry.signals_to_rerun(advanced_sources or set())
        selected = [
            definition
            for definition in definitions
            if definition.id in set(signal_ids) and definition.runner.kind == "duckdb"
        ]
    results: list[MaterializationResult] = []
    for definition in selected:
        results.append(await materialize_via_duckdb(session, definition.id))
    return results


async def _execute_signal_runner(
    session: AsyncSession,
    definition: SignalDefinition,
    provider: IntelligenceProvider,
    *,
    entity_id: str,
    entity_context: Mapping[str, str | None],
    lang: str,
) -> list[dict[str, Any]]:
    if definition.runner.kind == "pattern":
        pattern_results = await provider.run_pattern(
            session,
            definition.runner.ref,
            entity_id=entity_id,
            lang=lang,
            include_probable=False,
        )
        return [
            {
                "data": _normalize_pattern_data(result.data),
                "sources": result.sources,
                "description": result.description,
            }
            for result in pattern_results
        ]

    records = await execute_query(
        session,
        definition.runner.ref,
        {
            "entity_id": entity_id,
            "entity_key": entity_context["entity_key"],
            "entity_label": entity_context["entity_label"],
            "lang": lang,
            **_query_threshold_params(),
        },
    )
    source_ids = definition.sources_required or [f"neo4j:{definition.runner.ref}"]
    return [
        {
            "data": _normalize_pattern_data({key: record[key] for key in record}),
            "sources": [
                SourceAttribution(database=source_id)
                for source_id in source_ids
            ],
            "description": definition.description,
        }
        for record in records
    ]


async def refresh_entity_signals(
    session: AsyncSession,
    entity_id: str,
    provider: IntelligenceProvider,
    *,
    lang: str = "es",
    trigger: str = "manual",
    created_by: str | None = None,
) -> EntitySignalsResponse:
    entity_context = await _resolve_entity_context(session, entity_id)
    observed_at = _now_iso()
    run_id = str(uuid.uuid4())
    registry = load_signal_registry()
    git_sha = _git_sha()
    await execute_query(
        session,
        "signal_run_upsert",
        {
            "run_id": run_id,
            "registry_version": registry.registry_version,
            "entity_id": entity_context["entity_id"],
            "entity_key": entity_context["entity_key"],
            "scope_type": "entity",
            "scope_ref": entity_context["entity_key"],
            "lang": lang,
            "trigger": trigger,
            "created_by": created_by,
            "git_sha": git_sha,
            "data_snapshot_id": None,
            "started_at": observed_at,
            "finished_at": None,
            "status": "running",
            "hit_count": 0,
        },
    )

    hits_by_id: dict[str, SignalHitResponse] = {}
    for definition in list_signal_definitions():
        if not _entity_type_matches(definition, entity_context["entity_label"]):
            continue
        try:
            runner_rows = await _execute_signal_runner(
                session,
                definition,
                provider,
                entity_id=entity_id,
                entity_context=entity_context,
                lang=lang,
            )
        except TimeoutError:
            logger.warning("Signal '%s' timed out during materialization", definition.id)
            continue
        except Exception:
            logger.exception("Signal '%s' failed during materialization", definition.id)
            continue

        for row in runner_rows:
            hit = _build_hit(
                definition,
                entity_context,
                row["data"],
                row["sources"],
                row["description"],
                run_id,
                observed_at,
            )
            await _persist_hit(session, hit, engine=definition.runner.kind)
            hits_by_id[hit.hit_id] = hit

    finished_at = _now_iso()
    await execute_query(
        session,
        "signal_run_upsert",
        {
            "run_id": run_id,
            "registry_version": registry.registry_version,
            "entity_id": entity_context["entity_id"],
            "entity_key": entity_context["entity_key"],
            "scope_type": "entity",
            "scope_ref": entity_context["entity_key"],
            "lang": lang,
            "trigger": trigger,
            "created_by": created_by,
            "git_sha": git_sha,
            "data_snapshot_id": None,
            "started_at": observed_at,
            "finished_at": finished_at,
            "status": "completed",
            "hit_count": len(hits_by_id),
        },
    )

    hits = sorted(hits_by_id.values(), key=_signal_sort_key)
    return EntitySignalsResponse(
        entity_id=str(entity_context["entity_id"] or entity_id),
        entity_key=str(entity_context["entity_key"] or entity_id),
        total=len(hits),
        last_run_id=run_id,
        last_refreshed_at=finished_at,
        stale=False,
        signals=hits,
    )


async def materialize_entity_signals(
    session: AsyncSession,
    entity_id: str,
    provider: IntelligenceProvider,
    lang: str = "es",
) -> EntitySignalsResponse:
    return await refresh_entity_signals(session, entity_id, provider, lang=lang)


def _record_to_signal_hit(record: Record) -> SignalHitResponse:
    canonical_signal_id = resolve_signal_id(str(record["signal_id"]))
    definition = get_signal_definition(canonical_signal_id)
    data_json = record["data_json"] or "{}"
    data = _normalize_pattern_data(json.loads(data_json))
    sources = [
        SourceAttribution(database=str(source))
        for source in (record["sources"] or [])
        if str(source).strip()
    ]
    evidence_items = [
        EvidenceItemResponse(
            item_id=str(item["item_id"]),
            source_id=item.get("source_id"),
            record_id=item.get("record_id"),
            url=item.get("url"),
            label=item.get("label"),
            item_type=str(item.get("item_type") or "reference"),
            node_ref=item.get("node_ref"),
            observed_at=item.get("observed_at"),
            public_safe=bool(item.get("public_safe", True)),
            identity_match_type=item.get("identity_match_type"),
            identity_quality=item.get("identity_quality"),
        )
        for item in (record["evidence_items"] or [])
        if item and item.get("item_id")
    ]
    return SignalHitResponse(
        hit_id=str(record["hit_id"]),
        run_id=record.get("run_id"),
        signal_id=canonical_signal_id,
        signal_version=int(record["signal_version"] or (definition.version if definition else 1)),
        title=str(record["title"]),
        description=str(record["description"]),
        category=str(record["category"]),
        severity=str(record["severity"]),  # type: ignore[arg-type]
        public_safe=bool(record["public_safe"]),
        reviewer_only=bool(record["reviewer_only"]),
        entity_id=str(record["entity_id"]),
        entity_key=str(record["entity_key"]),
        entity_label=record.get("entity_label"),
        scope_key=record.get("scope_key"),
        scope_type=str(record.get("scope_type") or "entity"),
        dedup_key=str(record["dedup_key"]),
        score=float(record["score"] or 0),
        identity_confidence=float(record["identity_confidence"] or 0),
        identity_match_type=record.get("identity_match_type"),
        identity_quality=record.get("identity_quality"),
        evidence_count=int(record["evidence_count"] or 0),
        evidence_bundle_id=record.get("evidence_bundle_id"),
        evidence_refs=[str(item) for item in (record["evidence_refs"] or [])],
        data=data,
        sources=sources,
        evidence_items=evidence_items,
        created_at=str(record["created_at"]) if record["created_at"] is not None else None,
        first_seen_at=str(record["first_seen_at"]) if record["first_seen_at"] is not None else None,
        last_seen_at=str(record["last_seen_at"]) if record["last_seen_at"] is not None else None,
    )


async def get_stored_entity_signals(
    session: AsyncSession,
    entity_id: str,
) -> EntitySignalsResponse:
    entity_context = await _resolve_entity_context(session, entity_id)
    latest_run = await execute_query_single(
        session,
        "signal_latest_run_for_entity",
        {
            "entity_id": entity_context["entity_id"],
            "entity_key": entity_context["entity_key"],
        },
    )
    if latest_run is None:
        return EntitySignalsResponse(
            entity_id=str(entity_context["entity_id"] or entity_id),
            entity_key=str(entity_context["entity_key"] or entity_id),
            total=0,
            last_run_id=None,
            last_refreshed_at=None,
            stale=True,
            signals=[],
        )

    records = await execute_query(
        session,
        "signal_hits_by_run",
        {"run_id": latest_run["run_id"]},
    )
    hits = sorted((_record_to_signal_hit(record) for record in records), key=_signal_sort_key)
    return EntitySignalsResponse(
        entity_id=str(entity_context["entity_id"] or entity_id),
        entity_key=str(entity_context["entity_key"] or entity_id),
        total=len(hits),
        last_run_id=str(latest_run["run_id"]),
        last_refreshed_at=(
            str(latest_run["finished_at"])
            if latest_run["finished_at"] is not None
            else None
        ),
        stale=bool(latest_run.get("status") != "completed"),
        signals=hits,
    )


async def list_signal_summaries(session: AsyncSession) -> list[SignalListItem]:
    counts = await execute_query(session, "signal_hit_counts")
    counts_by_id: dict[str, dict[str, int | str | None]] = {}
    for row in counts:
        signal_id = resolve_signal_id(str(row["signal_id"]))
        meta = counts_by_id.setdefault(
            signal_id,
            {"hit_count": 0, "last_seen_at": None},
        )
        meta["hit_count"] = int(meta["hit_count"] or 0) + int(row["hit_count"] or 0)
        row_last_seen = str(row["last_seen_at"]) if row["last_seen_at"] is not None else None
        if row_last_seen and (
            meta["last_seen_at"] is None or row_last_seen > str(meta["last_seen_at"])
        ):
            meta["last_seen_at"] = row_last_seen
    items: list[SignalListItem] = []
    for definition in list_signal_definitions():
        count_meta = counts_by_id.get(definition.id, {})
        items.append(SignalListItem(
            **definition.model_dump(),
            hit_count=int(count_meta.get("hit_count", 0)),
            last_seen_at=count_meta.get("last_seen_at"),
        ))
    return items


async def get_latest_materializer_run(
    session: AsyncSession,
) -> tuple[str | None, str | None]:
    record = await execute_query_single(session, "signal_latest_completed_run")
    if record is None:
        return None, None
    return (
        str(record["run_id"]) if record["run_id"] is not None else None,
        str(record["finished_at"]) if record["finished_at"] is not None else None,
    )


async def get_signal_samples(
    session: AsyncSession,
    signal_id: str,
    limit: int = 10,
) -> list[SignalHitResponse]:
    canonical_signal_id = resolve_signal_id(signal_id)
    registry = load_signal_registry()
    accepted_ids = [
        legacy_id
        for legacy_id, canonical_id in registry.aliases.items()
        if canonical_id == canonical_signal_id
    ]
    accepted_ids.append(canonical_signal_id)
    records = await execute_query(
        session,
        "signal_hit_samples",
        {"signal_ids": accepted_ids, "limit": limit},
    )
    return [_record_to_signal_hit(record) for record in records]


def filter_signal_hits_for_viewer(
    hits: list[SignalHitResponse],
    *,
    can_view_reviewer: bool,
) -> list[SignalHitResponse]:
    if can_view_reviewer:
        return hits
    return [hit for hit in hits if not hit.reviewer_only]


async def _run_cli(args: argparse.Namespace) -> None:
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        async with driver.session(database=settings.neo4j_database) as session:
            results = await materialize_dependency_signals(
                session,
                advanced_sources=(
                    set(args.advanced_sources.split(",")) if args.advanced_sources else None
                ),
                all_signals=args.all,
            )
    finally:
        await driver.close()

    for result in results:
        status = "skipped" if result.skipped else "completed"
        print(f"{result.signal_id}: {status}, hits={result.hit_count}, run={result.run_id}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--advanced-sources", default="")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run_cli(args))


if __name__ == "__main__":
    main()
