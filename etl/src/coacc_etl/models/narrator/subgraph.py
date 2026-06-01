from __future__ import annotations

import base64
import binascii
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import duckdb

from coacc_etl.lakehouse.paths import lake_root

if TYPE_CHECKING:
    from pathlib import Path

ANOMALY_CASE_PREFIX = "anomaly:"
SECOP_CONTRACTS_DATASET_ID = "jbjy-vk9h"


class NarratorError(RuntimeError):
    """Raised when a case subgraph or narrative cannot be built safely."""


@dataclass(frozen=True)
class EvidenceCitation:
    dataset_id: str
    row_key: str
    label: str
    url: str | None = None


@dataclass(frozen=True)
class SubgraphNode:
    node_id: str
    name: str
    node_type: str


@dataclass(frozen=True)
class SubgraphSignal:
    signal_id: str
    title: str
    severity: str
    score: float
    evidence_refs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AnomalyContext:
    score: float
    score_confidence: str
    top_features: list[str]
    score_run_id: str
    model_run_id: str | None = None
    scored_at: str | None = None


@dataclass(frozen=True)
class CaseSubgraph:
    case_id: str
    contract_id: str
    contract_reference: str | None
    process_url: str | None
    buyer_name: str | None
    supplier_name: str | None
    procurement_modality: str | None
    contract_value: float | None
    signing_date: str | None
    anomaly: AnomalyContext
    nodes: list[SubgraphNode] = field(default_factory=list)
    signals: list[SubgraphSignal] = field(default_factory=list)
    evidence: list[EvidenceCitation] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _SignalRun:
    run_id: str
    signal_hits_path: str
    evidence_bundles_path: str


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str | int | float):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _anomaly_case_contract_id(case_id: str) -> str | None:
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


def _score_partition(run_id: str) -> Path:
    return lake_root() / "curated" / "anomaly_scores" / f"run_id={run_id}"


def _score_glob(run_id: str) -> str:
    return str(_score_partition(run_id) / "*.parquet")


def _latest_score_run_id() -> str | None:
    manifest_path = lake_root() / "models" / "anomaly" / "current.json"
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        score_run_id = str(payload.get("score_run_id") or payload.get("run_id") or "").strip()
        if score_run_id and any(_score_partition(score_run_id).glob("*.parquet")):
            return score_run_id

    root = lake_root() / "curated" / "anomaly_scores"
    if not root.exists():
        return None
    partitions = [
        path
        for path in root.glob("run_id=*")
        if path.is_dir() and any(path.glob("*.parquet"))
    ]
    if not partitions:
        return None
    return max(partitions, key=lambda path: path.stat().st_mtime).name.removeprefix("run_id=")


def _score_row_for_contract(contract_id: str) -> dict[str, Any] | None:
    run_id = _latest_score_run_id()
    if run_id is None:
        return None
    con = duckdb.connect(database=":memory:")
    try:
        cursor = con.execute(
            f"""
            SELECT *
            FROM read_parquet({_sql_string(_score_glob(run_id))})
            WHERE contract_id = ?
            ORDER BY scored_at DESC NULLS LAST, score DESC NULLS LAST
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
    return dict(zip(columns, row, strict=False))


def _contract_table_glob() -> str | None:
    root = lake_root() / "curated" / "table=fct_procurement_contract_awards"
    if not root.exists() or not any(root.glob("*.parquet")):
        return None
    return str(root / "*.parquet")


def _contract_row(contract_id: str) -> dict[str, Any] | None:
    glob = _contract_table_glob()
    if glob is None:
        return None
    con = duckdb.connect(database=":memory:")
    try:
        cursor = con.execute(
            f"""
            SELECT
                contract_id,
                contract_reference,
                process_url,
                buyer_name,
                supplier_name,
                procurement_modality,
                contract_value,
                signing_date
            FROM read_parquet({_sql_string(glob)})
            WHERE contract_id = ?
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
    return dict(zip(columns, row, strict=False))


def _latest_signal_run() -> _SignalRun | None:
    manifest_dir = lake_root() / "meta" / "signal_runs"
    if not manifest_dir.exists():
        return None
    manifests = sorted(path for path in manifest_dir.glob("*.json") if path.is_file())
    for path in reversed(manifests):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("status") != "completed":
            continue
        run_id = str(payload.get("run_id") or path.stem)
        outputs = payload.get("outputs") if isinstance(payload.get("outputs"), dict) else {}
        hits_path = str(
            outputs.get("signal_hits")
            or lake_root() / "curated" / "signal_hits" / f"run_id={run_id}"
        )
        evidence_path = str(
            outputs.get("evidence_bundles")
            or lake_root() / "curated" / "evidence_bundles" / f"run_id={run_id}"
        )
        return _SignalRun(
            run_id=run_id,
            signal_hits_path=(
                hits_path if hits_path.endswith("*.parquet") else f"{hits_path}/*.parquet"
            ),
            evidence_bundles_path=(
                evidence_path
                if evidence_path.endswith("*.parquet")
                else f"{evidence_path}/*.parquet"
            ),
        )
    return None


def _signal_rows(contract_id: str, case_id: str) -> list[dict[str, Any]]:
    run = _latest_signal_run()
    if run is None:
        return []
    con = duckdb.connect(database=":memory:")
    try:
        cursor = con.execute(
            f"""
            SELECT *
            FROM read_parquet({_sql_string(run.signal_hits_path)})
            WHERE hit_id = ?
                OR scope_key = ?
                OR split_part(coalesce(scope_key, ''), ':', 1) = ?
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END DESC,
                score DESC NULLS LAST
            LIMIT 10
            """,
            [case_id, contract_id, contract_id],
        )
        columns = [item[0] for item in cursor.description or []]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    except duckdb.IOException:
        return []
    finally:
        con.close()


def _signal_contract_id(case_id: str) -> str | None:
    run = _latest_signal_run()
    if run is None:
        return None
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute(
            f"""
            SELECT scope_key
            FROM read_parquet({_sql_string(run.signal_hits_path)})
            WHERE hit_id = ?
            LIMIT 1
            """,
            [case_id],
        ).fetchone()
    except duckdb.IOException:
        return None
    finally:
        con.close()
    if row is None or row[0] is None:
        return None
    scope_key = str(row[0]).strip()
    return scope_key.split(":", 1)[0].strip() if scope_key else None


def _evidence_rows(hit_ids: list[str]) -> list[dict[str, Any]]:
    run = _latest_signal_run()
    unique_hit_ids = list(dict.fromkeys(hit_id for hit_id in hit_ids if hit_id))
    if run is None or not unique_hit_ids:
        return []
    con = duckdb.connect(database=":memory:")
    try:
        cursor = con.execute(
            f"""
            SELECT *
            FROM read_parquet({_sql_string(run.evidence_bundles_path)})
            WHERE hit_id IN (SELECT unnest(?))
            ORDER BY hit_id, item_index
            """,
            [unique_hit_ids],
        )
        columns = [item[0] for item in cursor.description or []]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    except duckdb.IOException:
        return []
    finally:
        con.close()


def _source_to_dataset_id(source_id: str | None) -> str:
    if source_id == "secop_ii_contracts" or source_id is None:
        return SECOP_CONTRACTS_DATASET_ID
    return source_id


def _evidence_from_rows(
    contract_id: str,
    process_url: str | None,
    rows: list[dict[str, Any]],
) -> list[EvidenceCitation]:
    citations = [
        EvidenceCitation(
            dataset_id=SECOP_CONTRACTS_DATASET_ID,
            row_key=contract_id,
            label="SECOP II contract",
            url=process_url,
        )
    ]
    seen = {(citations[0].dataset_id, citations[0].row_key)}
    for row in rows:
        dataset_id = _source_to_dataset_id(_optional_str(row.get("source_id")))
        row_key = (
            _optional_str(row.get("record_id"))
            or _optional_str(row.get("row_selector"))
            or _optional_str(row.get("label"))
        )
        if row_key is None:
            continue
        key = (dataset_id, row_key)
        if key in seen:
            continue
        seen.add(key)
        citations.append(
            EvidenceCitation(
                dataset_id=dataset_id,
                row_key=row_key,
                label=_optional_str(row.get("label")) or row_key,
                url=_optional_str(row.get("url")),
            )
        )
    return citations


def extract(case_id: str) -> CaseSubgraph:
    contract_id = _anomaly_case_contract_id(case_id) or case_id
    score_row = _score_row_for_contract(contract_id)
    if score_row is None:
        signal_contract_id = _signal_contract_id(case_id)
        if signal_contract_id:
            contract_id = signal_contract_id
            score_row = _score_row_for_contract(contract_id)
    if score_row is None:
        raise NarratorError(f"missing anomaly score for case_id: {case_id}")

    contract_id = str(score_row.get("contract_id") or contract_id)
    contract_row = _contract_row(contract_id) or {}
    signal_rows = _signal_rows(contract_id, case_id)
    evidence_rows = _evidence_rows([str(row.get("hit_id") or "") for row in signal_rows])

    supplier_name = _optional_str(contract_row.get("supplier_name")) or _optional_str(
        score_row.get("entity_uid")
    )
    buyer_name = _optional_str(contract_row.get("buyer_name"))
    process_url = _optional_str(contract_row.get("process_url")) or _optional_str(
        score_row.get("process_url")
    )

    nodes = [
        node
        for node in (
            SubgraphNode(node_id=f"contract:{contract_id}", name=contract_id, node_type="contract"),
            SubgraphNode(node_id="buyer", name=buyer_name or "", node_type="buyer"),
            SubgraphNode(node_id="supplier", name=supplier_name or "", node_type="supplier"),
        )
        if node.name
    ]
    signals = [
        SubgraphSignal(
            signal_id=str(row.get("signal_id") or ""),
            title=str(row.get("title") or row.get("signal_id") or "Signal"),
            severity=str(row.get("severity") or "medium"),
            score=float(row.get("score") or 0.0),
            evidence_refs=_normalize_list(row.get("evidence_refs")),
        )
        for row in signal_rows
    ]
    evidence = _evidence_from_rows(contract_id, process_url, evidence_rows)
    scored_at = _optional_str(score_row.get("scored_at"))

    return CaseSubgraph(
        case_id=case_id,
        contract_id=contract_id,
        contract_reference=_optional_str(contract_row.get("contract_reference")),
        process_url=process_url,
        buyer_name=buyer_name,
        supplier_name=supplier_name,
        procurement_modality=_optional_str(contract_row.get("procurement_modality")),
        contract_value=_optional_float(contract_row.get("contract_value")),
        signing_date=_optional_str(contract_row.get("signing_date")),
        anomaly=AnomalyContext(
            score=float(score_row.get("score") or 0.0),
            score_confidence=str(score_row.get("score_confidence") or "unknown"),
            top_features=_normalize_list(score_row.get("top_features")),
            score_run_id=str(score_row.get("run_id") or ""),
            model_run_id=_optional_str(score_row.get("model_run_id")),
            scored_at=scored_at or datetime.now(UTC).isoformat(),
        ),
        nodes=nodes,
        signals=signals,
        evidence=evidence,
    )
