from __future__ import annotations

import json
import re
import shutil
import subprocess
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb

from coacc_etl.lakehouse.paths import lake_root, meta_path
from coacc_etl.signals.registry import get_signal_definition, resolve_signal_id

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from coacc_etl.signals.registry import MaterializableSignalDefinition

SUPPORTED_SIGNAL_IDS = (
    "procurement_single_bidder_high_value",
    "procurement_large_modifications",
    "procurement_contract_modification_ladder_review_only",
    "procurement_sanctioned_supplier_awarded",
    "procurement_secop_sanction_later_awards_review_only",
    "fiscal_procurement_chronology_review_only",
    "siri_antecedent_procurement_chronology_review_only",
    "procurement_supplier_concentration_across_entities",
    "procurement_contract_value_outlier_by_category",
    "procurement_repeat_awards_same_supplier",
    "procurement_buyer_supplier_network_density",
    "procurement_cartel_risk_cobidding",
    "procurement_related_bidders_same_process_review_only",
    "procurement_shared_representative_same_buyer_cluster_review_only",
    "procurement_payment_plan_anomalies",
    "procurement_guarantee_advance_execution_chain",
    "procurement_guarantee_policy_reuse_review_only",
    "procurement_budget_chain_reconciliation_review_only",
    "procurement_invoice_budget_reconciliation_review_only",
    "procurement_payment_plan_reconciliation_review_only",
    "health_pae_service_delivery_gap_review_only",
    "pae_beneficiary_territory_delivery_gap_review_only",
    "procurement_contract_suspensions",
    "procurement_contract_execution_delay",
    "procurement_short_bidding_window",
    "procurement_offers_competition_drop",
    "procurement_public_servant_conflict_disclosure_overlap",
    "procurement_role_supplier_same_buyer_review_only",
    "public_declaration_supplier_chronology_review_only",
    "public_declaration_company_bridge_current_risk_review_only",
    "cuentas_claras_donor_supplier_overlap",
    "cuentas_claras_donor_ineligibility_review",
    "pida_full30_meta",
    "pida5_pida27_pida4_chain",
    "project_bpin_procurement_overlap",
    "project_regalias_execution_procurement_overlap",
    "sgr_ocad_executor_capacity_gap",
    "dnp_sgr_beneficiary_delivery_gap_review_only",
    "bpin_dnp_vs_pida27_obras_prioritarias",
    "tvec_multi_entity_capture",
    "tvec_item_price_dispersion_review_only",
    "procurement_politically_exposed_position_supplier_overlap",
    "procurement_related_companies_shared_officer",
    "procurement_cross_source_identity_inconsistency",
    "rues_supplier_capacity_status_review_only",
    "cross_signal_compound_risk_review_only",
    "secop_i_legacy_supplier_current_risk_review_only",
    "secop_i_legacy_representative_current_risk_review_only",
    "secop_interadmin_executor_network_review_only",
)
_RUN_ID_SAFE = re.compile(r"[^A-Za-z0-9_.=-]+")


class SignalMaterializationError(RuntimeError):
    """Raised when signal materialization cannot complete from the local lake."""


@dataclass(frozen=True)
class SignalMaterializationSignalResult:
    signal_id: str
    source_table: str
    hit_count: int


@dataclass(frozen=True)
class SignalMaterializationRun:
    run_id: str
    generated_at: str
    signal_results: list[SignalMaterializationSignalResult]
    signal_hits_path: str
    evidence_bundles_path: str
    evidence_count: int
    manifest_path: str

    @property
    def hit_count(self) -> int:
        return sum(result.hit_count for result in self.signal_results)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_bool(value: bool) -> str:
    return "true" if value else "false"


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


def _new_run_id() -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def _clean_run_id(run_id: str) -> str:
    cleaned = _RUN_ID_SAFE.sub("_", run_id.strip()).strip("._")
    if not cleaned:
        raise SignalMaterializationError("run_id cannot be empty")
    return cleaned


def _feature_table(signal_id: str) -> str:
    return f"signal_feature_{signal_id}"


def _feature_table_dir(signal_id: str) -> Path:
    return lake_root() / "curated" / f"table={_feature_table(signal_id)}"


def _feature_table_glob(signal_id: str) -> str:
    table_dir = _feature_table_dir(signal_id)
    files = sorted(path for path in table_dir.glob("*.parquet") if path.is_file())
    if not files:
        raise SignalMaterializationError(
            f"missing curated signal feature parquet for {signal_id}: {table_dir}"
        )
    return str(table_dir / "*.parquet")


def _feature_table_columns(parquet_glob: str) -> set[str]:
    con = duckdb.connect(database=":memory:")
    try:
        rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_sql_string(parquet_glob)})"
        ).fetchall()
    finally:
        con.close()
    return {str(row[0]) for row in rows}


def _selected_signals(signal_ids: Sequence[str] | None) -> tuple[str, ...]:
    selected = signal_ids or SUPPORTED_SIGNAL_IDS
    canonical: list[str] = []
    unknown: list[str] = []
    for signal_id in selected:
        resolved = resolve_signal_id(signal_id)
        if resolved not in SUPPORTED_SIGNAL_IDS:
            unknown.append(signal_id)
            continue
        if resolved not in canonical:
            canonical.append(resolved)
    if unknown:
        raise SignalMaterializationError(
            "unsupported signal(s) for lake materialization: " + ", ".join(sorted(unknown))
        )
    if not canonical:
        raise SignalMaterializationError("no signals selected for materialization")
    return tuple(canonical)


def _identity_confidence_sql() -> str:
    return """
        coalesce(
            try_cast(identity_confidence AS DOUBLE),
            CASE
                WHEN identity_quality = 'exact' THEN 1.0
                WHEN identity_quality = 'high' THEN 0.9
                WHEN identity_quality = 'probable' THEN 0.75
                ELSE 0.5
            END
        )
    """


def _hit_select(
    *,
    run_id: str,
    generated_at: str,
    signal_id: str,
    definition: MaterializableSignalDefinition,
    parquet_glob: str,
) -> str:
    source_table = _feature_table(signal_id)
    source_path = str(_feature_table_dir(signal_id))
    feature_columns = _feature_table_columns(parquet_glob)
    severity_sql = (
        "coalesce(nullif(trim(severity), ''), " + _sql_string(definition.severity) + ")"
        if "severity" in feature_columns
        else _sql_string(definition.severity)
    )
    return f"""
        WITH normalized AS (
            SELECT
                {_sql_string(run_id)} AS run_id,
                {_sql_string(signal_id)} AS signal_id,
                {int(definition.version)} AS signal_version,
                coalesce(nullif(trim(entity_id), ''), 'entity:' || coalesce(entity_key, scope_key))
                    AS entity_uid,
                nullif(trim(entity_key), '') AS entity_key,
                nullif(trim(entity_label), '') AS entity_label,
                nullif(trim(scope_key), '') AS scope_key,
                coalesce(nullif(trim(scope_type), ''), {_sql_string(definition.scope_type)})
                    AS scope_type,
                {severity_sql} AS severity,
                coalesce(try_cast(risk_signal AS DOUBLE), 0.0) AS score,
                {_sql_string(definition.title)} AS title,
                {_sql_string(definition.description)} AS description,
                {_sql_bool(definition.public_safe)} AS public_safe,
                {_sql_bool(definition.reviewer_only)} AS reviewer_only,
                {_identity_confidence_sql()} AS identity_confidence,
                nullif(trim(identity_match_type), '') AS identity_match_type,
                nullif(trim(identity_quality), '') AS identity_quality,
                coalesce(evidence_refs, []::VARCHAR[]) AS evidence_refs,
                {_sql_string(generated_at)} AS created_at,
                {_sql_string(generated_at)} AS first_seen_at,
                {_sql_string(generated_at)} AS last_seen_at,
                {_sql_string(source_table)} AS source_feature_table,
                {_sql_string(source_path)} AS source_feature_path,
                concat_ws(
                    '|',
                    {_sql_string(signal_id)},
                    coalesce(entity_key, ''),
                    coalesce(scope_key, ''),
                    coalesce(cast(evidence_refs AS VARCHAR), '')
                ) AS dedup_key
            FROM read_parquet({_sql_string(parquet_glob)})
            WHERE nullif(trim(scope_key), '') IS NOT NULL
        ),
        hashed AS (
            SELECT *, substr(sha256(dedup_key), 1, 32) AS hit_id
            FROM normalized
        ),
        deduped AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY hit_id
                        ORDER BY
                            score DESC NULLS LAST,
                            identity_confidence DESC NULLS LAST,
                            scope_key
                    ) AS hit_rank
                FROM hashed
            )
            WHERE hit_rank = 1
        )
        SELECT
            run_id,
            signal_id,
            signal_version,
            hit_id,
            entity_uid,
            entity_key,
            entity_label,
            scope_key,
            scope_type,
            severity,
            score,
            title,
            description,
            public_safe,
            reviewer_only,
            identity_confidence,
            identity_match_type,
            identity_quality,
            list_count(evidence_refs) AS evidence_count,
            'bundle:' || hit_id AS evidence_bundle_id,
            evidence_refs,
            created_at,
            first_seen_at,
            last_seen_at,
            source_feature_table,
            source_feature_path
        FROM deduped
    """


def _signal_hits_query(
    run_id: str,
    generated_at: str,
    signal_ids: Sequence[str],
) -> str:
    selects: list[str] = []
    for index, signal_id in enumerate(signal_ids):
        definition = get_signal_definition(signal_id)
        if definition is None:
            raise SignalMaterializationError(f"signal not found in registry: {signal_id}")
        signal_sql = _hit_select(
            run_id=run_id,
            generated_at=generated_at,
            signal_id=signal_id,
            definition=definition,
            parquet_glob=_feature_table_glob(signal_id),
        )
        selects.append(
            "SELECT * FROM ("
            + signal_sql
            + f") AS materialized_signal_{index}"
        )
    return " UNION ALL ".join(selects)


def _evidence_bundles_query(signal_hits_sql: str) -> str:
    return f"""
        WITH hits AS ({signal_hits_sql})
        SELECT
            run_id,
            evidence_bundle_id AS bundle_id,
            hit_id,
            signal_id,
            cast(evidence_index AS INTEGER) AS item_index,
            CASE
                WHEN evidence_ref LIKE '%paco%' THEN 'paco_sanctions'
                WHEN signal_id IN (
                    'procurement_short_bidding_window',
                    'procurement_offers_competition_drop',
                    'procurement_cartel_risk_cobidding'
                )
                    AND (evidence_ref LIKE 'http://%' OR evidence_ref LIKE 'https://%')
                    THEN 'secop_ii_processes'
                WHEN signal_id IN (
                    'bpin_dnp_vs_pida27_obras_prioritarias',
                    'pida_full30_meta',
                    'pida5_pida27_pida4_chain'
                )
                    AND (evidence_ref LIKE 'http://%' OR evidence_ref LIKE 'https://%')
                    THEN 'secop_integrado'
                WHEN evidence_ref LIKE 'http://%' OR evidence_ref LIKE 'https://%'
                    THEN 'secop_ii_contracts'
                WHEN strpos(evidence_ref, ':') > 0 THEN split_part(evidence_ref, ':', 1)
                ELSE NULL
            END AS source_id,
            CASE
                WHEN evidence_ref LIKE 'http://%' OR evidence_ref LIKE 'https://%' THEN NULL
                ELSE evidence_ref
            END AS record_id,
            source_feature_path AS parquet_path,
            concat(
                'signal_id=', signal_id,
                ';entity_key=', coalesce(entity_key, ''),
                ';scope_key=', coalesce(scope_key, ''),
                ';evidence_index=', cast(evidence_index AS VARCHAR)
            ) AS row_selector,
            evidence_ref AS label,
            CASE
                WHEN evidence_ref LIKE 'http://%' OR evidence_ref LIKE 'https://%'
                    THEN evidence_ref
                ELSE NULL
            END AS url,
            last_seen_at AS observed_at,
            public_safe,
            identity_match_type,
            identity_quality
        FROM hits, unnest(evidence_refs) WITH ORDINALITY AS evidence(evidence_ref, evidence_index)
        WHERE evidence_ref IS NOT NULL
            AND nullif(trim(evidence_ref), '') IS NOT NULL
    """


def _run_partition_path(name: str, run_id: str) -> Path:
    return lake_root() / "curated" / name / f"run_id={run_id}"


def _replace_partition(
    con: duckdb.DuckDBPyConnection,
    *,
    name: str,
    run_id: str,
    sql: str,
) -> tuple[Path, int]:
    out = _run_partition_path(name, run_id)
    tmp = out.parent / f".inflight-{name}-{run_id}-{uuid.uuid4().hex}"
    tmp.mkdir(parents=True, exist_ok=False)
    part = tmp / "part-00000.parquet"
    try:
        con.execute(
            f"COPY ({sql}) TO {_sql_string(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row = con.execute(
            f"SELECT count(*) FROM read_parquet({_sql_string(str(part))})"
        ).fetchone()
        count = int(row[0]) if row is not None else 0
        if out.exists():
            shutil.rmtree(out)
        tmp.rename(out)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return out, count


def _signal_hit_counts(
    con: duckdb.DuckDBPyConnection,
    signal_hits_path: Path,
) -> dict[str, int]:
    rows = con.execute(
        "SELECT signal_id, count(*) AS hit_count FROM read_parquet(?) GROUP BY signal_id",
        [str(signal_hits_path / "*.parquet")],
    ).fetchall()
    return {str(signal_id): int(hit_count) for signal_id, hit_count in rows}


def _write_manifest(
    *,
    run_id: str,
    generated_at: str,
    signal_results: Sequence[SignalMaterializationSignalResult],
    signal_hits_path: Path,
    evidence_bundles_path: Path,
    evidence_count: int,
) -> Path:
    manifest_dir = meta_path() / "signal_runs"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / f"{run_id}.json"
    payload = {
        "run_id": run_id,
        "generated_at": generated_at,
        "finished_at": datetime.now(tz=UTC).isoformat(),
        "status": "completed",
        "git_sha": _git_sha(),
        "hit_count": sum(result.hit_count for result in signal_results),
        "evidence_count": evidence_count,
        "outputs": {
            "signal_hits": str(signal_hits_path),
            "evidence_bundles": str(evidence_bundles_path),
        },
        "signals": [asdict(result) for result in signal_results],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def materialize_signals(
    signal_ids: Sequence[str] | None = None,
    *,
    run_id: str | None = None,
    allow_empty: bool = False,
) -> SignalMaterializationRun:
    """Materialize supported curated signal feature tables into run parquet."""
    selected = _selected_signals(signal_ids)
    safe_run_id = _clean_run_id(run_id or _new_run_id())
    generated_at = datetime.now(tz=UTC).isoformat()
    con = duckdb.connect(database=":memory:")
    signal_hits_path: Path | None = None
    evidence_bundles_path: Path | None = None
    try:
        signal_hits_sql = _signal_hits_query(safe_run_id, generated_at, selected)
        signal_hits_path, _ = _replace_partition(
            con,
            name="signal_hits",
            run_id=safe_run_id,
            sql=signal_hits_sql,
        )
        hit_counts = _signal_hit_counts(con, signal_hits_path)
        missing_or_empty = [
            signal_id for signal_id in selected if hit_counts.get(signal_id, 0) == 0
        ]
        if missing_or_empty and not allow_empty:
            raise SignalMaterializationError(
                "selected signal(s) produced zero hits: " + ", ".join(missing_or_empty)
            )
        evidence_bundles_path, evidence_count = _replace_partition(
            con,
            name="evidence_bundles",
            run_id=safe_run_id,
            sql=_evidence_bundles_query(signal_hits_sql),
        )
        signal_results = [
            SignalMaterializationSignalResult(
                signal_id=signal_id,
                source_table=_feature_table(signal_id),
                hit_count=hit_counts.get(signal_id, 0),
            )
            for signal_id in selected
        ]
        manifest_path = _write_manifest(
            run_id=safe_run_id,
            generated_at=generated_at,
            signal_results=signal_results,
            signal_hits_path=signal_hits_path,
            evidence_bundles_path=evidence_bundles_path,
            evidence_count=evidence_count,
        )
    except Exception:
        if signal_hits_path is not None:
            shutil.rmtree(signal_hits_path, ignore_errors=True)
        if evidence_bundles_path is not None:
            shutil.rmtree(evidence_bundles_path, ignore_errors=True)
        raise
    finally:
        con.close()

    return SignalMaterializationRun(
        run_id=safe_run_id,
        generated_at=generated_at,
        signal_results=signal_results,
        signal_hits_path=str(signal_hits_path),
        evidence_bundles_path=str(evidence_bundles_path),
        evidence_count=evidence_count,
        manifest_path=str(manifest_path),
    )
