from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING

import duckdb

from coacc_etl.lakehouse import reader
from coacc_etl.lakehouse.paths import lake_root
from coacc_etl.models.anomaly.common import (
    AnomalyModelError,
    clean_run_id,
    curated_partition,
    feature_schema_hash,
    new_run_id,
    now_iso,
    replace_dir,
    sql_string,
    write_json,
)

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class AnomalyFeatureBuildResult:
    run_id: str
    feature_path: str
    rows: int
    manifest_path: str
    built_at: str


def _table_glob(table: str) -> str:
    root = lake_root() / "curated" / f"table={table}"
    if not root.exists() or not any(root.glob("*.parquet")):
        raise AnomalyModelError(f"missing curated table parquet: {root}")
    return str(root / "*.parquet")


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join(sql_string(value) for value in values) + "]"


def _offer_key_sql(prefix: str = "") -> str:
    return f"""
        coalesce(
            nullif(trim({prefix}identificador_de_la_oferta), ''),
            nullif(trim({prefix}referencia_de_la_oferta), ''),
            nullif(
                regexp_replace(
                    coalesce(cast({prefix}nit_del_proveedor AS VARCHAR), ''),
                    '[^0-9]',
                    '',
                    'g'
                ),
                ''
            ),
            nullif(trim({prefix}nombre_proveedor), '')
        )
    """


def _create_offer_counts_view(
    con: duckdb.DuckDBPyConnection,
    *,
    contracts_glob: str,
) -> bool:
    files = [str(path) for path in reader.source_files("secop_offers")]
    if not files:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW anomaly_offer_counts AS
            SELECT
                NULL::VARCHAR AS process_id,
                0::INTEGER AS effective_offer_count
            WHERE false
        """)
        return False

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW anomaly_award_processes AS
        SELECT DISTINCT nullif(trim(process_id), '') AS process_id
        FROM read_parquet({sql_string(contracts_glob)})
        WHERE nullif(trim(process_id), '') IS NOT NULL
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW anomaly_offer_counts AS
        SELECT
            process_id,
            count(DISTINCT offer_key)::INTEGER AS effective_offer_count
        FROM (
            SELECT
                nullif(trim(o.id_del_proceso_de_compra), '') AS process_id,
                {_offer_key_sql("o.")} AS offer_key
            FROM read_parquet(
                {_sql_list(files)},
                union_by_name = true,
                hive_partitioning = true
            ) AS o
            INNER JOIN anomaly_award_processes AS p
                ON p.process_id = nullif(trim(o.id_del_proceso_de_compra), '')
            WHERE nullif(trim(o.id_del_proceso_de_compra), '') IS NOT NULL
                AND {_offer_key_sql("o.")} IS NOT NULL
        )
        GROUP BY process_id
        """
    )
    return True


def _replace_partition_from_sql(
    con: duckdb.DuckDBPyConnection,
    *,
    name: str,
    run_id: str,
    sql: str,
) -> tuple[Path, int]:
    out = curated_partition(name, run_id)
    tmp = out.parent / f".inflight-{name}-{run_id}"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True, exist_ok=False)
    part = tmp / "part-00000.parquet"
    try:
        con.execute(
            f"COPY ({sql}) TO {sql_string(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_string(str(part))})"
        ).fetchone()
        count = int(row[0]) if row is not None else 0
        replace_dir(tmp, out)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return out, count


def _feature_sql(
    *,
    run_id: str,
    built_at: str,
    contracts_glob: str,
    sanctions_glob: str,
) -> str:
    return f"""
        WITH awards AS (
            SELECT
                contract_id,
                contract_reference,
                process_id,
                process_url,
                supplier_entity_id AS entity_uid,
                supplier_document_key,
                supplier_nit_base,
                supplier_name,
                buyer_document_id,
                buyer_name,
                procurement_modality,
                contract_type,
                greatest(coalesce(contract_value, 0.0), 0.0) AS contract_value,
                signing_date,
                source_id,
                ln(1 + greatest(coalesce(contract_value, 0.0), 0.0)) AS log_value,
                coalesce(extract('dow' FROM signing_date), 0)::INTEGER AS signing_dow
            FROM read_parquet({sql_string(contracts_glob)})
            WHERE contract_id IS NOT NULL
                AND supplier_entity_id IS NOT NULL
                AND supplier_document_key IS NOT NULL
        ),
        sanction_keys AS (
            SELECT DISTINCT
                nullif(trim(coalesce(sanction_subject_document_digits, entity_key)), '')
                    AS subject_key,
                try_cast(sanction_date AS DATE) AS sanction_date
            FROM read_parquet({sql_string(sanctions_glob)})
            WHERE nullif(trim(coalesce(sanction_subject_document_digits, entity_key)), '')
                IS NOT NULL
        ),
        profiled AS (
            SELECT
                awards.*,
                avg(log_value) OVER (PARTITION BY buyer_document_id) AS buyer_avg_log_value,
                stddev_pop(log_value) OVER (PARTITION BY buyer_document_id) AS buyer_std_log_value,
                avg(log_value) OVER (PARTITION BY procurement_modality) AS modality_avg_log_value,
                stddev_pop(log_value) OVER (
                    PARTITION BY procurement_modality
                ) AS modality_std_log_value,
                quantile_cont(log_value, 0.05) OVER (
                    PARTITION BY procurement_modality
                ) AS modality_log_p05,
                quantile_cont(log_value, 0.95) OVER (
                    PARTITION BY procurement_modality
                ) AS modality_log_p95,
                count(*) OVER (
                    PARTITION BY buyer_document_id, signing_dow
                ) AS buyer_dow_count,
                count(*) OVER (PARTITION BY buyer_document_id) AS buyer_total_count,
                count(*) OVER (
                    PARTITION BY buyer_document_id
                    ORDER BY coalesce(signing_date, DATE '1900-01-01')
                    RANGE BETWEEN INTERVAL 365 DAYS PRECEDING
                        AND INTERVAL 1 DAY PRECEDING
                ) AS prior_buyer_count,
                count(*) OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY coalesce(signing_date, DATE '1900-01-01')
                    RANGE BETWEEN INTERVAL 365 DAYS PRECEDING
                        AND INTERVAL 1 DAY PRECEDING
                ) AS prior_supplier_count,
                sum(contract_value) OVER (
                    PARTITION BY buyer_document_id
                    ORDER BY coalesce(signing_date, DATE '1900-01-01')
                    RANGE BETWEEN INTERVAL 365 DAYS PRECEDING
                        AND INTERVAL 1 DAY PRECEDING
                ) AS prior_buyer_value,
                sum(contract_value) OVER (
                    PARTITION BY buyer_document_id, supplier_document_key
                    ORDER BY coalesce(signing_date, DATE '1900-01-01')
                    RANGE BETWEEN INTERVAL 365 DAYS PRECEDING
                        AND INTERVAL 1 DAY PRECEDING
                ) AS prior_buyer_supplier_value
            FROM awards
        )
        SELECT
            {sql_string(run_id)} AS run_id,
            {sql_string(built_at)} AS built_at,
            contract_id,
            contract_reference,
            entity_uid,
            supplier_document_key,
            supplier_nit_base,
            supplier_name,
            buyer_document_id,
            buyer_name,
            procurement_modality,
            contract_type,
            contract_value,
            signing_date,
            profiled.process_id,
            process_url,
            source_id,
            log_value,
            coalesce((log_value - buyer_avg_log_value) / nullif(buyer_std_log_value, 0), 0.0)
                AS log_value_z_buyer,
            coalesce(
                (log_value - modality_avg_log_value) / nullif(modality_std_log_value, 0),
                0.0
            ) AS log_value_z_modality,
            coalesce(prior_buyer_supplier_value / nullif(prior_buyer_value, 0), 0.0)
                AS buyer_supplier_concentration,
            coalesce(prior_buyer_count, 0)::INTEGER AS n_prior_contracts_12mo_buyer,
            coalesce(prior_supplier_count, 0)::INTEGER AS n_prior_contracts_12mo_supplier,
            coalesce(contract_value / nullif(prior_buyer_value, 0), 0.0)
                AS share_of_buyer_total_value_12mo,
            1.0 - coalesce(buyer_dow_count / nullif(buyer_total_count, 0), 0.0)
                AS timing_anomaly_score,
            (
                log_value < coalesce(modality_log_p05, log_value)
                OR log_value > coalesce(modality_log_p95, log_value)
            ) AS modality_value_mismatch,
            (
                offer_counts.process_id IS NOT NULL
                AND offer_counts.effective_offer_count <= 1
            ) AS single_bidder,
            EXISTS (
                SELECT 1
                FROM sanction_keys
                WHERE subject_key IN (supplier_document_key, supplier_nit_base)
                    AND (
                        signing_date IS NULL
                        OR sanction_date IS NULL
                        OR sanction_date <= signing_date
                )
            ) AS prior_sanction_supplier,
            CASE
                WHEN coalesce(prior_supplier_count, 0) < 5 THEN 'low'
                ELSE 'standard'
            END AS score_confidence
        FROM profiled
        LEFT JOIN anomaly_offer_counts AS offer_counts
            ON offer_counts.process_id = profiled.process_id
    """


def build_anomaly_features(run_id: str | None = None) -> AnomalyFeatureBuildResult:
    safe_run_id = clean_run_id(run_id or new_run_id())
    built_at = now_iso()
    contracts_glob = _table_glob("fct_procurement_contract_awards")
    sanctions_glob = _table_glob("signal_feature_procurement_sanctioned_supplier_awarded")
    con = duckdb.connect(database=":memory:")
    try:
        offers_available = _create_offer_counts_view(con, contracts_glob=contracts_glob)
        feature_path, rows = _replace_partition_from_sql(
            con,
            name="anomaly_features",
            run_id=safe_run_id,
            sql=_feature_sql(
                run_id=safe_run_id,
                built_at=built_at,
                contracts_glob=contracts_glob,
                sanctions_glob=sanctions_glob,
            ),
        )
    finally:
        con.close()
    if rows == 0:
        raise AnomalyModelError("anomaly feature builder produced zero rows")

    manifest_path = lake_root() / "meta" / "anomaly_features" / f"{safe_run_id}.json"
    write_json(
        manifest_path,
        {
            "run_id": safe_run_id,
            "built_at": built_at,
            "feature_path": str(feature_path),
            "rows": rows,
            "feature_schema_hash": feature_schema_hash(),
            "sources": [
                "fct_procurement_contract_awards",
                "signal_feature_procurement_sanctioned_supplier_awarded",
                *(["secop_offers"] if offers_available else []),
            ],
            "offer_features": {
                "available": offers_available,
                "source": "secop_offers",
            },
        },
    )
    return AnomalyFeatureBuildResult(
        run_id=safe_run_id,
        feature_path=str(feature_path),
        rows=rows,
        manifest_path=str(manifest_path),
        built_at=built_at,
    )
