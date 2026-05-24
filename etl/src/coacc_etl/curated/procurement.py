from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb

from coacc_etl.lakehouse import reader
from coacc_etl.lakehouse.paths import curated_path, meta_path

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

_DEFAULT_TABLES = (
    "dim_subject_document",
    "fct_procurement_contract_awards",
    "signal_feature_procurement_sanctioned_supplier_awarded",
    "signal_feature_procurement_supplier_concentration_across_entities",
    "signal_feature_procurement_repeat_awards_same_supplier",
)
_TABLE_SOURCES = {
    "dim_subject_document": ("secop_ii_contracts", "paco_sanctions"),
    "fct_procurement_contract_awards": ("secop_ii_contracts",),
    "signal_feature_procurement_sanctioned_supplier_awarded": (
        "secop_ii_contracts",
        "paco_sanctions",
    ),
    "signal_feature_procurement_supplier_concentration_across_entities": (
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_repeat_awards_same_supplier": ("secop_ii_contracts",),
}


class CuratedBuildError(RuntimeError):
    """Raised when required lake inputs are missing or unsupported tables are requested."""


@dataclass(frozen=True)
class CuratedTableResult:
    table: str
    rows: int
    path: str


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _required_sources(tables: Sequence[str]) -> tuple[str, ...]:
    sources: list[str] = []
    for table in tables:
        for source in _TABLE_SOURCES[table]:
            if source not in sources:
                sources.append(source)
    return tuple(sources)


def _register_required_sources(
    con: duckdb.DuckDBPyConnection,
    tables: Sequence[str],
) -> tuple[str, ...]:
    required_sources = _required_sources(tables)
    missing = [source for source in required_sources if not reader.source_files(source)]
    if missing:
        raise CuratedBuildError(f"missing required lake source(s): {', '.join(missing)}")
    for source in required_sources:
        reader.register_source(con, source)
    return required_sources


def _install_macros(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE OR REPLACE MACRO coacc_doc_digits(value) AS (
            NULLIF(regexp_replace(coalesce(cast(value AS VARCHAR), ''), '[^0-9]', '', 'g'), '')
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_nit_base(value, doc_type) AS (
            CASE
                WHEN coacc_doc_digits(value) IS NULL THEN NULL
                WHEN upper(coalesce(cast(doc_type AS VARCHAR), '')) LIKE '%NIT%'
                    AND length(coacc_doc_digits(value)) = 10
                    THEN left(coacc_doc_digits(value), 9)
                ELSE NULL
            END
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_document_key(value, doc_type) AS (
            coalesce(coacc_nit_base(value, doc_type), coacc_doc_digits(value))
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_money(value) AS (
            try_cast(
                NULLIF(regexp_replace(coalesce(cast(value AS VARCHAR), ''), '[^0-9]', '', 'g'), '')
                AS DOUBLE
            )
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_reference_url(value) AS (
            CASE
                WHEN value IS NULL THEN NULL
                WHEN regexp_matches(cast(value AS VARCHAR), '''url'': ''https?://[^'']+''')
                    THEN regexp_extract(cast(value AS VARCHAR), '''url'': ''(https?://[^'']+)''', 1)
                WHEN regexp_matches(cast(value AS VARCHAR), 'https?://[^[:space:]''}]+')
                    THEN regexp_extract(cast(value AS VARCHAR), '(https?://[^[:space:]''}]+)', 1)
                ELSE nullif(trim(cast(value AS VARCHAR)), '')
            END
        )
    """)


def _create_views(con: duckdb.DuckDBPyConnection, required_sources: Sequence[str]) -> None:
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_contract_awards AS
        SELECT
            row_number() OVER () AS award_row_id,
            nullif(trim(contract_id), '') AS contract_id,
            nullif(trim(contract_reference), '') AS contract_reference,
            nullif(trim(procurement_process), '') AS process_id,
            coacc_reference_url(process_url) AS process_url,
            coacc_doc_digits(supplier_document) AS supplier_document_digits,
            coacc_nit_base(supplier_document, supplier_doc_type) AS supplier_nit_base,
            coacc_document_key(supplier_document, supplier_doc_type) AS supplier_document_key,
            'doc:' || coacc_document_key(supplier_document, supplier_doc_type)
                AS supplier_entity_id,
            nullif(trim(awarded_supplier), '') AS supplier_name,
            nullif(trim(supplier_doc_type), '') AS supplier_doc_type,
            nullif(trim(entity_nit), '') AS buyer_document_id,
            nullif(trim(entity_name), '') AS buyer_name,
            nullif(trim(department), '') AS department,
            nullif(trim(city), '') AS city,
            nullif(trim(sector), '') AS sector,
            nullif(trim(procurement_modality), '') AS procurement_modality,
            nullif(trim(contract_type), '') AS contract_type,
            coacc_money(contract_value) AS contract_value,
            try_cast(signing_date AS DATE) AS signing_date,
            try_cast(contract_start_date AS DATE) AS contract_start_date,
            try_cast(contract_end_date AS DATE) AS contract_end_date,
            try_cast(last_update AS TIMESTAMP) AS last_update_at,
            'secop_ii_contracts' AS source_id
        FROM src_secop_ii_contracts
        WHERE coacc_document_key(supplier_document, supplier_doc_type) IS NOT NULL
            AND nullif(trim(contract_id), '') IS NOT NULL
    """)
    include_paco = "paco_sanctions" in set(required_sources)
    if include_paco:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_paco_subjects AS
            WITH raw AS (
                SELECT row_number() OVER () AS paco_row_id, *
                FROM src_paco_sanctions
            )
            SELECT
                paco_row_id,
                coalesce(
                    nullif(trim(raw_paco_record_id), ''),
                    nullif(trim(source_id), ''),
                    'paco:' || cast(paco_row_id AS VARCHAR)
                ) AS paco_record_id,
                nullif(trim(paco_feed), '') AS paco_feed,
                nullif(trim(source_url), '') AS source_url,
                coacc_doc_digits(subject_document_id) AS subject_document_digits,
                coacc_document_key(subject_document_id, subject_type) AS subject_document_key,
                'doc:' || coacc_document_key(subject_document_id, subject_type)
                    AS subject_entity_id,
                nullif(trim(subject_name), '') AS subject_name,
                nullif(trim(subject_type), '') AS subject_type,
                nullif(trim(sanction_type), '') AS sanction_type,
                try_cast(sanction_date AS DATE) AS sanction_date,
                nullif(trim(reference), '') AS reference,
                nullif(trim(contract_id), '') AS sanction_contract_id,
                coacc_money(amount) AS amount,
                nullif(trim(affected_entity), '') AS affected_entity,
                raw_record_json
            FROM raw
            WHERE coacc_doc_digits(subject_document_id) IS NOT NULL
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_award_match_keys AS
            SELECT
                award_row_id,
                supplier_document_digits AS match_key,
                'document_exact' AS join_rule,
                1 AS match_rank
            FROM curated_contract_awards
            WHERE supplier_document_digits IS NOT NULL
            UNION ALL
            SELECT
                award_row_id,
                supplier_nit_base AS match_key,
                'nit_base_to_subject' AS join_rule,
                2 AS match_rank
            FROM curated_contract_awards
            WHERE supplier_nit_base IS NOT NULL
                AND supplier_nit_base != supplier_document_digits
            UNION ALL
            SELECT
                award_row_id,
                supplier_document_key AS match_key,
                'document_key' AS join_rule,
                3 AS match_rank
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND supplier_document_key NOT IN (
                    coalesce(supplier_document_digits, ''),
                    coalesce(supplier_nit_base, '')
                )
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_paco_match_keys AS
            SELECT
                paco_row_id,
                subject_document_digits AS match_key
            FROM curated_paco_subjects
            WHERE subject_document_digits IS NOT NULL
            UNION ALL
            SELECT
                paco_row_id,
                subject_document_key AS match_key
            FROM curated_paco_subjects
            WHERE subject_document_key IS NOT NULL
                AND subject_document_key != subject_document_digits
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_paco_subjects AS
            SELECT
                NULL::BIGINT AS paco_row_id,
                NULL::VARCHAR AS paco_record_id,
                NULL::VARCHAR AS paco_feed,
                NULL::VARCHAR AS source_url,
                NULL::VARCHAR AS subject_document_digits,
                NULL::VARCHAR AS subject_document_key,
                NULL::VARCHAR AS subject_entity_id,
                NULL::VARCHAR AS subject_name,
                NULL::VARCHAR AS subject_type,
                NULL::VARCHAR AS sanction_type,
                NULL::DATE AS sanction_date,
                NULL::VARCHAR AS reference,
                NULL::VARCHAR AS sanction_contract_id,
                NULL::DOUBLE AS amount,
                NULL::VARCHAR AS affected_entity,
                NULL::VARCHAR AS raw_record_json
            WHERE false
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_award_match_keys AS
            SELECT
                NULL::BIGINT AS award_row_id,
                NULL::VARCHAR AS match_key,
                NULL::VARCHAR AS join_rule,
                NULL::INTEGER AS match_rank
            WHERE false
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_paco_match_keys AS
            SELECT
                NULL::BIGINT AS paco_row_id,
                NULL::VARCHAR AS match_key
            WHERE false
        """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_subject_documents AS
        WITH unioned AS (
            SELECT
                supplier_document_key AS document_key,
                supplier_document_digits AS document_digits,
                supplier_nit_base AS nit_base,
                supplier_doc_type AS document_type,
                supplier_name AS display_name,
                'secop_ii_contracts' AS source_id
            FROM curated_contract_awards
            UNION ALL
            SELECT
                subject_document_key AS document_key,
                subject_document_digits AS document_digits,
                NULL AS nit_base,
                subject_type AS document_type,
                subject_name AS display_name,
                'paco_sanctions' AS source_id
            FROM curated_paco_subjects
        )
        SELECT
            'doc:' || document_key AS entity_id,
            document_key,
            max(document_digits) AS sample_document_digits,
            max(nit_base) AS nit_base,
            max(display_name) AS display_name,
            max(document_type) AS document_type,
            string_agg(DISTINCT source_id, ',') AS source_ids,
            count(*) AS source_row_count
        FROM unioned
        WHERE document_key IS NOT NULL
        GROUP BY document_key
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_sanctioned_awards AS
        SELECT
            'procurement_sanctioned_supplier_awarded' AS signal_id,
            a.supplier_entity_id AS entity_id,
            a.supplier_document_key AS entity_key,
            'Company' AS entity_label,
            concat_ws(
                ':',
                a.contract_id,
                coalesce(p.paco_feed, 'paco'),
                coalesce(p.reference, p.paco_record_id, p.subject_document_digits)
            ) AS scope_key,
            'sanction_record' AS scope_type,
            CASE
                WHEN p.sanction_date IS NOT NULL
                    AND a.signing_date IS NOT NULL
                    AND a.signing_date >= p.sanction_date THEN 1.0
                ELSE 0.75
            END AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            a.contract_id,
            a.contract_reference,
            a.process_id,
            a.process_url,
            a.supplier_name,
            a.supplier_doc_type,
            a.buyer_document_id,
            a.buyer_name,
            a.department,
            a.city,
            a.sector,
            a.procurement_modality,
            a.contract_type,
            a.contract_value,
            a.signing_date,
            p.paco_record_id,
            p.paco_feed,
            p.source_url AS paco_source_url,
            p.subject_document_digits AS sanction_subject_document_digits,
            p.subject_name AS sanction_subject_name,
            p.subject_type AS sanction_subject_type,
            p.sanction_type,
            p.sanction_date,
            p.reference AS sanction_reference,
            p.sanction_contract_id,
            p.amount AS sanction_amount,
            p.affected_entity,
            ak.join_rule,
            [
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id),
                coalesce(p.source_url, 'paco_sanctions:' || p.paco_record_id)
            ] AS evidence_refs
        FROM curated_award_match_keys ak
        JOIN curated_paco_match_keys pk
            ON ak.match_key = pk.match_key
        JOIN curated_contract_awards a
            ON ak.award_row_id = a.award_row_id
        JOIN curated_paco_subjects p
            ON pk.paco_row_id = p.paco_row_id
        QUALIFY row_number() OVER (
            PARTITION BY a.award_row_id, p.paco_row_id
            ORDER BY ak.match_rank
        ) = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_supplier_concentration AS
        WITH eligible AS (
            SELECT
                supplier_entity_id,
                supplier_document_key,
                supplier_name,
                buyer_document_id,
                buyer_name,
                department,
                contract_id,
                process_url,
                contract_value,
                signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND buyer_document_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        ranked_evidence AS (
            SELECT
                supplier_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id) AS evidence_ref,
                contract_value,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM eligible
            WHERE contract_id IS NOT NULL
        ),
        evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref) AS evidence_refs
            FROM ranked_evidence
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key
        ),
        buyer_names AS (
            SELECT
                supplier_document_key,
                list(buyer_name ORDER BY buyer_name) AS buyer_sample
            FROM (
                SELECT
                    supplier_document_key,
                    buyer_name,
                    row_number() OVER (
                        PARTITION BY supplier_document_key
                        ORDER BY buyer_name
                    ) AS buyer_rank
                FROM (
                    SELECT DISTINCT supplier_document_key, buyer_name
                    FROM eligible
                    WHERE buyer_name IS NOT NULL
                )
            )
            WHERE buyer_rank <= 10
            GROUP BY supplier_document_key
        ),
        supplier_rollup AS (
            SELECT
                supplier_entity_id AS entity_id,
                supplier_document_key AS entity_key,
                any_value(supplier_name) AS supplier_name,
                count(*) AS contract_count,
                count(DISTINCT buyer_document_id) AS distinct_buyer_count,
                count(DISTINCT department) AS distinct_department_count,
                sum(contract_value) AS total_contract_value,
                avg(contract_value) AS avg_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM eligible
            GROUP BY supplier_entity_id, supplier_document_key
        )
        SELECT
            'procurement_supplier_concentration_across_entities' AS signal_id,
            r.entity_id,
            r.entity_key,
            'Company' AS entity_label,
            'supplier:' || r.entity_key AS scope_key,
            'supplier' AS scope_type,
            least(
                1.0,
                0.35
                    + least(r.distinct_buyer_count / 150.0, 0.35)
                    + least(log10(greatest(r.total_contract_value, 1)) / 50.0, 0.30)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            r.supplier_name,
            r.contract_count,
            r.distinct_buyer_count,
            r.distinct_department_count,
            r.total_contract_value,
            r.avg_contract_value,
            r.first_signing_date,
            r.last_signing_date,
            b.buyer_sample,
            e.evidence_refs
        FROM supplier_rollup r
        JOIN evidence e
            ON e.supplier_document_key = r.entity_key
        LEFT JOIN buyer_names b
            ON b.supplier_document_key = r.entity_key
        WHERE r.contract_count >= 25
            AND r.distinct_buyer_count >= 50
            AND r.total_contract_value >= 1000000000
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_repeat_awards_same_supplier AS
        WITH eligible AS (
            SELECT
                supplier_entity_id,
                supplier_document_key,
                supplier_name,
                buyer_document_id,
                buyer_name,
                department,
                contract_id,
                process_url,
                contract_value,
                signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND buyer_document_id IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        ranked_evidence AS (
            SELECT
                supplier_document_key,
                buyer_document_id,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id) AS evidence_ref,
                contract_value,
                row_number() OVER (
                    PARTITION BY supplier_document_key, buyer_document_id
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM eligible
        ),
        evidence AS (
            SELECT
                supplier_document_key,
                buyer_document_id,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref) AS evidence_refs
            FROM ranked_evidence
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key, buyer_document_id
        ),
        pair_rollup AS (
            SELECT
                supplier_entity_id AS entity_id,
                supplier_document_key AS entity_key,
                buyer_document_id,
                any_value(supplier_name) AS supplier_name,
                any_value(buyer_name) AS buyer_name,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT department) AS distinct_department_count,
                sum(contract_value) AS total_contract_value,
                avg(contract_value) AS avg_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM eligible
            GROUP BY supplier_entity_id, supplier_document_key, buyer_document_id
        )
        SELECT
            'procurement_repeat_awards_same_supplier' AS signal_id,
            r.entity_id,
            r.entity_key,
            'Company' AS entity_label,
            'buyer:' || r.buyer_document_id AS scope_key,
            'buyer' AS scope_type,
            least(
                1.0,
                0.40
                    + least(r.contract_count / 100.0, 0.35)
                    + least(log10(greatest(r.total_contract_value, 1)) / 50.0, 0.25)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            r.supplier_name,
            r.buyer_document_id,
            r.buyer_name,
            r.contract_count,
            r.distinct_department_count,
            r.total_contract_value,
            r.avg_contract_value,
            r.first_signing_date,
            r.last_signing_date,
            e.evidence_refs
        FROM pair_rollup r
        JOIN evidence e
            ON e.supplier_document_key = r.entity_key
            AND e.buyer_document_id = r.buyer_document_id
        WHERE r.contract_count >= 10
            AND r.total_contract_value >= 1000000000
    """)


def _table_sql(table: str) -> str:
    if table == "dim_subject_document":
        return "SELECT * FROM curated_subject_documents"
    if table == "fct_procurement_contract_awards":
        return "SELECT * FROM curated_contract_awards"
    if table == "signal_feature_procurement_sanctioned_supplier_awarded":
        return "SELECT * FROM curated_sanctioned_awards"
    if table == "signal_feature_procurement_supplier_concentration_across_entities":
        return "SELECT * FROM curated_supplier_concentration"
    if table == "signal_feature_procurement_repeat_awards_same_supplier":
        return "SELECT * FROM curated_repeat_awards_same_supplier"
    raise CuratedBuildError(f"unknown curated table: {table}")


def _replace_curated_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    sql: str,
) -> CuratedTableResult:
    out = curated_path(table)
    tmp = out.parent / f".inflight-{table}-{uuid.uuid4().hex}"
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
        if row is None:
            raise CuratedBuildError(f"could not count rows for curated table: {table}")
        rows = int(row[0])
        if out.exists():
            shutil.rmtree(out)
        tmp.rename(out)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return CuratedTableResult(table=table, rows=rows, path=str(out))


def _write_manifest(results: Sequence[CuratedTableResult]) -> Path:
    manifest_dir = meta_path() / "curated"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / f"{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "tables": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def build_curated(tables: Sequence[str] | None = None) -> list[CuratedTableResult]:
    selected = tuple(tables or _DEFAULT_TABLES)
    unknown = sorted(set(selected) - set(_DEFAULT_TABLES))
    if unknown:
        raise CuratedBuildError(f"unknown curated table(s): {', '.join(unknown)}")

    con = duckdb.connect(database=":memory:")
    try:
        required_sources = _register_required_sources(con, selected)
        _install_macros(con)
        _create_views(con, required_sources)
        results = [_replace_curated_table(con, table, _table_sql(table)) for table in selected]
    finally:
        con.close()
    _write_manifest(results)
    return results
