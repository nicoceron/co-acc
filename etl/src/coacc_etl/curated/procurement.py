from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import duckdb

from coacc_etl.curated.canonical_cedula import canonicalize_cedula
from coacc_etl.curated.canonical_nit import canonicalize_nit
from coacc_etl.lakehouse import reader
from coacc_etl.lakehouse.paths import curated_path, meta_path

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

_DEFAULT_TABLES = (
    "dim_subject_document",
    "dim_company",
    "dim_buyer",
    "dim_person",
    "fct_procurement_contract_awards",
    "signal_feature_procurement_single_bidder_high_value",
    "signal_feature_procurement_large_modifications",
    "signal_feature_procurement_sanctioned_supplier_awarded",
    "signal_feature_procurement_supplier_concentration_across_entities",
    "signal_feature_procurement_contract_value_outlier_by_category",
    "signal_feature_procurement_repeat_awards_same_supplier",
    "signal_feature_procurement_buyer_supplier_network_density",
    "signal_feature_procurement_cartel_risk_cobidding",
    "signal_feature_procurement_payment_plan_anomalies",
    "signal_feature_procurement_contract_suspensions",
    "signal_feature_procurement_contract_execution_delay",
    "signal_feature_procurement_short_bidding_window",
    "signal_feature_procurement_offers_competition_drop",
    "signal_feature_procurement_public_servant_conflict_disclosure_overlap",
    "signal_feature_cuentas_claras_donor_supplier_overlap",
    "signal_feature_procurement_politically_exposed_position_supplier_overlap",
    "signal_feature_procurement_related_companies_shared_officer",
    "signal_feature_procurement_cross_source_identity_inconsistency",
)
_TABLE_SOURCES = {
    "dim_subject_document": ("secop_ii_contracts", "paco_sanctions"),
    "dim_company": ("secop_ii_contracts", "paco_sanctions"),
    "dim_buyer": ("secop_ii_contracts",),
    "dim_person": ("5u9e-g5w9", "8tz7-h3eu"),
    "fct_procurement_contract_awards": ("secop_ii_contracts",),
    "signal_feature_procurement_single_bidder_high_value": ("secop_ii_contracts",),
    "signal_feature_procurement_large_modifications": (
        "secop_ii_contracts",
        "secop_contract_modifications",
    ),
    "signal_feature_procurement_sanctioned_supplier_awarded": (
        "secop_ii_contracts",
        "paco_sanctions",
    ),
    "signal_feature_procurement_supplier_concentration_across_entities": (
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_contract_value_outlier_by_category": (
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_repeat_awards_same_supplier": ("secop_ii_contracts",),
    "signal_feature_procurement_buyer_supplier_network_density": ("secop_ii_contracts",),
    "signal_feature_procurement_cartel_risk_cobidding": (
        "secop_offers",
        "secop_ii_processes",
    ),
    "signal_feature_procurement_payment_plan_anomalies": ("secop_ii_contracts",),
    "signal_feature_procurement_contract_suspensions": (
        "secop_contract_suspensions",
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_contract_execution_delay": (
        "secop_contract_execution",
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_short_bidding_window": ("secop_ii_processes",),
    "signal_feature_procurement_offers_competition_drop": ("secop_ii_processes",),
    "signal_feature_procurement_public_servant_conflict_disclosure_overlap": (
        "conflict_disclosures",
        "secop_ii_contracts",
    ),
    "signal_feature_cuentas_claras_donor_supplier_overlap": (
        "secop_ii_contracts",
        "cuentas_claras_income_2019",
    ),
    "signal_feature_procurement_politically_exposed_position_supplier_overlap": (
        "secop_ii_contracts",
        "company_registry_c82u",
        "sigep_sensitive_positions",
    ),
    "signal_feature_procurement_related_companies_shared_officer": (
        "secop_ii_contracts",
        "company_registry_c82u",
    ),
    "signal_feature_procurement_cross_source_identity_inconsistency": (
        "company_registry_c82u",
        "secop_suppliers",
    ),
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


def _canonicalize_nit_for_duckdb(value: object, doc_type: object) -> str:
    return canonicalize_nit(value, document_type=doc_type) or ""


def _canonicalize_cedula_for_duckdb(value: object, doc_type: object) -> str:
    return canonicalize_cedula(value, document_type=doc_type) or ""


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
    con.create_function(
        "coacc_nit_canonical_raw",
        _canonicalize_nit_for_duckdb,
        [duckdb.sqltypes.VARCHAR, duckdb.sqltypes.VARCHAR],
        duckdb.sqltypes.VARCHAR,
    )
    con.create_function(
        "coacc_cedula_key_raw",
        _canonicalize_cedula_for_duckdb,
        [duckdb.sqltypes.VARCHAR, duckdb.sqltypes.VARCHAR],
        duckdb.sqltypes.VARCHAR,
    )
    con.execute("""
        CREATE OR REPLACE MACRO coacc_nit_canonical(value, doc_type) AS (
            NULLIF(coacc_nit_canonical_raw(value, doc_type), '')
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_cedula_key(value, doc_type) AS (
            NULLIF(coacc_cedula_key_raw(value, doc_type), '')
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


def _create_person_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    include_sigep_sensitive = "5u9e-g5w9" in set(required_sources)
    include_asset_disclosures = "8tz7-h3eu" in set(required_sources)
    person_source_sql: list[str] = []
    if include_sigep_sensitive:
        person_source_sql.append("""
            SELECT
                coacc_cedula_key(funcionario_id, document_type) AS cedula_canonical,
                coacc_doc_digits(funcionario_id) AS raw_document,
                document_type,
                nullif(trim(full_name), '') AS display_name,
                nullif(trim(institution_id), '') AS institution_id,
                nullif(trim(institution_name), '') AS institution_name,
                try_cast(start_date AS DATE) AS observed_date,
                '5u9e-g5w9' AS source_id
            FROM src_5u9e_g5w9
            WHERE coacc_cedula_key(funcionario_id, document_type) IS NOT NULL
        """)
    if include_asset_disclosures:
        person_source_sql.append("""
            SELECT
                coacc_cedula_key(document_id, document_type) AS cedula_canonical,
                coacc_doc_digits(document_id) AS raw_document,
                document_type,
                nullif(trim(concat_ws(
                    ' ',
                    declarant_first_name,
                    declarant_second_name,
                    declarant_first_lastname,
                    declarant_second_lastname
                )), '') AS display_name,
                NULL AS institution_id,
                nullif(trim(entity_name), '') AS institution_name,
                try_cast(publication_date AS DATE) AS observed_date,
                '8tz7-h3eu' AS source_id
            FROM src_8tz7_h3eu
            WHERE coacc_cedula_key(document_id, document_type) IS NOT NULL
        """)
    if person_source_sql:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW curated_person_sources AS "
            + " UNION ALL ".join(person_source_sql)
        )
    else:
        con.execute("""
            CREATE OR REPLACE TEMP VIEW curated_person_sources AS
            SELECT
                NULL::VARCHAR AS cedula_canonical,
                NULL::VARCHAR AS raw_document,
                NULL::VARCHAR AS document_type,
                NULL::VARCHAR AS display_name,
                NULL::VARCHAR AS institution_id,
                NULL::VARCHAR AS institution_name,
                NULL::DATE AS observed_date,
                NULL::VARCHAR AS source_id
            WHERE false
        """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_dim_person AS
        SELECT
            'person:' || cedula_canonical AS entity_uid,
            cedula_canonical,
            NULL::VARCHAR AS nit_canonical,
            list(DISTINCT raw_document ORDER BY raw_document)
                FILTER (WHERE raw_document IS NOT NULL) AS document_variants,
            min(display_name) FILTER (WHERE display_name IS NOT NULL) AS name_canonical,
            list(DISTINCT display_name ORDER BY display_name)
                FILTER (WHERE display_name IS NOT NULL) AS name_variants,
            list(DISTINCT document_type ORDER BY document_type)
                FILTER (WHERE document_type IS NOT NULL) AS document_types,
            min(observed_date) AS first_seen,
            max(observed_date) AS last_seen,
            list(DISTINCT institution_name ORDER BY institution_name)
                FILTER (WHERE institution_name IS NOT NULL) AS institution_names,
            list(DISTINCT source_id ORDER BY source_id) AS sources,
            count(*) AS source_row_count
        FROM curated_person_sources
        WHERE cedula_canonical IS NOT NULL
        GROUP BY cedula_canonical
    """)


def _create_process_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if "secop_ii_processes" not in set(required_sources):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_short_bidding_window AS
        WITH process_rows AS (
            SELECT
                coalesce(
                    nullif(trim(id_del_proceso), ''),
                    nullif(trim(referencia_del_proceso), ''),
                    nullif(trim(id_adjudicacion), '')
                ) AS process_id,
                nullif(trim(referencia_del_proceso), '') AS process_reference,
                nullif(trim(id_adjudicacion), '') AS award_id,
                coacc_reference_url(urlproceso) AS process_url,
                coacc_doc_digits(nit_del_proveedor_adjudicado)
                    AS supplier_document_digits,
                coacc_nit_base(nit_del_proveedor_adjudicado, 'NIT')
                    AS supplier_nit_base,
                coacc_nit_canonical(nit_del_proveedor_adjudicado, 'NIT')
                    AS supplier_nit_canonical,
                coacc_document_key(nit_del_proveedor_adjudicado, 'NIT')
                    AS supplier_document_key,
                'doc:' || coacc_document_key(nit_del_proveedor_adjudicado, 'NIT')
                    AS supplier_entity_id,
                nullif(trim(nombre_del_proveedor), '') AS supplier_name,
                coacc_doc_digits(nit_entidad) AS buyer_document_digits,
                coacc_nit_canonical(nit_entidad, 'NIT') AS buyer_nit_canonical,
                nullif(trim(nit_entidad), '') AS buyer_document_id,
                nullif(trim(entidad), '') AS buyer_name,
                nullif(trim(departamento_entidad), '') AS department,
                nullif(trim(ciudad_entidad), '') AS city,
                nullif(trim(modalidad_de_contratacion), '') AS procurement_modality,
                nullif(trim(tipo_de_contrato), '') AS contract_type,
                nullif(trim(fase), '') AS phase,
                nullif(trim(estado_del_procedimiento), '') AS process_status,
                nullif(trim(estado_resumen), '') AS status_summary,
                nullif(trim(adjudicado), '') AS awarded_flag,
                coacc_money(coalesce(valor_total_adjudicacion, precio_base))
                    AS estimated_value,
                coalesce(
                    try_cast(conteo_de_respuestas_a_ofertas AS INTEGER),
                    try_cast(respuestas_al_procedimiento AS INTEGER),
                    try_cast(respuestas_externas AS INTEGER),
                    try_cast(proveedores_unicos_con AS INTEGER)
                ) AS response_count,
                coalesce(
                    try_cast(proveedores_invitados AS INTEGER),
                    try_cast(proveedores_con_invitacion AS INTEGER)
                ) AS invited_supplier_count,
                coalesce(
                    try_cast(fecha_de_apertura_efectiva AS TIMESTAMP),
                    try_cast(fecha_de_apertura_de_respuesta AS TIMESTAMP)
                ) AS opened_at,
                try_cast(fecha_de_recepcion_de AS TIMESTAMP) AS closed_at
            FROM src_secop_ii_processes
            WHERE coacc_document_key(nit_del_proveedor_adjudicado, 'NIT') IS NOT NULL
        ),
        eligible AS (
            SELECT
                *,
                date_diff('hour', opened_at, closed_at) AS open_window_hours
            FROM process_rows
            WHERE process_id IS NOT NULL
                AND opened_at IS NOT NULL
                AND closed_at IS NOT NULL
                AND estimated_value IS NOT NULL
                AND estimated_value >= 100000000
                AND (
                    lower(coalesce(awarded_flag, '')) IN ('si', 'sí', 'true', '1')
                    OR estimated_value > 0
                )
        )
        SELECT
            'procurement_short_bidding_window' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            process_id AS scope_key,
            'procurement_process' AS scope_type,
            'low' AS severity,
            least(
                1.0,
                0.45
                    + least((72.0 - greatest(open_window_hours, 0)) / 240.0, 0.30)
                    + CASE
                        WHEN coalesce(response_count, 99) <= 1 THEN 0.15
                        WHEN coalesce(response_count, 99) = 2 THEN 0.08
                        ELSE 0.0
                    END
                    + least(log10(greatest(estimated_value, 1)) / 90.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            buyer_document_id,
            buyer_name,
            department,
            city,
            procurement_modality,
            contract_type,
            phase,
            process_status,
            status_summary,
            awarded_flag,
            estimated_value,
            response_count,
            invited_supplier_count,
            opened_at,
            closed_at,
            open_window_hours,
            process_reference,
            award_id,
            process_url,
            [
                coalesce(process_url, 'secop_ii_processes:' || process_id)
            ] AS evidence_refs
        FROM eligible
        WHERE open_window_hours BETWEEN 0 AND 72
            AND coalesce(response_count, 99) <= 2
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_offers_competition_drop AS
        WITH process_rows AS (
            SELECT
                coalesce(
                    nullif(trim(id_del_proceso), ''),
                    nullif(trim(referencia_del_proceso), ''),
                    nullif(trim(id_adjudicacion), '')
                ) AS process_id,
                nullif(trim(referencia_del_proceso), '') AS process_reference,
                coacc_reference_url(urlproceso) AS process_url,
                coacc_document_key(nit_entidad, 'NIT') AS buyer_document_key,
                coacc_doc_digits(nit_entidad) AS buyer_document_digits,
                coacc_nit_canonical(nit_entidad, 'NIT') AS buyer_nit_canonical,
                nullif(trim(nit_entidad), '') AS buyer_document_id,
                nullif(trim(entidad), '') AS buyer_name,
                nullif(trim(departamento_entidad), '') AS department,
                nullif(trim(ciudad_entidad), '') AS city,
                nullif(trim(modalidad_de_contratacion), '') AS procurement_modality,
                nullif(trim(tipo_de_contrato), '') AS contract_type,
                nullif(trim(estado_del_procedimiento), '') AS process_status,
                nullif(trim(estado_resumen), '') AS status_summary,
                coacc_money(coalesce(valor_total_adjudicacion, precio_base))
                    AS estimated_value,
                coalesce(
                    try_cast(conteo_de_respuestas_a_ofertas AS INTEGER),
                    try_cast(respuestas_al_procedimiento AS INTEGER),
                    try_cast(respuestas_externas AS INTEGER),
                    try_cast(proveedores_unicos_con AS INTEGER)
                ) AS response_count,
                coalesce(
                    try_cast(fecha_de_publicacion_del AS TIMESTAMP),
                    try_cast(fecha_de_publicacion_fase_3 AS TIMESTAMP),
                    try_cast(fecha_de_ultima_publicaci AS TIMESTAMP),
                    try_cast(fecha_de_apertura_efectiva AS TIMESTAMP),
                    try_cast(fecha_de_apertura_de_respuesta AS TIMESTAMP),
                    try_cast(fecha_de_recepcion_de AS TIMESTAMP)
                ) AS process_at
            FROM src_secop_ii_processes
            WHERE coacc_document_key(nit_entidad, 'NIT') IS NOT NULL
        ),
        eligible AS (
            SELECT *
            FROM process_rows
            WHERE process_id IS NOT NULL
                AND process_at IS NOT NULL
                AND response_count IS NOT NULL
                AND estimated_value IS NOT NULL
                AND estimated_value >= 10000000
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY buyer_document_key
                    ORDER BY process_at DESC NULLS LAST, process_id DESC
                ) AS recency_rank
            FROM eligible
        ),
        evidence_ranked AS (
            SELECT
                buyer_document_key,
                coalesce(process_url, 'secop_ii_processes:' || process_id)
                    AS evidence_ref,
                process_at,
                estimated_value,
                row_number() OVER (
                    PARTITION BY buyer_document_key
                    ORDER BY process_at DESC NULLS LAST, estimated_value DESC NULLS LAST,
                        process_id DESC
                ) AS evidence_rank
            FROM ranked
            WHERE recency_rank <= 20
                AND response_count <= 2
        ),
        evidence AS (
            SELECT
                buyer_document_key,
                list(evidence_ref ORDER BY process_at DESC, estimated_value DESC)
                    AS evidence_refs
            FROM evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY buyer_document_key
        ),
        rollup AS (
            SELECT
                buyer_document_key AS entity_key,
                'doc:' || buyer_document_key AS entity_id,
                any_value(buyer_document_id) AS buyer_document_id,
                any_value(buyer_name) AS buyer_name,
                any_value(department) AS department,
                any_value(city) AS city,
                count(*) AS process_count,
                count(*) FILTER (WHERE recency_rank <= 20) AS recent_process_count,
                count(*) FILTER (WHERE recency_rank > 20) AS prior_process_count,
                avg(response_count) FILTER (WHERE recency_rank <= 20)
                    AS recent_avg_response_count,
                avg(response_count) FILTER (WHERE recency_rank > 20)
                    AS prior_avg_response_count,
                avg(CASE WHEN response_count <= 2 THEN 1.0 ELSE 0.0 END)
                    FILTER (WHERE recency_rank <= 20) AS recent_low_response_share,
                avg(CASE WHEN response_count <= 2 THEN 1.0 ELSE 0.0 END)
                    FILTER (WHERE recency_rank > 20) AS prior_low_response_share,
                sum(estimated_value) FILTER (WHERE recency_rank <= 20)
                    AS recent_total_value,
                min(process_at) AS first_process_at,
                max(process_at) AS last_process_at
            FROM ranked
            GROUP BY buyer_document_key
        )
        SELECT
            'procurement_offers_competition_drop' AS signal_id,
            r.entity_id,
            r.entity_key,
            'Company' AS entity_label,
            'buyer:' || r.entity_key AS scope_key,
            'buyer' AS scope_type,
            least(
                1.0,
                0.50
                    + least(
                        greatest(
                            r.recent_low_response_share - r.prior_low_response_share,
                            0.0
                        ) / 0.50,
                        0.25
                    )
                    + least(
                        greatest(
                            r.prior_avg_response_count - r.recent_avg_response_count,
                            0.0
                        ) / 10.0,
                        0.15
                    )
                    + least(log10(greatest(r.recent_total_value, 1)) / 100.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            r.buyer_document_id,
            r.buyer_name,
            r.department,
            r.city,
            r.process_count,
            r.recent_process_count,
            r.prior_process_count,
            r.recent_avg_response_count,
            r.prior_avg_response_count,
            r.recent_low_response_share,
            r.prior_low_response_share,
            r.recent_total_value,
            r.first_process_at,
            r.last_process_at,
            e.evidence_refs
        FROM rollup r
        JOIN evidence e
            ON e.buyer_document_key = r.entity_key
        WHERE r.process_count >= 25
            AND r.recent_process_count >= 10
            AND r.prior_process_count >= 10
            AND r.recent_low_response_share >= 0.65
            AND r.recent_low_response_share >= r.prior_low_response_share + 0.20
            AND r.recent_avg_response_count <= greatest(
                2.5,
                r.prior_avg_response_count * 0.75
            )
    """)


def _create_cobidding_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_offers", "secop_ii_processes"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_cartel_risk_cobidding AS
        WITH process_metadata AS (
            SELECT
                coalesce(
                    nullif(trim(id_del_portafolio), ''),
                    nullif(trim(id_del_proceso), ''),
                    nullif(trim(referencia_del_proceso), ''),
                    nullif(trim(id_adjudicacion), '')
                ) AS process_key,
                any_value(nullif(trim(referencia_del_proceso), '')) AS process_reference,
                any_value(coacc_reference_url(urlproceso)) AS process_url,
                any_value(coacc_document_key(nit_entidad, 'NIT')) AS buyer_document_key,
                any_value(nullif(trim(nit_entidad), '')) AS buyer_document_id,
                any_value(nullif(trim(entidad), '')) AS buyer_name,
                any_value(nullif(trim(departamento_entidad), '')) AS department,
                any_value(nullif(trim(ciudad_entidad), '')) AS city,
                any_value(nullif(trim(modalidad_de_contratacion), ''))
                    AS procurement_modality,
                max(coalesce(
                    try_cast(fecha_de_publicacion_del AS TIMESTAMP),
                    try_cast(fecha_de_publicacion_fase_3 AS TIMESTAMP),
                    try_cast(fecha_de_ultima_publicaci AS TIMESTAMP),
                    try_cast(fecha_de_apertura_efectiva AS TIMESTAMP),
                    try_cast(fecha_de_apertura_de_respuesta AS TIMESTAMP),
                    try_cast(fecha_de_recepcion_de AS TIMESTAMP)
                )) AS process_at
            FROM src_secop_ii_processes
            WHERE coalesce(
                    nullif(trim(id_del_portafolio), ''),
                    nullif(trim(id_del_proceso), ''),
                    nullif(trim(referencia_del_proceso), ''),
                    nullif(trim(id_adjudicacion), '')
                ) IS NOT NULL
            GROUP BY 1
        ),
        process_supplier AS (
            SELECT
                coalesce(
                    nullif(trim(id_del_proceso_de_compra), ''),
                    nullif(trim(referencia_del_proceso), '')
                ) AS process_key,
                coacc_document_key(nit_del_proveedor, 'NIT') AS supplier_document_key,
                any_value(coacc_nit_canonical(nit_del_proveedor, 'NIT'))
                    AS supplier_nit_canonical,
                any_value(nullif(trim(nit_del_proveedor), '')) AS supplier_document_id,
                any_value(nullif(trim(nombre_proveedor), '')) AS supplier_name,
                any_value(coacc_document_key(nit_entidad_compradora, 'NIT'))
                    AS buyer_document_key,
                any_value(nullif(trim(nit_entidad_compradora), '')) AS buyer_document_id,
                any_value(nullif(trim(entidad_compradora), '')) AS buyer_name,
                any_value(nullif(trim(modalidad), '')) AS procurement_modality,
                max(coacc_money(valor_de_la_oferta)) AS supplier_offer_value,
                min(try_cast(fecha_de_registro AS TIMESTAMP)) AS first_offer_at,
                max(try_cast(fecha_de_registro AS TIMESTAMP)) AS last_offer_at,
                count(DISTINCT coalesce(
                    nullif(trim(identificador_de_la_oferta), ''),
                    nullif(trim(referencia_de_la_oferta), ''),
                    nullif(trim(cast(":id" AS VARCHAR)), '')
                )) AS offer_record_count
            FROM src_secop_offers
            WHERE coacc_nit_canonical(nit_del_proveedor, 'NIT') IS NOT NULL
                AND NOT regexp_matches(
                    coacc_document_key(nit_del_proveedor, 'NIT'),
                    '^0+$'
                )
                AND coalesce(
                    nullif(trim(id_del_proceso_de_compra), ''),
                    nullif(trim(referencia_del_proceso), '')
                ) IS NOT NULL
            GROUP BY 1, 2
        ),
        process_stats AS (
            SELECT
                process_key,
                count(*) AS supplier_count,
                sum(coalesce(supplier_offer_value, 0.0)) AS process_offer_value
            FROM process_supplier
            GROUP BY process_key
        ),
        bounded_process_supplier AS (
            SELECT
                ps.*,
                st.supplier_count,
                st.process_offer_value
            FROM process_supplier ps
            JOIN process_stats st
                ON st.process_key = ps.process_key
            WHERE st.supplier_count BETWEEN 2 AND 8
        ),
        supplier_totals AS (
            SELECT
                supplier_document_key,
                count(DISTINCT process_key) AS bounded_process_count,
                count(DISTINCT buyer_document_key)
                    FILTER (WHERE buyer_document_key IS NOT NULL)
                    AS bounded_buyer_count,
                sum(coalesce(supplier_offer_value, 0.0)) AS total_offer_value
            FROM bounded_process_supplier
            GROUP BY supplier_document_key
        ),
        pair_processes AS (
            SELECT
                a.supplier_document_key AS supplier_a_key,
                b.supplier_document_key AS supplier_b_key,
                a.supplier_name AS supplier_a_name,
                b.supplier_name AS supplier_b_name,
                a.supplier_document_id AS supplier_a_document_id,
                b.supplier_document_id AS supplier_b_document_id,
                a.process_key,
                coalesce(a.buyer_document_key, pm.buyer_document_key) AS buyer_document_key,
                coalesce(a.buyer_document_id, pm.buyer_document_id) AS buyer_document_id,
                coalesce(a.buyer_name, pm.buyer_name) AS buyer_name,
                pm.department,
                pm.city,
                coalesce(a.procurement_modality, pm.procurement_modality)
                    AS procurement_modality,
                a.supplier_count AS process_supplier_count,
                coalesce(a.supplier_offer_value, 0.0)
                    + coalesce(b.supplier_offer_value, 0.0) AS pair_process_offer_value,
                least(a.first_offer_at, b.first_offer_at) AS first_offer_at,
                greatest(a.last_offer_at, b.last_offer_at) AS last_offer_at,
                coalesce(pm.process_at, greatest(a.last_offer_at, b.last_offer_at))
                    AS evidence_at,
                coalesce(pm.process_url, 'secop_offers:' || a.process_key)
                    AS evidence_ref
            FROM bounded_process_supplier a
            JOIN bounded_process_supplier b
                ON b.process_key = a.process_key
                AND b.supplier_document_key > a.supplier_document_key
            LEFT JOIN process_metadata pm
                ON pm.process_key = a.process_key
        ),
        pair_rollup AS (
            SELECT
                supplier_a_key,
                supplier_b_key,
                any_value(supplier_a_name) AS supplier_a_name,
                any_value(supplier_b_name) AS supplier_b_name,
                any_value(supplier_a_document_id) AS supplier_a_document_id,
                any_value(supplier_b_document_id) AS supplier_b_document_id,
                count(DISTINCT process_key) AS shared_process_count,
                count(DISTINCT buyer_document_key)
                    FILTER (WHERE buyer_document_key IS NOT NULL) AS shared_buyer_count,
                avg(process_supplier_count) AS avg_process_supplier_count,
                sum(pair_process_offer_value) AS pair_offer_value,
                min(first_offer_at) AS first_offer_at,
                max(last_offer_at) AS last_offer_at
            FROM pair_processes
            GROUP BY supplier_a_key, supplier_b_key
        ),
        evidence_ranked AS (
            SELECT
                supplier_a_key,
                supplier_b_key,
                evidence_ref,
                evidence_at,
                process_key,
                row_number() OVER (
                    PARTITION BY supplier_a_key, supplier_b_key
                    ORDER BY evidence_at DESC NULLS LAST, process_key DESC
                ) AS evidence_rank
            FROM pair_processes
            WHERE evidence_ref IS NOT NULL
        ),
        evidence AS (
            SELECT
                supplier_a_key,
                supplier_b_key,
                list(evidence_ref ORDER BY evidence_at DESC, process_key DESC)
                    AS evidence_refs
            FROM evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY supplier_a_key, supplier_b_key
        ),
        scored AS (
            SELECT
                p.*,
                sa.bounded_process_count AS supplier_a_process_count,
                sb.bounded_process_count AS supplier_b_process_count,
                sa.bounded_buyer_count AS supplier_a_buyer_count,
                sb.bounded_buyer_count AS supplier_b_buyer_count,
                p.shared_process_count::DOUBLE
                    / greatest(sa.bounded_process_count, sb.bounded_process_count)
                    AS max_side_share,
                p.shared_process_count::DOUBLE
                    / least(sa.bounded_process_count, sb.bounded_process_count)
                    AS min_side_share,
                e.evidence_refs
            FROM pair_rollup p
            JOIN supplier_totals sa
                ON sa.supplier_document_key = p.supplier_a_key
            JOIN supplier_totals sb
                ON sb.supplier_document_key = p.supplier_b_key
            JOIN evidence e
                ON e.supplier_a_key = p.supplier_a_key
                AND e.supplier_b_key = p.supplier_b_key
        ),
        flagged_pairs AS (
            SELECT
                *,
                least(
                    1.0,
                    0.65
                        + least(shared_process_count / 200.0, 0.15)
                        + least(shared_buyer_count / 100.0, 0.10)
                        + least(max_side_share * 0.08, 0.08)
                        + least(min_side_share * 0.02, 0.02)
                ) AS risk_signal,
                'cobid_pair:' || supplier_a_key || ':' || supplier_b_key AS scope_key
            FROM scored
            WHERE shared_process_count >= 20
                AND shared_buyer_count >= 5
                AND max_side_share >= 0.10
                AND min_side_share >= 0.35
        )
        SELECT
            'procurement_cartel_risk_cobidding' AS signal_id,
            'doc:' || supplier_a_key AS entity_id,
            supplier_a_key AS entity_key,
            'Company' AS entity_label,
            scope_key,
            'cobid_cluster' AS scope_type,
            'high' AS severity,
            risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_a_name AS supplier_name,
            supplier_b_key AS counterpart_entity_key,
            supplier_b_name AS counterpart_name,
            shared_process_count,
            shared_buyer_count,
            supplier_a_process_count AS supplier_process_count,
            supplier_b_process_count AS counterpart_process_count,
            supplier_a_buyer_count AS supplier_buyer_count,
            supplier_b_buyer_count AS counterpart_buyer_count,
            max_side_share,
            min_side_share,
            avg_process_supplier_count,
            pair_offer_value,
            first_offer_at,
            last_offer_at,
            evidence_refs
        FROM flagged_pairs
        UNION ALL
        SELECT
            'procurement_cartel_risk_cobidding' AS signal_id,
            'doc:' || supplier_b_key AS entity_id,
            supplier_b_key AS entity_key,
            'Company' AS entity_label,
            scope_key,
            'cobid_cluster' AS scope_type,
            'high' AS severity,
            risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_b_name AS supplier_name,
            supplier_a_key AS counterpart_entity_key,
            supplier_a_name AS counterpart_name,
            shared_process_count,
            shared_buyer_count,
            supplier_b_process_count AS supplier_process_count,
            supplier_a_process_count AS counterpart_process_count,
            supplier_b_buyer_count AS supplier_buyer_count,
            supplier_a_buyer_count AS counterpart_buyer_count,
            max_side_share,
            min_side_share,
            avg_process_supplier_count,
            pair_offer_value,
            first_offer_at,
            last_offer_at,
            evidence_refs
        FROM flagged_pairs
    """)


def _create_company_registry_overlap_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_ii_contracts", "company_registry_c82u"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_related_companies_shared_officer AS
        WITH supplier_exposure AS (
            SELECT
                supplier_document_key AS company_document_key,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT buyer_document_id) AS distinct_buyer_count,
                sum(coalesce(contract_value, 0.0)) AS total_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
            GROUP BY supplier_document_key
        ),
        contract_evidence_ranked AS (
            SELECT
                supplier_document_key AS company_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                company_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY company_document_key
        ),
        raw_company_officers AS (
            SELECT
                coacc_document_key(document_id, identification_class)
                    AS company_document_key,
                coacc_nit_canonical(document_id, identification_class)
                    AS company_nit_canonical,
                nullif(trim(document_id), '') AS company_document_id,
                nullif(trim(business_name), '') AS company_name,
                nullif(trim(matricula), '') AS matricula,
                nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                nullif(trim(matricula_status), '') AS matricula_status,
                coacc_cedula_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) AS representative_document_key,
                nullif(trim(num_identificacion_representante_legal), '')
                    AS representative_document_id,
                nullif(trim(representante_legal), '') AS representative_name,
                nullif(trim(clase_identificacion_rl), '') AS representative_doc_type,
                coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    nullif(trim(matricula), ''),
                    nullif(trim(document_id), '')
                ) AS company_record_id
            FROM src_company_registry_c82u
            WHERE coacc_document_key(document_id, identification_class) IS NOT NULL
                AND coacc_cedula_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) IS NOT NULL
        ),
        company_officers AS (
            SELECT *
            FROM raw_company_officers
            QUALIFY row_number() OVER (
                PARTITION BY company_document_key, representative_document_key
                ORDER BY company_record_id
            ) = 1
        ),
        exposed_companies AS (
            SELECT
                c.company_document_key,
                c.company_nit_canonical,
                c.company_document_id,
                c.company_name,
                c.matricula,
                c.chamber_of_commerce,
                c.matricula_status,
                c.representative_document_key,
                c.representative_document_id,
                c.representative_name,
                c.representative_doc_type,
                c.company_record_id,
                e.supplier_name,
                e.contract_count,
                e.distinct_buyer_count,
                e.total_contract_value,
                e.first_signing_date,
                e.last_signing_date,
                ce.contract_evidence_refs
            FROM company_officers c
            JOIN supplier_exposure e
                ON c.company_document_key = e.company_document_key
            JOIN contract_evidence ce
                ON c.company_document_key = ce.company_document_key
            WHERE length(c.company_document_key) >= 5
                AND NOT regexp_matches(c.company_document_key, '^0+$')
                AND length(c.representative_document_key) >= 5
                AND NOT regexp_matches(c.representative_document_key, '^0+$')
                AND c.company_record_id IS NOT NULL
                AND e.contract_count >= 3
                AND e.total_contract_value >= 100000000
        ),
        officer_clusters AS (
            SELECT
                representative_document_key,
                count(DISTINCT company_document_key) AS linked_company_count,
                sum(contract_count) AS cluster_contract_count,
                sum(distinct_buyer_count) AS cluster_distinct_buyer_count,
                sum(total_contract_value) AS cluster_total_contract_value,
                min(first_signing_date) AS cluster_first_signing_date,
                max(last_signing_date) AS cluster_last_signing_date
            FROM exposed_companies
            GROUP BY representative_document_key
            HAVING count(DISTINCT company_document_key) >= 2
        ),
        registry_evidence_ranked AS (
            SELECT
                representative_document_key,
                'company_registry_c82u:' || company_record_id AS evidence_ref,
                total_contract_value,
                company_document_key,
                row_number() OVER (
                    PARTITION BY representative_document_key
                    ORDER BY total_contract_value DESC NULLS LAST, company_document_key
                ) AS evidence_rank
            FROM exposed_companies
        ),
        registry_evidence AS (
            SELECT
                representative_document_key,
                list(evidence_ref ORDER BY total_contract_value DESC, evidence_ref)
                    AS registry_evidence_refs
            FROM registry_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY representative_document_key
        ),
        company_samples_ranked AS (
            SELECT
                representative_document_key,
                coalesce(company_name, company_document_key) AS company_sample,
                total_contract_value,
                row_number() OVER (
                    PARTITION BY representative_document_key
                    ORDER BY total_contract_value DESC NULLS LAST, company_document_key
                ) AS sample_rank
            FROM exposed_companies
        ),
        company_samples AS (
            SELECT
                representative_document_key,
                list(company_sample ORDER BY total_contract_value DESC, company_sample)
                    AS linked_company_sample
            FROM company_samples_ranked
            WHERE sample_rank <= 10
            GROUP BY representative_document_key
        )
        SELECT
            'procurement_related_companies_shared_officer' AS signal_id,
            'doc:' || e.company_document_key AS entity_id,
            e.company_document_key AS entity_key,
            'Company' AS entity_label,
            'officer_cluster:' || e.representative_document_key AS scope_key,
            'officer_cluster' AS scope_type,
            least(
                1.0,
                0.65
                    + least(c.linked_company_count / 10.0, 0.15)
                    + least(e.contract_count / 100.0, 0.10)
                    + least(log10(greatest(c.cluster_total_contract_value, 1)) / 120.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            e.company_nit_canonical,
            e.company_document_id,
            e.company_name,
            e.matricula,
            e.chamber_of_commerce,
            e.matricula_status,
            e.representative_document_key,
            e.representative_document_id,
            e.representative_name,
            e.representative_doc_type,
            e.supplier_name,
            e.contract_count,
            e.distinct_buyer_count,
            e.total_contract_value,
            e.first_signing_date,
            e.last_signing_date,
            c.linked_company_count,
            c.cluster_contract_count,
            c.cluster_distinct_buyer_count,
            c.cluster_total_contract_value,
            c.cluster_first_signing_date,
            c.cluster_last_signing_date,
            s.linked_company_sample,
            list_concat(r.registry_evidence_refs, e.contract_evidence_refs) AS evidence_refs
        FROM exposed_companies e
        JOIN officer_clusters c
            ON c.representative_document_key = e.representative_document_key
        JOIN registry_evidence r
            ON r.representative_document_key = e.representative_document_key
        LEFT JOIN company_samples s
            ON s.representative_document_key = e.representative_document_key
    """)

    if "sigep_sensitive_positions" not in set(required_sources):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_politically_exposed_supplier_overlap AS
        WITH supplier_exposure AS (
            SELECT
                supplier_document_key AS company_document_key,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT contract_id) AS contract_count,
                sum(coalesce(contract_value, 0.0)) AS total_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
            GROUP BY supplier_document_key
        ),
        contract_evidence_ranked AS (
            SELECT
                supplier_document_key AS company_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                company_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY company_document_key
        ),
        raw_company_officers AS (
            SELECT
                coacc_document_key(document_id, identification_class)
                    AS company_document_key,
                coacc_nit_canonical(document_id, identification_class)
                    AS company_nit_canonical,
                nullif(trim(document_id), '') AS company_document_id,
                nullif(trim(business_name), '') AS company_name,
                nullif(trim(matricula), '') AS matricula,
                nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                nullif(trim(matricula_status), '') AS matricula_status,
                coacc_cedula_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) AS representative_document_key,
                nullif(trim(num_identificacion_representante_legal), '')
                    AS representative_document_id,
                nullif(trim(representante_legal), '') AS representative_name,
                nullif(trim(clase_identificacion_rl), '') AS representative_doc_type,
                coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    nullif(trim(matricula), ''),
                    nullif(trim(document_id), '')
                ) AS company_record_id
            FROM src_company_registry_c82u
            WHERE coacc_document_key(document_id, identification_class) IS NOT NULL
                AND coacc_cedula_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) IS NOT NULL
        ),
        company_officers AS (
            SELECT *
            FROM raw_company_officers
            QUALIFY row_number() OVER (
                PARTITION BY company_document_key, representative_document_key
                ORDER BY company_record_id
            ) = 1
        ),
        sensitive_positions AS (
            SELECT
                coacc_cedula_key(funcionario_id, document_type) AS person_document_key,
                nullif(trim(funcionario_id), '') AS person_document_id,
                nullif(trim(full_name), '') AS person_name,
                nullif(trim(document_type), '') AS person_doc_type,
                nullif(trim(institution_id), '') AS institution_id,
                nullif(trim(institution_name), '') AS institution_name,
                nullif(trim(institution_department), '') AS institution_department,
                nullif(trim(institution_municipality), '') AS institution_municipality,
                nullif(trim(administrative_sector), '') AS administrative_sector,
                nullif(trim(job_hierarchy_level), '') AS job_hierarchy_level,
                nullif(trim(appointment_type), '') AS appointment_type,
                nullif(trim(current_job_title), '') AS current_job_title,
                try_cast(start_date AS TIMESTAMP) AS position_start_at
            FROM src_sigep_sensitive_positions
            WHERE coacc_cedula_key(funcionario_id, document_type) IS NOT NULL
        ),
        matched AS (
            SELECT
                c.company_document_key,
                c.company_nit_canonical,
                c.company_document_id,
                c.company_name,
                c.matricula,
                c.chamber_of_commerce,
                c.matricula_status,
                c.representative_document_key,
                c.representative_document_id,
                c.representative_name,
                c.representative_doc_type,
                c.company_record_id,
                p.person_document_key,
                p.person_document_id,
                p.person_name,
                p.person_doc_type,
                p.institution_id,
                p.institution_name,
                p.institution_department,
                p.institution_municipality,
                p.administrative_sector,
                p.job_hierarchy_level,
                p.appointment_type,
                p.current_job_title,
                p.position_start_at,
                e.supplier_name,
                e.contract_count,
                e.total_contract_value,
                e.first_signing_date,
                e.last_signing_date,
                ce.contract_evidence_refs
            FROM company_officers c
            JOIN sensitive_positions p
                ON c.representative_document_key = p.person_document_key
            JOIN supplier_exposure e
                ON c.company_document_key = e.company_document_key
            JOIN contract_evidence ce
                ON c.company_document_key = ce.company_document_key
            WHERE length(c.company_document_key) >= 5
                AND NOT regexp_matches(c.company_document_key, '^0+$')
        )
        SELECT
            'procurement_politically_exposed_position_supplier_overlap' AS signal_id,
            'doc:' || company_document_key AS entity_id,
            company_document_key AS entity_key,
            'Company' AS entity_label,
            concat_ws(
                ':',
                'sensitive_position',
                representative_document_key,
                coalesce(institution_id, 'unknown'),
                company_document_key
            ) AS scope_key,
            'sensitive_position' AS scope_type,
            least(
                1.0,
                0.70
                    + least(contract_count / 200.0, 0.15)
                    + least(log10(greatest(total_contract_value, 1)) / 120.0, 0.15)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            company_nit_canonical,
            company_document_id,
            company_name,
            matricula,
            chamber_of_commerce,
            matricula_status,
            representative_document_key,
            representative_document_id,
            representative_name,
            representative_doc_type,
            person_name,
            person_doc_type,
            institution_id,
            institution_name,
            institution_department,
            institution_municipality,
            administrative_sector,
            job_hierarchy_level,
            appointment_type,
            current_job_title,
            position_start_at,
            supplier_name,
            contract_count,
            total_contract_value,
            first_signing_date,
            last_signing_date,
            list_concat(
                [
                    'company_registry_c82u:' || company_record_id,
                    'sigep_sensitive_positions:' || representative_document_key
                ],
                contract_evidence_refs
            ) AS evidence_refs
        FROM matched
        WHERE representative_document_key IS NOT NULL
            AND company_record_id IS NOT NULL
    """)


def _create_contract_suspension_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_contract_suspensions", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_contract_suspensions AS
        WITH raw_events AS (
            SELECT DISTINCT
                nullif(trim(id_contrato), '') AS contract_id,
                lower(nullif(trim(tipo), '')) AS event_type_norm,
                nullif(trim(tipo), '') AS event_type,
                coalesce(
                    try_cast(fecha_de_aprobacion AS TIMESTAMP),
                    try_cast(fecha_de_creacion AS TIMESTAMP)
                ) AS event_at,
                try_cast(fecha_de_inicio_del_contrato AS DATE) AS event_start_date,
                try_cast(fecha_de_fin_del_contrato AS DATE) AS event_end_date,
                nullif(trim(proposito_de_la_modificacion), '') AS event_purpose
            FROM src_secop_contract_suspensions
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        event_rollup AS (
            SELECT
                contract_id,
                count(*) FILTER (
                    WHERE event_type_norm LIKE '%suspension%'
                ) AS suspension_event_count,
                count(*) FILTER (
                    WHERE event_type_norm LIKE '%reanudacion%'
                ) AS resumption_event_count,
                count(DISTINCT event_at) FILTER (
                    WHERE event_type_norm LIKE '%suspension%'
                        AND event_at IS NOT NULL
                ) AS distinct_suspension_dates,
                min(event_at) FILTER (
                    WHERE event_type_norm LIKE '%suspension%'
                ) AS first_suspension_at,
                max(event_at) FILTER (
                    WHERE event_type_norm LIKE '%suspension%'
                ) AS last_suspension_at
            FROM raw_events
            GROUP BY contract_id
        ),
        ranked_suspension_events AS (
            SELECT
                contract_id,
                concat_ws(
                    ':',
                    'secop_contract_suspensions',
                    contract_id,
                    coalesce(strftime(event_at, '%Y-%m-%d'), 'unknown')
                ) AS evidence_ref,
                event_at,
                event_purpose,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY event_at DESC NULLS LAST, event_purpose DESC NULLS LAST
                ) AS evidence_rank
            FROM raw_events
            WHERE event_type_norm LIKE '%suspension%'
        ),
        event_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY event_at DESC, evidence_ref)
                    AS suspension_evidence_refs,
                list(event_purpose ORDER BY event_at DESC, event_purpose)
                    FILTER (WHERE event_purpose IS NOT NULL) AS suspension_purpose_sample
            FROM ranked_suspension_events
            WHERE evidence_rank <= 4
            GROUP BY contract_id
        ),
        eligible AS (
            SELECT
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_name,
                a.supplier_doc_type,
                a.buyer_document_id,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.contract_value,
                a.signing_date,
                a.contract_start_date,
                a.contract_end_date,
                nullif(trim(c.contract_status), '') AS contract_status,
                try_cast(c.added_days AS INTEGER) AS added_days,
                r.suspension_event_count,
                r.resumption_event_count,
                r.distinct_suspension_dates,
                r.first_suspension_at,
                r.last_suspension_at,
                date_diff('day', r.first_suspension_at, r.last_suspension_at)
                    AS suspension_span_days,
                e.suspension_purpose_sample,
                list_concat(
                    [
                        coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    ],
                    e.suspension_evidence_refs
                ) AS evidence_refs
            FROM event_rollup r
            JOIN curated_contract_awards a
                ON a.contract_id = r.contract_id
            JOIN src_secop_ii_contracts c
                ON c.contract_id = r.contract_id
            JOIN event_evidence e
                ON e.contract_id = r.contract_id
            WHERE a.supplier_nit_canonical IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
                AND a.contract_value >= 100000000
                AND r.suspension_event_count >= 2
                AND r.distinct_suspension_dates >= 2
                AND a.process_url IS NOT NULL
        )
        SELECT
            'procurement_contract_suspensions' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            contract_id AS scope_key,
            'contract' AS scope_type,
            'medium' AS severity,
            least(
                1.0,
                0.55
                    + least(distinct_suspension_dates / 20.0, 0.25)
                    + least(coalesce(suspension_span_days, 0) / 1000.0, 0.10)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            contract_status,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            added_days,
            suspension_event_count,
            resumption_event_count,
            distinct_suspension_dates,
            first_suspension_at,
            last_suspension_at,
            suspension_span_days,
            suspension_purpose_sample,
            evidence_refs
        FROM eligible
    """)


def _create_contract_execution_delay_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_contract_execution", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_contract_execution_delay AS
        WITH raw_execution AS (
            SELECT
                nullif(trim(identificadorcontrato), '') AS contract_id,
                coalesce(
                    nullif(trim(referencia_de_articulos), ''),
                    nullif(trim(nombreplan), ''),
                    nullif(trim(identificadorcontrato), '') || ':' ||
                        cast(row_number() OVER () AS VARCHAR)
                ) AS execution_item_id,
                nullif(trim(tipoejecucion), '') AS execution_type,
                nullif(trim(nombreplan), '') AS execution_plan_name,
                nullif(trim(referencia_de_articulos), '') AS article_reference,
                nullif(trim(descripci_n), '') AS execution_description,
                nullif(trim(unidad), '') AS unit,
                try_cast(fechadeentregaesperada AS TIMESTAMP) AS expected_delivery_at,
                try_cast(fechadeentregareal AS TIMESTAMP) AS actual_delivery_at,
                try_cast(fechacreacion AS TIMESTAMP) AS observed_at,
                try_cast(
                    replace(
                        regexp_replace(
                            coalesce(cast(porcentajedeavanceesperado AS VARCHAR), ''),
                            '[^0-9,.-]',
                            '',
                            'g'
                        ),
                        ',',
                        '.'
                    )
                    AS DOUBLE
                ) AS expected_progress_pct,
                try_cast(
                    replace(
                        regexp_replace(
                            coalesce(cast(porcentaje_de_avance_real AS VARCHAR), ''),
                            '[^0-9,.-]',
                            '',
                            'g'
                        ),
                        ',',
                        '.'
                    )
                    AS DOUBLE
                ) AS actual_progress_pct,
                try_cast(
                    replace(
                        regexp_replace(
                            coalesce(cast(cantidad_planeada AS VARCHAR), ''),
                            '[^0-9,.-]',
                            '',
                            'g'
                        ),
                        ',',
                        '.'
                    )
                    AS DOUBLE
                ) AS planned_quantity,
                try_cast(
                    replace(
                        regexp_replace(
                            coalesce(cast(cantidadrecibida AS VARCHAR), ''),
                            '[^0-9,.-]',
                            '',
                            'g'
                        ),
                        ',',
                        '.'
                    )
                    AS DOUBLE
                ) AS received_quantity
            FROM src_secop_contract_execution
            WHERE nullif(trim(identificadorcontrato), '') IS NOT NULL
        ),
        scored_execution AS (
            SELECT
                *,
                CASE
                    WHEN expected_delivery_at IS NOT NULL
                        AND actual_delivery_at IS NOT NULL
                        THEN date_diff('day', expected_delivery_at, actual_delivery_at)
                    ELSE NULL
                END AS delivery_delay_days,
                CASE
                    WHEN expected_delivery_at IS NOT NULL
                        AND actual_delivery_at IS NULL
                        AND observed_at IS NOT NULL
                        AND observed_at > expected_delivery_at
                        THEN date_diff('day', expected_delivery_at, observed_at)
                    ELSE NULL
                END AS unresolved_delay_days,
                expected_progress_pct - actual_progress_pct AS progress_gap_pct,
                planned_quantity - received_quantity AS quantity_gap
            FROM raw_execution
        ),
        delayed_items AS (
            SELECT
                *,
                greatest(
                    coalesce(delivery_delay_days, 0),
                    coalesce(unresolved_delay_days, 0)
                ) AS effective_delay_days,
                (
                    coalesce(delivery_delay_days, unresolved_delay_days, 0) >= 30
                    OR (
                        expected_progress_pct >= 50
                        AND coalesce(progress_gap_pct, 0) >= 30
                    )
                    OR (
                        planned_quantity IS NOT NULL
                        AND planned_quantity > 0
                        AND coalesce(received_quantity, 0) / planned_quantity <= 0.50
                        AND expected_delivery_at IS NOT NULL
                        AND observed_at IS NOT NULL
                        AND observed_at > expected_delivery_at
                    )
                ) AS lag_flag
            FROM scored_execution
        ),
        execution_rollup AS (
            SELECT
                contract_id,
                count(*) AS execution_item_count,
                count(*) FILTER (WHERE lag_flag) AS delayed_item_count,
                max(effective_delay_days) AS max_delay_days,
                max(progress_gap_pct) AS max_progress_gap_pct,
                max(quantity_gap) AS max_quantity_gap,
                min(expected_delivery_at) AS first_expected_delivery_at,
                max(expected_delivery_at) AS last_expected_delivery_at,
                max(actual_delivery_at) AS last_actual_delivery_at,
                max(observed_at) AS last_execution_observed_at
            FROM delayed_items
            GROUP BY contract_id
        ),
        execution_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_contract_execution:' || contract_id || ':' || execution_item_id
                    AS evidence_ref,
                effective_delay_days,
                progress_gap_pct,
                execution_item_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY effective_delay_days DESC NULLS LAST,
                        progress_gap_pct DESC NULLS LAST,
                        execution_item_id
                ) AS evidence_rank
            FROM delayed_items
            WHERE lag_flag
        ),
        execution_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY effective_delay_days DESC, progress_gap_pct DESC)
                    AS execution_evidence_refs
            FROM execution_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY contract_id
        ),
        eligible AS (
            SELECT
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_name,
                a.supplier_doc_type,
                a.buyer_document_id,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.contract_value,
                a.signing_date,
                a.contract_start_date,
                a.contract_end_date,
                r.execution_item_count,
                r.delayed_item_count,
                r.max_delay_days,
                r.max_progress_gap_pct,
                r.max_quantity_gap,
                r.first_expected_delivery_at,
                r.last_expected_delivery_at,
                r.last_actual_delivery_at,
                r.last_execution_observed_at,
                list_concat(
                    [
                        coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    ],
                    e.execution_evidence_refs
                ) AS evidence_refs
            FROM execution_rollup r
            JOIN curated_contract_awards a
                ON a.contract_id = r.contract_id
            JOIN execution_evidence e
                ON e.contract_id = r.contract_id
            WHERE a.supplier_nit_canonical IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND r.delayed_item_count > 0
        )
        SELECT
            'procurement_contract_execution_delay' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            contract_id AS scope_key,
            'contract' AS scope_type,
            CASE
                WHEN max_delay_days >= 180
                    OR coalesce(max_progress_gap_pct, 0) >= 70
                    THEN 'medium'
                ELSE 'low'
            END AS severity,
            least(
                1.0,
                0.45
                    + least(coalesce(max_delay_days, 0) / 365.0, 0.25)
                    + least(coalesce(max_progress_gap_pct, 0) / 200.0, 0.15)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.15)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            execution_item_count,
            delayed_item_count,
            max_delay_days,
            max_progress_gap_pct,
            max_quantity_gap,
            first_expected_delivery_at,
            last_expected_delivery_at,
            last_actual_delivery_at,
            last_execution_observed_at,
            evidence_refs
        FROM eligible
        WHERE max_delay_days >= 30
            OR coalesce(max_progress_gap_pct, 0) >= 30
    """)


def _create_supplier_identity_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"company_registry_c82u", "secop_suppliers"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_cross_source_identity_inconsistency AS
        WITH registry_companies AS (
            SELECT
                coacc_document_key(document_id, identification_class) AS company_document_key,
                coacc_nit_canonical(document_id, identification_class)
                    AS company_nit_canonical,
                nullif(trim(document_id), '') AS company_document_id,
                nullif(trim(identification_class), '') AS company_document_type,
                nullif(trim(business_name), '') AS company_name,
                regexp_replace(
                    regexp_replace(
                        lower(coalesce(cast(business_name AS VARCHAR), '')),
                        '[^a-z0-9]+',
                        '',
                        'g'
                    ),
                    '(sociedadporaccionessimplificada|sas|sa|ltda|limitada)$',
                    '',
                    'g'
                ) AS company_name_key,
                nullif(trim(matricula), '') AS matricula,
                nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                nullif(trim(matricula_status), '') AS matricula_status,
                lower(coalesce(cast(matricula_status AS VARCHAR), '')) AS matricula_status_norm,
                coacc_cedula_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) AS registry_representative_document_key,
                nullif(trim(num_identificacion_representante_legal), '')
                    AS registry_representative_document_id,
                nullif(trim(representante_legal), '') AS registry_representative_name,
                nullif(trim(clase_identificacion_rl), '') AS registry_representative_doc_type,
                coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    nullif(trim(matricula), ''),
                    nullif(trim(document_id), '')
                ) AS company_record_id
            FROM src_company_registry_c82u
            WHERE coacc_document_key(document_id, identification_class) IS NOT NULL
        ),
        supplier_registry AS (
            SELECT
                coacc_document_key(nit, 'NIT') AS supplier_document_key,
                coacc_nit_canonical(nit, 'NIT') AS supplier_nit_canonical,
                nullif(trim(nit), '') AS supplier_document_id,
                nullif(trim(codigo), '') AS supplier_code,
                nullif(trim(nombre), '') AS supplier_name,
                regexp_replace(
                    regexp_replace(
                        lower(coalesce(cast(nombre AS VARCHAR), '')),
                        '[^a-z0-9]+',
                        '',
                        'g'
                    ),
                    '(sociedadporaccionessimplificada|sas|sa|ltda|limitada)$',
                    '',
                    'g'
                ) AS supplier_name_key,
                nullif(trim(esta_activa), '') AS supplier_active_raw,
                lower(coalesce(cast(esta_activa AS VARCHAR), '')) AS supplier_active_norm,
                nullif(trim(es_entidad), '') AS supplier_is_entity_raw,
                nullif(trim(es_grupo), '') AS supplier_is_group_raw,
                nullif(trim(espyme), '') AS supplier_is_pyme_raw,
                nullif(trim(tipo_empresa), '') AS supplier_company_type,
                nullif(trim(departamento), '') AS supplier_department,
                nullif(trim(municipio), '') AS supplier_municipality,
                nullif(trim(direccion), '') AS supplier_address,
                nullif(trim(correo), '') AS supplier_email,
                nullif(trim(sitio_web), '') AS supplier_website,
                coacc_cedula_key(
                    n_mero_doc_representante_legal,
                    tipo_doc_representante_legal
                ) AS supplier_representative_document_key,
                nullif(trim(n_mero_doc_representante_legal), '')
                    AS supplier_representative_document_id,
                nullif(trim(nombre_representante_legal), '')
                    AS supplier_representative_name,
                nullif(trim(tipo_doc_representante_legal), '')
                    AS supplier_representative_doc_type
            FROM src_secop_suppliers
            WHERE coacc_document_key(nit, 'NIT') IS NOT NULL
        ),
        joined AS (
            SELECT
                r.*,
                s.supplier_nit_canonical,
                s.supplier_document_id,
                s.supplier_code,
                s.supplier_name,
                s.supplier_name_key,
                s.supplier_active_raw,
                s.supplier_active_norm,
                s.supplier_is_entity_raw,
                s.supplier_is_group_raw,
                s.supplier_is_pyme_raw,
                s.supplier_company_type,
                s.supplier_department,
                s.supplier_municipality,
                s.supplier_address,
                s.supplier_email,
                s.supplier_website,
                s.supplier_representative_document_key,
                s.supplier_representative_document_id,
                s.supplier_representative_name,
                s.supplier_representative_doc_type,
                (
                    length(coalesce(r.company_name_key, '')) >= 8
                    AND length(coalesce(s.supplier_name_key, '')) >= 8
                    AND r.company_name_key != s.supplier_name_key
                    AND strpos(r.company_name_key, s.supplier_name_key) = 0
                    AND strpos(s.supplier_name_key, r.company_name_key) = 0
                ) AS name_mismatch_flag,
                (
                    r.registry_representative_document_key IS NOT NULL
                    AND s.supplier_representative_document_key IS NOT NULL
                    AND r.registry_representative_document_key
                        != s.supplier_representative_document_key
                ) AS representative_document_mismatch_flag,
                (
                    regexp_matches(r.matricula_status_norm, 'cancel|inactiv|suspend')
                    AND s.supplier_active_norm IN ('true', 't', 'si', 'sí', '1', 'activo')
                ) AS status_mismatch_flag
            FROM registry_companies r
            JOIN supplier_registry s
                ON s.supplier_document_key = r.company_document_key
            WHERE length(r.company_document_key) >= 5
                AND NOT regexp_matches(r.company_document_key, '^0+$')
        ),
        scored AS (
            SELECT
                *,
                cast(name_mismatch_flag AS INTEGER)
                    + cast(representative_document_mismatch_flag AS INTEGER)
                    + cast(status_mismatch_flag AS INTEGER) AS mismatch_dimension_count,
                list_filter(
                    [
                        CASE WHEN name_mismatch_flag THEN 'name_mismatch' ELSE NULL END,
                        CASE
                            WHEN representative_document_mismatch_flag
                                THEN 'representative_document_mismatch'
                            ELSE NULL
                        END,
                        CASE WHEN status_mismatch_flag THEN 'status_mismatch' ELSE NULL END
                    ],
                    item -> item IS NOT NULL
                ) AS inconsistency_types
            FROM joined
        )
        SELECT
            'procurement_cross_source_identity_inconsistency' AS signal_id,
            'doc:' || company_document_key AS entity_id,
            company_document_key AS entity_key,
            'Company' AS entity_label,
            'alias_cluster:' || company_document_key AS scope_key,
            'alias_cluster' AS scope_type,
            'low' AS severity,
            least(
                1.0,
                0.42
                    + least(mismatch_dimension_count * 0.16, 0.40)
                    + CASE WHEN status_mismatch_flag THEN 0.08 ELSE 0.0 END
                    + CASE
                        WHEN representative_document_mismatch_flag THEN 0.06
                        ELSE 0.0
                    END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            company_nit_canonical,
            company_document_id,
            company_document_type,
            company_name,
            supplier_nit_canonical,
            supplier_document_id,
            supplier_code,
            supplier_name,
            name_mismatch_flag,
            representative_document_mismatch_flag,
            status_mismatch_flag,
            mismatch_dimension_count,
            inconsistency_types,
            matricula,
            chamber_of_commerce,
            matricula_status,
            supplier_active_raw,
            supplier_is_entity_raw,
            supplier_is_group_raw,
            supplier_is_pyme_raw,
            supplier_company_type,
            supplier_department,
            supplier_municipality,
            supplier_address,
            supplier_email,
            supplier_website,
            registry_representative_document_key,
            registry_representative_document_id,
            registry_representative_name,
            registry_representative_doc_type,
            supplier_representative_document_key,
            supplier_representative_document_id,
            supplier_representative_name,
            supplier_representative_doc_type,
            [
                'company_registry_c82u:' || company_record_id,
                'secop_suppliers:' || coalesce(supplier_code, company_document_key)
            ] AS evidence_refs
        FROM scored
        WHERE mismatch_dimension_count >= 2
            AND company_record_id IS NOT NULL
    """)


def _create_cuentas_claras_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"cuentas_claras_income_2019", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_cuentas_claras_donor_supplier_overlap AS
        WITH supplier_exposure AS (
            SELECT
                supplier_document_key,
                any_value(supplier_entity_id) AS supplier_entity_id,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT contract_id) AS post_2019_contract_count,
                count(DISTINCT buyer_document_id) AS post_2019_buyer_count,
                sum(coalesce(contract_value, 0.0)) AS post_2019_contract_value,
                min(signing_date) AS first_post_2019_signing_date,
                max(signing_date) AS last_post_2019_signing_date
            FROM curated_contract_awards
            WHERE supplier_nit_canonical IS NOT NULL
                AND supplier_document_key IS NOT NULL
                AND NOT regexp_matches(supplier_document_key, '^0+$')
                AND signing_date >= DATE '2020-01-01'
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
            GROUP BY supplier_document_key
        ),
        contract_evidence_ranked AS (
            SELECT
                supplier_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM curated_contract_awards
            WHERE supplier_nit_canonical IS NOT NULL
                AND supplier_document_key IS NOT NULL
                AND signing_date >= DATE '2020-01-01'
                AND contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY supplier_document_key
        ),
        donor_income AS (
            SELECT
                row_number() OVER () AS income_row_id,
                coacc_document_key(income_party_id, tid_name) AS donor_document_key,
                coacc_nit_canonical(income_party_id, tid_name) AS donor_nit_canonical,
                nullif(trim(income_party_id), '') AS donor_document_id,
                nullif(trim(tid_name), '') AS donor_document_type,
                nullif(trim(person_name), '') AS donor_name,
                nullif(trim(candidate_id), '') AS candidate_document_id,
                nullif(trim(candidate_name), '') AS candidate_name,
                nullif(trim(cnd_name), '') AS office_name,
                nullif(trim(class_name), '') AS election_class,
                nullif(trim(department_name), '') AS campaign_department,
                nullif(trim(municipality_name), '') AS campaign_municipality,
                nullif(trim(organization_name), '') AS campaign_organization,
                nullif(trim(party_coalition), '') AS party_coalition,
                nullif(trim(cco_id), '') AS campaign_account_id,
                nullif(trim(tdo_name), '') AS income_type,
                nullif(trim(income_concept), '') AS income_concept,
                nullif(trim(income_act), '') AS income_act,
                nullif(trim(income_voucher), '') AS income_voucher,
                coacc_money(income_amount) AS income_amount,
                try_cast(voucher_date AS DATE) AS voucher_date
            FROM src_cuentas_claras_income_2019
            WHERE coacc_nit_canonical(income_party_id, tid_name) IS NOT NULL
                AND coacc_document_key(income_party_id, tid_name) IS NOT NULL
                AND NOT regexp_matches(
                    coacc_document_key(income_party_id, tid_name),
                    '^0+$'
                )
        ),
        donor_rollup AS (
            SELECT
                donor_document_key,
                any_value(donor_nit_canonical) AS donor_nit_canonical,
                any_value(donor_document_id) AS donor_document_id,
                any_value(donor_document_type) AS donor_document_type,
                any_value(donor_name) AS donor_name,
                coalesce(
                    candidate_document_id,
                    campaign_account_id,
                    campaign_organization,
                    party_coalition,
                    'unknown'
                ) AS campaign_key,
                any_value(candidate_document_id) AS candidate_document_id,
                any_value(candidate_name) AS candidate_name,
                any_value(office_name) AS office_name,
                any_value(election_class) AS election_class,
                any_value(campaign_department) AS campaign_department,
                any_value(campaign_municipality) AS campaign_municipality,
                any_value(campaign_organization) AS campaign_organization,
                any_value(party_coalition) AS party_coalition,
                any_value(campaign_account_id) AS campaign_account_id,
                count(*) AS income_record_count,
                count(DISTINCT income_type) FILTER (WHERE income_type IS NOT NULL)
                    AS income_type_count,
                sum(coalesce(income_amount, 0.0)) AS total_income_amount,
                max(coalesce(income_amount, 0.0)) AS max_income_amount,
                min(voucher_date) FILTER (
                    WHERE voucher_date BETWEEN DATE '2018-01-01' AND DATE '2021-12-31'
                ) AS first_valid_voucher_date,
                max(voucher_date) FILTER (
                    WHERE voucher_date BETWEEN DATE '2018-01-01' AND DATE '2021-12-31'
                ) AS last_valid_voucher_date
            FROM donor_income
            WHERE income_amount IS NOT NULL
                AND income_amount > 0
            GROUP BY
                donor_document_key,
                coalesce(
                    candidate_document_id,
                    campaign_account_id,
                    campaign_organization,
                    party_coalition,
                    'unknown'
                )
        ),
        donor_evidence_ranked AS (
            SELECT
                donor_document_key,
                coalesce(
                    candidate_document_id,
                    campaign_account_id,
                    campaign_organization,
                    party_coalition,
                    'unknown'
                ) AS campaign_key,
                'cuentas_claras_income_2019:' || coalesce(
                    income_voucher,
                    income_act,
                    campaign_account_id,
                    cast(income_row_id AS VARCHAR)
                ) AS evidence_ref,
                income_amount,
                voucher_date,
                row_number() OVER (
                    PARTITION BY donor_document_key,
                        coalesce(
                            candidate_document_id,
                            campaign_account_id,
                            campaign_organization,
                            party_coalition,
                            'unknown'
                        )
                    ORDER BY income_amount DESC NULLS LAST,
                        voucher_date DESC NULLS LAST,
                        income_row_id
                ) AS evidence_rank
            FROM donor_income
            WHERE income_amount IS NOT NULL
                AND income_amount > 0
        ),
        donor_evidence AS (
            SELECT
                donor_document_key,
                campaign_key,
                list(evidence_ref ORDER BY income_amount DESC, voucher_date DESC)
                    AS donor_evidence_refs
            FROM donor_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY donor_document_key, campaign_key
        ),
        eligible AS (
            SELECT
                e.supplier_entity_id,
                e.supplier_document_key,
                e.supplier_name,
                e.post_2019_contract_count,
                e.post_2019_buyer_count,
                e.post_2019_contract_value,
                e.first_post_2019_signing_date,
                e.last_post_2019_signing_date,
                d.donor_nit_canonical,
                d.donor_document_id,
                d.donor_document_type,
                d.donor_name,
                d.campaign_key,
                d.candidate_document_id,
                d.candidate_name,
                d.office_name,
                d.election_class,
                d.campaign_department,
                d.campaign_municipality,
                d.campaign_organization,
                d.party_coalition,
                d.campaign_account_id,
                d.income_record_count,
                d.income_type_count,
                d.total_income_amount,
                d.max_income_amount,
                d.first_valid_voucher_date,
                d.last_valid_voucher_date,
                list_concat(de.donor_evidence_refs, ce.contract_evidence_refs)
                    AS evidence_refs
            FROM donor_rollup d
            JOIN supplier_exposure e
                ON e.supplier_document_key = d.donor_document_key
            JOIN donor_evidence de
                ON de.donor_document_key = d.donor_document_key
                AND de.campaign_key = d.campaign_key
            JOIN contract_evidence ce
                ON ce.supplier_document_key = d.donor_document_key
            WHERE d.total_income_amount >= 1000000
                AND e.post_2019_contract_value >= 100000000
        )
        SELECT
            'cuentas_claras_donor_supplier_overlap' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            'election:2019:' || supplier_document_key || ':' || campaign_key AS scope_key,
            'election' AS scope_type,
            'medium' AS severity,
            least(
                1.0,
                0.50
                    + least(log10(greatest(total_income_amount, 1)) / 120.0, 0.18)
                    + least(log10(greatest(post_2019_contract_value, 1)) / 120.0, 0.18)
                    + least(post_2019_buyer_count / 50.0, 0.14)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            donor_nit_canonical,
            donor_document_id,
            donor_document_type,
            donor_name,
            candidate_document_id,
            candidate_name,
            office_name,
            election_class,
            campaign_department,
            campaign_municipality,
            campaign_organization,
            party_coalition,
            campaign_account_id,
            income_record_count,
            income_type_count,
            total_income_amount,
            max_income_amount,
            first_valid_voucher_date,
            last_valid_voucher_date,
            post_2019_contract_count,
            post_2019_buyer_count,
            post_2019_contract_value,
            first_post_2019_signing_date,
            last_post_2019_signing_date,
            evidence_refs
        FROM eligible
    """)


def _create_public_servant_conflict_disclosure_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"conflict_disclosures", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_public_servant_conflict_disclosure_overlap AS
        WITH normalized_disclosures AS (
            SELECT
                coacc_cedula_key(document_id, document_type) AS person_document_key,
                coacc_doc_digits(document_id) AS person_document_id,
                nullif(trim(document_type), '') AS person_doc_type,
                nullif(trim(form_number), '') AS form_number,
                try_cast(publication_date AS TIMESTAMP) AS publication_at,
                nullif(trim(declaration_status), '') AS declaration_status,
                nullif(trim(declaration_type), '') AS declaration_type,
                nullif(trim(entity_name), '') AS disclosure_entity_name,
                nullif(trim(declarant_role), '') AS declarant_role,
                nullif(trim(concat_ws(
                    ' ',
                    declarant_first_name,
                    declarant_second_name,
                    declarant_first_lastname,
                    declarant_second_lastname
                )), '') AS person_name,
                lower(trim(coalesce(declarant_is_contractor, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS declarant_is_contractor_flag,
                lower(trim(coalesce(direct_interest_actions, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS direct_interest_flag,
                lower(trim(coalesce(conflict_relatives, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS relative_conflict_flag,
                lower(trim(coalesce(conflict_donations, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS donation_conflict_flag,
                lower(trim(coalesce(other_potential_conflicts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS other_conflict_flag,
                lower(trim(coalesce(conflict_trusts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS trust_conflict_flag,
                lower(trim(coalesce(other_conflict_investments, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS investment_conflict_flag
            FROM src_conflict_disclosures
            WHERE coacc_cedula_key(document_id, document_type) IS NOT NULL
                AND nullif(trim(form_number), '') IS NOT NULL
        ),
        scored_disclosures AS (
            SELECT
                *,
                cast(direct_interest_flag AS INTEGER)
                    + cast(relative_conflict_flag AS INTEGER)
                    + cast(donation_conflict_flag AS INTEGER)
                    + cast(other_conflict_flag AS INTEGER)
                    + cast(trust_conflict_flag AS INTEGER)
                    + cast(investment_conflict_flag AS INTEGER)
                    AS conflict_flag_count
            FROM normalized_disclosures
        ),
        selected_disclosures AS (
            SELECT *
            FROM scored_disclosures
            WHERE declarant_is_contractor_flag
                AND conflict_flag_count > 0
            QUALIFY row_number() OVER (
                PARTITION BY person_document_key
                ORDER BY conflict_flag_count DESC,
                    publication_at DESC NULLS LAST,
                    form_number DESC
            ) = 1
        ),
        person_contracts AS (
            SELECT
                coacc_cedula_key(supplier_document_digits, supplier_doc_type)
                    AS person_document_key,
                supplier_document_digits AS supplier_document_id,
                supplier_doc_type,
                supplier_name,
                buyer_document_id,
                buyer_name,
                department,
                city,
                sector,
                procurement_modality,
                contract_type,
                contract_id,
                contract_reference,
                process_id,
                process_url,
                contract_value,
                signing_date,
                contract_start_date,
                contract_end_date,
                last_update_at
            FROM curated_contract_awards
            WHERE coacc_cedula_key(supplier_document_digits, supplier_doc_type)
                    IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        exposure AS (
            SELECT
                person_document_key,
                any_value(supplier_document_id) AS supplier_document_id,
                any_value(supplier_doc_type) AS supplier_doc_type,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT coalesce(buyer_document_id, buyer_name))
                    FILTER (WHERE coalesce(buyer_document_id, buyer_name) IS NOT NULL)
                    AS buyer_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM person_contracts
            GROUP BY person_document_key
        ),
        contract_evidence_ranked AS (
            SELECT
                person_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM person_contracts
            WHERE contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY person_document_key
        ),
        eligible AS (
            SELECT
                d.*,
                e.supplier_document_id,
                e.supplier_doc_type,
                e.supplier_name,
                e.contract_count,
                e.buyer_count,
                e.total_contract_value,
                e.max_contract_value,
                e.first_signing_date,
                e.last_signing_date,
                ce.contract_evidence_refs
            FROM selected_disclosures d
            JOIN exposure e
                ON e.person_document_key = d.person_document_key
            JOIN contract_evidence ce
                ON ce.person_document_key = d.person_document_key
            WHERE e.contract_count >= 3
                AND e.buyer_count >= 2
                AND e.total_contract_value >= 2000000000
        )
        SELECT
            'procurement_public_servant_conflict_disclosure_overlap' AS signal_id,
            'person:' || person_document_key AS entity_id,
            person_document_key AS entity_key,
            'Person' AS entity_label,
            'disclosure:' || form_number || ':' || person_document_key AS scope_key,
            'disclosure' AS scope_type,
            CASE
                WHEN total_contract_value >= 5000000000 OR contract_count >= 10
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.55
                    + least(log10(greatest(total_contract_value, 1)) / 120.0, 0.18)
                    + least(contract_count / 120.0, 0.12)
                    + least(buyer_count / 50.0, 0.10)
                    + least(conflict_flag_count / 20.0, 0.05)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_PERSON_DOCUMENT' AS identity_match_type,
            'exact' AS identity_quality,
            person_document_id,
            person_doc_type,
            person_name,
            form_number,
            publication_at,
            declaration_status,
            declaration_type,
            disclosure_entity_name,
            declarant_role,
            declarant_is_contractor_flag,
            conflict_flag_count,
            direct_interest_flag,
            relative_conflict_flag,
            donation_conflict_flag,
            other_conflict_flag,
            trust_conflict_flag,
            investment_conflict_flag,
            supplier_document_id,
            supplier_doc_type,
            supplier_name,
            contract_count,
            buyer_count,
            total_contract_value,
            max_contract_value,
            first_signing_date,
            last_signing_date,
            list_concat(
                ['conflict_disclosures:' || form_number],
                contract_evidence_refs
            ) AS evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY total_contract_value DESC NULLS LAST,
                contract_count DESC NULLS LAST,
                buyer_count DESC NULLS LAST,
                person_document_key
        ) <= 1000
    """)


def _create_large_modification_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if "secop_contract_modifications" not in set(required_sources):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_large_modifications AS
        WITH raw_modifications AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                coalesce(
                    nullif(trim(identificador_modificacion), ''),
                    nullif(trim(identificador), ''),
                    nullif(trim(identificador_requerimiento), ''),
                    nullif(trim(id_contrato), '') || ':' || cast(row_number() OVER () AS VARCHAR)
                ) AS modification_id,
                coacc_money(valor_modificacion) AS modification_value,
                try_cast(dias_extendidos AS INTEGER) AS extended_days,
                nullif(trim(estado_modificacion), '') AS modification_status,
                nullif(trim(proposito_modificacion), '') AS modification_purpose,
                nullif(trim(descripcion), '') AS modification_description,
                nullif(trim(codigo_bpin), '') AS bpin_code,
                coalesce(
                    try_cast(fecha_de_aprobacion AS TIMESTAMP),
                    try_cast(fecha_version AS TIMESTAMP),
                    try_cast(fecha_de_carga AS TIMESTAMP),
                    try_cast(fecha_creacion AS TIMESTAMP)
                ) AS modification_at
            FROM src_secop_contract_modifications
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        eligible_modifications AS (
            SELECT *
            FROM raw_modifications
            WHERE modification_value IS NOT NULL
                AND modification_value > 0
        ),
        modification_rollup AS (
            SELECT
                contract_id,
                count(*) AS modification_event_count,
                sum(modification_value) AS total_modification_value,
                max(modification_value) AS max_modification_value,
                sum(coalesce(extended_days, 0)) AS total_extended_days,
                max(extended_days) AS max_extended_days,
                min(modification_at) AS first_modification_at,
                max(modification_at) AS last_modification_at
            FROM eligible_modifications
            GROUP BY contract_id
        ),
        modification_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_contract_modifications:' || modification_id AS evidence_ref,
                modification_value,
                modification_at,
                modification_purpose,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY modification_value DESC NULLS LAST,
                        modification_at DESC NULLS LAST,
                        modification_id
                ) AS evidence_rank
            FROM eligible_modifications
        ),
        modification_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY modification_value DESC, modification_at DESC)
                    AS modification_evidence_refs,
                list(modification_purpose ORDER BY modification_value DESC, modification_at DESC)
                    FILTER (WHERE modification_purpose IS NOT NULL)
                    AS modification_purpose_sample
            FROM modification_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY contract_id
        ),
        eligible AS (
            SELECT
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_name,
                a.supplier_doc_type,
                a.buyer_document_id,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.contract_value,
                a.signing_date,
                a.contract_start_date,
                a.contract_end_date,
                r.modification_event_count,
                r.total_modification_value,
                r.max_modification_value,
                r.total_extended_days,
                r.max_extended_days,
                r.first_modification_at,
                r.last_modification_at,
                r.total_modification_value / nullif(a.contract_value, 0)
                    AS modification_value_share,
                e.modification_purpose_sample,
                list_concat(
                    [
                        coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    ],
                    e.modification_evidence_refs
                ) AS evidence_refs
            FROM modification_rollup r
            JOIN curated_contract_awards a
                ON a.contract_id = r.contract_id
            JOIN modification_evidence e
                ON e.contract_id = r.contract_id
            WHERE a.supplier_nit_canonical IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND a.process_url IS NOT NULL
        )
        SELECT
            'procurement_large_modifications' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            contract_id AS scope_key,
            'contract' AS scope_type,
            'high' AS severity,
            least(
                1.0,
                0.62
                    + least(coalesce(modification_value_share, 0.0) / 2.0, 0.22)
                    + least(log10(greatest(total_modification_value, 1)) / 140.0, 0.10)
                    + least(modification_event_count / 20.0, 0.06)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            modification_event_count,
            total_modification_value,
            max_modification_value,
            modification_value_share,
            total_extended_days,
            max_extended_days,
            first_modification_at,
            last_modification_at,
            modification_purpose_sample,
            evidence_refs
        FROM eligible
        WHERE total_modification_value >= 100000000
            OR modification_value_share >= 0.50
    """)


def _create_views(con: duckdb.DuckDBPyConnection, required_sources: Sequence[str]) -> None:
    if not ({"secop_ii_contracts", "paco_sanctions"} & set(required_sources)):
        _create_person_views(con, required_sources)
        _create_process_views(con, required_sources)
        _create_cobidding_views(con, required_sources)
        _create_company_registry_overlap_views(con, required_sources)
        _create_contract_suspension_views(con, required_sources)
        _create_contract_execution_delay_views(con, required_sources)
        _create_supplier_identity_views(con, required_sources)
        _create_cuentas_claras_views(con, required_sources)
        _create_public_servant_conflict_disclosure_views(con, required_sources)
        _create_large_modification_views(con, required_sources)
        return

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
            coacc_nit_canonical(supplier_document, supplier_doc_type) AS supplier_nit_canonical,
            coacc_document_key(supplier_document, supplier_doc_type) AS supplier_document_key,
            'doc:' || coacc_document_key(supplier_document, supplier_doc_type)
                AS supplier_entity_id,
            nullif(trim(awarded_supplier), '') AS supplier_name,
            nullif(trim(supplier_doc_type), '') AS supplier_doc_type,
            coacc_doc_digits(entity_nit) AS buyer_document_digits,
            coacc_nit_canonical(entity_nit, 'NIT') AS buyer_nit_canonical,
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
                coacc_nit_canonical(subject_document_id, subject_type) AS subject_nit_canonical,
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
                NULL::VARCHAR AS subject_nit_canonical,
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
                supplier_nit_canonical AS nit_canonical,
                supplier_doc_type AS document_type,
                supplier_name AS display_name,
                'secop_ii_contracts' AS source_id
            FROM curated_contract_awards
            UNION ALL
            SELECT
                subject_document_key AS document_key,
                subject_document_digits AS document_digits,
                NULL AS nit_base,
                subject_nit_canonical AS nit_canonical,
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
            max(nit_canonical) AS nit_canonical,
            max(display_name) AS display_name,
            max(document_type) AS document_type,
            string_agg(DISTINCT source_id, ',') AS source_ids,
            count(*) AS source_row_count
        FROM unioned
        WHERE document_key IS NOT NULL
        GROUP BY document_key
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_dim_company AS
        WITH company_sources AS (
            SELECT
                supplier_nit_canonical AS nit_canonical,
                supplier_document_digits AS raw_nit,
                supplier_name AS display_name,
                signing_date AS observed_date,
                'secop_ii_contracts' AS source_id
            FROM curated_contract_awards
            WHERE supplier_nit_canonical IS NOT NULL
            UNION ALL
            SELECT
                subject_nit_canonical AS nit_canonical,
                subject_document_digits AS raw_nit,
                subject_name AS display_name,
                sanction_date AS observed_date,
                'paco_sanctions' AS source_id
            FROM curated_paco_subjects
            WHERE subject_nit_canonical IS NOT NULL
        )
        SELECT
            'company:' || nit_canonical AS entity_uid,
            nit_canonical,
            list(DISTINCT raw_nit ORDER BY raw_nit)
                FILTER (WHERE raw_nit IS NOT NULL) AS nit_variants,
            min(display_name) FILTER (WHERE display_name IS NOT NULL) AS name_canonical,
            list(DISTINCT display_name ORDER BY display_name)
                FILTER (WHERE display_name IS NOT NULL) AS name_variants,
            min(observed_date) AS first_seen,
            max(observed_date) AS last_seen,
            list(DISTINCT source_id ORDER BY source_id) AS sources,
            count(*) AS source_row_count
        FROM company_sources
        WHERE nit_canonical IS NOT NULL
        GROUP BY nit_canonical
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_dim_buyer AS
        WITH buyer_sources AS (
            SELECT
                buyer_nit_canonical AS nit_canonical,
                buyer_document_digits AS raw_nit,
                buyer_name AS display_name,
                signing_date AS observed_date,
                contract_id,
                contract_value,
                'secop_ii_contracts' AS source_id
            FROM curated_contract_awards
            WHERE buyer_nit_canonical IS NOT NULL
        )
        SELECT
            'buyer:' || nit_canonical AS entity_uid,
            nit_canonical,
            list(DISTINCT raw_nit ORDER BY raw_nit)
                FILTER (WHERE raw_nit IS NOT NULL) AS nit_variants,
            min(display_name) FILTER (WHERE display_name IS NOT NULL) AS name_canonical,
            list(DISTINCT display_name ORDER BY display_name)
                FILTER (WHERE display_name IS NOT NULL) AS name_variants,
            min(observed_date) AS first_seen,
            max(observed_date) AS last_seen,
            count(DISTINCT contract_id) AS contract_count,
            coalesce(sum(coalesce(contract_value, 0.0)), 0.0) AS total_contract_value,
            list(DISTINCT source_id ORDER BY source_id) AS sources
        FROM buyer_sources
        WHERE nit_canonical IS NOT NULL
        GROUP BY nit_canonical
    """)
    _create_person_views(con, required_sources)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_single_bidder_high_value AS
        WITH eligible AS (
            SELECT
                supplier_entity_id,
                supplier_document_key,
                supplier_name,
                buyer_document_id,
                buyer_name,
                department,
                city,
                sector,
                procurement_modality,
                contract_type,
                contract_id,
                contract_reference,
                process_id,
                process_url,
                contract_value,
                signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value >= 1000000000
                AND (
                    lower(procurement_modality) LIKE '%contrataci%n directa%'
                    OR lower(procurement_modality) LIKE '%r%gimen especial%'
                    OR lower(procurement_modality) LIKE '%m%nima cuant%a%'
                )
        )
        SELECT
            'procurement_single_bidder_high_value' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            coalesce(process_id, contract_id) AS scope_key,
            'procurement_process' AS scope_type,
            'medium' AS severity,
            least(
                1.0,
                0.55
                    + least(log10(greatest(contract_value, 1)) / 40.0, 0.35)
                    + CASE
                        WHEN lower(procurement_modality) LIKE '%con ofertas%' THEN 0.0
                        ELSE 0.10
                    END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            contract_value,
            signing_date,
            [
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
            ] AS evidence_refs
        FROM eligible
        WHERE coalesce(process_id, contract_id) IS NOT NULL
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
        CREATE OR REPLACE TEMP VIEW curated_contract_value_outlier_by_category AS
        WITH eligible AS (
            SELECT
                supplier_entity_id,
                supplier_document_key,
                supplier_name,
                buyer_document_id,
                buyer_name,
                department,
                city,
                sector,
                procurement_modality,
                contract_type,
                contract_id,
                contract_reference,
                process_id,
                process_url,
                contract_value,
                signing_date,
                coalesce(nullif(trim(department), ''), 'unknown')
                    AS category_department,
                coalesce(cast(date_part('year', signing_date) AS VARCHAR), 'unknown')
                    AS category_year,
                coalesce(nullif(trim(sector), ''), 'unknown')
                    AS category_sector,
                coalesce(nullif(trim(procurement_modality), ''), 'unknown')
                    AS category_modality,
                coalesce(nullif(trim(contract_type), ''), 'unknown')
                    AS category_contract_type
            FROM curated_contract_awards
            WHERE contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
                AND signing_date IS NOT NULL
        ),
        category_stats AS (
            SELECT
                category_department,
                category_year,
                category_sector,
                category_modality,
                category_contract_type,
                count(*) AS category_contract_count,
                avg(contract_value) AS category_avg_value,
                stddev_samp(contract_value) AS category_stddev_value,
                quantile_cont(contract_value, 0.5) AS category_median_value,
                quantile_cont(contract_value, 0.25) AS category_q1_value,
                quantile_cont(contract_value, 0.75) AS category_q3_value
            FROM eligible
            GROUP BY
                category_department,
                category_year,
                category_sector,
                category_modality,
                category_contract_type
            HAVING count(*) >= 100
        ),
        scored AS (
            SELECT
                e.*,
                s.category_contract_count,
                s.category_avg_value,
                s.category_stddev_value,
                s.category_median_value,
                s.category_q1_value,
                s.category_q3_value,
                s.category_q3_value - s.category_q1_value AS category_iqr_value,
                (e.contract_value - s.category_avg_value)
                    / nullif(s.category_stddev_value, 0) AS category_z_score,
                e.contract_value / nullif(s.category_median_value, 0)
                    AS median_multiple,
                row_number() OVER (
                    PARTITION BY
                        e.category_department,
                        e.category_year,
                        e.category_sector,
                        e.category_modality,
                        e.category_contract_type
                    ORDER BY e.contract_value DESC NULLS LAST, e.contract_id
                ) AS category_value_rank
            FROM eligible e
            JOIN category_stats s
                USING (
                    category_department,
                    category_year,
                    category_sector,
                    category_modality,
                    category_contract_type
                )
        )
        SELECT
            'procurement_contract_value_outlier_by_category' AS signal_id,
            'contract:' || contract_id AS entity_id,
            contract_id AS entity_key,
            'Contract' AS entity_label,
            contract_id AS scope_key,
            'contract' AS scope_type,
            least(
                1.0,
                0.45
                    + least(coalesce(category_z_score, 0) / 30.0, 0.30)
                    + least(coalesce(median_multiple, 0) / 200.0, 0.25)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_CONTRACT_KEY' AS identity_match_type,
            'exact' AS identity_quality,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            supplier_document_key,
            supplier_name,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_value,
            signing_date,
            category_department,
            category_year,
            category_sector,
            category_modality,
            category_contract_type,
            category_contract_count,
            category_avg_value,
            category_stddev_value,
            category_median_value,
            category_q1_value,
            category_q3_value,
            category_iqr_value,
            category_z_score,
            median_multiple,
            category_value_rank,
            [
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
            ] AS evidence_refs
        FROM scored
        WHERE category_value_rank <= 3
            AND contract_value >= 5000000000
            AND contract_value >= category_median_value * 20
            AND (
                (
                    category_stddev_value > 0
                    AND contract_value >= category_avg_value + 5 * category_stddev_value
                )
                OR (
                    category_iqr_value > 0
                    AND contract_value >= category_q3_value + 8 * category_iqr_value
                )
            )
    """)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_payment_plan_anomalies AS
        WITH eligible AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                nullif(trim(contract_reference), '') AS contract_reference,
                nullif(trim(procurement_process), '') AS process_id,
                coacc_reference_url(process_url) AS process_url,
                coacc_document_key(supplier_document, supplier_doc_type)
                    AS supplier_document_key,
                coacc_nit_canonical(supplier_document, supplier_doc_type)
                    AS supplier_nit_canonical,
                'doc:' || coacc_document_key(supplier_document, supplier_doc_type)
                    AS supplier_entity_id,
                nullif(trim(supplier_document), '') AS supplier_document_id,
                nullif(trim(awarded_supplier), '') AS supplier_name,
                nullif(trim(entity_nit), '') AS buyer_document_id,
                nullif(trim(entity_name), '') AS buyer_name,
                nullif(trim(department), '') AS department,
                nullif(trim(city), '') AS city,
                nullif(trim(sector), '') AS sector,
                nullif(trim(procurement_modality), '') AS procurement_modality,
                nullif(trim(contract_type), '') AS contract_type,
                nullif(trim(contract_status), '') AS contract_status,
                nullif(trim(enables_advance_payment), '') AS enables_advance_payment,
                nullif(trim(liquidation), '') AS liquidation,
                coacc_money(contract_value) AS contract_value,
                coacc_money(advance_payment_value) AS advance_payment_value,
                coacc_money(invoiced_value) AS invoiced_value,
                coacc_money(pending_payment_value) AS pending_payment_value,
                coacc_money(paid_value) AS paid_value,
                coacc_money(amortized_value) AS amortized_value,
                coacc_money(pending_value) AS pending_value,
                coacc_money(pending_execution_value) AS pending_execution_value,
                try_cast(signing_date AS DATE) AS signing_date
            FROM src_secop_ii_contracts
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND coacc_nit_canonical(supplier_document, supplier_doc_type) IS NOT NULL
                AND NOT regexp_matches(
                    coacc_document_key(supplier_document, supplier_doc_type),
                    '^0+$'
                )
        ),
        scored AS (
            SELECT
                *,
                advance_payment_value / nullif(contract_value, 0)
                    AS advance_payment_share,
                paid_value / nullif(contract_value, 0) AS paid_value_share,
                invoiced_value / nullif(contract_value, 0) AS invoiced_value_share,
                CASE
                    WHEN contract_value >= 500000000
                        AND advance_payment_value >= 100000000
                        AND advance_payment_value / nullif(contract_value, 0) >= 0.50
                        THEN 'high_advance_payment_share'
                    WHEN contract_value >= 100000000
                        AND paid_value - contract_value >= 100000000
                        AND paid_value / nullif(contract_value, 0) >= 1.25
                        THEN 'paid_value_exceeds_contract'
                    WHEN contract_value >= 100000000
                        AND invoiced_value - contract_value >= 100000000
                        AND invoiced_value / nullif(contract_value, 0) >= 1.25
                        THEN 'invoiced_value_exceeds_contract'
                    ELSE NULL
                END AS anomaly_type,
                (
                    contract_value >= 500000000
                    AND advance_payment_value >= 100000000
                    AND advance_payment_value / nullif(contract_value, 0) >= 0.50
                ) AS high_advance_payment_flag,
                (
                    contract_value >= 100000000
                    AND paid_value - contract_value >= 100000000
                    AND paid_value / nullif(contract_value, 0) >= 1.25
                ) AS paid_over_contract_flag,
                (
                    contract_value >= 100000000
                    AND invoiced_value - contract_value >= 100000000
                    AND invoiced_value / nullif(contract_value, 0) >= 1.25
                ) AS invoiced_over_contract_flag
            FROM eligible
            WHERE contract_value IS NOT NULL
                AND contract_value > 0
        )
        SELECT
            'procurement_payment_plan_anomalies' AS signal_id,
            supplier_entity_id AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            contract_id AS scope_key,
            'contract' AS scope_type,
            'medium' AS severity,
            least(
                1.0,
                greatest(
                    CASE
                        WHEN high_advance_payment_flag THEN
                            0.62
                                + least(advance_payment_share - 0.50, 0.20)
                                + least(log10(greatest(contract_value, 1)) / 120.0, 0.08)
                        ELSE 0.0
                    END,
                    CASE
                        WHEN paid_over_contract_flag THEN
                            0.72
                                + least(paid_value_share - 1.25, 0.18)
                                + least(
                                    log10(greatest(paid_value - contract_value, 1))
                                        / 120.0,
                                    0.10
                                )
                        ELSE 0.0
                    END,
                    CASE
                        WHEN invoiced_over_contract_flag THEN
                            0.70
                                + least(invoiced_value_share - 1.25, 0.18)
                                + least(
                                    log10(greatest(invoiced_value - contract_value, 1))
                                        / 120.0,
                                    0.10
                                )
                        ELSE 0.0
                    END
                )
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            anomaly_type,
            high_advance_payment_flag,
            paid_over_contract_flag,
            invoiced_over_contract_flag,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            supplier_document_id,
            supplier_name,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_status,
            enables_advance_payment,
            liquidation,
            contract_value,
            advance_payment_value,
            advance_payment_share,
            invoiced_value,
            invoiced_value_share,
            pending_payment_value,
            paid_value,
            paid_value_share,
            amortized_value,
            pending_value,
            pending_execution_value,
            signing_date,
            [
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
            ] AS evidence_refs
        FROM scored
        WHERE anomaly_type IS NOT NULL
    """)
    _create_contract_suspension_views(con, required_sources)
    _create_contract_execution_delay_views(con, required_sources)
    _create_cuentas_claras_views(con, required_sources)
    _create_public_servant_conflict_disclosure_views(con, required_sources)
    _create_large_modification_views(con, required_sources)
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
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_buyer_supplier_network_density AS
        WITH eligible AS (
            SELECT
                supplier_entity_id,
                supplier_document_key,
                supplier_nit_canonical,
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
                AND supplier_nit_canonical IS NOT NULL
                AND buyer_document_id IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        pair_rollup AS (
            SELECT
                supplier_document_key,
                buyer_document_id,
                any_value(supplier_entity_id) AS supplier_entity_id,
                any_value(supplier_name) AS supplier_name,
                any_value(buyer_name) AS buyer_name,
                count(DISTINCT contract_id) AS pair_contract_count,
                count(DISTINCT department) AS pair_department_count,
                sum(contract_value) AS pair_total_value,
                avg(contract_value) AS pair_avg_value,
                min(signing_date) AS pair_first_signing_date,
                max(signing_date) AS pair_last_signing_date
            FROM eligible
            GROUP BY supplier_document_key, buyer_document_id
        ),
        supplier_rollup AS (
            SELECT
                any_value(p.supplier_entity_id) AS entity_id,
                p.supplier_document_key AS entity_key,
                any_value(p.supplier_name) AS supplier_name,
                count(DISTINCT p.buyer_document_id) AS distinct_buyer_count,
                cast(sum(p.pair_contract_count) AS BIGINT) AS contract_count,
                cast(sum(p.pair_total_value) AS DOUBLE) AS total_contract_value,
                cast(
                    sum(CASE WHEN p.pair_contract_count >= 5 THEN 1 ELSE 0 END)
                    AS BIGINT
                ) AS repeated_buyer_count,
                cast(
                    sum(
                        CASE
                            WHEN p.pair_contract_count >= 5 THEN p.pair_contract_count
                            ELSE 0
                        END
                    )
                    AS BIGINT
                ) AS repeated_contract_count,
                cast(
                    sum(
                        CASE
                            WHEN p.pair_contract_count >= 5
                                AND p.pair_total_value >= 500000000
                                THEN 1
                            ELSE 0
                        END
                    )
                    AS BIGINT
                ) AS high_value_repeated_buyer_count,
                max(p.pair_contract_count) AS max_pair_contract_count,
                min(p.pair_first_signing_date) AS first_signing_date,
                max(p.pair_last_signing_date) AS last_signing_date
            FROM pair_rollup p
            GROUP BY p.supplier_document_key
        ),
        scored AS (
            SELECT
                *,
                cast(repeated_contract_count AS DOUBLE)
                    / nullif(cast(contract_count AS DOUBLE), 0)
                    AS repeated_contract_share
            FROM supplier_rollup
        ),
        ranked_evidence AS (
            SELECT
                e.supplier_document_key,
                coalesce(e.process_url, 'secop_ii_contracts:' || e.contract_id)
                    AS evidence_ref,
                e.contract_value,
                e.signing_date,
                row_number() OVER (
                    PARTITION BY e.supplier_document_key
                    ORDER BY e.contract_value DESC NULLS LAST,
                        e.signing_date DESC NULLS LAST,
                        e.contract_id
                ) AS evidence_rank
            FROM eligible e
            JOIN pair_rollup p
                ON p.supplier_document_key = e.supplier_document_key
                AND p.buyer_document_id = e.buyer_document_id
            WHERE p.pair_contract_count >= 5
        ),
        evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS evidence_refs
            FROM ranked_evidence
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key
        ),
        buyer_sample_ranked AS (
            SELECT
                supplier_document_key,
                coalesce(buyer_name, buyer_document_id) AS buyer_sample,
                pair_total_value,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY pair_total_value DESC NULLS LAST, buyer_document_id
                ) AS sample_rank
            FROM pair_rollup
            WHERE pair_contract_count >= 5
        ),
        buyer_samples AS (
            SELECT
                supplier_document_key,
                list(buyer_sample ORDER BY pair_total_value DESC, buyer_sample)
                    AS repeated_buyer_sample
            FROM buyer_sample_ranked
            WHERE sample_rank <= 10
            GROUP BY supplier_document_key
        )
        SELECT
            'procurement_buyer_supplier_network_density' AS signal_id,
            entity_id,
            entity_key,
            'Company' AS entity_label,
            'supplier_network:' || entity_key AS scope_key,
            'supplier_network' AS scope_type,
            least(
                1.0,
                0.45
                    + least(repeated_contract_share, 1.0) * 0.25
                    + least(repeated_buyer_count / 50.0, 0.15)
                    + least(log10(greatest(total_contract_value, 1)) / 120.0, 0.15)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            distinct_buyer_count,
            contract_count,
            total_contract_value,
            repeated_buyer_count,
            repeated_contract_count,
            high_value_repeated_buyer_count,
            repeated_contract_share,
            max_pair_contract_count,
            first_signing_date,
            last_signing_date,
            b.repeated_buyer_sample,
            e.evidence_refs
        FROM scored s
        JOIN evidence e
            ON e.supplier_document_key = s.entity_key
        LEFT JOIN buyer_samples b
            ON b.supplier_document_key = s.entity_key
        WHERE contract_count >= 50
            AND distinct_buyer_count >= 10
            AND repeated_buyer_count >= 8
            AND high_value_repeated_buyer_count >= 2
            AND repeated_contract_share >= 0.55
            AND total_contract_value >= 2000000000
    """)
    _create_process_views(con, required_sources)
    _create_cobidding_views(con, required_sources)
    _create_company_registry_overlap_views(con, required_sources)
    _create_supplier_identity_views(con, required_sources)


def _table_sql(table: str) -> str:
    if table == "dim_subject_document":
        return "SELECT * FROM curated_subject_documents"
    if table == "dim_company":
        return "SELECT * FROM curated_dim_company"
    if table == "dim_buyer":
        return "SELECT * FROM curated_dim_buyer"
    if table == "dim_person":
        return "SELECT * FROM curated_dim_person"
    if table == "fct_procurement_contract_awards":
        return "SELECT * FROM curated_contract_awards"
    if table == "signal_feature_procurement_single_bidder_high_value":
        return "SELECT * FROM curated_single_bidder_high_value"
    if table == "signal_feature_procurement_large_modifications":
        return "SELECT * FROM curated_large_modifications"
    if table == "signal_feature_procurement_sanctioned_supplier_awarded":
        return "SELECT * FROM curated_sanctioned_awards"
    if table == "signal_feature_procurement_supplier_concentration_across_entities":
        return "SELECT * FROM curated_supplier_concentration"
    if table == "signal_feature_procurement_contract_value_outlier_by_category":
        return "SELECT * FROM curated_contract_value_outlier_by_category"
    if table == "signal_feature_procurement_repeat_awards_same_supplier":
        return "SELECT * FROM curated_repeat_awards_same_supplier"
    if table == "signal_feature_procurement_buyer_supplier_network_density":
        return "SELECT * FROM curated_buyer_supplier_network_density"
    if table == "signal_feature_procurement_cartel_risk_cobidding":
        return "SELECT * FROM curated_cartel_risk_cobidding"
    if table == "signal_feature_procurement_payment_plan_anomalies":
        return "SELECT * FROM curated_payment_plan_anomalies"
    if table == "signal_feature_procurement_contract_suspensions":
        return "SELECT * FROM curated_contract_suspensions"
    if table == "signal_feature_procurement_contract_execution_delay":
        return "SELECT * FROM curated_contract_execution_delay"
    if table == "signal_feature_procurement_short_bidding_window":
        return "SELECT * FROM curated_short_bidding_window"
    if table == "signal_feature_procurement_offers_competition_drop":
        return "SELECT * FROM curated_offers_competition_drop"
    if table == "signal_feature_procurement_public_servant_conflict_disclosure_overlap":
        return "SELECT * FROM curated_public_servant_conflict_disclosure_overlap"
    if table == "signal_feature_cuentas_claras_donor_supplier_overlap":
        return "SELECT * FROM curated_cuentas_claras_donor_supplier_overlap"
    if table == "signal_feature_procurement_politically_exposed_position_supplier_overlap":
        return "SELECT * FROM curated_politically_exposed_supplier_overlap"
    if table == "signal_feature_procurement_related_companies_shared_officer":
        return "SELECT * FROM curated_related_companies_shared_officer"
    if table == "signal_feature_procurement_cross_source_identity_inconsistency":
        return "SELECT * FROM curated_cross_source_identity_inconsistency"
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
