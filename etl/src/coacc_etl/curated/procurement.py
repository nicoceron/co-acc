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
    "signal_feature_procurement_contract_modification_ladder_review_only",
    "signal_feature_procurement_sanctioned_supplier_awarded",
    "signal_feature_procurement_secop_sanction_later_awards_review_only",
    "signal_feature_fiscal_procurement_chronology_review_only",
    "signal_feature_procurement_supplier_concentration_across_entities",
    "signal_feature_procurement_contract_value_outlier_by_category",
    "signal_feature_procurement_repeat_awards_same_supplier",
    "signal_feature_procurement_buyer_supplier_network_density",
    "signal_feature_procurement_cartel_risk_cobidding",
    "signal_feature_procurement_related_bidders_same_process_review_only",
    "signal_feature_procurement_shared_representative_same_buyer_cluster_review_only",
    "signal_feature_procurement_payment_plan_anomalies",
    "signal_feature_procurement_guarantee_advance_execution_chain",
    "signal_feature_procurement_guarantee_policy_reuse_review_only",
    "signal_feature_procurement_budget_chain_reconciliation_review_only",
    "signal_feature_procurement_invoice_budget_reconciliation_review_only",
    "signal_feature_procurement_payment_plan_reconciliation_review_only",
    "signal_feature_procurement_contract_suspensions",
    "signal_feature_procurement_contract_execution_delay",
    "signal_feature_health_pae_service_delivery_gap_review_only",
    "signal_feature_pae_beneficiary_territory_delivery_gap_review_only",
    "signal_feature_project_bpin_procurement_overlap",
    "signal_feature_project_regalias_execution_procurement_overlap",
    "signal_feature_sgr_ocad_executor_capacity_gap",
    "signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only",
    "signal_feature_bpin_dnp_vs_pida27_obras_prioritarias",
    "signal_feature_procurement_short_bidding_window",
    "signal_feature_procurement_offers_competition_drop",
    "signal_feature_procurement_public_servant_conflict_disclosure_overlap",
    "signal_feature_procurement_role_supplier_same_buyer_review_only",
    "signal_feature_public_declaration_supplier_chronology_review_only",
    "signal_feature_public_declaration_company_bridge_current_risk_review_only",
    "signal_feature_cuentas_claras_donor_supplier_overlap",
    "signal_feature_pida_full30_meta",
    "signal_feature_pida5_pida27_pida4_chain",
    "signal_feature_tvec_multi_entity_capture",
    "signal_feature_tvec_item_price_dispersion_review_only",
    "signal_feature_procurement_politically_exposed_position_supplier_overlap",
    "signal_feature_procurement_related_companies_shared_officer",
    "signal_feature_procurement_cross_source_identity_inconsistency",
    "signal_feature_rues_supplier_capacity_status_review_only",
    "signal_feature_cuentas_claras_donor_ineligibility_review",
    "signal_feature_cross_signal_compound_risk_review_only",
    "signal_feature_secop_i_legacy_supplier_current_risk_review_only",
    "signal_feature_secop_i_legacy_representative_current_risk_review_only",
    "signal_feature_secop_interadmin_executor_network_review_only",
    "signal_feature_siri_antecedent_procurement_chronology_review_only",
)
_CROSS_SIGNAL_REQUIRED_SOURCES = (
    "asset_disclosures",
    "company_registry_c82u",
    "conflict_disclosures",
    "cuentas_claras_income_2019",
    "fiscal_findings",
    "fiscal_responsibility",
    "paco_sanctions",
    "secop_budget_commitments",
    "secop_budget_items",
    "secop_cdp_requests",
    "secop_contract_execution",
    "secop_contract_modifications",
    "secop_contract_suspensions",
    "secop_guarantees",
    "secop_ii_contracts",
    "secop_ii_processes",
    "secop_invoices",
    "secop_integrado",
    "secop_offers",
    "secop_payment_plans",
    "health_providers",
    "pae_indicators",
    "secop_process_bpin",
    "secop_sanctions",
    "secop_suppliers",
    "sgr_expense_execution",
    "sgr_projects",
    "dnp_project_executors",
    "dnp_project_locations",
    "dnp_project_beneficiary_locations",
    "dnp_project_beneficiary_characterization",
    "sigep_sensitive_positions",
    "tvec_orders_consolidated",
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
    "signal_feature_procurement_contract_modification_ladder_review_only": (
        "secop_ii_contracts",
        "secop_contract_modifications",
        "secop_contract_suspensions",
        "secop_contract_execution",
    ),
    "signal_feature_procurement_sanctioned_supplier_awarded": (
        "secop_ii_contracts",
        "paco_sanctions",
    ),
    "signal_feature_procurement_secop_sanction_later_awards_review_only": (
        "secop_ii_contracts",
        "secop_sanctions",
    ),
    "signal_feature_fiscal_procurement_chronology_review_only": (
        "secop_ii_contracts",
        "fiscal_findings",
        "fiscal_responsibility",
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
    "signal_feature_procurement_related_bidders_same_process_review_only": (
        "secop_offers",
        "secop_ii_contracts",
        "company_registry_c82u",
    ),
    "signal_feature_procurement_shared_representative_same_buyer_cluster_review_only": (
        "secop_ii_contracts",
        "company_registry_c82u",
    ),
    "signal_feature_procurement_payment_plan_anomalies": ("secop_ii_contracts",),
    "signal_feature_procurement_guarantee_advance_execution_chain": (
        "secop_ii_contracts",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_procurement_guarantee_policy_reuse_review_only": (
        "secop_ii_contracts",
        "secop_guarantees",
    ),
    "signal_feature_procurement_budget_chain_reconciliation_review_only": (
        "secop_ii_contracts",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_procurement_invoice_budget_reconciliation_review_only": (
        "secop_ii_contracts",
        "secop_invoices",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_procurement_payment_plan_reconciliation_review_only": (
        "secop_ii_contracts",
        "secop_payment_plans",
        "secop_invoices",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_health_pae_service_delivery_gap_review_only": (
        "health_providers",
        "secop_ii_contracts",
        "secop_invoices",
        "secop_payment_plans",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_pae_beneficiary_territory_delivery_gap_review_only": (
        "health_providers",
        "pae_indicators",
        "secop_ii_contracts",
        "secop_invoices",
        "secop_payment_plans",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    ),
    "signal_feature_procurement_contract_suspensions": (
        "secop_contract_suspensions",
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_contract_execution_delay": (
        "secop_contract_execution",
        "secop_ii_contracts",
    ),
    "signal_feature_project_bpin_procurement_overlap": (
        "secop_process_bpin",
        "secop_ii_contracts",
    ),
    "signal_feature_project_regalias_execution_procurement_overlap": (
        "sgr_expense_execution",
        "sgr_projects",
        "secop_process_bpin",
        "secop_ii_contracts",
    ),
    "signal_feature_sgr_ocad_executor_capacity_gap": (
        "sgr_expense_execution",
        "sgr_projects",
        "secop_process_bpin",
        "secop_ii_contracts",
        "company_registry_c82u",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_contract_execution",
        "secop_guarantees",
        "secop_sanctions",
    ),
    "signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only": (
        "sgr_expense_execution",
        "sgr_projects",
        "secop_process_bpin",
        "secop_ii_contracts",
        "company_registry_c82u",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_contract_execution",
        "secop_guarantees",
        "secop_sanctions",
        "dnp_project_executors",
        "dnp_project_locations",
        "dnp_project_beneficiary_locations",
        "dnp_project_beneficiary_characterization",
    ),
    "signal_feature_bpin_dnp_vs_pida27_obras_prioritarias": (
        "secop_process_bpin",
        "secop_integrado",
    ),
    "signal_feature_procurement_short_bidding_window": ("secop_ii_processes",),
    "signal_feature_procurement_offers_competition_drop": ("secop_ii_processes",),
    "signal_feature_procurement_public_servant_conflict_disclosure_overlap": (
        "conflict_disclosures",
        "secop_ii_contracts",
    ),
    "signal_feature_procurement_role_supplier_same_buyer_review_only": (
        "secop_ii_contracts",
    ),
    "signal_feature_public_declaration_supplier_chronology_review_only": (
        "asset_disclosures",
        "conflict_disclosures",
        "sigep_sensitive_positions",
        "secop_ii_contracts",
    ),
    "signal_feature_public_declaration_company_bridge_current_risk_review_only": (
        *_CROSS_SIGNAL_REQUIRED_SOURCES,
    ),
    "signal_feature_cuentas_claras_donor_supplier_overlap": (
        "secop_ii_contracts",
        "cuentas_claras_income_2019",
    ),
    "signal_feature_cuentas_claras_donor_ineligibility_review": (
        "secop_ii_contracts",
        "cuentas_claras_income_2019",
    ),
    "signal_feature_pida_full30_meta": ("secop_integrado",),
    "signal_feature_pida5_pida27_pida4_chain": (
        "secop_integrado",
        "secop_sanctions",
    ),
    "signal_feature_tvec_multi_entity_capture": (
        "tvec_orders_consolidated",
        "secop_ii_contracts",
    ),
    "signal_feature_tvec_item_price_dispersion_review_only": (
        "tvec_orders_consolidated",
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
    "signal_feature_rues_supplier_capacity_status_review_only": (
        "company_registry_c82u",
        "secop_ii_contracts",
    ),
    "signal_feature_cross_signal_compound_risk_review_only": _CROSS_SIGNAL_REQUIRED_SOURCES,
    "signal_feature_secop_i_legacy_supplier_current_risk_review_only": (
        "secop_i_historical_processes",
        *_CROSS_SIGNAL_REQUIRED_SOURCES,
    ),
    "signal_feature_secop_i_legacy_representative_current_risk_review_only": (
        "secop_i_historical_processes",
        *_CROSS_SIGNAL_REQUIRED_SOURCES,
    ),
    "signal_feature_secop_interadmin_executor_network_review_only": (
        "secop_interadmin_agreements",
        "secop_ii_contracts",
        "secop_ii_processes",
        "secop_offers",
        "paco_sanctions",
        "fiscal_findings",
        "fiscal_responsibility",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_cdp_requests",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_guarantees",
        "secop_invoices",
        "secop_payment_plans",
        "health_providers",
        "secop_sanctions",
        "company_registry_c82u",
        "secop_suppliers",
    ),
    "signal_feature_siri_antecedent_procurement_chronology_review_only": (
        "siri_antecedents",
        "secop_ii_contracts",
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
        CREATE OR REPLACE MACRO coacc_money_decimal(value) AS (
            CASE
                WHEN value IS NULL THEN NULL
                WHEN try_cast(cast(value AS VARCHAR) AS DOUBLE) IS NOT NULL
                    THEN try_cast(cast(value AS VARCHAR) AS DOUBLE)
                WHEN regexp_matches(
                    regexp_replace(coalesce(cast(value AS VARCHAR), ''), '[^0-9,.-]', '', 'g'),
                    '^[0-9]{1,3}(\\.[0-9]{3})+(,[0-9]+)?$'
                )
                    THEN try_cast(
                        replace(
                            replace(
                                regexp_replace(
                                    coalesce(cast(value AS VARCHAR), ''),
                                    '[^0-9,.-]',
                                    '',
                                    'g'
                                ),
                                '.',
                                ''
                            ),
                            ',',
                            '.'
                        ) AS DOUBLE
                    )
                WHEN regexp_matches(
                    regexp_replace(coalesce(cast(value AS VARCHAR), ''), '[^0-9,.-]', '', 'g'),
                    '^[0-9]{1,3}(,[0-9]{3})+(\\.[0-9]+)?$'
                )
                    THEN try_cast(
                        replace(
                            regexp_replace(
                                coalesce(cast(value AS VARCHAR), ''),
                                '[^0-9,.-]',
                                '',
                                'g'
                            ),
                            ',',
                            ''
                        ) AS DOUBLE
                    )
                WHEN regexp_matches(
                    regexp_replace(coalesce(cast(value AS VARCHAR), ''), '[^0-9,.-]', '', 'g'),
                    '^[0-9]+,[0-9]{1,6}$'
                )
                    THEN try_cast(
                        replace(
                            regexp_replace(
                                coalesce(cast(value AS VARCHAR), ''),
                                '[^0-9,.-]',
                                '',
                                'g'
                            ),
                            ',',
                            '.'
                        ) AS DOUBLE
                    )
                ELSE coacc_money(value)
            END
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
    con.execute("""
        CREATE OR REPLACE MACRO coacc_text_key(value) AS (
            regexp_replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(
                                    replace(
                                        lower(coalesce(cast(value AS VARCHAR), '')),
                                        'á',
                                        'a'
                                    ),
                                    'é',
                                    'e'
                                ),
                                'í',
                                'i'
                            ),
                            'ó',
                            'o'
                        ),
                        'ú',
                        'u'
                    ),
                    'ñ',
                    'n'
                ),
                '[^a-z0-9]+',
                '',
                'g'
            )
        )
    """)
    con.execute("""
        CREATE OR REPLACE MACRO coacc_parse_date(value) AS (
            coalesce(
                try_cast(value AS DATE),
                try_strptime(
                    nullif(trim(cast(value AS VARCHAR)), ''),
                    '%Y%m%d'
                )::DATE,
                try_strptime(
                    nullif(trim(cast(value AS VARCHAR)), ''),
                    '%Y/%m/%d %H:%M:%S.%n'
                )::DATE,
                try_strptime(
                    nullif(trim(cast(value AS VARCHAR)), ''),
                    '%Y/%m/%d'
                )::DATE
            )
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

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_shared_representative_same_buyer_cluster AS
        WITH awards AS (
            SELECT
                supplier_document_key,
                supplier_name,
                supplier_doc_type,
                buyer_document_id,
                buyer_nit_canonical,
                buyer_name,
                contract_id,
                process_id,
                process_url,
                procurement_modality,
                contract_type,
                department,
                city,
                contract_value,
                signing_date,
                date_part('year', signing_date)::INTEGER AS signing_year
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND buyer_document_id IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value >= 10000000
                AND signing_date IS NOT NULL
        ),
        company_officers AS (
            SELECT *
            FROM (
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
                    ) AS company_record_id,
                    row_number() OVER (
                        PARTITION BY
                            coacc_document_key(document_id, identification_class),
                            coacc_cedula_key(
                                num_identificacion_representante_legal,
                                clase_identificacion_rl
                            )
                        ORDER BY matricula DESC NULLS LAST
                    ) AS officer_rank
                FROM src_company_registry_c82u
                WHERE coacc_document_key(document_id, identification_class) IS NOT NULL
                    AND coacc_nit_canonical(document_id, identification_class) IS NOT NULL
                    AND coacc_cedula_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    ) IS NOT NULL
            )
            WHERE officer_rank = 1
                AND length(representative_document_key) >= 5
                AND NOT regexp_matches(representative_document_key, '^0+$')
                AND representative_document_key != company_document_key
        ),
        representative_company_counts AS (
            SELECT
                representative_document_key,
                count(DISTINCT company_document_key) AS representative_company_count
            FROM company_officers
            GROUP BY representative_document_key
        ),
        award_officers AS (
            SELECT
                a.*,
                o.company_nit_canonical,
                o.company_document_id,
                coalesce(o.company_name, a.supplier_name) AS company_name,
                o.matricula,
                o.chamber_of_commerce,
                o.matricula_status,
                o.representative_document_key,
                o.representative_document_id,
                o.representative_name,
                o.representative_doc_type,
                o.company_record_id,
                rc.representative_company_count,
                regexp_matches(
                    lower(coalesce(a.procurement_modality, '')),
                    'directa|menor cuant|contrataci[oó]n directa|convenio'
                ) AS direct_or_exception_modality_flag,
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    AS contract_evidence_ref,
                'company_registry_c82u:' || o.company_record_id AS registry_evidence_ref
            FROM awards a
            JOIN company_officers o
                ON o.company_document_key = a.supplier_document_key
            JOIN representative_company_counts rc
                ON rc.representative_document_key = o.representative_document_key
        ),
        clusters AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                any_value(buyer_nit_canonical) AS buyer_nit_canonical,
                min(buyer_name) FILTER (WHERE buyer_name IS NOT NULL) AS buyer_name,
                signing_year,
                count(DISTINCT supplier_document_key) AS cluster_supplier_count,
                count(DISTINCT contract_id) AS cluster_contract_count,
                sum(contract_value) AS cluster_total_contract_value,
                sum(CASE WHEN direct_or_exception_modality_flag THEN 1 ELSE 0 END)
                    AS cluster_direct_or_exception_count,
                max(representative_company_count) AS representative_company_count,
                min(signing_date) AS cluster_first_signing_date,
                max(signing_date) AS cluster_last_signing_date
            FROM award_officers
            GROUP BY representative_document_key, buyer_document_id, signing_year
            HAVING count(DISTINCT supplier_document_key) BETWEEN 2 AND 6
                AND count(DISTINCT contract_id) >= 3
                AND sum(contract_value) >= 5000000000
                AND max(representative_company_count) <= 10
                AND (
                    sum(CASE WHEN direct_or_exception_modality_flag THEN 1 ELSE 0 END) >= 2
                    OR sum(contract_value) >= 10000000000
                    OR count(DISTINCT contract_id) >= 5
                )
        ),
        supplier_rollup AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key,
                any_value(company_nit_canonical) AS company_nit_canonical,
                any_value(company_document_id) AS company_document_id,
                min(company_name) FILTER (WHERE company_name IS NOT NULL)
                    AS company_name,
                any_value(supplier_doc_type) AS supplier_doc_type,
                any_value(matricula) AS matricula,
                any_value(chamber_of_commerce) AS chamber_of_commerce,
                any_value(matricula_status) AS matricula_status,
                any_value(representative_document_id) AS representative_document_id,
                any_value(representative_name) AS representative_name,
                any_value(representative_doc_type) AS representative_doc_type,
                count(DISTINCT contract_id) AS supplier_contract_count,
                sum(contract_value) AS supplier_contract_value,
                sum(CASE WHEN direct_or_exception_modality_flag THEN 1 ELSE 0 END)
                    AS supplier_direct_or_exception_count,
                min(signing_date) AS supplier_first_signing_date,
                max(signing_date) AS supplier_last_signing_date
            FROM award_officers
            GROUP BY
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key
        ),
        supplier_samples_ranked AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                coalesce(company_name, supplier_document_key) AS supplier_sample,
                supplier_contract_value,
                row_number() OVER (
                    PARTITION BY representative_document_key, buyer_document_id, signing_year
                    ORDER BY supplier_contract_value DESC NULLS LAST,
                        coalesce(company_name, supplier_document_key)
                ) AS sample_rank
            FROM supplier_rollup
        ),
        supplier_samples AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                list(supplier_sample ORDER BY supplier_contract_value DESC, supplier_sample)
                    AS cluster_supplier_samples
            FROM supplier_samples_ranked
            WHERE sample_rank <= 10
            GROUP BY representative_document_key, buyer_document_id, signing_year
        ),
        supplier_evidence_ranked AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key,
                contract_evidence_ref,
                contract_value,
                signing_date,
                row_number() OVER (
                    PARTITION BY
                        representative_document_key,
                        buyer_document_id,
                        signing_year,
                        supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM award_officers
        ),
        supplier_evidence AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key,
                list(contract_evidence_ref ORDER BY evidence_rank)
                    AS supplier_evidence_refs
            FROM supplier_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key
        ),
        cluster_evidence_ranked AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                contract_evidence_ref,
                contract_value,
                signing_date,
                row_number() OVER (
                    PARTITION BY representative_document_key, buyer_document_id, signing_year
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        supplier_document_key,
                        contract_id
                ) AS evidence_rank
            FROM award_officers
        ),
        cluster_evidence AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                list(contract_evidence_ref ORDER BY evidence_rank)
                    AS cluster_evidence_refs
            FROM cluster_evidence_ranked
            WHERE evidence_rank <= 8
            GROUP BY representative_document_key, buyer_document_id, signing_year
        ),
        registry_evidence_candidates AS (
            SELECT DISTINCT
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key,
                registry_evidence_ref
            FROM award_officers
        ),
        registry_evidence_ranked AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key,
                registry_evidence_ref,
                row_number() OVER (
                    PARTITION BY representative_document_key, buyer_document_id, signing_year
                    ORDER BY supplier_document_key
                ) AS evidence_rank
            FROM registry_evidence_candidates
        ),
        registry_evidence AS (
            SELECT
                representative_document_key,
                buyer_document_id,
                signing_year,
                list(registry_evidence_ref ORDER BY evidence_rank)
                    AS registry_evidence_refs
            FROM registry_evidence_ranked
            WHERE evidence_rank <= 6
            GROUP BY representative_document_key, buyer_document_id, signing_year
        ),
        candidates AS (
            SELECT
                s.*,
                c.buyer_nit_canonical,
                c.buyer_name,
                c.cluster_supplier_count,
                c.cluster_contract_count,
                c.cluster_total_contract_value,
                c.cluster_direct_or_exception_count,
                c.representative_company_count,
                c.cluster_first_signing_date,
                c.cluster_last_signing_date,
                samples.cluster_supplier_samples,
                se.supplier_evidence_refs,
                ce.cluster_evidence_refs,
                re.registry_evidence_refs
            FROM supplier_rollup s
            JOIN clusters c
                ON c.representative_document_key = s.representative_document_key
                AND c.buyer_document_id = s.buyer_document_id
                AND c.signing_year = s.signing_year
            LEFT JOIN supplier_samples samples
                ON samples.representative_document_key = s.representative_document_key
                AND samples.buyer_document_id = s.buyer_document_id
                AND samples.signing_year = s.signing_year
            LEFT JOIN supplier_evidence se
                ON se.representative_document_key = s.representative_document_key
                AND se.buyer_document_id = s.buyer_document_id
                AND se.signing_year = s.signing_year
                AND se.supplier_document_key = s.supplier_document_key
            LEFT JOIN cluster_evidence ce
                ON ce.representative_document_key = s.representative_document_key
                AND ce.buyer_document_id = s.buyer_document_id
                AND ce.signing_year = s.signing_year
            LEFT JOIN registry_evidence re
                ON re.representative_document_key = s.representative_document_key
                AND re.buyer_document_id = s.buyer_document_id
                AND re.signing_year = s.signing_year
        )
        SELECT
            'procurement_shared_representative_same_buyer_cluster_review_only'
                AS signal_id,
            'doc:' || supplier_document_key AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            'shared_representative_same_buyer:'
                || representative_document_key || ':'
                || buyer_document_id || ':'
                || cast(signing_year AS VARCHAR) || ':'
                || supplier_document_key AS scope_key,
            'shared_representative_same_buyer_cluster' AS scope_type,
            CASE
                WHEN cluster_total_contract_value >= 50000000000
                    OR cluster_direct_or_exception_count >= 10
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.70
                    + least(log10(greatest(cluster_total_contract_value, 1)) / 140.0, 0.08)
                    + least(cluster_supplier_count / 20.0, 0.08)
                    + least(cluster_contract_count / 100.0, 0.08)
                    + CASE WHEN cluster_direct_or_exception_count >= 2 THEN 0.04 ELSE 0.0 END
                    + CASE WHEN cluster_total_contract_value >= 10000000000 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN representative_company_count <= 5 THEN 0.02 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            company_nit_canonical,
            company_document_id,
            company_name,
            supplier_doc_type,
            matricula,
            chamber_of_commerce,
            matricula_status,
            buyer_document_id,
            buyer_nit_canonical,
            buyer_name,
            signing_year,
            representative_document_key,
            representative_document_id,
            representative_name,
            representative_doc_type,
            representative_company_count,
            cluster_supplier_count,
            cluster_contract_count,
            cluster_total_contract_value,
            cluster_direct_or_exception_count,
            cluster_first_signing_date,
            cluster_last_signing_date,
            supplier_contract_count,
            supplier_contract_value,
            supplier_direct_or_exception_count,
            supplier_first_signing_date,
            supplier_last_signing_date,
            cluster_supplier_samples,
            true AS exact_representative_bridge_flag,
            (cluster_supplier_count >= 2) AS same_buyer_multi_supplier_flag,
            (cluster_total_contract_value >= 5000000000) AS high_value_cluster_flag,
            (
                cluster_direct_or_exception_count >= 2
                OR cluster_total_contract_value >= 10000000000
                OR cluster_contract_count >= 5
            ) AS direct_or_high_volume_cluster_flag,
            (representative_company_count <= 10) AS bounded_representative_flag,
            (
                'Exact RUES legal-representative overlap among same-buyer '
                || 'supplier companies does not prove beneficial ownership, '
                || 'control, collusion, simulated competition, contract splitting, '
                || 'legal disability, breach, nonperformance, or corrupt intent; '
                || 'it prioritizes buyer-year related-supplier cluster review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    coalesce(registry_evidence_refs, []::VARCHAR[]),
                    coalesce(supplier_evidence_refs, []::VARCHAR[])
                ),
                coalesce(cluster_evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN cluster_total_contract_value >= 50000000000
                        OR cluster_direct_or_exception_count >= 10
                        THEN 1
                    ELSE 0
                END DESC,
                cluster_total_contract_value DESC NULLS LAST,
                cluster_contract_count DESC,
                representative_document_key,
                buyer_document_id,
                signing_year,
                supplier_document_key
        ) <= 1000
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


def _create_related_bidders_same_process_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not (
        {"secop_offers", "secop_ii_contracts", "company_registry_c82u"}
        <= set(required_sources)
    ):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_related_bidders_same_process AS
        WITH shared_companies AS (
            SELECT DISTINCT
                entity_key AS company_document_key,
                company_nit_canonical,
                company_document_id,
                coalesce(company_name, supplier_name) AS company_name,
                representative_document_key,
                representative_document_id,
                representative_name,
                representative_doc_type,
                scope_key AS shared_officer_scope_key,
                linked_company_count,
                cluster_contract_count,
                cluster_distinct_buyer_count,
                cluster_total_contract_value,
                evidence_refs AS shared_officer_evidence_refs
            FROM curated_related_companies_shared_officer
            WHERE entity_key IS NOT NULL
                AND representative_document_key IS NOT NULL
                AND length(entity_key) >= 5
                AND NOT regexp_matches(entity_key, '^0+$')
                AND entity_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111',
                    '123456789',
                    '1234567890'
                )
                AND length(representative_document_key) >= 5
                AND NOT regexp_matches(representative_document_key, '^0+$')
        ),
        award_candidates AS (
            SELECT
                nullif(trim(process_id), '') AS process_key,
                supplier_document_key AS winner_document_key,
                supplier_nit_canonical AS winner_nit_canonical,
                supplier_document_digits AS winner_document_id,
                supplier_name AS winner_name,
                buyer_document_digits AS buyer_document_key,
                buyer_document_id,
                buyer_name,
                department,
                city,
                procurement_modality,
                contract_type,
                contract_id,
                contract_reference,
                contract_value,
                signing_date,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS contract_evidence_ref,
                row_number() OVER (
                    PARTITION BY nullif(trim(process_id), ''), supplier_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS award_rank
            FROM curated_contract_awards
            WHERE nullif(trim(process_id), '') IS NOT NULL
                AND supplier_document_key IN (
                    SELECT company_document_key FROM shared_companies
                )
                AND supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value >= 500000000
        ),
        awards AS (
            SELECT *
            FROM award_candidates
            WHERE award_rank = 1
        ),
        offer_supplier AS (
            SELECT
                coalesce(
                    nullif(trim(id_del_proceso_de_compra), ''),
                    nullif(trim(referencia_del_proceso), '')
                ) AS process_key,
                coacc_document_key(nit_del_proveedor, 'NIT') AS supplier_document_key,
                any_value(coacc_nit_canonical(nit_del_proveedor, 'NIT'))
                    AS supplier_nit_canonical,
                any_value(nullif(trim(nit_del_proveedor), ''))
                    AS supplier_document_id,
                any_value(nullif(trim(nombre_proveedor), '')) AS supplier_name,
                any_value(nullif(trim(referencia_de_la_oferta), ''))
                    AS offer_reference,
                any_value(nullif(trim(identificador_de_la_oferta), ''))
                    AS offer_id,
                max(coacc_money_decimal(valor_de_la_oferta)) AS supplier_offer_value,
                min(try_cast(fecha_de_registro AS TIMESTAMP)) AS first_offer_at,
                max(try_cast(fecha_de_registro AS TIMESTAMP)) AS last_offer_at,
                count(*) AS raw_offer_rows,
                count(DISTINCT coalesce(
                    nullif(trim(identificador_de_la_oferta), ''),
                    nullif(trim(referencia_de_la_oferta), ''),
                    nullif(trim(cast(":id" AS VARCHAR)), '')
                )) AS offer_record_count
            FROM src_secop_offers
            WHERE coalesce(
                    nullif(trim(id_del_proceso_de_compra), ''),
                    nullif(trim(referencia_del_proceso), '')
                ) IS NOT NULL
                AND coacc_document_key(nit_del_proveedor, 'NIT') IS NOT NULL
                AND length(coacc_document_key(nit_del_proveedor, 'NIT')) >= 5
                AND NOT regexp_matches(
                    coacc_document_key(nit_del_proveedor, 'NIT'),
                    '^0+$'
                )
                AND coacc_document_key(nit_del_proveedor, 'NIT') NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111',
                    '123456789',
                    '1234567890'
                )
                AND upper(trim(coalesce(cast(nit_del_proveedor AS VARCHAR), '')))
                    NOT IN ('NO APLICA', 'N/A', 'NA', 'NO REGISTRA', 'SIN NIT')
            GROUP BY 1, 2
        ),
        process_stats AS (
            SELECT
                process_key,
                count(*) AS process_supplier_count,
                sum(raw_offer_rows) AS process_raw_offer_rows,
                sum(coalesce(supplier_offer_value, 0.0)) AS process_offer_value
            FROM offer_supplier
            GROUP BY process_key
        ),
        candidate_links AS (
            SELECT
                a.process_key,
                a.contract_id,
                a.contract_reference,
                a.contract_value,
                a.signing_date,
                a.contract_evidence_ref,
                a.buyer_document_key,
                a.buyer_document_id,
                a.buyer_name,
                a.department,
                a.city,
                a.procurement_modality,
                a.contract_type,
                a.winner_document_key,
                a.winner_nit_canonical,
                a.winner_document_id,
                coalesce(a.winner_name, sw.company_name) AS winner_name,
                o.supplier_document_key AS related_offerer_document_key,
                o.supplier_nit_canonical AS related_offerer_nit_canonical,
                o.supplier_document_id AS related_offerer_document_id,
                coalesce(o.supplier_name, so.company_name) AS related_offerer_name,
                o.offer_reference,
                o.offer_id,
                o.supplier_offer_value AS related_offer_value,
                o.first_offer_at AS related_first_offer_at,
                o.last_offer_at AS related_last_offer_at,
                o.offer_record_count AS related_offer_record_count,
                ps.process_supplier_count,
                ps.process_raw_offer_rows,
                ps.process_offer_value,
                sw.representative_document_key,
                sw.representative_document_id,
                sw.representative_name,
                sw.representative_doc_type,
                sw.linked_company_count,
                sw.cluster_contract_count,
                sw.cluster_distinct_buyer_count,
                sw.cluster_total_contract_value,
                sw.shared_officer_scope_key AS winner_shared_officer_scope_key,
                so.shared_officer_scope_key AS related_shared_officer_scope_key,
                sw.shared_officer_evidence_refs
                    AS winner_shared_officer_evidence_refs,
                so.shared_officer_evidence_refs
                    AS related_shared_officer_evidence_refs,
                'secop_offers:' || a.process_key || ':' || coalesce(
                    o.offer_id,
                    o.offer_reference,
                    o.supplier_document_key
                ) AS offer_evidence_ref
            FROM awards a
            JOIN shared_companies sw
                ON sw.company_document_key = a.winner_document_key
            JOIN offer_supplier o
                ON o.process_key = a.process_key
                AND o.supplier_document_key != a.winner_document_key
            JOIN shared_companies so
                ON so.company_document_key = o.supplier_document_key
                AND so.representative_document_key = sw.representative_document_key
            JOIN process_stats ps
                ON ps.process_key = a.process_key
            WHERE ps.process_supplier_count >= 2
        ),
        scored AS (
            SELECT
                *,
                count(DISTINCT process_key) OVER (
                    PARTITION BY
                        winner_document_key,
                        related_offerer_document_key,
                        representative_document_key
                ) AS repeated_pair_process_count,
                count(DISTINCT buyer_document_key) OVER (
                    PARTITION BY
                        winner_document_key,
                        related_offerer_document_key,
                        representative_document_key
                ) AS repeated_pair_buyer_count
            FROM candidate_links
        ),
        gated AS (
            SELECT
                *,
                process_supplier_count <= 8 AS bounded_competition_flag,
                repeated_pair_process_count >= 2 AS repeated_pair_flag,
                contract_value >= 1000000000 AS very_high_value_flag
            FROM scored
            WHERE contract_value >= 500000000
                AND (
                    process_supplier_count <= 8
                    OR repeated_pair_process_count >= 2
                    OR contract_value >= 1000000000
                )
        )
        SELECT
            'procurement_related_bidders_same_process_review_only' AS signal_id,
            'doc:' || winner_document_key AS entity_id,
            winner_document_key AS entity_key,
            'Company' AS entity_label,
            'related_bidders_process:' || process_key || ':'
                || winner_document_key || ':' || related_offerer_document_key
                || ':' || representative_document_key AS scope_key,
            'related_bidders_process' AS scope_type,
            CASE
                WHEN very_high_value_flag OR repeated_pair_flag THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                1.0,
                0.70
                    + CASE WHEN bounded_competition_flag THEN 0.08 ELSE 0.0 END
                    + CASE WHEN process_supplier_count <= 3 THEN 0.04 ELSE 0.0 END
                    + CASE WHEN repeated_pair_flag THEN 0.08 ELSE 0.0 END
                    + CASE WHEN very_high_value_flag THEN 0.06 ELSE 0.0 END
                    + least(log10(greatest(contract_value, 1)) / 160.0, 0.06)
                    + least(linked_company_count / 50.0, 0.04)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            process_key,
            contract_id,
            contract_reference,
            contract_value,
            signing_date,
            buyer_document_key,
            buyer_document_id,
            buyer_name,
            department,
            city,
            procurement_modality,
            contract_type,
            winner_nit_canonical,
            winner_document_id,
            winner_name,
            related_offerer_document_key,
            related_offerer_nit_canonical,
            related_offerer_document_id,
            related_offerer_name,
            related_offer_value,
            related_first_offer_at,
            related_last_offer_at,
            related_offer_record_count,
            process_supplier_count,
            process_raw_offer_rows,
            process_offer_value,
            repeated_pair_process_count,
            repeated_pair_buyer_count,
            representative_document_key,
            representative_document_id,
            representative_name,
            representative_doc_type,
            linked_company_count,
            cluster_contract_count,
            cluster_distinct_buyer_count,
            cluster_total_contract_value,
            bounded_competition_flag,
            repeated_pair_flag,
            very_high_value_flag,
            winner_shared_officer_scope_key,
            related_shared_officer_scope_key,
            (
                'Exact same-process winner/offerer linkage plus shared RUES '
                || 'representative does not prove collusion, beneficial ownership, '
                || 'control, simulated competition, bid suppression, legal '
                || 'ineligibility, contract breach, or corrupt intent; it '
                || 'prioritizes reviewer verification of relationship and process files'
            ) AS what_is_unproven,
            list_concat(
                list_filter(
                    [
                        contract_evidence_ref,
                        offer_evidence_ref,
                        'signal_feature_procurement_related_companies_shared_officer:'
                            || winner_shared_officer_scope_key || ':'
                            || winner_document_key,
                        'signal_feature_procurement_related_companies_shared_officer:'
                            || related_shared_officer_scope_key || ':'
                            || related_offerer_document_key
                    ],
                    item -> item IS NOT NULL
                ),
                list_concat(
                    coalesce(winner_shared_officer_evidence_refs, []::VARCHAR[]),
                    coalesce(related_shared_officer_evidence_refs, []::VARCHAR[])
                )
            ) AS evidence_refs
        FROM gated
        QUALIFY row_number() OVER (
            PARTITION BY
                process_key,
                winner_document_key,
                related_offerer_document_key,
                representative_document_key
            ORDER BY contract_value DESC NULLS LAST,
                related_offer_value DESC NULLS LAST,
                signing_date DESC NULLS LAST,
                contract_id
        ) = 1
    """)


def _create_rues_supplier_capacity_status_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_ii_contracts", "company_registry_c82u"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_rues_supplier_capacity_status AS
        WITH supplier_exposure AS (
            SELECT
                supplier_document_key,
                supplier_nit_canonical,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT buyer_document_id) AS buyer_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND supplier_nit_canonical IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
                AND length(supplier_document_key) >= 5
                AND NOT regexp_matches(supplier_document_key, '^0+$')
                AND supplier_document_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
            GROUP BY supplier_document_key, supplier_nit_canonical
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
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key
        ),
        registry_raw AS (
            SELECT
                coacc_document_key(document_id, identification_class) AS doc_key,
                coacc_nit_canonical(document_id, identification_class)
                    AS company_nit_canonical,
                nullif(trim(document_id), '') AS document_id,
                nullif(trim(identification_class), '') AS identification_class,
                nullif(trim(business_name), '') AS business_name,
                nullif(trim(matricula), '') AS matricula,
                nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                nullif(trim(matricula_category), '') AS matricula_category,
                nullif(trim(ciiu_primary_activity_code), '')
                    AS ciiu_primary_activity_code,
                nullif(trim(matricula_status_code), '') AS matricula_status_code,
                nullif(trim(matricula_status), '') AS matricula_status,
                lower(coalesce(cast(matricula_status AS VARCHAR), ''))
                    AS matricula_status_norm,
                coacc_parse_date(cancellation_date) AS cancellation_date,
                coacc_parse_date(matricula_date) AS matricula_date,
                coacc_parse_date(validity_date) AS validity_date,
                coacc_parse_date(update_date) AS update_date,
                coacc_parse_date(fecha_renovacion) AS renewal_date,
                try_cast(last_renewed_year AS INTEGER) AS last_renewed_year,
                coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    nullif(trim(matricula), ''),
                    nullif(trim(document_id), '')
                ) AS company_record_id
            FROM src_company_registry_c82u
            WHERE coacc_document_key(document_id, identification_class) IS NOT NULL
                AND coacc_nit_canonical(document_id, identification_class) IS NOT NULL
                AND coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    nullif(trim(matricula), ''),
                    nullif(trim(document_id), '')
                ) IS NOT NULL
        ),
        registry_flags AS (
            SELECT
                *,
                (
                    regexp_matches(
                        matricula_status_norm,
                        'activa|nueva|constituci'
                    )
                    AND NOT regexp_matches(
                        matricula_status_norm,
                        'cancel|inactiv|suspend|no matric'
                    )
                ) AS active_like,
                regexp_matches(
                    matricula_status_norm,
                    'cancel|liquida|inactiv|suspend'
                ) AS inactive_like,
                coalesce(
                    cancellation_date,
                    update_date,
                    renewal_date,
                    matricula_date,
                    DATE '1900-01-01'
                ) AS evidence_sort_date
            FROM registry_raw
        ),
        registry_rollup AS (
            SELECT
                doc_key,
                any_value(company_nit_canonical) AS company_nit_canonical,
                any_value(document_id) AS document_id,
                any_value(identification_class) AS identification_class,
                arg_max(business_name, evidence_sort_date) AS company_name,
                count(*) AS registry_row_count,
                count(DISTINCT matricula) AS matricula_count,
                count(DISTINCT chamber_of_commerce) AS chamber_count,
                bool_or(active_like) AS has_active_registry_row,
                bool_or(inactive_like) AS has_inactive_registry_row,
                max(cancellation_date) AS latest_cancellation_date,
                min(matricula_date) AS first_matricula_date,
                max(matricula_date) AS latest_matricula_date,
                max(validity_date) AS latest_validity_date,
                max(renewal_date) AS latest_renewal_date,
                max(last_renewed_year) AS last_renewed_year,
                arg_max(matricula_status, evidence_sort_date) AS evidence_matricula_status,
                arg_max(matricula_status_code, evidence_sort_date)
                    AS evidence_matricula_status_code,
                arg_max(matricula, evidence_sort_date) AS evidence_matricula,
                arg_max(chamber_of_commerce, evidence_sort_date)
                    AS evidence_chamber_of_commerce,
                arg_max(matricula_category, evidence_sort_date)
                    AS evidence_matricula_category,
                arg_max(ciiu_primary_activity_code, evidence_sort_date)
                    AS evidence_ciiu_primary_activity_code,
                arg_max(company_record_id, evidence_sort_date) AS evidence_record_id
            FROM registry_flags
            GROUP BY doc_key
        ),
        registry_evidence_ranked AS (
            SELECT
                doc_key,
                'company_registry_c82u:' || company_record_id AS evidence_ref,
                inactive_like,
                active_like,
                evidence_sort_date,
                company_record_id,
                row_number() OVER (
                    PARTITION BY doc_key
                    ORDER BY
                        CASE WHEN inactive_like THEN 1 ELSE 0 END DESC,
                        evidence_sort_date DESC NULLS LAST,
                        company_record_id
                ) AS evidence_rank
            FROM registry_flags
        ),
        registry_evidence AS (
            SELECT
                doc_key,
                list(evidence_ref ORDER BY inactive_like DESC, evidence_sort_date DESC)
                    AS registry_evidence_refs
            FROM registry_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY doc_key
        ),
        joined AS (
            SELECT
                e.*,
                r.company_nit_canonical,
                r.document_id AS registry_document_id,
                r.identification_class AS registry_document_type,
                r.company_name,
                r.registry_row_count,
                r.matricula_count,
                r.chamber_count,
                r.has_active_registry_row,
                r.has_inactive_registry_row,
                r.latest_cancellation_date,
                r.first_matricula_date,
                r.latest_matricula_date,
                r.latest_validity_date,
                r.latest_renewal_date,
                r.last_renewed_year,
                r.evidence_matricula_status,
                r.evidence_matricula_status_code,
                r.evidence_matricula,
                r.evidence_chamber_of_commerce AS chamber_of_commerce,
                r.evidence_matricula_category AS matricula_category,
                r.evidence_ciiu_primary_activity_code AS ciiu_primary_activity_code,
                r.evidence_record_id,
                (
                    r.has_inactive_registry_row
                    AND NOT r.has_active_registry_row
                    AND r.latest_cancellation_date IS NOT NULL
                    AND e.last_signing_date > r.latest_cancellation_date
                    AND e.total_contract_value >= 500000000
                ) AS post_inactive_contract_flag,
                (
                    r.has_active_registry_row
                    AND r.last_renewed_year IS NOT NULL
                    AND r.last_renewed_year <= 2021
                    AND e.last_signing_date >= DATE '2024-01-01'
                    AND e.total_contract_value >= 2000000000
                ) AS stale_renewal_high_value_flag,
                (
                    r.first_matricula_date IS NOT NULL
                    AND e.first_signing_date BETWEEN r.first_matricula_date
                        AND r.first_matricula_date + INTERVAL 180 DAY
                    AND (
                        e.total_contract_value >= 1000000000
                        OR e.max_contract_value >= 500000000
                    )
                ) AS recent_registration_large_award_flag,
                (
                    r.matricula_count >= 3
                    AND e.total_contract_value >= 1000000000
                ) AS multi_matricula_supplier_flag,
                list_concat(re.registry_evidence_refs, ce.contract_evidence_refs)
                    AS evidence_refs
            FROM supplier_exposure e
            JOIN registry_rollup r
                ON r.doc_key = e.supplier_document_key
            JOIN registry_evidence re
                ON re.doc_key = e.supplier_document_key
            JOIN contract_evidence ce
                ON ce.supplier_document_key = e.supplier_document_key
        ),
        scored AS (
            SELECT
                *,
                cast(post_inactive_contract_flag AS INTEGER)
                    + cast(stale_renewal_high_value_flag AS INTEGER)
                    + cast(recent_registration_large_award_flag AS INTEGER)
                    + cast(multi_matricula_supplier_flag AS INTEGER)
                    AS capacity_flag_count,
                list_filter(
                    [
                        CASE
                            WHEN post_inactive_contract_flag
                                THEN 'post_inactive_contract'
                            ELSE NULL
                        END,
                        CASE
                            WHEN stale_renewal_high_value_flag
                                THEN 'stale_renewal_high_value'
                            ELSE NULL
                        END,
                        CASE
                            WHEN recent_registration_large_award_flag
                                THEN 'recent_registration_large_award'
                            ELSE NULL
                        END,
                        CASE
                            WHEN multi_matricula_supplier_flag
                                THEN 'multi_matricula_supplier'
                            ELSE NULL
                        END
                    ],
                    item -> item IS NOT NULL
                ) AS capacity_review_types
            FROM joined
            WHERE post_inactive_contract_flag
                OR stale_renewal_high_value_flag
                OR recent_registration_large_award_flag
                OR multi_matricula_supplier_flag
        )
        SELECT
            'rues_supplier_capacity_status_review_only' AS signal_id,
            coalesce('doc:' || supplier_document_key, 'doc:' || company_nit_canonical)
                AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            'rues_capacity:' || supplier_document_key AS scope_key,
            'rues_supplier_capacity_status' AS scope_type,
            CASE
                WHEN post_inactive_contract_flag
                    OR capacity_flag_count >= 2
                    OR total_contract_value >= 50000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.96,
                0.48
                    + least(capacity_flag_count * 0.13, 0.32)
                    + CASE WHEN post_inactive_contract_flag THEN 0.12 ELSE 0.0 END
                    + least(log10(greatest(total_contract_value, 1)) / 120.0, 0.10)
                    + least(contract_count / 500.0, 0.06)
                    + least(buyer_count / 100.0, 0.06)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_nit_canonical,
            company_nit_canonical,
            registry_document_id,
            registry_document_type,
            supplier_name,
            company_name,
            contract_count,
            buyer_count,
            total_contract_value,
            max_contract_value,
            first_signing_date,
            last_signing_date,
            registry_row_count,
            matricula_count,
            chamber_count,
            has_active_registry_row,
            has_inactive_registry_row,
            latest_cancellation_date,
            first_matricula_date,
            latest_matricula_date,
            latest_validity_date,
            latest_renewal_date,
            last_renewed_year,
            evidence_matricula_status,
            evidence_matricula_status_code,
            evidence_matricula,
            chamber_of_commerce,
            matricula_category,
            ciiu_primary_activity_code,
            post_inactive_contract_flag,
            stale_renewal_high_value_flag,
            recent_registration_large_award_flag,
            multi_matricula_supplier_flag,
            capacity_flag_count,
            capacity_review_types,
            'RUES status, renewal, registration age, or multiple-registration pattern combined with procurement exposure; legal capacity, actual beneficial ownership, contract invalidity, and corrupt intent are not proven by this feature'
                AS what_is_unproven,
            evidence_refs
        FROM scored
        QUALIFY row_number() OVER (
            ORDER BY capacity_flag_count DESC,
                total_contract_value DESC NULLS LAST,
                supplier_document_key
        ) <= 5000
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


def _create_project_bpin_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_process_bpin", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_project_bpin_procurement_overlap AS
        WITH bpin_links AS (
            SELECT DISTINCT
                nullif(trim(codigo_bpin), '') AS bpin_code,
                nullif(trim(anno_bpin), '') AS bpin_year,
                nullif(trim(id_proceso), '') AS process_id,
                nullif(trim(id_contracto), '') AS contract_id,
                nullif(trim(id_portafolio), '') AS portfolio_id,
                nullif(trim(validacion_bpin), '') AS validation_status
            FROM src_secop_process_bpin
            WHERE nullif(trim(codigo_bpin), '') IS NOT NULL
                AND nullif(trim(id_contracto), '') IS NOT NULL
                AND lower(trim(coalesce(id_contracto, ''))) != 'no definido'
                AND regexp_matches(nullif(trim(codigo_bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigo_bpin), ''), '^0+$')
                AND lower(coalesce(validacion_bpin, '')) NOT LIKE '%no validado%'
        ),
        joined AS (
            SELECT *
            FROM (
                SELECT
                    b.bpin_code,
                    b.bpin_year,
                    b.process_id AS bpin_process_id,
                    b.contract_id,
                    b.portfolio_id,
                    b.validation_status,
                    c.supplier_entity_id,
                    c.supplier_document_key,
                    c.supplier_nit_canonical,
                    c.supplier_name,
                    c.buyer_document_id,
                    c.buyer_name,
                    c.department,
                    c.city AS municipality,
                    c.sector,
                    c.procurement_modality,
                    c.contract_type,
                    c.contract_reference,
                    c.process_id AS contract_process_id,
                    c.process_url,
                    c.contract_value,
                    c.signing_date,
                    c.contract_start_date,
                    c.contract_end_date,
                    row_number() OVER (
                        PARTITION BY b.bpin_code, b.contract_id
                        ORDER BY c.contract_value DESC NULLS LAST,
                            c.signing_date DESC NULLS LAST,
                            c.award_row_id
                    ) AS contract_rank
                FROM bpin_links b
                JOIN curated_contract_awards c
                    ON c.contract_id = b.contract_id
                WHERE c.contract_value IS NOT NULL
                    AND c.contract_value > 0
            )
            WHERE contract_rank = 1
        ),
        rollup AS (
            SELECT
                bpin_code,
                min(bpin_year) AS bpin_year,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT supplier_document_key)
                    FILTER (WHERE supplier_document_key IS NOT NULL)
                    AS supplier_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS buyer_count,
                count(DISTINCT coalesce(department, ''))
                    FILTER (WHERE department IS NOT NULL)
                    AS department_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM joined
            GROUP BY bpin_code
        ),
        contract_evidence_ranked AS (
            SELECT
                bpin_code,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM joined
        ),
        contract_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC, contract_id)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        bpin_evidence_ranked AS (
            SELECT
                bpin_code,
                'secop_process_bpin:' || bpin_code || ':' || contract_id
                    AS evidence_ref,
                contract_value,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM joined
        ),
        bpin_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref)
                    AS bpin_evidence_refs
            FROM bpin_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        territory_samples_ranked AS (
            SELECT
                bpin_code,
                coalesce(department, 'NACIONAL') || ':' || coalesce(municipality, 'NACIONAL')
                    AS territory_sample,
                sum(contract_value) AS territory_value,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY sum(contract_value) DESC NULLS LAST,
                        coalesce(department, 'NACIONAL'),
                        coalesce(municipality, 'NACIONAL')
                ) AS sample_rank
            FROM joined
            GROUP BY bpin_code,
                coalesce(department, 'NACIONAL'),
                coalesce(municipality, 'NACIONAL')
        ),
        territory_samples AS (
            SELECT
                bpin_code,
                list(territory_sample ORDER BY territory_value DESC, territory_sample)
                    AS territory_sample
            FROM territory_samples_ranked
            WHERE sample_rank <= 10
            GROUP BY bpin_code
        )
        SELECT
            'project_bpin_procurement_overlap' AS signal_id,
            'project:' || r.bpin_code AS entity_id,
            r.bpin_code AS entity_key,
            'Project' AS entity_label,
            'bpin:' || r.bpin_code AS scope_key,
            'project' AS scope_type,
            CASE
                WHEN r.total_contract_value >= 1000000000000
                    OR r.contract_count >= 5000
                    THEN 'high'
                WHEN r.total_contract_value >= 100000000000
                    THEN 'medium'
                ELSE 'low'
            END AS severity,
            least(
                1.0,
                0.50
                    + least(log10(greatest(r.total_contract_value, 1)) / 120.0, 0.18)
                    + least(r.contract_count / 10000.0, 0.14)
                    + least(r.supplier_count / 10000.0, 0.10)
                    + least(r.department_count / 50.0, 0.08)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_BPIN' AS identity_match_type,
            'exact' AS identity_quality,
            r.bpin_year,
            r.contract_count,
            r.supplier_count,
            r.buyer_count,
            r.department_count,
            r.total_contract_value,
            r.max_contract_value,
            r.first_signing_date,
            r.last_signing_date,
            t.territory_sample,
            list_concat(be.bpin_evidence_refs, ce.contract_evidence_refs)
                AS evidence_refs
        FROM rollup r
        JOIN bpin_evidence be
            ON be.bpin_code = r.bpin_code
        JOIN contract_evidence ce
            ON ce.bpin_code = r.bpin_code
        LEFT JOIN territory_samples t
            ON t.bpin_code = r.bpin_code
        WHERE r.total_contract_value >= 100000000000
            AND r.contract_count >= 2
        QUALIFY row_number() OVER (
            ORDER BY r.total_contract_value DESC NULLS LAST,
                r.contract_count DESC,
                r.bpin_code
        ) <= 1000
    """)


def _create_bpin_priority_work_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_process_bpin", "secop_integrado"} <= set(required_sources)):
        return

    con.execute(r"""
        CREATE OR REPLACE TEMP VIEW curated_bpin_dnp_vs_pida27_obras_prioritarias AS
        WITH bpin_links AS (
            SELECT DISTINCT
                nullif(trim(codigo_bpin), '') AS bpin_code,
                nullif(trim(anno_bpin), '') AS bpin_year,
                nullif(trim(id_proceso), '') AS process_id,
                nullif(trim(id_contracto), '') AS contract_id,
                nullif(trim(id_portafolio), '') AS portfolio_id,
                nullif(trim(validacion_bpin), '') AS validation_status
            FROM src_secop_process_bpin
            WHERE nullif(trim(codigo_bpin), '') IS NOT NULL
                AND nullif(trim(id_contracto), '') IS NOT NULL
                AND lower(trim(coalesce(id_contracto, ''))) != 'no definido'
                AND regexp_matches(nullif(trim(codigo_bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigo_bpin), ''), '^0+$')
                AND lower(coalesce(validacion_bpin, '')) NOT LIKE '%no validado%'
        ),
        integrated_contracts AS (
            SELECT *
            FROM (
                SELECT
                    nullif(trim(contract_number), '') AS contract_id,
                    nullif(trim(process_number), '') AS process_id,
                    coacc_document_key(supplier_document, supplier_doc_type)
                        AS supplier_document_key,
                    coacc_nit_canonical(supplier_document, supplier_doc_type)
                        AS supplier_nit_canonical,
                    nullif(trim(supplier_document), '') AS supplier_document_id,
                    nullif(trim(supplier_doc_type), '') AS supplier_doc_type,
                    nullif(trim(contractor_business_name), '') AS supplier_name,
                    nullif(trim(entity_secop_code), '') AS buyer_secop_code,
                    nullif(trim(entity_nit), '') AS buyer_document_id,
                    nullif(trim(entity_name), '') AS buyer_name,
                    upper(coalesce(nullif(trim(entity_department), ''), 'NACIONAL'))
                        AS department,
                    upper(coalesce(nullif(trim(entity_municipality), ''), 'NACIONAL'))
                        AS municipality,
                    nullif(trim(procurement_modality), '') AS procurement_modality,
                    nullif(trim(contract_type), '') AS contract_type,
                    coacc_money(contract_value) AS contract_value,
                    try_cast(contract_signing_date AS DATE) AS signing_date,
                    try_cast(contract_start_date AS DATE) AS contract_start_date,
                    try_cast(contract_end_date AS DATE) AS contract_end_date,
                    coacc_reference_url(contract_url) AS contract_url,
                    lower(
                        coalesce(contract_object, '') || ' ' ||
                        coalesce(process_object, '') || ' ' ||
                        coalesce(contract_type, '') || ' ' ||
                        coalesce(procurement_modality, '')
                    ) AS text_blob,
                    row_number() OVER (
                        PARTITION BY nullif(trim(contract_number), '')
                        ORDER BY coacc_money(contract_value) DESC NULLS LAST,
                            try_cast(contract_signing_date AS DATE) DESC NULLS LAST
                    ) AS contract_rank
                FROM src_secop_integrado
                WHERE nullif(trim(contract_number), '') IS NOT NULL
                    AND coacc_money(contract_value) IS NOT NULL
                    AND coacc_money(contract_value) > 0
            )
            WHERE contract_rank = 1
        ),
        categorized AS (
            SELECT
                b.bpin_code,
                b.bpin_year,
                b.process_id AS bpin_process_id,
                b.contract_id,
                b.portfolio_id,
                b.validation_status,
                c.process_id AS contract_process_id,
                c.supplier_document_key,
                c.supplier_nit_canonical,
                c.supplier_document_id,
                c.supplier_doc_type,
                c.supplier_name,
                c.buyer_secop_code,
                c.buyer_document_id,
                c.buyer_name,
                c.department,
                c.municipality,
                c.procurement_modality,
                c.contract_type,
                c.contract_value,
                c.signing_date,
                c.contract_start_date,
                c.contract_end_date,
                c.contract_url,
                CASE
                    WHEN regexp_matches(
                        c.text_blob,
                        '(v[ií]a |vial|carretera|paviment|placa huella|puente)'
                    ) THEN 'roads_transport'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(acueducto|alcantarillado|agua potable|saneamiento|ptar|ptap)'
                    ) THEN 'water_sanitation'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(hospital|salud|m[eé]dic|ambulancia|biom[eé]dic)'
                    ) THEN 'health'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(educaci[oó]n|colegio|instituci[oó]n educativa|aula)'
                    ) THEN 'education'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(vivienda|habitacional|urbanizaci[oó]n)'
                    ) THEN 'housing'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(energ[ií]a|el[eé]ctric|alumbrado|solar|gas combustible)'
                    ) THEN 'energy'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(deporte|recreaci[oó]n|cancha|parque|cultura)'
                    ) THEN 'sport_culture'
                    WHEN regexp_matches(
                        c.text_blob,
                        '(obra|infraestructura|construcci[oó]n|mejoramiento|'
                        || 'rehabilitaci[oó]n|interventor[ií]a)'
                    ) THEN 'public_infrastructure'
                    ELSE NULL
                END AS priority_work_category
            FROM bpin_links b
            JOIN integrated_contracts c
                ON c.contract_id = b.contract_id
            WHERE c.municipality NOT IN ('NO DEFINIDO', 'NACIONAL')
        ),
        priority_contracts AS (
            SELECT *
            FROM categorized
            WHERE priority_work_category IS NOT NULL
        ),
        rollup AS (
            SELECT
                bpin_code,
                min(bpin_year) AS bpin_year,
                any_value(department) AS department,
                any_value(municipality) AS municipality,
                count(DISTINCT contract_id) AS priority_contract_count,
                count(DISTINCT priority_work_category) AS priority_category_count,
                count(DISTINCT supplier_document_key)
                    FILTER (WHERE supplier_document_key IS NOT NULL)
                    AS supplier_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS buyer_count,
                count(DISTINCT department) AS department_count,
                count(DISTINCT municipality) AS municipality_count,
                sum(contract_value) AS priority_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date,
                list(DISTINCT priority_work_category ORDER BY priority_work_category)
                    AS priority_work_categories
            FROM priority_contracts
            GROUP BY bpin_code
        ),
        bpin_evidence_ranked AS (
            SELECT
                bpin_code,
                'secop_process_bpin:' || bpin_code || ':' || contract_id
                    AS evidence_ref,
                contract_value,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM priority_contracts
        ),
        bpin_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref)
                    AS bpin_evidence_refs
            FROM bpin_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        contract_evidence_ranked AS (
            SELECT
                bpin_code,
                coalesce(contract_url, 'secop_integrado:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM priority_contracts
        ),
        contract_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        )
        SELECT
            'bpin_dnp_vs_pida27_obras_prioritarias' AS signal_id,
            'project:' || r.bpin_code AS entity_id,
            r.bpin_code AS entity_key,
            'Project' AS entity_label,
            'bpin_priority_work:' || r.bpin_code AS scope_key,
            'project' AS scope_type,
            CASE
                WHEN r.priority_contract_value >= 100000000000
                    OR r.priority_contract_count >= 10
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.50
                    + least(log10(greatest(r.priority_contract_value, 1)) / 120.0, 0.18)
                    + least(r.priority_contract_count / 100.0, 0.14)
                    + least(r.supplier_count / 100.0, 0.08)
                    + least(r.priority_category_count / 10.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_BPIN_CONTRACT_LINK' AS identity_match_type,
            'exact' AS identity_quality,
            r.bpin_year,
            r.department,
            r.municipality,
            r.priority_contract_count,
            r.priority_category_count,
            r.supplier_count,
            r.buyer_count,
            r.department_count,
            r.municipality_count,
            r.priority_contract_value,
            r.max_contract_value,
            r.first_signing_date,
            r.last_signing_date,
            r.priority_work_categories,
            list_concat(be.bpin_evidence_refs, ce.contract_evidence_refs)
                AS evidence_refs
        FROM rollup r
        JOIN bpin_evidence be
            ON be.bpin_code = r.bpin_code
        JOIN contract_evidence ce
            ON ce.bpin_code = r.bpin_code
        WHERE r.priority_contract_count >= 3
            AND r.priority_contract_value >= 25000000000
        QUALIFY row_number() OVER (
            ORDER BY r.priority_contract_value DESC NULLS LAST,
                r.priority_contract_count DESC,
                r.bpin_code
        ) <= 1000
    """)


def _create_project_regalias_execution_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not (
        {
            "sgr_expense_execution",
            "sgr_projects",
            "secop_process_bpin",
            "secop_ii_contracts",
        }
        <= set(required_sources)
    ):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_project_regalias_execution_procurement_overlap AS
        WITH bpin_links AS (
            SELECT DISTINCT
                nullif(trim(codigo_bpin), '') AS bpin_code,
                nullif(trim(anno_bpin), '') AS bpin_year,
                nullif(trim(id_proceso), '') AS process_id,
                nullif(trim(id_contracto), '') AS contract_id,
                nullif(trim(id_portafolio), '') AS portfolio_id,
                nullif(trim(validacion_bpin), '') AS validation_status
            FROM src_secop_process_bpin
            WHERE nullif(trim(codigo_bpin), '') IS NOT NULL
                AND nullif(trim(id_contracto), '') IS NOT NULL
                AND lower(trim(coalesce(id_contracto, ''))) != 'no definido'
                AND regexp_matches(nullif(trim(codigo_bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigo_bpin), ''), '^0+$')
                AND lower(coalesce(validacion_bpin, '')) NOT LIKE '%no validado%'
        ),
        contract_joined AS (
            SELECT *
            FROM (
                SELECT
                    b.bpin_code,
                    b.bpin_year,
                    b.process_id AS bpin_process_id,
                    b.contract_id,
                    b.portfolio_id,
                    b.validation_status,
                    c.supplier_document_key,
                    c.supplier_name,
                    c.buyer_document_id,
                    c.buyer_name,
                    c.department,
                    c.city AS municipality,
                    c.sector,
                    c.contract_reference,
                    c.process_url,
                    c.contract_value,
                    c.signing_date,
                    row_number() OVER (
                        PARTITION BY b.bpin_code, b.contract_id
                        ORDER BY c.contract_value DESC NULLS LAST,
                            c.signing_date DESC NULLS LAST,
                            c.award_row_id
                    ) AS contract_rank
                FROM bpin_links b
                JOIN curated_contract_awards c
                    ON c.contract_id = b.contract_id
                WHERE c.contract_value IS NOT NULL
                    AND c.contract_value > 0
            )
            WHERE contract_rank = 1
        ),
        contract_rollup AS (
            SELECT
                bpin_code,
                min(bpin_year) AS bpin_year,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT supplier_document_key)
                    FILTER (WHERE supplier_document_key IS NOT NULL)
                    AS supplier_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS buyer_count,
                count(DISTINCT coalesce(department, ''))
                    FILTER (WHERE department IS NOT NULL)
                    AS department_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM contract_joined
            GROUP BY bpin_code
        ),
        expense_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(period), '') AS period_code,
                try_strptime(nullif(trim(period), ''), '%Y%m%d') AS period_date,
                nullif(trim(entity_code), '') AS entity_code,
                nullif(trim(entity_name), '') AS entity_name,
                nullif(trim(account), '') AS account,
                nullif(trim(account_name), '') AS account_name,
                try_cast(replace(cast(commitments AS VARCHAR), ',', '.') AS DOUBLE)
                    AS commitments_value,
                try_cast(replace(cast(obligations AS VARCHAR), ',', '.') AS DOUBLE)
                    AS obligations_value,
                try_cast(replace(cast(payments AS VARCHAR), ',', '.') AS DOUBLE)
                    AS payments_value
            FROM src_sgr_expense_execution
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        expense_rollup AS (
            SELECT
                bpin_code,
                count(*) AS expense_row_count,
                count(DISTINCT entity_code)
                    FILTER (WHERE entity_code IS NOT NULL)
                    AS execution_entity_count,
                sum(coalesce(commitments_value, 0)) AS commitments_total,
                sum(coalesce(obligations_value, 0)) AS obligations_total,
                sum(coalesce(payments_value, 0)) AS payments_total,
                min(period_date) AS first_execution_period,
                max(period_date) AS last_execution_period
            FROM expense_raw
            GROUP BY bpin_code
        ),
        project_rollup AS (
            SELECT
                nullif(trim(codigobpin), '') AS bpin_code,
                any_value(nullif(trim(nombre), '')) AS project_title,
                any_value(nullif(trim(estado), '')) AS project_status,
                any_value(nullif(trim(departamento), '')) AS project_department,
                any_value(nullif(trim(sector), '')) AS project_sector,
                any_value(nullif(trim(codejecutor), '')) AS executor_code,
                any_value(nullif(trim(entidadejecutora), '')) AS executor_name,
                max(try_cast(replace(cast(valortotal AS VARCHAR), ',', '.') AS DOUBLE))
                    AS project_total_value,
                max(
                    try_cast(replace(cast(ejecucionfinanciera AS VARCHAR), ',', '.') AS DOUBLE)
                ) AS financial_execution_pct,
                max(
                    try_cast(replace(cast(ejecucionfisica AS VARCHAR), ',', '.') AS DOUBLE)
                ) AS physical_execution_pct
            FROM src_sgr_projects
            WHERE regexp_matches(nullif(trim(codigobpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigobpin), ''), '^0+$')
            GROUP BY nullif(trim(codigobpin), '')
        ),
        joined AS (
            SELECT
                c.bpin_code,
                c.bpin_year,
                p.project_title,
                p.project_status,
                p.project_department,
                p.project_sector,
                p.executor_code,
                p.executor_name,
                p.project_total_value,
                p.financial_execution_pct,
                p.physical_execution_pct,
                c.contract_count,
                c.supplier_count,
                c.buyer_count,
                c.department_count,
                c.total_contract_value,
                c.max_contract_value,
                c.first_signing_date,
                c.last_signing_date,
                e.expense_row_count,
                e.execution_entity_count,
                e.commitments_total,
                e.obligations_total,
                e.payments_total,
                greatest(
                    coalesce(e.commitments_total, 0),
                    coalesce(e.obligations_total, 0),
                    coalesce(e.payments_total, 0)
                ) AS max_sgr_execution_value,
                e.first_execution_period,
                e.last_execution_period
            FROM contract_rollup c
            JOIN expense_rollup e
                ON e.bpin_code = c.bpin_code
            JOIN project_rollup p
                ON p.bpin_code = c.bpin_code
            WHERE c.contract_count >= 2
                AND c.total_contract_value >= 50000000000
                AND greatest(
                    coalesce(e.commitments_total, 0),
                    coalesce(e.obligations_total, 0),
                    coalesce(e.payments_total, 0)
                ) >= 50000000000
        ),
        project_evidence AS (
            SELECT
                bpin_code,
                ['sgr_projects:' || bpin_code] AS project_evidence_refs
            FROM joined
        ),
        expense_evidence_ranked AS (
            SELECT
                bpin_code,
                'sgr_expense_execution:' || bpin_code || ':' ||
                    coalesce(period_code, 'period') || ':' ||
                    coalesce(entity_code, 'entity') || ':' ||
                    coalesce(account, 'account') AS evidence_ref,
                greatest(
                    coalesce(commitments_value, 0),
                    coalesce(obligations_value, 0),
                    coalesce(payments_value, 0)
                ) AS execution_value,
                period_date,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY greatest(
                            coalesce(commitments_value, 0),
                            coalesce(obligations_value, 0),
                            coalesce(payments_value, 0)
                        ) DESC NULLS LAST,
                        period_date DESC NULLS LAST,
                        entity_code,
                        account
                ) AS evidence_rank
            FROM expense_raw
        ),
        expense_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY execution_value DESC, period_date DESC, evidence_ref)
                    AS expense_evidence_refs
            FROM expense_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        bpin_evidence_ranked AS (
            SELECT
                bpin_code,
                'secop_process_bpin:' || bpin_code || ':' || contract_id
                    AS evidence_ref,
                contract_value,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM contract_joined
        ),
        bpin_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref)
                    AS bpin_evidence_refs
            FROM bpin_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        contract_evidence_ranked AS (
            SELECT
                bpin_code,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM contract_joined
        ),
        contract_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC, contract_id)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        )
        SELECT
            'project_regalias_execution_procurement_overlap' AS signal_id,
            'project:' || j.bpin_code AS entity_id,
            j.bpin_code AS entity_key,
            'Project' AS entity_label,
            'sgr_bpin:' || j.bpin_code AS scope_key,
            'project' AS scope_type,
            CASE
                WHEN j.total_contract_value >= 100000000000
                    AND j.max_sgr_execution_value >= 100000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.50
                    + least(log10(greatest(j.total_contract_value, 1)) / 120.0, 0.16)
                    + least(log10(greatest(j.max_sgr_execution_value, 1)) / 120.0, 0.16)
                    + least(j.contract_count / 1000.0, 0.10)
                    + least(j.expense_row_count / 100.0, 0.08)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_BPIN' AS identity_match_type,
            'exact' AS identity_quality,
            j.bpin_year,
            j.project_title,
            j.project_status,
            j.project_department,
            j.project_sector,
            j.executor_code,
            j.executor_name,
            j.project_total_value,
            j.financial_execution_pct,
            j.physical_execution_pct,
            j.contract_count,
            j.supplier_count,
            j.buyer_count,
            j.department_count,
            j.total_contract_value,
            j.max_contract_value,
            j.first_signing_date,
            j.last_signing_date,
            j.expense_row_count,
            j.execution_entity_count,
            j.commitments_total,
            j.obligations_total,
            j.payments_total,
            j.max_sgr_execution_value,
            j.first_execution_period,
            j.last_execution_period,
            list_concat(
                list_concat(
                    list_concat(pe.project_evidence_refs, ee.expense_evidence_refs),
                    be.bpin_evidence_refs
                ),
                ce.contract_evidence_refs
            ) AS evidence_refs
        FROM joined j
        JOIN project_evidence pe
            ON pe.bpin_code = j.bpin_code
        JOIN expense_evidence ee
            ON ee.bpin_code = j.bpin_code
        JOIN bpin_evidence be
            ON be.bpin_code = j.bpin_code
        JOIN contract_evidence ce
            ON ce.bpin_code = j.bpin_code
        QUALIFY row_number() OVER (
            ORDER BY j.total_contract_value DESC NULLS LAST,
                j.max_sgr_execution_value DESC NULLS LAST,
                j.bpin_code
        ) <= 1000
    """)


def _create_sgr_ocad_executor_capacity_gap_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not (
        {
            "sgr_expense_execution",
            "sgr_projects",
            "secop_process_bpin",
            "secop_ii_contracts",
            "company_registry_c82u",
            "secop_contract_suspensions",
            "secop_contract_modifications",
            "secop_contract_execution",
            "secop_guarantees",
            "secop_sanctions",
        }
        <= set(required_sources)
    ):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_sgr_ocad_executor_capacity_gap AS
        WITH bpin_links AS (
            SELECT DISTINCT
                nullif(trim(codigo_bpin), '') AS bpin_code,
                nullif(trim(anno_bpin), '') AS bpin_year,
                nullif(trim(id_proceso), '') AS process_id,
                nullif(trim(id_contracto), '') AS contract_id,
                nullif(trim(id_portafolio), '') AS portfolio_id,
                nullif(trim(validacion_bpin), '') AS validation_status
            FROM src_secop_process_bpin
            WHERE nullif(trim(codigo_bpin), '') IS NOT NULL
                AND nullif(trim(id_contracto), '') IS NOT NULL
                AND lower(trim(coalesce(id_contracto, ''))) != 'no definido'
                AND regexp_matches(nullif(trim(codigo_bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigo_bpin), ''), '^0+$')
                AND lower(coalesce(validacion_bpin, '')) NOT LIKE '%no validado%'
        ),
        contract_joined AS (
            SELECT *
            FROM (
                SELECT
                    b.bpin_code,
                    b.bpin_year,
                    b.process_id AS bpin_process_id,
                    b.contract_id,
                    b.portfolio_id,
                    b.validation_status,
                    c.supplier_document_key,
                    c.supplier_name,
                    c.buyer_document_id,
                    c.buyer_name,
                    c.department,
                    c.city AS municipality,
                    c.sector,
                    c.contract_reference,
                    c.process_url,
                    c.contract_value,
                    c.signing_date,
                    row_number() OVER (
                        PARTITION BY b.bpin_code, b.contract_id
                        ORDER BY c.contract_value DESC NULLS LAST,
                            c.signing_date DESC NULLS LAST,
                            c.award_row_id
                    ) AS contract_rank
                FROM bpin_links b
                JOIN curated_contract_awards c
                    ON c.contract_id = b.contract_id
                WHERE c.contract_value IS NOT NULL
                    AND c.contract_value > 0
            )
            WHERE contract_rank = 1
        ),
        contract_rollup AS (
            SELECT
                bpin_code,
                min(bpin_year) AS bpin_year,
                count(DISTINCT contract_id) AS contract_count,
                count(DISTINCT supplier_document_key)
                    FILTER (WHERE supplier_document_key IS NOT NULL)
                    AS supplier_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS buyer_count,
                count(DISTINCT coalesce(department, ''))
                    FILTER (WHERE department IS NOT NULL)
                    AS department_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date
            FROM contract_joined
            GROUP BY bpin_code
        ),
        expense_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(period), '') AS period_code,
                try_strptime(nullif(trim(period), ''), '%Y%m%d') AS period_date,
                nullif(trim(entity_code), '') AS entity_code,
                nullif(trim(entity_name), '') AS entity_name,
                nullif(trim(account), '') AS account,
                nullif(trim(account_name), '') AS account_name,
                try_cast(replace(cast(commitments AS VARCHAR), ',', '.') AS DOUBLE)
                    AS commitments_value,
                try_cast(replace(cast(obligations AS VARCHAR), ',', '.') AS DOUBLE)
                    AS obligations_value,
                try_cast(replace(cast(payments AS VARCHAR), ',', '.') AS DOUBLE)
                    AS payments_value
            FROM src_sgr_expense_execution
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        expense_rollup AS (
            SELECT
                bpin_code,
                count(*) AS expense_row_count,
                count(DISTINCT entity_code)
                    FILTER (WHERE entity_code IS NOT NULL)
                    AS execution_entity_count,
                sum(coalesce(commitments_value, 0)) AS commitments_total,
                sum(coalesce(obligations_value, 0)) AS obligations_total,
                sum(coalesce(payments_value, 0)) AS payments_total,
                min(period_date) AS first_execution_period,
                max(period_date) AS last_execution_period
            FROM expense_raw
            GROUP BY bpin_code
        ),
        project_rollup AS (
            SELECT
                nullif(trim(codigobpin), '') AS bpin_code,
                any_value(nullif(trim(nombre), '')) AS project_title,
                any_value(nullif(trim(estado), '')) AS project_status,
                any_value(nullif(trim(departamento), '')) AS project_department,
                any_value(nullif(trim(sector), '')) AS project_sector,
                any_value(nullif(trim(nomocad), '')) AS ocad_name,
                any_value(nullif(trim(codejecutor), '')) AS executor_code,
                any_value(nullif(trim(entidadejecutora), '')) AS executor_name,
                bool_or(
                    regexp_matches(lower(coalesce(cast(proyecto_paz AS VARCHAR), '')), 'si|true|1')
                ) AS peace_project_flag,
                bool_or(
                    regexp_matches(lower(coalesce(cast(proyecto_covid AS VARCHAR), '')), 'si|true|1')
                ) AS covid_project_flag,
                bool_or(
                    regexp_matches(
                        lower(coalesce(cast(proyecto_grupo_etnico AS VARCHAR), '')),
                        'indig|afro|raizal|palen|rom|gitano|etnic'
                    )
                ) AS ethnic_project_flag,
                max(try_cast(replace(cast(valortotal AS VARCHAR), ',', '.') AS DOUBLE))
                    AS project_total_value,
                max(
                    try_cast(replace(cast(ejecucionfinanciera AS VARCHAR), ',', '.') AS DOUBLE)
                ) AS financial_execution_pct,
                max(
                    try_cast(replace(cast(ejecucionfisica AS VARCHAR), ',', '.') AS DOUBLE)
                ) AS physical_execution_pct
            FROM src_sgr_projects
            WHERE regexp_matches(nullif(trim(codigobpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(codigobpin), ''), '^0+$')
            GROUP BY nullif(trim(codigobpin), '')
        ),
        contract_red_flags AS (
            SELECT
                c.bpin_code,
                count(DISTINCT c.contract_id)
                    FILTER (WHERE lm.contract_id IS NOT NULL)
                    AS large_modification_contract_count,
                count(DISTINCT c.contract_id)
                    FILTER (WHERE susp.contract_id IS NOT NULL)
                    AS suspension_contract_count,
                count(DISTINCT c.contract_id)
                    FILTER (WHERE guar.contract_id IS NOT NULL)
                    AS guarantee_chain_contract_count,
                count(DISTINCT c.supplier_document_key)
                    FILTER (WHERE rues.entity_key IS NOT NULL)
                    AS rues_capacity_supplier_count
            FROM contract_joined c
            LEFT JOIN curated_large_modifications lm
                ON lm.contract_id = c.contract_id
            LEFT JOIN curated_contract_suspensions susp
                ON susp.contract_id = c.contract_id
            LEFT JOIN curated_guarantee_advance_execution_chain guar
                ON guar.contract_id = c.contract_id
            LEFT JOIN curated_rues_supplier_capacity_status rues
                ON rues.entity_key = c.supplier_document_key
            GROUP BY c.bpin_code
        ),
        joined AS (
            SELECT
                c.bpin_code,
                c.bpin_year,
                p.project_title,
                p.project_status,
                p.project_department,
                p.project_sector,
                p.ocad_name,
                p.executor_code,
                p.executor_name,
                p.peace_project_flag,
                p.covid_project_flag,
                p.ethnic_project_flag,
                p.project_total_value,
                p.financial_execution_pct,
                p.physical_execution_pct,
                c.contract_count,
                c.supplier_count,
                c.buyer_count,
                c.department_count,
                c.total_contract_value,
                c.max_contract_value,
                c.first_signing_date,
                c.last_signing_date,
                e.expense_row_count,
                e.execution_entity_count,
                e.commitments_total,
                e.obligations_total,
                e.payments_total,
                greatest(
                    coalesce(e.commitments_total, 0),
                    coalesce(e.obligations_total, 0),
                    coalesce(e.payments_total, 0)
                ) AS max_sgr_execution_value,
                e.first_execution_period,
                e.last_execution_period,
                coalesce(r.large_modification_contract_count, 0)
                    AS large_modification_contract_count,
                coalesce(r.suspension_contract_count, 0)
                    AS suspension_contract_count,
                coalesce(r.guarantee_chain_contract_count, 0)
                    AS guarantee_chain_contract_count,
                coalesce(r.rues_capacity_supplier_count, 0)
                    AS rues_capacity_supplier_count,
                coalesce(c.total_contract_value / nullif(p.project_total_value, 0), 0)
                    AS contract_project_value_ratio,
                coalesce(e.payments_total / nullif(p.project_total_value, 0), 0)
                    AS payment_project_value_ratio,
                (
                    (
                        coalesce(p.financial_execution_pct, 100) < 50
                        OR coalesce(p.physical_execution_pct, 100) < 50
                    )
                    AND p.project_total_value >= 10000000000
                ) AS low_project_execution_flag,
                (
                    coalesce(e.payments_total, 0) >= 5000000000
                    AND coalesce(p.physical_execution_pct, 100) < 50
                ) AS paid_low_physical_flag,
                (
                    coalesce(c.total_contract_value, 0) >= 10000000000
                    AND coalesce(p.physical_execution_pct, 100) < 50
                ) AS contract_low_physical_flag,
                (coalesce(r.rues_capacity_supplier_count, 0) > 0)
                    AS rues_capacity_flag,
                (
                    coalesce(r.large_modification_contract_count, 0) > 0
                    OR coalesce(r.suspension_contract_count, 0) > 0
                    OR coalesce(r.guarantee_chain_contract_count, 0) > 0
                ) AS procurement_failure_flag
            FROM contract_rollup c
            JOIN expense_rollup e
                ON e.bpin_code = c.bpin_code
            JOIN project_rollup p
                ON p.bpin_code = c.bpin_code
            LEFT JOIN contract_red_flags r
                ON r.bpin_code = c.bpin_code
        ),
        scored AS (
            SELECT
                *,
                cast(low_project_execution_flag AS INTEGER)
                    + cast(paid_low_physical_flag AS INTEGER)
                    + cast(contract_low_physical_flag AS INTEGER)
                    + cast(rues_capacity_flag AS INTEGER)
                    + cast(procurement_failure_flag AS INTEGER)
                    + cast(coalesce(peace_project_flag, false) AS INTEGER)
                    + cast(coalesce(ethnic_project_flag, false) AS INTEGER)
                    AS capacity_gap_flag_count,
                list_filter(
                    [
                        CASE
                            WHEN low_project_execution_flag
                                THEN 'low_sgr_project_execution'
                            ELSE NULL
                        END,
                        CASE
                            WHEN paid_low_physical_flag
                                THEN 'paid_low_physical_execution'
                            ELSE NULL
                        END,
                        CASE
                            WHEN contract_low_physical_flag
                                THEN 'high_contracting_low_physical_execution'
                            ELSE NULL
                        END,
                        CASE
                            WHEN rues_capacity_flag
                                THEN 'rues_supplier_capacity_flag'
                            ELSE NULL
                        END,
                        CASE
                            WHEN procurement_failure_flag
                                THEN 'procurement_failure_chain_flag'
                            ELSE NULL
                        END,
                        CASE
                            WHEN coalesce(peace_project_flag, false)
                                THEN 'ocad_paz_project'
                            ELSE NULL
                        END,
                        CASE
                            WHEN coalesce(ethnic_project_flag, false)
                                THEN 'ethnic_focus_project'
                            ELSE NULL
                        END
                    ],
                    item -> item IS NOT NULL
                ) AS capacity_gap_types
            FROM joined
        ),
        candidates AS (
            SELECT *
            FROM scored
            WHERE project_total_value >= 10000000000
                AND (
                    low_project_execution_flag
                    OR paid_low_physical_flag
                    OR contract_low_physical_flag
                )
                AND (
                    rues_capacity_flag
                    OR procurement_failure_flag
                    OR coalesce(peace_project_flag, false)
                    OR total_contract_value >= 50000000000
                )
        ),
        project_evidence AS (
            SELECT
                bpin_code,
                ['sgr_projects:' || bpin_code] AS project_evidence_refs
            FROM candidates
        ),
        expense_evidence_ranked AS (
            SELECT
                bpin_code,
                'sgr_expense_execution:' || bpin_code || ':' ||
                    coalesce(period_code, 'period') || ':' ||
                    coalesce(entity_code, 'entity') || ':' ||
                    coalesce(account, 'account') AS evidence_ref,
                greatest(
                    coalesce(commitments_value, 0),
                    coalesce(obligations_value, 0),
                    coalesce(payments_value, 0)
                ) AS execution_value,
                period_date,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY greatest(
                            coalesce(commitments_value, 0),
                            coalesce(obligations_value, 0),
                            coalesce(payments_value, 0)
                        ) DESC NULLS LAST,
                        period_date DESC NULLS LAST,
                        entity_code,
                        account
                ) AS evidence_rank
            FROM expense_raw
            WHERE bpin_code IN (SELECT bpin_code FROM candidates)
        ),
        expense_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY execution_value DESC, period_date DESC, evidence_ref)
                    AS expense_evidence_refs
            FROM expense_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        bpin_evidence_ranked AS (
            SELECT
                bpin_code,
                'secop_process_bpin:' || bpin_code || ':' || contract_id
                    AS evidence_ref,
                contract_value,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST, contract_id
                ) AS evidence_rank
            FROM contract_joined
            WHERE bpin_code IN (SELECT bpin_code FROM candidates)
        ),
        bpin_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, evidence_ref)
                    AS bpin_evidence_refs
            FROM bpin_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        contract_evidence_ranked AS (
            SELECT
                bpin_code,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM contract_joined
            WHERE bpin_code IN (SELECT bpin_code FROM candidates)
        ),
        contract_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC, contract_id)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY bpin_code
        ),
        support_evidence_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY priority, contract_value DESC NULLS LAST, evidence_ref
                ) AS support_rank
            FROM (
                SELECT DISTINCT
                    c.bpin_code,
                    'signal_feature_procurement_guarantee_advance_execution_chain:'
                        || guar.contract_id AS evidence_ref,
                    c.contract_value,
                    1 AS priority
                FROM contract_joined c
                JOIN curated_guarantee_advance_execution_chain guar
                    ON guar.contract_id = c.contract_id
                WHERE c.bpin_code IN (SELECT bpin_code FROM candidates)
                UNION ALL
                SELECT DISTINCT
                    c.bpin_code,
                    'signal_feature_procurement_contract_suspensions:'
                        || susp.contract_id AS evidence_ref,
                    c.contract_value,
                    2 AS priority
                FROM contract_joined c
                JOIN curated_contract_suspensions susp
                    ON susp.contract_id = c.contract_id
                WHERE c.bpin_code IN (SELECT bpin_code FROM candidates)
                UNION ALL
                SELECT DISTINCT
                    c.bpin_code,
                    'signal_feature_procurement_large_modifications:'
                        || lm.contract_id AS evidence_ref,
                    c.contract_value,
                    3 AS priority
                FROM contract_joined c
                JOIN curated_large_modifications lm
                    ON lm.contract_id = c.contract_id
                WHERE c.bpin_code IN (SELECT bpin_code FROM candidates)
                UNION ALL
                SELECT DISTINCT
                    c.bpin_code,
                    'signal_feature_rues_supplier_capacity_status_review_only:'
                        || rues.entity_key AS evidence_ref,
                    c.contract_value,
                    4 AS priority
                FROM contract_joined c
                JOIN curated_rues_supplier_capacity_status rues
                    ON rues.entity_key = c.supplier_document_key
                WHERE c.bpin_code IN (SELECT bpin_code FROM candidates)
            )
        ),
        support_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY support_rank) AS support_evidence_refs
            FROM support_evidence_ranked
            WHERE support_rank <= 5
            GROUP BY bpin_code
        )
        SELECT
            'sgr_ocad_executor_capacity_gap' AS signal_id,
            'project:' || c.bpin_code AS entity_id,
            c.bpin_code AS entity_key,
            'Project' AS entity_label,
            'sgr_ocad_capacity:' || c.bpin_code AS scope_key,
            'sgr_ocad_project' AS scope_type,
            CASE
                WHEN c.capacity_gap_flag_count >= 4
                    OR c.project_total_value >= 100000000000
                    THEN 'critical'
                WHEN c.capacity_gap_flag_count >= 3
                    OR c.total_contract_value >= 50000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.98,
                0.56
                    + least(c.capacity_gap_flag_count * 0.06, 0.24)
                    + least(log10(greatest(c.project_total_value, 1)) / 140.0, 0.10)
                    + least(log10(greatest(c.total_contract_value, 1)) / 140.0, 0.08)
                    + CASE WHEN c.procurement_failure_flag THEN 0.06 ELSE 0.0 END
                    + CASE WHEN c.rues_capacity_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN coalesce(c.peace_project_flag, false) THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_BPIN' AS identity_match_type,
            'exact' AS identity_quality,
            c.bpin_year,
            c.project_title,
            c.project_status,
            c.project_department,
            c.project_sector,
            c.ocad_name,
            c.executor_code,
            c.executor_name,
            c.project_total_value,
            c.financial_execution_pct,
            c.physical_execution_pct,
            c.contract_count,
            c.supplier_count,
            c.buyer_count,
            c.department_count,
            c.total_contract_value,
            c.max_contract_value,
            c.first_signing_date,
            c.last_signing_date,
            c.expense_row_count,
            c.execution_entity_count,
            c.commitments_total,
            c.obligations_total,
            c.payments_total,
            c.max_sgr_execution_value,
            c.first_execution_period,
            c.last_execution_period,
            c.contract_project_value_ratio,
            c.payment_project_value_ratio,
            c.low_project_execution_flag,
            c.paid_low_physical_flag,
            c.contract_low_physical_flag,
            c.rues_capacity_flag,
            c.procurement_failure_flag,
            c.peace_project_flag,
            c.covid_project_flag,
            c.ethnic_project_flag,
            c.large_modification_contract_count,
            c.suspension_contract_count,
            c.guarantee_chain_contract_count,
            c.rues_capacity_supplier_count,
            c.capacity_gap_flag_count,
            c.capacity_gap_types,
            'BPIN/SGR/SECOP/RUES linkage does not prove non-delivery, legal breach, executor capacity failure, or corrupt intent; it only prioritizes projects for document review.'
                AS what_is_unproven,
            list_concat(
                list_concat(
                    list_concat(
                        list_concat(pe.project_evidence_refs, ee.expense_evidence_refs),
                        be.bpin_evidence_refs
                    ),
                    ce.contract_evidence_refs
                ),
                coalesce(se.support_evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM candidates c
        JOIN project_evidence pe
            ON pe.bpin_code = c.bpin_code
        JOIN expense_evidence ee
            ON ee.bpin_code = c.bpin_code
        JOIN bpin_evidence be
            ON be.bpin_code = c.bpin_code
        JOIN contract_evidence ce
            ON ce.bpin_code = c.bpin_code
        LEFT JOIN support_evidence se
            ON se.bpin_code = c.bpin_code
        QUALIFY row_number() OVER (
            ORDER BY c.capacity_gap_flag_count DESC,
                c.project_total_value DESC NULLS LAST,
                c.total_contract_value DESC NULLS LAST,
                c.bpin_code
        ) <= 1000
    """)


def _create_dnp_sgr_beneficiary_delivery_gap_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "sgr_expense_execution",
        "sgr_projects",
        "secop_process_bpin",
        "secop_ii_contracts",
        "company_registry_c82u",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_contract_execution",
        "secop_guarantees",
        "secop_sanctions",
        "dnp_project_executors",
        "dnp_project_locations",
        "dnp_project_beneficiary_locations",
        "dnp_project_beneficiary_characterization",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_dnp_sgr_beneficiary_delivery_gap AS
        WITH dnp_executor_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(codigoentidadejecutora), '') AS executor_code,
                nullif(trim(entidadejecutora), '') AS executor_name,
                nullif(trim(nombreproyecto), '') AS project_name
            FROM src_dnp_project_executors
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        dnp_executors AS (
            SELECT
                bpin_code,
                count(*) AS dnp_executor_row_count,
                count(DISTINCT executor_code)
                    FILTER (WHERE executor_code IS NOT NULL)
                    AS dnp_executor_count,
                list(DISTINCT executor_code ORDER BY executor_code)
                    FILTER (WHERE executor_code IS NOT NULL)
                    AS dnp_executor_codes,
                list(DISTINCT executor_name ORDER BY executor_name)
                    FILTER (WHERE executor_name IS NOT NULL)
                    AS dnp_executor_names
            FROM dnp_executor_raw
            GROUP BY bpin_code
        ),
        project_location_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(codigodepartamento), '') AS department_code,
                nullif(trim(departamento), '') AS department,
                nullif(trim(codigomunicipio), '') AS municipality_code,
                nullif(trim(municipio), '') AS municipality,
                nullif(trim(region), '') AS region,
                nullif(trim(sector), '') AS dnp_sector,
                nullif(trim(entidadresponsable), '') AS responsible_entity,
                nullif(trim(codigoentidadresponsable), '') AS responsible_entity_code
            FROM src_dnp_project_locations
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        project_locations AS (
            SELECT
                bpin_code,
                count(*) AS dnp_location_row_count,
                count(DISTINCT department)
                    FILTER (WHERE department IS NOT NULL)
                    AS dnp_department_count,
                count(DISTINCT municipality)
                    FILTER (WHERE municipality IS NOT NULL)
                    AS dnp_municipality_count,
                list(DISTINCT department ORDER BY department)
                    FILTER (WHERE department IS NOT NULL)
                    AS dnp_departments,
                list(DISTINCT municipality ORDER BY municipality)
                    FILTER (WHERE municipality IS NOT NULL)
                    AS dnp_municipalities,
                any_value(dnp_sector) AS dnp_sector,
                any_value(responsible_entity) AS responsible_entity,
                any_value(responsible_entity_code) AS responsible_entity_code
            FROM project_location_raw
            GROUP BY bpin_code
        ),
        beneficiary_location_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(departamento), '') AS beneficiary_department,
                nullif(trim(municipio), '') AS beneficiary_municipality,
                nullif(trim(sector), '') AS beneficiary_sector,
                nullif(trim(entidadresponsable), '') AS beneficiary_responsible_entity,
                try_cast(replace(cast(totalbeneficiario AS VARCHAR), ',', '.') AS DOUBLE)
                    AS beneficiary_location_total
            FROM src_dnp_project_beneficiary_locations
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        beneficiary_locations AS (
            SELECT
                bpin_code,
                count(*) AS beneficiary_location_row_count,
                count(DISTINCT beneficiary_department)
                    FILTER (WHERE beneficiary_department IS NOT NULL)
                    AS beneficiary_department_count,
                count(DISTINCT beneficiary_municipality)
                    FILTER (WHERE beneficiary_municipality IS NOT NULL)
                    AS beneficiary_municipality_count,
                count(
                    DISTINCT beneficiary_department || '|' || beneficiary_municipality
                )
                    FILTER (
                        WHERE beneficiary_department IS NOT NULL
                            OR beneficiary_municipality IS NOT NULL
                    )
                    AS beneficiary_territory_count,
                max(beneficiary_location_total) AS max_beneficiary_location_total,
                sum(coalesce(beneficiary_location_total, 0))
                    AS beneficiary_location_total_sum,
                list(DISTINCT beneficiary_department ORDER BY beneficiary_department)
                    FILTER (WHERE beneficiary_department IS NOT NULL)
                    AS beneficiary_departments,
                list(DISTINCT beneficiary_municipality ORDER BY beneficiary_municipality)
                    FILTER (WHERE beneficiary_municipality IS NOT NULL)
                    AS beneficiary_municipalities
            FROM beneficiary_location_raw
            GROUP BY bpin_code
        ),
        demographic_raw AS (
            SELECT
                nullif(trim(bpin), '') AS bpin_code,
                nullif(trim(caracteristicademografica), '') AS demographic_category,
                try_cast(replace(cast(cantidad AS VARCHAR), ',', '.') AS DOUBLE)
                    AS demographic_count,
                nullif(trim(sector), '') AS demographic_sector,
                nullif(trim(entidadresponsable), '') AS demographic_responsible_entity
            FROM src_dnp_project_beneficiary_characterization
            WHERE regexp_matches(nullif(trim(bpin), ''), '^[0-9]{8,}$')
                AND NOT regexp_matches(nullif(trim(bpin), ''), '^0+$')
        ),
        demographic_flagged AS (
            SELECT
                *,
                regexp_matches(
                    lower(coalesce(demographic_category, '')),
                    'ind[ií]gen|afro|negra|raizal|palenquer|rrom|gitano|'
                    || 'victim|v[ií]ctim|desplaz|discapacidad|reincorporaci[oó]n|'
                    || 'reintegraci[oó]n|primera infancia|infancia|adolescencia|'
                    || 'vejez|mayor de 60|60 a|0 a 14'
                ) AS vulnerable_category_flag
            FROM demographic_raw
        ),
        demographics AS (
            SELECT
                bpin_code,
                count(*) AS demographic_row_count,
                count(DISTINCT demographic_category)
                    FILTER (WHERE demographic_category IS NOT NULL)
                    AS demographic_category_count,
                sum(coalesce(demographic_count, 0))
                    AS demographic_observation_count,
                sum(coalesce(demographic_count, 0))
                    FILTER (WHERE vulnerable_category_flag)
                    AS vulnerable_demographic_observation_count,
                count(DISTINCT demographic_category)
                    FILTER (WHERE vulnerable_category_flag)
                    AS vulnerable_demographic_category_count,
                list(DISTINCT demographic_category ORDER BY demographic_category)
                    FILTER (WHERE vulnerable_category_flag)
                    AS vulnerable_demographic_categories
            FROM demographic_flagged
            GROUP BY bpin_code
        ),
        executor_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY evidence_ref) AS executor_evidence_refs
            FROM (
                SELECT DISTINCT
                    bpin_code,
                    'dnp_project_executors:' || bpin_code || ':' ||
                        coalesce(executor_code, 'executor') AS evidence_ref
                FROM dnp_executor_raw
            )
            GROUP BY bpin_code
        ),
        location_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY evidence_ref) AS location_evidence_refs
            FROM (
                SELECT DISTINCT
                    bpin_code,
                    'dnp_project_locations:' || bpin_code || ':' ||
                        coalesce(municipality_code, department_code, 'territory')
                        AS evidence_ref
                FROM project_location_raw
            )
            GROUP BY bpin_code
        ),
        beneficiary_location_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY evidence_ref)
                    AS beneficiary_location_evidence_refs
            FROM (
                SELECT DISTINCT
                    bpin_code,
                    'dnp_project_beneficiary_locations:' || bpin_code || ':' ||
                        coalesce(beneficiary_department, 'department') || ':' ||
                        coalesce(beneficiary_municipality, 'municipality')
                        AS evidence_ref
                FROM beneficiary_location_raw
            )
            GROUP BY bpin_code
        ),
        demographic_evidence AS (
            SELECT
                bpin_code,
                list(evidence_ref ORDER BY observation DESC NULLS LAST, evidence_ref)
                    AS demographic_evidence_refs
            FROM (
                SELECT
                    bpin_code,
                    'dnp_project_beneficiary_characterization:' || bpin_code || ':' ||
                        coalesce(demographic_category, 'category') AS evidence_ref,
                    max(demographic_count) AS observation
                FROM demographic_flagged
                WHERE vulnerable_category_flag
                    OR demographic_count > 0
                GROUP BY bpin_code, demographic_category
                QUALIFY row_number() OVER (
                    PARTITION BY bpin_code
                    ORDER BY max(demographic_count) DESC NULLS LAST,
                        demographic_category
                ) <= 5
            )
            GROUP BY bpin_code
        ),
        joined AS (
            SELECT
                b.*,
                e.dnp_executor_row_count,
                e.dnp_executor_count,
                e.dnp_executor_codes,
                e.dnp_executor_names,
                l.dnp_location_row_count,
                l.dnp_department_count,
                l.dnp_municipality_count,
                l.dnp_departments,
                l.dnp_municipalities,
                l.dnp_sector,
                l.responsible_entity,
                l.responsible_entity_code,
                bl.beneficiary_location_row_count,
                bl.beneficiary_department_count,
                bl.beneficiary_municipality_count,
                bl.beneficiary_territory_count,
                bl.max_beneficiary_location_total,
                bl.beneficiary_location_total_sum,
                bl.beneficiary_departments,
                bl.beneficiary_municipalities,
                d.demographic_row_count,
                d.demographic_category_count,
                d.demographic_observation_count,
                d.vulnerable_demographic_observation_count,
                d.vulnerable_demographic_category_count,
                d.vulnerable_demographic_categories,
                coalesce(bl.beneficiary_territory_count, 0) >= 5
                    AS multi_territory_beneficiary_flag,
                coalesce(d.vulnerable_demographic_observation_count, 0) > 0
                    AS vulnerable_population_flag,
                coalesce(d.demographic_observation_count, 0) >= 10000
                    AS high_demographic_observation_flag,
                coalesce(e.dnp_executor_count, 0) > 1
                    AS multiple_dnp_executor_flag,
                (
                    coalesce(bl.beneficiary_location_row_count, 0) > 0
                    OR coalesce(d.demographic_row_count, 0) > 0
                ) AS has_dnp_beneficiary_context
            FROM curated_sgr_ocad_executor_capacity_gap b
            LEFT JOIN dnp_executors e
                ON e.bpin_code = b.entity_key
            LEFT JOIN project_locations l
                ON l.bpin_code = b.entity_key
            LEFT JOIN beneficiary_locations bl
                ON bl.bpin_code = b.entity_key
            LEFT JOIN demographics d
                ON d.bpin_code = b.entity_key
        ),
        scored AS (
            SELECT
                *,
                cast(has_dnp_beneficiary_context AS INTEGER)
                    + cast(multi_territory_beneficiary_flag AS INTEGER)
                    + cast(vulnerable_population_flag AS INTEGER)
                    + cast(high_demographic_observation_flag AS INTEGER)
                    + cast(multiple_dnp_executor_flag AS INTEGER)
                    + cast(coalesce(peace_project_flag, false) AS INTEGER)
                    + cast(coalesce(ethnic_project_flag, false) AS INTEGER)
                    + cast(procurement_failure_flag AS INTEGER)
                    AS dnp_delivery_flag_count,
                list_filter(
                    [
                        CASE
                            WHEN has_dnp_beneficiary_context
                                THEN 'dnp_beneficiary_context'
                            ELSE NULL
                        END,
                        CASE
                            WHEN multi_territory_beneficiary_flag
                                THEN 'multi_territory_beneficiary_context'
                            ELSE NULL
                        END,
                        CASE
                            WHEN vulnerable_population_flag
                                THEN 'vulnerable_population_context'
                            ELSE NULL
                        END,
                        CASE
                            WHEN high_demographic_observation_flag
                                THEN 'high_demographic_observation_context'
                            ELSE NULL
                        END,
                        CASE
                            WHEN multiple_dnp_executor_flag
                                THEN 'multiple_dnp_executors'
                            ELSE NULL
                        END,
                        CASE
                            WHEN coalesce(peace_project_flag, false)
                                THEN 'ocad_paz_project'
                            ELSE NULL
                        END,
                        CASE
                            WHEN coalesce(ethnic_project_flag, false)
                                THEN 'ethnic_focus_project'
                            ELSE NULL
                        END,
                        CASE
                            WHEN procurement_failure_flag
                                THEN 'procurement_failure_chain_flag'
                            ELSE NULL
                        END
                    ],
                    item -> item IS NOT NULL
                ) AS dnp_delivery_gap_types
            FROM joined
        )
        SELECT
            'dnp_sgr_beneficiary_delivery_gap_review_only' AS signal_id,
            entity_id,
            entity_key,
            entity_label,
            'dnp_sgr_beneficiary_delivery:' || entity_key AS scope_key,
            'dnp_sgr_beneficiary_delivery_gap' AS scope_type,
            CASE
                WHEN severity = 'critical'
                    OR dnp_delivery_flag_count >= 5
                    THEN 'critical'
                WHEN dnp_delivery_flag_count >= 3
                    OR total_contract_value >= 50000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.99,
                risk_signal + least(dnp_delivery_flag_count * 0.025, 0.14)
            ) AS risk_signal,
            identity_confidence,
            identity_match_type,
            identity_quality,
            project_title,
            project_status,
            project_department,
            project_sector,
            ocad_name,
            executor_code,
            executor_name,
            dnp_executor_count,
            dnp_executor_codes,
            dnp_executor_names,
            responsible_entity,
            responsible_entity_code,
            dnp_sector,
            dnp_department_count,
            dnp_municipality_count,
            dnp_departments,
            dnp_municipalities,
            beneficiary_department_count,
            beneficiary_municipality_count,
            beneficiary_territory_count,
            beneficiary_departments,
            beneficiary_municipalities,
            demographic_category_count,
            demographic_observation_count,
            vulnerable_demographic_observation_count,
            vulnerable_demographic_category_count,
            vulnerable_demographic_categories,
            project_total_value,
            total_contract_value,
            payments_total,
            financial_execution_pct,
            physical_execution_pct,
            capacity_gap_types,
            dnp_delivery_flag_count,
            dnp_delivery_gap_types,
            multi_territory_beneficiary_flag,
            vulnerable_population_flag,
            high_demographic_observation_flag,
            multiple_dnp_executor_flag,
            low_project_execution_flag,
            paid_low_physical_flag,
            contract_low_physical_flag,
            procurement_failure_flag,
            'exact BPIN DNP beneficiary/executor/location context plus SGR/SECOP low-execution queue; this does not prove beneficiary harm, non-delivery, incorrect targeting, legal breach, or corrupt intent'
                AS what_is_unproven,
            list_concat(
                list_concat(
                    list_concat(
                        list_concat(
                            evidence_refs,
                            coalesce(ex.executor_evidence_refs, []::VARCHAR[])
                        ),
                        coalesce(le.location_evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(ble.beneficiary_location_evidence_refs, []::VARCHAR[])
                ),
                coalesce(de.demographic_evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM scored s
        LEFT JOIN executor_evidence ex
            ON ex.bpin_code = s.entity_key
        LEFT JOIN location_evidence le
            ON le.bpin_code = s.entity_key
        LEFT JOIN beneficiary_location_evidence ble
            ON ble.bpin_code = s.entity_key
        LEFT JOIN demographic_evidence de
            ON de.bpin_code = s.entity_key
        WHERE has_dnp_beneficiary_context
        QUALIFY row_number() OVER (
            ORDER BY dnp_delivery_flag_count DESC,
                project_total_value DESC NULLS LAST,
                total_contract_value DESC NULLS LAST,
                entity_key
        ) <= 1000
    """)


def _create_tvec_multi_entity_capture_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"tvec_orders_consolidated", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_tvec_multi_entity_capture AS
        WITH raw_tvec AS (
            SELECT
                coacc_document_key(supplier_nit, 'NIT') AS supplier_document_key,
                coacc_doc_digits(supplier_nit) AS supplier_document_digits,
                nullif(trim(supplier_name), '') AS supplier_name,
                coacc_doc_digits(buyer_nit) AS buyer_document_digits,
                nullif(trim(buyer_name), '') AS buyer_name,
                nullif(trim(order_id), '') AS order_id,
                try_cast(order_date AS DATE) AS order_date,
                nullif(trim(item_name), '') AS item_name,
                try_cast(replace(cast(line_total AS VARCHAR), ',', '.') AS DOUBLE)
                    AS line_value
            FROM src_tvec_orders_consolidated
            WHERE nullif(trim(order_id), '') IS NOT NULL
                AND coacc_document_key(supplier_nit, 'NIT') IS NOT NULL
                AND coacc_doc_digits(buyer_nit) IS NOT NULL
        ),
        valid_tvec AS (
            SELECT *
            FROM raw_tvec
            WHERE length(supplier_document_key) = 9
                AND NOT regexp_matches(supplier_document_key, '^0+$')
                AND length(buyer_document_digits) BETWEEN 8 AND 10
                AND NOT regexp_matches(buyer_document_digits, '^0+$')
                AND line_value IS NOT NULL
                AND line_value > 0
        ),
        order_rollup AS (
            SELECT
                supplier_document_key,
                any_value(supplier_name) AS supplier_name,
                buyer_document_digits,
                any_value(buyer_name) AS buyer_name,
                order_id,
                min(order_date) AS order_date,
                sum(line_value) AS order_value,
                count(*) AS order_line_count
            FROM valid_tvec
            GROUP BY supplier_document_key, buyer_document_digits, order_id
        ),
        tvec_supplier_rollup AS (
            SELECT
                supplier_document_key,
                any_value(supplier_name) AS supplier_name,
                count(DISTINCT order_id) AS tvec_order_count,
                count(DISTINCT buyer_document_digits) AS tvec_buyer_count,
                sum(order_value) AS tvec_total_value,
                sum(order_line_count) AS tvec_line_count,
                min(order_date) AS first_tvec_order_date,
                max(order_date) AS last_tvec_order_date
            FROM order_rollup
            GROUP BY supplier_document_key
        ),
        secop_rollup AS (
            SELECT
                supplier_document_key,
                any_value(supplier_entity_id) AS supplier_entity_id,
                any_value(supplier_name) AS secop_supplier_name,
                count(DISTINCT contract_id) AS secop_contract_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS secop_buyer_count,
                sum(contract_value) AS secop_total_contract_value,
                min(signing_date) AS first_secop_signing_date,
                max(signing_date) AS last_secop_signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
            GROUP BY supplier_document_key
        ),
        tvec_evidence_ranked AS (
            SELECT
                supplier_document_key,
                'tvec_orders_consolidated:' || order_id AS evidence_ref,
                order_value,
                order_date,
                order_id,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY order_value DESC NULLS LAST,
                        order_date DESC NULLS LAST,
                        order_id
                ) AS evidence_rank
            FROM order_rollup
        ),
        tvec_evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY order_value DESC, order_date DESC, order_id)
                    AS tvec_evidence_refs
            FROM tvec_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key
        ),
        secop_evidence_ranked AS (
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
            WHERE supplier_document_key IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        secop_evidence AS (
            SELECT
                supplier_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC, contract_id)
                    AS secop_evidence_refs
            FROM secop_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY supplier_document_key
        ),
        buyer_samples_ranked AS (
            SELECT
                supplier_document_key,
                coalesce(buyer_name, buyer_document_digits) AS buyer_sample,
                sum(order_value) AS buyer_order_value,
                row_number() OVER (
                    PARTITION BY supplier_document_key
                    ORDER BY sum(order_value) DESC NULLS LAST,
                        coalesce(buyer_name, buyer_document_digits)
                ) AS sample_rank
            FROM order_rollup
            GROUP BY supplier_document_key,
                coalesce(buyer_name, buyer_document_digits)
        ),
        buyer_samples AS (
            SELECT
                supplier_document_key,
                list(buyer_sample ORDER BY buyer_order_value DESC, buyer_sample)
                    AS tvec_buyer_sample
            FROM buyer_samples_ranked
            WHERE sample_rank <= 10
            GROUP BY supplier_document_key
        ),
        joined AS (
            SELECT
                t.supplier_document_key,
                coalesce(s.supplier_entity_id, 'doc:' || t.supplier_document_key)
                    AS supplier_entity_id,
                coalesce(t.supplier_name, s.secop_supplier_name) AS supplier_name,
                t.tvec_order_count,
                t.tvec_buyer_count,
                t.tvec_total_value,
                t.tvec_line_count,
                t.first_tvec_order_date,
                t.last_tvec_order_date,
                s.secop_contract_count,
                s.secop_buyer_count,
                s.secop_total_contract_value,
                s.first_secop_signing_date,
                s.last_secop_signing_date
            FROM tvec_supplier_rollup t
            JOIN secop_rollup s
                ON s.supplier_document_key = t.supplier_document_key
            WHERE t.tvec_buyer_count >= 50
                AND t.tvec_order_count >= 100
                AND t.tvec_total_value >= 1000000000
                AND s.secop_contract_count >= 20
                AND s.secop_total_contract_value >= 5000000000
        )
            SELECT
                'tvec_multi_entity_capture' AS signal_id,
                j.supplier_entity_id AS entity_id,
                j.supplier_document_key AS entity_key,
                'Company' AS entity_label,
                'tvec_supplier:' || j.supplier_document_key AS scope_key,
                'tvec_order' AS scope_type,
                CASE
                    WHEN j.tvec_buyer_count >= 200
                        OR j.tvec_total_value >= 100000000000
                        OR j.secop_total_contract_value >= 100000000000
                        THEN 'high'
                    ELSE 'medium'
                END AS severity,
                least(
                    1.0,
                    0.50
                        + least(j.tvec_buyer_count / 1000.0, 0.20)
                        + least(j.tvec_order_count / 5000.0, 0.15)
                        + least(log10(greatest(j.tvec_total_value, 1)) / 120.0, 0.08)
                        + least(j.secop_buyer_count / 500.0, 0.07)
                ) AS risk_signal,
                1.0 AS identity_confidence,
                'EXACT_COMPANY_NIT' AS identity_match_type,
                'exact' AS identity_quality,
                j.supplier_name,
                j.tvec_order_count,
                j.tvec_buyer_count,
                j.tvec_total_value,
                j.tvec_line_count,
                j.first_tvec_order_date,
                j.last_tvec_order_date,
                j.secop_contract_count,
                j.secop_buyer_count,
                j.secop_total_contract_value,
                j.first_secop_signing_date,
                j.last_secop_signing_date,
                b.tvec_buyer_sample,
                list_concat(te.tvec_evidence_refs, se.secop_evidence_refs)
                    AS evidence_refs
        FROM joined j
        JOIN tvec_evidence te
            ON te.supplier_document_key = j.supplier_document_key
        JOIN secop_evidence se
            ON se.supplier_document_key = j.supplier_document_key
        LEFT JOIN buyer_samples b
            ON b.supplier_document_key = j.supplier_document_key
            QUALIFY row_number() OVER (
                ORDER BY j.tvec_buyer_count DESC,
                    j.tvec_total_value DESC NULLS LAST,
                    j.supplier_document_key
            ) <= 1000
    """)


def _create_tvec_item_price_dispersion_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if "tvec_orders_consolidated" not in set(required_sources):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_tvec_item_price_dispersion AS
        WITH raw_tvec AS (
            SELECT
                coacc_document_key(supplier_nit, 'NIT') AS supplier_document_key,
                coacc_doc_digits(supplier_nit) AS supplier_document_digits,
                nullif(trim(supplier_nit), '') AS supplier_document_id,
                nullif(trim(supplier_name), '') AS supplier_name,
                coacc_doc_digits(buyer_nit) AS buyer_document_digits,
                nullif(trim(buyer_nit), '') AS buyer_document_id,
                nullif(trim(buyer_name), '') AS buyer_name,
                nullif(trim(order_id), '') AS order_id,
                try_cast(order_date AS DATE) AS order_date,
                nullif(trim(item_name), '') AS item_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(item_name), ''), '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ) AS item_name_normalized,
                regexp_replace(
                    lower(coalesce(nullif(trim(item_name), ''), '')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS item_key,
                nullif(trim(unit), '') AS unit_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(unit), ''), 'unidad')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS unit_key,
                try_cast(replace(cast(unit_price AS VARCHAR), ',', '.') AS DOUBLE)
                    AS unit_price,
                try_cast(replace(cast(quantity AS VARCHAR), ',', '.') AS DOUBLE)
                    AS quantity,
                try_cast(replace(cast(line_total AS VARCHAR), ',', '.') AS DOUBLE)
                    AS line_value,
                nullif(trim(cdp), '') AS cdp
            FROM src_tvec_orders_consolidated
            WHERE nullif(trim(order_id), '') IS NOT NULL
        ),
        valid_tvec AS (
            SELECT
                *,
                CASE
                    WHEN quantity IS NULL THEN 'q_unknown'
                    WHEN quantity <= 1 THEN 'q_001'
                    WHEN quantity <= 5 THEN 'q_002_005'
                    WHEN quantity <= 10 THEN 'q_006_010'
                    WHEN quantity <= 50 THEN 'q_011_050'
                    WHEN quantity <= 100 THEN 'q_051_100'
                    WHEN quantity <= 500 THEN 'q_101_500'
                    ELSE 'q_501_plus'
                END AS quantity_band,
                coalesce(cast(year(order_date) AS VARCHAR), 'undated') AS year_bucket,
                'tvec_orders_consolidated:' || order_id || ':' || left(item_key, 96)
                    AS evidence_ref
            FROM raw_tvec
            WHERE supplier_document_key IS NOT NULL
                AND length(supplier_document_key) = 9
                AND NOT regexp_matches(supplier_document_key, '^0+$')
                AND buyer_document_digits IS NOT NULL
                AND length(buyer_document_digits) BETWEEN 8 AND 10
                AND NOT regexp_matches(buyer_document_digits, '^0+$')
                AND unit_price IS NOT NULL
                AND unit_price > 0
                AND line_value IS NOT NULL
                AND line_value > 0
                AND item_key IS NOT NULL
                AND length(item_key) >= 8
                AND unit_key IS NOT NULL
                AND length(unit_key) >= 2
                AND NOT regexp_matches(
                    item_key,
                    '(nodefinido|presupuesto|cotizacion|cotizacin|serviciointegral|paquete|bolsa)'
                )
        ),
        comparator_stats AS (
            SELECT
                item_key,
                any_value(item_name_normalized) AS comparator_item_name,
                unit_key,
                any_value(unit_name) AS comparator_unit_name,
                quantity_band,
                year_bucket,
                count(*) AS comparable_line_count,
                count(DISTINCT supplier_document_key) AS comparable_supplier_count,
                count(DISTINCT buyer_document_digits) AS comparable_buyer_count,
                quantile_cont(unit_price, 0.5) AS median_unit_price,
                quantile_cont(unit_price, 0.9) AS p90_unit_price,
                quantile_cont(unit_price, 0.95) AS p95_unit_price,
                min(unit_price) AS min_unit_price,
                max(unit_price) AS max_unit_price,
                sum(line_value) AS comparable_total_value,
                min(order_date) AS first_comparable_order_date,
                max(order_date) AS last_comparable_order_date
            FROM valid_tvec
            GROUP BY item_key, unit_key, quantity_band, year_bucket
            HAVING count(*) >= 20
                AND count(DISTINCT supplier_document_key) >= 3
                AND count(DISTINCT buyer_document_digits) >= 5
                AND quantile_cont(unit_price, 0.5) > 0
                AND quantile_cont(unit_price, 0.9)
                    >= quantile_cont(unit_price, 0.5) * 2
        ),
        supplier_capture_proxy AS (
            SELECT
                supplier_document_key,
                count(DISTINCT order_id) AS supplier_tvec_order_count,
                count(DISTINCT buyer_document_digits) AS supplier_tvec_buyer_count,
                sum(line_value) AS supplier_tvec_total_value
            FROM valid_tvec
            GROUP BY supplier_document_key
        ),
        high_price_lines AS (
            SELECT
                v.*,
                s.comparator_item_name,
                s.comparator_unit_name,
                s.comparable_line_count,
                s.comparable_supplier_count,
                s.comparable_buyer_count,
                s.median_unit_price,
                s.p90_unit_price,
                s.p95_unit_price,
                s.min_unit_price,
                s.max_unit_price,
                s.comparable_total_value,
                s.first_comparable_order_date,
                s.last_comparable_order_date,
                v.unit_price / nullif(s.median_unit_price, 0) AS unit_price_to_median_ratio,
                v.unit_price / nullif(s.p95_unit_price, 0) AS unit_price_to_p95_ratio
            FROM valid_tvec v
            JOIN comparator_stats s
                ON s.item_key = v.item_key
                AND s.unit_key = v.unit_key
                AND s.quantity_band = v.quantity_band
                AND s.year_bucket = v.year_bucket
            WHERE v.unit_price >= s.p95_unit_price
                AND v.unit_price >= s.median_unit_price * 2
        ),
        order_item_rollup AS (
            SELECT
                supplier_document_key,
                any_value(supplier_document_digits) AS supplier_document_digits,
                any_value(supplier_document_id) AS supplier_document_id,
                any_value(supplier_name) AS supplier_name,
                buyer_document_digits,
                any_value(buyer_document_id) AS buyer_document_id,
                any_value(buyer_name) AS buyer_name,
                order_id,
                min(order_date) AS order_date,
                item_key,
                any_value(item_name) AS item_name,
                any_value(item_name_normalized) AS item_name_normalized,
                unit_key,
                any_value(unit_name) AS unit_name,
                quantity_band,
                year_bucket,
                max(unit_price) AS observed_unit_price,
                sum(quantity) AS observed_quantity,
                sum(line_value) AS high_price_line_value,
                count(*) AS high_price_line_count,
                any_value(cdp) AS cdp,
                any_value(comparator_item_name) AS comparator_item_name,
                any_value(comparator_unit_name) AS comparator_unit_name,
                any_value(comparable_line_count) AS comparable_line_count,
                any_value(comparable_supplier_count) AS comparable_supplier_count,
                any_value(comparable_buyer_count) AS comparable_buyer_count,
                any_value(median_unit_price) AS median_unit_price,
                any_value(p90_unit_price) AS p90_unit_price,
                any_value(p95_unit_price) AS p95_unit_price,
                any_value(min_unit_price) AS min_unit_price,
                any_value(max_unit_price) AS max_unit_price,
                any_value(comparable_total_value) AS comparable_total_value,
                any_value(first_comparable_order_date) AS first_comparable_order_date,
                any_value(last_comparable_order_date) AS last_comparable_order_date,
                max(unit_price_to_median_ratio) AS unit_price_to_median_ratio,
                max(unit_price_to_p95_ratio) AS unit_price_to_p95_ratio,
                list(evidence_ref ORDER BY line_value DESC, unit_price DESC)
                    AS high_price_evidence_refs
            FROM high_price_lines
            GROUP BY
                supplier_document_key,
                buyer_document_digits,
                order_id,
                item_key,
                unit_key,
                quantity_band,
                year_bucket
        ),
        median_examples_ranked AS (
            SELECT
                h.supplier_document_key,
                h.buyer_document_digits,
                h.order_id,
                h.item_key,
                h.unit_key,
                h.quantity_band,
                h.year_bucket,
                v.evidence_ref,
                abs(v.unit_price - h.median_unit_price) AS distance_to_median,
                v.unit_price,
                v.line_value,
                row_number() OVER (
                    PARTITION BY
                        h.supplier_document_key,
                        h.buyer_document_digits,
                        h.order_id,
                        h.item_key,
                        h.unit_key,
                        h.quantity_band,
                        h.year_bucket
                    ORDER BY
                        abs(v.unit_price - h.median_unit_price),
                        v.line_value DESC NULLS LAST,
                        v.order_id
                ) AS evidence_rank
            FROM order_item_rollup h
            JOIN valid_tvec v
                ON v.item_key = h.item_key
                AND v.unit_key = h.unit_key
                AND v.quantity_band = h.quantity_band
                AND v.year_bucket = h.year_bucket
            WHERE v.evidence_ref <> h.high_price_evidence_refs[1]
        ),
        median_examples AS (
            SELECT
                supplier_document_key,
                buyer_document_digits,
                order_id,
                item_key,
                unit_key,
                quantity_band,
                year_bucket,
                list(evidence_ref ORDER BY distance_to_median, line_value DESC)
                    AS median_example_refs
            FROM median_examples_ranked
            WHERE evidence_rank <= 2
            GROUP BY
                supplier_document_key,
                buyer_document_digits,
                order_id,
                item_key,
                unit_key,
                quantity_band,
                year_bucket
        ),
        buyer_repeat_high AS (
            SELECT
                buyer_document_digits,
                item_key,
                unit_key,
                quantity_band,
                year_bucket,
                count(DISTINCT order_id) AS buyer_high_price_order_count,
                count(DISTINCT supplier_document_key) AS buyer_high_price_supplier_count,
                sum(high_price_line_value) AS buyer_high_price_value
            FROM order_item_rollup
            GROUP BY
                buyer_document_digits,
                item_key,
                unit_key,
                quantity_band,
                year_bucket
        ),
        joined AS (
            SELECT
                h.*,
                coalesce(p.supplier_tvec_order_count, 0) AS supplier_tvec_order_count,
                coalesce(p.supplier_tvec_buyer_count, 0) AS supplier_tvec_buyer_count,
                coalesce(p.supplier_tvec_total_value, 0.0) AS supplier_tvec_total_value,
                coalesce(p.supplier_tvec_buyer_count, 0) >= 50
                    AND coalesce(p.supplier_tvec_order_count, 0) >= 100
                    AND coalesce(p.supplier_tvec_total_value, 0.0) >= 1000000000
                    AS supplier_multi_entity_capture_proxy,
                b.buyer_high_price_order_count,
                b.buyer_high_price_supplier_count,
                b.buyer_high_price_value,
                m.median_example_refs
            FROM order_item_rollup h
            LEFT JOIN supplier_capture_proxy p
                ON p.supplier_document_key = h.supplier_document_key
            LEFT JOIN buyer_repeat_high b
                ON b.buyer_document_digits = h.buyer_document_digits
                AND b.item_key = h.item_key
                AND b.unit_key = h.unit_key
                AND b.quantity_band = h.quantity_band
                AND b.year_bucket = h.year_bucket
            LEFT JOIN median_examples m
                ON m.supplier_document_key = h.supplier_document_key
                AND m.buyer_document_digits = h.buyer_document_digits
                AND m.order_id = h.order_id
                AND m.item_key = h.item_key
                AND m.unit_key = h.unit_key
                AND m.quantity_band = h.quantity_band
                AND m.year_bucket = h.year_bucket
        )
        SELECT
            'tvec_item_price_dispersion_review_only' AS signal_id,
            'doc:' || supplier_document_key AS entity_id,
            supplier_document_key AS entity_key,
            'Company' AS entity_label,
            'tvec_price:' || item_key || ':' || unit_key || ':' || quantity_band
                || ':' || year_bucket || ':' || order_id || ':' || supplier_document_key
                AS scope_key,
            'tvec_price' AS scope_type,
            CASE
                WHEN high_price_line_value >= 100000000
                    OR unit_price_to_median_ratio >= 5
                    OR buyer_high_price_order_count >= 3
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.50
                    + least(greatest(unit_price_to_median_ratio - 2, 0) / 10.0, 0.20)
                    + CASE WHEN high_price_line_value >= 100000000 THEN 0.12 ELSE 0.0 END
                    + CASE WHEN supplier_multi_entity_capture_proxy THEN 0.10 ELSE 0.0 END
                    + least(coalesce(buyer_high_price_order_count, 0) / 20.0, 0.08)
                    + least(log10(greatest(high_price_line_value, 1)) / 140.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            supplier_document_digits,
            supplier_document_id,
            supplier_name,
            buyer_document_digits,
            buyer_document_id,
            buyer_name,
            order_id,
            order_date,
            item_key,
            item_name,
            item_name_normalized,
            unit_key,
            unit_name,
            quantity_band,
            year_bucket,
            observed_unit_price,
            observed_quantity,
            high_price_line_value,
            high_price_line_count,
            cdp,
            comparable_line_count,
            comparable_supplier_count,
            comparable_buyer_count,
            median_unit_price,
            p90_unit_price,
            p95_unit_price,
            min_unit_price,
            max_unit_price,
            unit_price_to_median_ratio,
            unit_price_to_p95_ratio,
            comparable_total_value,
            first_comparable_order_date,
            last_comparable_order_date,
            supplier_tvec_order_count,
            supplier_tvec_buyer_count,
            supplier_tvec_total_value,
            supplier_multi_entity_capture_proxy,
            buyer_high_price_order_count,
            buyer_high_price_supplier_count,
            buyer_high_price_value,
            'TVEC item-name comparator; does not prove overpricing, collusion, delivery failure, or corrupt intent'
                AS what_is_unproven,
            list_concat(high_price_evidence_refs, coalesce(median_example_refs, []::VARCHAR[]))
                AS evidence_refs
        FROM joined
        QUALIFY row_number() OVER (
            ORDER BY
                high_price_line_value DESC NULLS LAST,
                unit_price_to_median_ratio DESC NULLS LAST,
                comparable_line_count DESC NULLS LAST,
                order_id,
                supplier_document_key
        ) <= 25000
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
    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_cuentas_claras_donor_ineligibility_review AS
        WITH donor_income AS (
            SELECT
                row_number() OVER () AS income_row_id,
                coacc_document_key(income_party_id, tid_name) AS donor_document_key,
                coacc_doc_digits(income_party_id) AS donor_document_digits,
                coacc_nit_canonical(income_party_id, tid_name) AS donor_nit_canonical,
                nullif(trim(income_party_id), '') AS donor_document_id,
                nullif(trim(tid_name), '') AS donor_document_type,
                nullif(trim(person_name), '') AS donor_name,
                nullif(trim(candidate_id), '') AS candidate_document_id,
                nullif(trim(candidate_name), '') AS candidate_name,
                nullif(trim(cnd_name), '') AS office_name,
                nullif(trim(class_name), '') AS election_class,
                CASE
                    WHEN regexp_matches(
                        upper(coalesce(cnd_name, '') || ' ' || coalesce(class_name, '')),
                        'ALCALD'
                    ) THEN 'mayor'
                    WHEN regexp_matches(
                        upper(coalesce(cnd_name, '') || ' ' || coalesce(class_name, '')),
                        'GOBERN'
                    ) THEN 'governor'
                    ELSE 'other'
                END AS office_level,
                nullif(trim(department_name), '') AS campaign_department,
                nullif(coacc_text_key(department_name), '') AS campaign_department_key,
                nullif(trim(municipality_name), '') AS campaign_municipality,
                nullif(coacc_text_key(municipality_name), '') AS campaign_municipality_key,
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
            WHERE coacc_document_key(income_party_id, tid_name) IS NOT NULL
                AND NOT regexp_matches(
                    coacc_document_key(income_party_id, tid_name),
                    '^0+$'
                )
        ),
        valid_donor_income AS (
            SELECT *
            FROM donor_income
            WHERE income_amount IS NOT NULL
                AND income_amount > 0
                AND voucher_date BETWEEN DATE '2018-01-01' AND DATE '2021-12-31'
        ),
        donor_rollup AS (
            SELECT
                donor_document_key,
                any_value(donor_document_digits) AS donor_document_digits,
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
                any_value(office_level) AS office_level,
                any_value(campaign_department) AS campaign_department,
                any_value(campaign_department_key) AS campaign_department_key,
                any_value(campaign_municipality) AS campaign_municipality,
                any_value(campaign_municipality_key) AS campaign_municipality_key,
                any_value(campaign_organization) AS campaign_organization,
                any_value(party_coalition) AS party_coalition,
                any_value(campaign_account_id) AS campaign_account_id,
                count(*) AS income_record_count,
                count(DISTINCT income_type) FILTER (WHERE income_type IS NOT NULL)
                    AS income_type_count,
                sum(income_amount) AS total_income_amount,
                max(income_amount) AS max_income_amount,
                min(voucher_date) AS first_valid_voucher_date,
                max(voucher_date) AS last_valid_voucher_date
            FROM valid_donor_income
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
            FROM valid_donor_income
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
        contract_exposure AS (
            SELECT
                supplier_document_key,
                any_value(supplier_entity_id) AS supplier_entity_id,
                any_value(supplier_nit_canonical) AS supplier_nit_canonical,
                any_value(supplier_doc_type) AS supplier_doc_type,
                any_value(supplier_name) AS supplier_name,
                buyer_document_digits,
                any_value(buyer_document_id) AS buyer_document_id,
                any_value(buyer_name) AS buyer_name,
                any_value(department) AS buyer_department,
                nullif(coacc_text_key(department), '') AS buyer_department_key,
                any_value(city) AS buyer_city,
                nullif(coacc_text_key(city), '') AS buyer_city_key,
                count(DISTINCT contract_id) AS term_contract_count,
                sum(coalesce(contract_value, 0.0)) AS term_contract_value,
                min(signing_date) AS first_term_signing_date,
                max(signing_date) AS last_term_signing_date
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND NOT regexp_matches(supplier_document_key, '^0+$')
                AND signing_date BETWEEN DATE '2020-01-01' AND DATE '2023-12-31'
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
            GROUP BY
                supplier_document_key,
                buyer_document_digits,
                nullif(coacc_text_key(department), ''),
                nullif(coacc_text_key(city), '')
        ),
        contract_evidence_ranked AS (
            SELECT
                supplier_document_key,
                buyer_document_digits,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY supplier_document_key, buyer_document_digits
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM curated_contract_awards
            WHERE supplier_document_key IS NOT NULL
                AND signing_date BETWEEN DATE '2020-01-01' AND DATE '2023-12-31'
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
        ),
        contract_evidence AS (
            SELECT
                supplier_document_key,
                buyer_document_digits,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY supplier_document_key, buyer_document_digits
        ),
        eligible AS (
            SELECT
                c.supplier_entity_id,
                c.supplier_document_key,
                c.supplier_nit_canonical,
                c.supplier_doc_type,
                c.supplier_name,
                c.buyer_document_digits,
                c.buyer_document_id,
                c.buyer_name,
                c.buyer_department,
                c.buyer_department_key,
                c.buyer_city,
                c.buyer_city_key,
                c.term_contract_count,
                c.term_contract_value,
                c.first_term_signing_date,
                c.last_term_signing_date,
                d.donor_document_digits,
                d.donor_nit_canonical,
                d.donor_document_id,
                d.donor_document_type,
                d.donor_name,
                d.campaign_key,
                d.candidate_document_id,
                d.candidate_name,
                d.office_name,
                d.election_class,
                d.office_level,
                d.campaign_department,
                d.campaign_department_key,
                d.campaign_municipality,
                d.campaign_municipality_key,
                d.campaign_organization,
                d.party_coalition,
                d.campaign_account_id,
                d.income_record_count,
                d.income_type_count,
                d.total_income_amount,
                d.max_income_amount,
                d.first_valid_voucher_date,
                d.last_valid_voucher_date,
                CASE
                    WHEN d.office_level = 'mayor' THEN 'mayor_municipality'
                    WHEN d.office_level = 'governor' THEN 'governor_department'
                    ELSE 'unknown'
                END AS jurisdiction_match_type,
                list_concat(de.donor_evidence_refs, ce.contract_evidence_refs)
                    AS evidence_refs
            FROM donor_rollup d
            JOIN contract_exposure c
                ON c.supplier_document_key = d.donor_document_key
            JOIN donor_evidence de
                ON de.donor_document_key = d.donor_document_key
                AND de.campaign_key = d.campaign_key
            JOIN contract_evidence ce
                ON ce.supplier_document_key = c.supplier_document_key
                AND ce.buyer_document_digits = c.buyer_document_digits
            WHERE d.office_level IN ('mayor', 'governor')
                AND d.total_income_amount >= 1000000
                AND c.term_contract_value >= 100000000
                AND d.campaign_department_key IS NOT NULL
                AND c.buyer_department_key IS NOT NULL
                AND (
                    (
                        d.office_level = 'mayor'
                        AND d.campaign_department_key = c.buyer_department_key
                        AND d.campaign_municipality_key IS NOT NULL
                        AND c.buyer_city_key IS NOT NULL
                        AND d.campaign_municipality_key = c.buyer_city_key
                    )
                    OR (
                        d.office_level = 'governor'
                        AND d.campaign_department_key = c.buyer_department_key
                    )
                )
        )
        SELECT
            'cuentas_claras_donor_ineligibility_review' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            'donor_contract:2019:'
                || office_level
                || ':'
                || campaign_key
                || ':'
                || supplier_document_key
                || ':'
                || coalesce(buyer_document_digits, 'unknown') AS scope_key,
            'donor_contract_ineligibility_review' AS scope_type,
            'medium' AS severity,
            least(
                0.90,
                0.55
                    + least(log10(greatest(total_income_amount, 1)) / 120.0, 0.15)
                    + least(log10(greatest(term_contract_value, 1)) / 120.0, 0.15)
                    + CASE WHEN office_level = 'mayor' THEN 0.05 ELSE 0.03 END
                    + least(term_contract_count / 25.0, 0.07)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_nit_canonical,
            supplier_doc_type,
            donor_nit_canonical,
            donor_document_id,
            donor_document_digits,
            donor_document_type,
            donor_name,
            campaign_key,
            candidate_document_id,
            candidate_name,
            office_name,
            election_class,
            office_level,
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
            buyer_document_digits,
            buyer_document_id,
            buyer_name,
            buyer_department,
            buyer_city,
            jurisdiction_match_type,
            true AS same_jurisdiction_flag,
            term_contract_count,
            term_contract_value,
            first_term_signing_date,
            last_term_signing_date,
            DATE '2020-01-01' AS inferred_term_start_date,
            DATE '2023-12-31' AS inferred_term_end_date,
            'missing_campaign_cap' AS threshold_status,
            'missing_election_result' AS election_result_status,
            'inferred_2019_local_term' AS term_status,
            'needs_legal_threshold_and_election_result' AS legal_review_status,
            'Exact donor-supplier match and same elected jurisdiction; campaign cap, election result, and final legal ineligibility are not proven by this feature.'
                AS what_is_unproven,
            evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY total_income_amount DESC NULLS LAST,
                term_contract_value DESC NULLS LAST,
                term_contract_count DESC NULLS LAST,
                supplier_document_key
        ) <= 5000
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


def _create_public_declaration_supplier_chronology_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "asset_disclosures",
        "conflict_disclosures",
        "sigep_sensitive_positions",
        "secop_ii_contracts",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_public_declaration_supplier_chronology AS
        WITH asset_disclosures AS (
            SELECT
                coacc_cedula_key(ad.document_id, ad.document_type) AS person_document_key,
                coacc_doc_digits(ad.document_id) AS person_document_id,
                nullif(trim(ad.document_type), '') AS person_doc_type,
                nullif(trim(ad.form_number), '') AS form_number,
                cast(try_cast(ad.publication_date AS TIMESTAMP) AS DATE) AS publication_date,
                nullif(trim(ad.declaration_status), '') AS declaration_status,
                nullif(trim(ad.declaration_type), '') AS declaration_type,
                nullif(trim(ad.entity_name), '') AS disclosure_entity_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(ad.entity_name), ''), '')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS disclosure_entity_norm,
                nullif(trim(ad.declarant_role), '') AS declarant_role,
                nullif(trim(concat_ws(
                    ' ',
                    ad.declarant_first_name,
                    ad.declarant_second_name,
                    ad.declarant_first_lastname,
                    ad.declarant_second_lastname
                )), '') AS person_name,
                lower(trim(coalesce(ad.declarant_is_contractor, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS declarant_is_contractor_flag,
                lower(trim(coalesce(ad.private_economic_activities, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS private_economic_activity_flag,
                lower(trim(coalesce(ad.corp_society_participations, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS corporate_interest_flag,
                lower(trim(coalesce(ad.board_council_participations, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS board_interest_flag,
                'asset_disclosures:' || coalesce(
                    nullif(trim(ad.form_number), ''),
                    coacc_cedula_key(ad.document_id, ad.document_type)
                        || ':'
                        || coalesce(
                            strftime(cast(try_cast(ad.publication_date AS TIMESTAMP) AS DATE), '%Y%m%d'),
                            'undated'
                        )
                ) AS evidence_ref
            FROM src_asset_disclosures ad
            WHERE coacc_cedula_key(ad.document_id, ad.document_type) IS NOT NULL
        ),
        conflict_disclosures AS (
            SELECT
                coacc_cedula_key(cd.document_id, cd.document_type) AS person_document_key,
                coacc_doc_digits(cd.document_id) AS person_document_id,
                nullif(trim(cd.document_type), '') AS person_doc_type,
                nullif(trim(cd.form_number), '') AS form_number,
                cast(try_cast(cd.publication_date AS TIMESTAMP) AS DATE) AS publication_date,
                nullif(trim(cd.declaration_status), '') AS declaration_status,
                nullif(trim(cd.declaration_type), '') AS declaration_type,
                nullif(trim(cd.entity_name), '') AS disclosure_entity_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(cd.entity_name), ''), '')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS disclosure_entity_norm,
                nullif(trim(cd.declarant_role), '') AS declarant_role,
                nullif(trim(concat_ws(
                    ' ',
                    cd.declarant_first_name,
                    cd.declarant_second_name,
                    cd.declarant_first_lastname,
                    cd.declarant_second_lastname
                )), '') AS person_name,
                lower(trim(coalesce(cd.declarant_is_contractor, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS declarant_is_contractor_flag,
                lower(trim(coalesce(cd.direct_interest_actions, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS direct_interest_flag,
                lower(trim(coalesce(cd.conflict_relatives, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS relative_conflict_flag,
                lower(trim(coalesce(cd.conflict_donations, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS donation_conflict_flag,
                lower(trim(coalesce(cd.other_potential_conflicts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS other_conflict_flag,
                lower(trim(coalesce(cd.conflict_trusts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS trust_conflict_flag,
                lower(trim(coalesce(cd.other_conflict_investments, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS investment_conflict_flag,
                'conflict_disclosures:' || coalesce(
                    nullif(trim(cd.form_number), ''),
                    coacc_cedula_key(cd.document_id, cd.document_type)
                        || ':'
                        || coalesce(
                            strftime(cast(try_cast(cd.publication_date AS TIMESTAMP) AS DATE), '%Y%m%d'),
                            'undated'
                        )
                ) AS evidence_ref
            FROM src_conflict_disclosures cd
            WHERE coacc_cedula_key(cd.document_id, cd.document_type) IS NOT NULL
        ),
        conflict_scored AS (
            SELECT
                *,
                cast(direct_interest_flag AS INTEGER)
                    + cast(relative_conflict_flag AS INTEGER)
                    + cast(donation_conflict_flag AS INTEGER)
                    + cast(other_conflict_flag AS INTEGER)
                    + cast(trust_conflict_flag AS INTEGER)
                    + cast(investment_conflict_flag AS INTEGER)
                    AS conflict_flag_count
            FROM conflict_disclosures
        ),
        sensitive_positions AS (
            SELECT
                coacc_cedula_key(sp.funcionario_id, sp.document_type) AS person_document_key,
                coacc_doc_digits(sp.funcionario_id) AS person_document_id,
                nullif(trim(sp.document_type), '') AS person_doc_type,
                nullif(trim(sp.full_name), '') AS person_name,
                nullif(trim(sp.institution_id), '') AS institution_id,
                nullif(trim(sp.institution_name), '') AS institution_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(sp.institution_name), ''), '')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS institution_norm,
                nullif(trim(sp.job_hierarchy_level), '') AS job_hierarchy_level,
                nullif(trim(sp.appointment_type), '') AS appointment_type,
                nullif(trim(sp.current_job_title), '') AS current_job_title,
                try_cast(sp.start_date AS DATE) AS start_date,
                'sigep_sensitive_positions:'
                    || coacc_cedula_key(sp.funcionario_id, sp.document_type)
                    || ':'
                    || coalesce(nullif(trim(sp.institution_id), ''), 'unknown')
                    || ':'
                    || coalesce(strftime(try_cast(sp.start_date AS DATE), '%Y%m%d'), 'undated')
                    AS evidence_ref
            FROM src_sigep_sensitive_positions sp
            WHERE coacc_cedula_key(sp.funcionario_id, sp.document_type) IS NOT NULL
        ),
        declaration_entity_norms AS (
            SELECT DISTINCT person_document_key, disclosure_entity_norm AS entity_norm
            FROM asset_disclosures
            WHERE length(coalesce(disclosure_entity_norm, '')) >= 8
            UNION
            SELECT DISTINCT person_document_key, disclosure_entity_norm AS entity_norm
            FROM conflict_scored
            WHERE length(coalesce(disclosure_entity_norm, '')) >= 8
            UNION
            SELECT DISTINCT person_document_key, institution_norm AS entity_norm
            FROM sensitive_positions
            WHERE length(coalesce(institution_norm, '')) >= 8
        ),
        profile_keys AS (
            SELECT DISTINCT person_document_key FROM asset_disclosures
            UNION
            SELECT DISTINCT person_document_key FROM conflict_scored
            UNION
            SELECT DISTINCT person_document_key FROM sensitive_positions
        ),
        asset_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                any_value(person_name) AS person_name,
                any_value(disclosure_entity_name) AS asset_entity_name,
                any_value(declarant_role) AS asset_declarant_role,
                count(DISTINCT coalesce(form_number, evidence_ref)) AS asset_disclosure_count,
                min(publication_date) AS first_asset_publication_date,
                max(publication_date) AS last_asset_publication_date,
                bool_or(declarant_is_contractor_flag) AS asset_contractor_flag,
                bool_or(private_economic_activity_flag) AS asset_private_economic_activity_flag,
                bool_or(corporate_interest_flag) AS asset_corporate_interest_flag,
                bool_or(board_interest_flag) AS asset_board_interest_flag
            FROM asset_disclosures
            GROUP BY person_document_key
        ),
        conflict_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                any_value(person_name) AS person_name,
                any_value(disclosure_entity_name) AS conflict_entity_name,
                any_value(declarant_role) AS conflict_declarant_role,
                count(DISTINCT coalesce(form_number, evidence_ref)) AS conflict_disclosure_count,
                min(publication_date) AS first_conflict_publication_date,
                max(publication_date) AS last_conflict_publication_date,
                bool_or(declarant_is_contractor_flag) AS conflict_contractor_flag,
                max(conflict_flag_count) AS conflict_flag_count,
                bool_or(direct_interest_flag) AS conflict_direct_interest_flag,
                bool_or(relative_conflict_flag) AS conflict_relative_flag,
                bool_or(donation_conflict_flag) AS conflict_donation_flag,
                bool_or(other_conflict_flag) AS conflict_other_flag,
                bool_or(trust_conflict_flag) AS conflict_trust_flag,
                bool_or(investment_conflict_flag) AS conflict_investment_flag
            FROM conflict_scored
            GROUP BY person_document_key
        ),
        sensitive_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                any_value(person_name) AS person_name,
                any_value(institution_id) AS sensitive_institution_id,
                any_value(institution_name) AS sensitive_institution_name,
                any_value(job_hierarchy_level) AS sensitive_job_hierarchy_level,
                any_value(appointment_type) AS sensitive_appointment_type,
                any_value(current_job_title) AS sensitive_current_job_title,
                count(*) AS sensitive_position_count,
                min(start_date) AS first_sensitive_start_date,
                max(start_date) AS last_sensitive_start_date
            FROM sensitive_positions
            GROUP BY person_document_key
        ),
        asset_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                publication_date,
                form_number,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY publication_date DESC NULLS LAST, form_number DESC NULLS LAST
                ) AS evidence_rank
            FROM asset_disclosures
        ),
        asset_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY publication_date DESC, form_number DESC)
                    AS asset_evidence_refs
            FROM asset_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        conflict_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                publication_date,
                form_number,
                conflict_flag_count,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY
                        conflict_flag_count DESC,
                        publication_date DESC NULLS LAST,
                        form_number DESC NULLS LAST
                ) AS evidence_rank
            FROM conflict_scored
        ),
        conflict_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY conflict_flag_count DESC, publication_date DESC)
                    AS conflict_evidence_refs
            FROM conflict_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        sensitive_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                start_date,
                institution_id,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY start_date DESC NULLS LAST, institution_id
                ) AS evidence_rank
            FROM sensitive_positions
        ),
        sensitive_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY start_date DESC, institution_id)
                    AS sensitive_evidence_refs
            FROM sensitive_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        declaration_profile AS (
            SELECT
                k.person_document_key,
                coalesce(a.person_document_id, c.person_document_id, s.person_document_id)
                    AS person_document_id,
                coalesce(a.person_doc_type, c.person_doc_type, s.person_doc_type)
                    AS person_doc_type,
                coalesce(a.person_name, c.person_name, s.person_name) AS person_name,
                a.asset_entity_name,
                c.conflict_entity_name,
                s.sensitive_institution_id,
                s.sensitive_institution_name,
                coalesce(a.asset_declarant_role, c.conflict_declarant_role)
                    AS declarant_role,
                s.sensitive_job_hierarchy_level,
                s.sensitive_appointment_type,
                s.sensitive_current_job_title,
                coalesce(a.asset_disclosure_count, 0) AS asset_disclosure_count,
                coalesce(c.conflict_disclosure_count, 0) AS conflict_disclosure_count,
                coalesce(s.sensitive_position_count, 0) AS sensitive_position_count,
                a.first_asset_publication_date,
                a.last_asset_publication_date,
                c.first_conflict_publication_date,
                c.last_conflict_publication_date,
                s.first_sensitive_start_date,
                s.last_sensitive_start_date,
                nullif(
                    least(
                        coalesce(a.first_asset_publication_date, DATE '9999-12-31'),
                        coalesce(c.first_conflict_publication_date, DATE '9999-12-31'),
                        coalesce(s.first_sensitive_start_date, DATE '9999-12-31')
                    ),
                    DATE '9999-12-31'
                ) AS first_observed_date,
                coalesce(a.asset_contractor_flag, false) AS asset_contractor_flag,
                coalesce(a.asset_private_economic_activity_flag, false)
                    AS asset_private_economic_activity_flag,
                coalesce(a.asset_corporate_interest_flag, false)
                    AS asset_corporate_interest_flag,
                coalesce(a.asset_board_interest_flag, false) AS asset_board_interest_flag,
                coalesce(c.conflict_contractor_flag, false) AS conflict_contractor_flag,
                coalesce(c.conflict_flag_count, 0) AS conflict_flag_count,
                coalesce(c.conflict_direct_interest_flag, false) AS conflict_direct_interest_flag,
                coalesce(c.conflict_relative_flag, false) AS conflict_relative_flag,
                coalesce(c.conflict_donation_flag, false) AS conflict_donation_flag,
                coalesce(c.conflict_other_flag, false) AS conflict_other_flag,
                coalesce(c.conflict_trust_flag, false) AS conflict_trust_flag,
                coalesce(c.conflict_investment_flag, false) AS conflict_investment_flag,
                ae.asset_evidence_refs,
                ce.conflict_evidence_refs,
                se.sensitive_evidence_refs
            FROM profile_keys k
            LEFT JOIN asset_profile a
                ON a.person_document_key = k.person_document_key
            LEFT JOIN conflict_profile c
                ON c.person_document_key = k.person_document_key
            LEFT JOIN sensitive_profile s
                ON s.person_document_key = k.person_document_key
            LEFT JOIN asset_evidence ae
                ON ae.person_document_key = k.person_document_key
            LEFT JOIN conflict_evidence ce
                ON ce.person_document_key = k.person_document_key
            LEFT JOIN sensitive_evidence se
                ON se.person_document_key = k.person_document_key
        ),
        person_contracts AS (
            SELECT
                coacc_cedula_key(supplier_document_digits, supplier_doc_type)
                    AS person_document_key,
                supplier_document_digits AS supplier_document_id,
                supplier_doc_type,
                supplier_name,
                buyer_document_digits,
                buyer_document_id,
                buyer_name,
                regexp_replace(
                    lower(coalesce(nullif(trim(buyer_name), ''), '')),
                    '[^a-z0-9]+',
                    '',
                    'g'
                ) AS buyer_name_norm,
                department,
                city,
                sector,
                procurement_modality,
                contract_type,
                contract_id,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_start_date,
                contract_end_date
            FROM curated_contract_awards
            WHERE coacc_cedula_key(supplier_document_digits, supplier_doc_type)
                    IS NOT NULL
                AND supplier_document_digits IS NOT NULL
                AND length(supplier_document_digits) BETWEEN 6 AND 10
                AND NOT regexp_matches(supplier_document_digits, '^0+$')
                AND upper(coalesce(supplier_doc_type, '')) NOT IN ('NIT', 'RUT')
                AND buyer_document_digits IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
                AND signing_date IS NOT NULL
        ),
        pair_contracts AS (
            SELECT
                pc.*,
                coalesce(pc.buyer_document_digits, pc.buyer_name_norm) AS buyer_scope_key,
                den.entity_norm IS NOT NULL AS same_entity_match
            FROM person_contracts pc
            JOIN declaration_profile dp
                ON dp.person_document_key = pc.person_document_key
            LEFT JOIN declaration_entity_norms den
                ON den.person_document_key = pc.person_document_key
                AND den.entity_norm = pc.buyer_name_norm
            WHERE dp.first_observed_date IS NOT NULL
                AND pc.signing_date >= dp.first_observed_date
        ),
        pair_rollup AS (
            SELECT
                person_document_key,
                buyer_scope_key,
                any_value(supplier_document_id) AS supplier_document_id,
                any_value(supplier_doc_type) AS supplier_doc_type,
                any_value(supplier_name) AS supplier_name,
                any_value(buyer_document_digits) AS buyer_document_digits,
                any_value(buyer_document_id) AS buyer_document_id,
                any_value(buyer_name) AS buyer_name,
                any_value(department) AS department,
                any_value(city) AS city,
                any_value(sector) AS sector,
                any_value(procurement_modality) AS procurement_modality,
                any_value(contract_type) AS contract_type,
                count(DISTINCT contract_id) AS pair_contract_count,
                sum(contract_value) AS pair_total_contract_value,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date,
                sum(CASE WHEN same_entity_match THEN 1 ELSE 0 END)
                    AS same_entity_contract_count
            FROM pair_contracts
            GROUP BY person_document_key, buyer_scope_key
        ),
        person_exposure AS (
            SELECT
                person_document_key,
                count(DISTINCT contract_id) AS total_person_contract_count,
                count(DISTINCT buyer_scope_key) AS total_person_buyer_count,
                sum(contract_value) AS total_person_contract_value
            FROM pair_contracts
            GROUP BY person_document_key
        ),
        contract_evidence_ranked AS (
            SELECT
                person_document_key,
                buyer_scope_key,
                evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY person_document_key, buyer_scope_key
                    ORDER BY
                        contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM pair_contracts
        ),
        contract_evidence AS (
            SELECT
                person_document_key,
                buyer_scope_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 4
            GROUP BY person_document_key, buyer_scope_key
        ),
        eligible AS (
            SELECT
                dp.*,
                pr.buyer_scope_key,
                pr.supplier_document_id,
                pr.supplier_doc_type,
                pr.supplier_name,
                pr.buyer_document_digits,
                pr.buyer_document_id,
                pr.buyer_name,
                pr.department,
                pr.city,
                pr.sector,
                pr.procurement_modality,
                pr.contract_type,
                pr.pair_contract_count,
                pr.pair_total_contract_value,
                pr.max_contract_value,
                pr.first_signing_date,
                pr.last_signing_date,
                pr.same_entity_contract_count,
                pe.total_person_contract_count,
                pe.total_person_buyer_count,
                pe.total_person_contract_value,
                ce.contract_evidence_refs,
                (
                    dp.asset_contractor_flag
                    OR dp.asset_private_economic_activity_flag
                    OR dp.asset_corporate_interest_flag
                    OR dp.asset_board_interest_flag
                    OR dp.conflict_contractor_flag
                    OR dp.conflict_flag_count > 0
                    OR dp.sensitive_position_count > 0
                ) AS has_context_signal
            FROM declaration_profile dp
            JOIN pair_rollup pr
                ON pr.person_document_key = dp.person_document_key
            JOIN person_exposure pe
                ON pe.person_document_key = dp.person_document_key
            JOIN contract_evidence ce
                ON ce.person_document_key = pr.person_document_key
                AND ce.buyer_scope_key = pr.buyer_scope_key
            WHERE (
                    pr.same_entity_contract_count > 0
                    OR pr.pair_contract_count >= 2
                    OR pr.pair_total_contract_value >= 100000000
                )
                AND (
                    dp.asset_contractor_flag
                    OR dp.asset_private_economic_activity_flag
                    OR dp.asset_corporate_interest_flag
                    OR dp.asset_board_interest_flag
                    OR dp.conflict_contractor_flag
                    OR dp.conflict_flag_count > 0
                    OR dp.sensitive_position_count > 0
                    OR pr.same_entity_contract_count > 0
                )
        )
        SELECT
            'public_declaration_supplier_chronology_review_only' AS signal_id,
            'person:' || person_document_key AS entity_id,
            person_document_key AS entity_key,
            'Person' AS entity_label,
            'declaration_supplier:' || person_document_key || ':' || buyer_scope_key
                AS scope_key,
            'declaration_supplier' AS scope_type,
            CASE
                WHEN same_entity_contract_count > 0
                    OR pair_total_contract_value >= 2000000000
                    OR pair_contract_count >= 3
                    OR (
                        conflict_flag_count > 0
                        AND pair_total_contract_value >= 500000000
                    )
                    OR (
                        sensitive_position_count > 0
                        AND pair_total_contract_value >= 500000000
                    )
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.45
                    + CASE WHEN same_entity_contract_count > 0 THEN 0.18 ELSE 0.0 END
                    + CASE WHEN sensitive_position_count > 0 THEN 0.10 ELSE 0.0 END
                    + CASE
                        WHEN asset_contractor_flag OR conflict_contractor_flag
                            THEN 0.12
                        ELSE 0.0
                    END
                    + CASE
                        WHEN asset_private_economic_activity_flag
                            OR asset_corporate_interest_flag
                            OR asset_board_interest_flag
                            THEN 0.10
                        ELSE 0.0
                    END
                    + least(conflict_flag_count / 20.0, 0.12)
                    + least(log10(greatest(pair_total_contract_value, 1)) / 120.0, 0.12)
                    + least(pair_contract_count / 40.0, 0.08)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_PERSON_DOCUMENT' AS identity_match_type,
            'exact' AS identity_quality,
            person_document_id,
            person_doc_type,
            person_name,
            asset_entity_name,
            conflict_entity_name,
            sensitive_institution_id,
            sensitive_institution_name,
            declarant_role,
            sensitive_job_hierarchy_level,
            sensitive_appointment_type,
            sensitive_current_job_title,
            asset_disclosure_count,
            conflict_disclosure_count,
            sensitive_position_count,
            first_asset_publication_date,
            last_asset_publication_date,
            first_conflict_publication_date,
            last_conflict_publication_date,
            first_sensitive_start_date,
            last_sensitive_start_date,
            first_observed_date,
            asset_contractor_flag,
            asset_private_economic_activity_flag,
            asset_corporate_interest_flag,
            asset_board_interest_flag,
            conflict_contractor_flag,
            conflict_flag_count,
            conflict_direct_interest_flag,
            conflict_relative_flag,
            conflict_donation_flag,
            conflict_other_flag,
            conflict_trust_flag,
            conflict_investment_flag,
            supplier_document_id,
            supplier_doc_type,
            supplier_name,
            buyer_document_digits,
            buyer_document_id,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            pair_contract_count,
            pair_total_contract_value,
            max_contract_value,
            first_signing_date,
            last_signing_date,
            same_entity_contract_count,
            total_person_contract_count,
            total_person_buyer_count,
            total_person_contract_value,
            has_context_signal,
            'exact document chronology; does not prove conflict, employment status, omission, or corrupt intent'
                AS what_is_unproven,
            list_concat(
                list_concat(
                    list_concat(
                        coalesce(asset_evidence_refs, []::VARCHAR[]),
                        coalesce(conflict_evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(sensitive_evidence_refs, []::VARCHAR[])
                ),
                contract_evidence_refs
            ) AS evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN same_entity_contract_count > 0 THEN 1
                    ELSE 0
                END DESC,
                pair_total_contract_value DESC NULLS LAST,
                pair_contract_count DESC NULLS LAST,
                conflict_flag_count DESC NULLS LAST,
                person_document_key,
                buyer_scope_key
        ) <= 25000
    """)


def _create_pida_full30_meta_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if "secop_integrado" not in set(required_sources):
        return

    con.execute(r"""
        CREATE OR REPLACE TEMP VIEW curated_pida_full30_meta AS
        WITH raw AS (
            SELECT
                upper(coalesce(nullif(trim(entity_department), ''), 'NACIONAL'))
                    AS department,
                upper(coalesce(nullif(trim(entity_municipality), ''), 'NACIONAL'))
                    AS municipality,
                coalesce(
                    nullif(trim(contract_number), ''),
                    nullif(trim(process_number), '')
                ) AS contract_key,
                coalesce(
                    nullif(trim(contract_url), ''),
                    'secop_integrado:' || coalesce(
                        nullif(trim(contract_number), ''),
                        nullif(trim(process_number), '')
                    )
                ) AS evidence_ref,
                coacc_money(contract_value) AS contract_value,
                try_cast(contract_signing_date AS DATE) AS signing_date,
                lower(
                    coalesce(contract_object, '') || ' ' ||
                    coalesce(process_object, '') || ' ' ||
                    coalesce(contract_type, '') || ' ' ||
                    coalesce(procurement_modality, '')
                ) AS text_blob
            FROM src_secop_integrado
            WHERE coalesce(
                    nullif(trim(contract_number), ''),
                    nullif(trim(process_number), '')
                ) IS NOT NULL
        ),
        tagged AS (
            SELECT
                *,
                CASE
                    WHEN regexp_matches(
                        text_blob,
                        '(' ||
                        'alimentaci[oó]n escolar|\bpae\b|restaurante escolar|' ||
                        'complemento alimentario)'
                    ) THEN 'school_feeding'
                    WHEN regexp_matches(
                        text_blob,
                        '(hospital|salud|m[eé]dic|ambulancia|vacuna|biom[eé]dic|medicamento)'
                    ) THEN 'health'
                    WHEN regexp_matches(
                        text_blob,
                        '(acueducto|alcantarillado|agua potable|saneamiento|ptar|ptap)'
                    ) THEN 'water_sanitation'
                    WHEN regexp_matches(
                        text_blob,
                        '(v[ií]a |vial|carretera|paviment|placa huella|puente|camino|transporte)'
                    ) THEN 'roads_transport'
                    WHEN regexp_matches(
                        text_blob,
                        '(vivienda|habitacional|urbanizaci[oó]n)'
                    ) THEN 'housing'
                    WHEN regexp_matches(
                        text_blob,
                        '(' ||
                        'educaci[oó]n|colegio|instituci[oó]n educativa|' ||
                        'aula|biblioteca|universidad)'
                    ) THEN 'education'
                    WHEN regexp_matches(
                        text_blob,
                        '(energ[ií]a|el[eé]ctric|alumbrado|solar|gas combustible)'
                    ) THEN 'energy'
                    WHEN regexp_matches(
                        text_blob,
                        '(' ||
                        'internet|conectividad|software|tecnolog|' ||
                        'sistemas de informaci[oó]n|\btic\b)'
                    ) THEN 'digital_connectivity'
                    WHEN regexp_matches(
                        text_blob,
                        '(seguridad|polic[ií]a|c[aá]mara|convivencia|defensa|militar)'
                    ) THEN 'security'
                    WHEN regexp_matches(
                        text_blob,
                        '(deporte|recreaci[oó]n|cancha|parque|cultura|escenario deportivo)'
                    ) THEN 'sport_culture'
                    WHEN regexp_matches(
                        text_blob,
                        '(ambiente|ambiental|residuos|reforestaci[oó]n|riesgo|desastre|emergencia)'
                    ) THEN 'environment_risk'
                    WHEN regexp_matches(
                        text_blob,
                        '(agro|rural|campesin|productiv|riego|pecuario|agr[ií]col)'
                    ) THEN 'agriculture_rural'
                    WHEN regexp_matches(
                        text_blob,
                        '(minero|miner[ií]a|hidrocarburo|licencia ambiental)'
                    ) THEN 'extractives'
                    WHEN regexp_matches(
                        text_blob,
                        '(v[ií]ctima|paz|reincorporaci[oó]n|\bpdet\b|\bpnis\b|posconflicto)'
                    ) THEN 'peace_victims'
                    ELSE NULL
                END AS pida_category
            FROM raw
            WHERE contract_value IS NOT NULL
                AND contract_value >= 100000000
                AND municipality NOT IN ('NO DEFINIDO', 'NACIONAL')
        ),
        categorized AS (
            SELECT *
            FROM tagged
            WHERE pida_category IS NOT NULL
        ),
        rollup AS (
            SELECT
                department,
                municipality,
                department || ':' || municipality AS territory_key,
                count(DISTINCT contract_key) AS contract_count,
                count(DISTINCT pida_category) AS pida_category_count,
                sum(contract_value) AS total_contract_value,
                max(contract_value) AS max_contract_value,
                count(DISTINCT contract_key)
                    FILTER (WHERE contract_value >= 1000000000)
                    AS very_high_value_contract_count,
                min(signing_date) AS first_signing_date,
                max(signing_date) AS last_signing_date,
                list(DISTINCT pida_category ORDER BY pida_category) AS pida_categories
            FROM categorized
            GROUP BY department, municipality
        ),
        evidence_ranked AS (
            SELECT
                department || ':' || municipality AS territory_key,
                evidence_ref,
                contract_value,
                signing_date,
                contract_key,
                row_number() OVER (
                    PARTITION BY department, municipality
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_key
                ) AS evidence_rank
            FROM categorized
        ),
        evidence AS (
            SELECT
                territory_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC, contract_key)
                    AS evidence_refs
            FROM evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY territory_key
        )
        SELECT
            'pida_full30_meta' AS signal_id,
            'territory:' || r.territory_key AS entity_id,
            r.territory_key AS entity_key,
            'Territory' AS entity_label,
            'pida_full30_meta:' || r.territory_key AS scope_key,
            'territory' AS scope_type,
            CASE
                WHEN r.pida_category_count >= 14
                    AND r.total_contract_value >= 50000000000000 THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.45
                    + least(r.pida_category_count / 20.0, 0.25)
                    + least(log10(greatest(r.total_contract_value, 1)) / 90.0, 0.20)
                    + least(r.very_high_value_contract_count / 1000.0, 0.10)
            ) AS risk_signal,
            0.9 AS identity_confidence,
            'TERRITORY_AGGREGATE' AS identity_match_type,
            'aggregate' AS identity_quality,
            r.department,
            r.municipality,
            r.contract_count,
            r.pida_category_count,
            r.total_contract_value,
            r.max_contract_value,
            r.very_high_value_contract_count,
            r.first_signing_date,
            r.last_signing_date,
            r.pida_categories,
            e.evidence_refs
        FROM rollup r
        JOIN evidence e
            ON e.territory_key = r.territory_key
        WHERE r.pida_category_count >= 12
            AND r.contract_count >= 500
            AND r.very_high_value_contract_count >= 50
            AND r.total_contract_value >= 1000000000000
        QUALIFY row_number() OVER (
            ORDER BY r.total_contract_value DESC NULLS LAST,
                r.pida_category_count DESC,
                r.territory_key
        ) <= 1000
    """)


def _create_pida_chain_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_integrado", "secop_sanctions"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_pida5_pida27_pida4_chain AS
        WITH sanctions AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                nullif(trim(process_id), '') AS sanction_process_id,
                nullif(trim(process_reference), '') AS sanction_process_reference,
                coalesce(
                    nullif(trim(act_number), ''),
                    nullif(trim(process_id), ''),
                    nullif(trim(contract_id), '')
                ) AS sanction_record_id,
                nullif(trim(entity_id), '') AS sanction_entity_id,
                nullif(trim(entity_name), '') AS sanction_entity_name,
                nullif(trim(supplier_code), '') AS sanction_supplier_code,
                nullif(trim(supplier_name), '') AS sanction_supplier_name,
                coacc_money(amount) AS sanction_amount,
                coacc_money(amount_paid) AS sanction_amount_paid,
                try_cast(event_date AS DATE) AS sanction_event_date,
                nullif(trim(applied_warranties), '') AS applied_warranties,
                nullif(trim(sanction_type), '') AS sanction_type,
                nullif(trim(status), '') AS sanction_status,
                nullif(trim(type), '') AS sanction_event_type,
                nullif(trim(version_number), '') AS sanction_version_number
            FROM src_secop_sanctions
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND (
                    try_cast(event_date AS DATE) IS NULL
                    OR try_cast(event_date AS DATE) <= current_date
                )
        ),
        integrated_contracts AS (
            SELECT *
            FROM (
                SELECT
                    nullif(trim(contract_number), '') AS contract_id,
                    nullif(trim(process_number), '') AS process_id,
                    coacc_document_key(supplier_document, supplier_doc_type)
                        AS supplier_document_key,
                    coacc_nit_canonical(supplier_document, supplier_doc_type)
                        AS supplier_nit_canonical,
                    coacc_cedula_key(supplier_document, supplier_doc_type)
                        AS supplier_person_key,
                    nullif(trim(supplier_document), '') AS supplier_document_id,
                    nullif(trim(supplier_doc_type), '') AS supplier_doc_type,
                    nullif(trim(contractor_business_name), '') AS supplier_name,
                    nullif(trim(entity_secop_code), '') AS buyer_secop_code,
                    nullif(trim(entity_nit), '') AS buyer_document_id,
                    nullif(trim(entity_name), '') AS buyer_name,
                    coalesce(nullif(trim(entity_department), ''), 'NACIONAL')
                        AS department,
                    coalesce(nullif(trim(entity_municipality), ''), 'NACIONAL')
                        AS municipality,
                    nullif(trim(entity_level), '') AS entity_level,
                    nullif(trim(process_status), '') AS process_status,
                    nullif(trim(procurement_modality), '') AS procurement_modality,
                    nullif(trim(contract_type), '') AS contract_type,
                    nullif(trim(origin), '') AS origin,
                    nullif(trim(contract_object), '') AS contract_object,
                    nullif(trim(process_object), '') AS process_object,
                    coacc_money(contract_value) AS contract_value,
                    try_cast(contract_signing_date AS DATE) AS signing_date,
                    try_cast(contract_start_date AS DATE) AS contract_start_date,
                    try_cast(contract_end_date AS DATE) AS contract_end_date,
                    coacc_reference_url(contract_url) AS contract_url,
                    row_number() OVER (
                        PARTITION BY nullif(trim(contract_number), '')
                        ORDER BY coacc_money(contract_value) DESC NULLS LAST,
                            try_cast(contract_signing_date AS DATE) DESC NULLS LAST
                    ) AS contract_rank
                FROM src_secop_integrado
                WHERE nullif(trim(contract_number), '') IS NOT NULL
                    AND coacc_money(contract_value) IS NOT NULL
                    AND coacc_money(contract_value) > 0
            )
            WHERE contract_rank = 1
        ),
        joined AS (
            SELECT
                c.*,
                s.sanction_process_id,
                s.sanction_process_reference,
                s.sanction_record_id,
                s.sanction_entity_id,
                s.sanction_entity_name,
                s.sanction_supplier_code,
                s.sanction_supplier_name,
                s.sanction_amount,
                s.sanction_amount_paid,
                s.sanction_event_date,
                s.applied_warranties,
                s.sanction_type,
                s.sanction_status,
                s.sanction_event_type,
                s.sanction_version_number,
                department || ':' || municipality AS territory_key
            FROM sanctions s
            JOIN integrated_contracts c
                ON c.contract_id = s.contract_id
        ),
        rollup AS (
            SELECT
                territory_key,
                any_value(department) AS department,
                any_value(municipality) AS municipality,
                count(*) AS sanction_event_count,
                count(DISTINCT contract_id) AS sanctioned_contract_count,
                count(DISTINCT supplier_document_key)
                    FILTER (WHERE supplier_document_key IS NOT NULL)
                    AS sanctioned_supplier_count,
                count(DISTINCT buyer_document_id)
                    FILTER (WHERE buyer_document_id IS NOT NULL)
                    AS buyer_count,
                sum(contract_value) AS sanctioned_contract_value,
                sum(coalesce(sanction_amount, 0.0)) AS sanction_amount_total,
                max(contract_value) AS max_contract_value,
                min(signing_date) AS first_contract_signing_date,
                max(signing_date) AS last_contract_signing_date,
                min(sanction_event_date) AS first_sanction_event_date,
                max(sanction_event_date) AS last_sanction_event_date
            FROM joined
            GROUP BY territory_key
        ),
        sanction_evidence_ranked AS (
            SELECT
                territory_key,
                'secop_sanctions:' || sanction_record_id AS evidence_ref,
                coalesce(sanction_amount, 0.0) AS sanction_amount,
                sanction_event_date,
                contract_value,
                row_number() OVER (
                    PARTITION BY territory_key
                    ORDER BY coalesce(sanction_amount, 0.0) DESC,
                        contract_value DESC NULLS LAST,
                        sanction_event_date DESC NULLS LAST,
                        sanction_record_id
                ) AS evidence_rank
            FROM joined
            WHERE sanction_record_id IS NOT NULL
        ),
        sanction_evidence AS (
            SELECT
                territory_key,
                list(evidence_ref ORDER BY sanction_amount DESC, contract_value DESC)
                    AS sanction_evidence_refs
            FROM sanction_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY territory_key
        ),
        contract_evidence_ranked AS (
            SELECT
                territory_key,
                coalesce(contract_url, 'secop_integrado:' || contract_id)
                    AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY territory_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM joined
            WHERE contract_id IS NOT NULL
        ),
        contract_evidence AS (
            SELECT
                territory_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS contract_evidence_refs
            FROM contract_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY territory_key
        ),
        supplier_samples_ranked AS (
            SELECT
                territory_key,
                coalesce(supplier_name, sanction_supplier_name, supplier_document_key)
                    AS supplier_sample,
                sum(contract_value) AS supplier_contract_value,
                row_number() OVER (
                    PARTITION BY territory_key
                    ORDER BY sum(contract_value) DESC NULLS LAST,
                        coalesce(supplier_name, sanction_supplier_name, supplier_document_key)
                ) AS sample_rank
            FROM joined
            GROUP BY territory_key,
                coalesce(supplier_name, sanction_supplier_name, supplier_document_key)
        ),
        supplier_samples AS (
            SELECT
                territory_key,
                list(supplier_sample ORDER BY supplier_contract_value DESC, supplier_sample)
                    AS supplier_sample
            FROM supplier_samples_ranked
            WHERE sample_rank <= 10
            GROUP BY territory_key
        )
        SELECT
            'pida5_pida27_pida4_chain' AS signal_id,
            'territory:' || r.territory_key AS entity_id,
            r.territory_key AS entity_key,
            'Territory' AS entity_label,
            'pida5_pida27_pida4_chain:' || r.territory_key AS scope_key,
            'territory' AS scope_type,
            CASE
                WHEN r.sanctioned_contract_value >= 1000000000000
                    OR r.sanction_event_count >= 25
                    THEN 'critical'
                WHEN r.sanctioned_contract_value >= 10000000000
                    OR r.sanction_event_count >= 5
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.55
                    + least(log10(greatest(r.sanctioned_contract_value, 1)) / 120.0, 0.18)
                    + least(r.sanction_event_count / 100.0, 0.15)
                    + least(r.sanctioned_supplier_count / 50.0, 0.07)
                    + least(r.buyer_count / 50.0, 0.05)
            ) AS risk_signal,
            0.9 AS identity_confidence,
            'EXACT_CONTRACT_KEY_AGGREGATE' AS identity_match_type,
            'aggregate' AS identity_quality,
            r.department,
            r.municipality,
            r.sanction_event_count,
            r.sanctioned_contract_count,
            r.sanctioned_supplier_count,
            r.buyer_count,
            r.sanctioned_contract_value,
            r.sanction_amount_total,
            r.max_contract_value,
            r.first_contract_signing_date,
            r.last_contract_signing_date,
            r.first_sanction_event_date,
            r.last_sanction_event_date,
            s.supplier_sample,
            list_concat(se.sanction_evidence_refs, ce.contract_evidence_refs)
                AS evidence_refs
        FROM rollup r
        JOIN sanction_evidence se
            ON se.territory_key = r.territory_key
        JOIN contract_evidence ce
            ON ce.territory_key = r.territory_key
        LEFT JOIN supplier_samples s
            ON s.territory_key = r.territory_key
        WHERE r.sanctioned_contract_value >= 100000000
        QUALIFY row_number() OVER (
            ORDER BY r.sanctioned_contract_value DESC NULLS LAST,
                r.sanction_event_count DESC,
                r.territory_key
        ) <= 1000
    """)


def _create_secop_sanction_later_awards_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if not ({"secop_sanctions", "secop_ii_contracts"} <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_secop_sanction_later_awards AS
        WITH raw_sanctions AS (
            SELECT
                row_number() OVER () AS sanction_row_id,
                nullif(trim(contract_id), '') AS sanction_contract_id,
                nullif(trim(process_id), '') AS sanction_process_id,
                nullif(trim(process_reference), '') AS sanction_process_reference,
                nullif(trim(entity_id), '') AS sanction_entity_id,
                nullif(trim(entity_name), '') AS sanction_entity_name,
                nullif(trim(supplier_code), '') AS sanction_supplier_code,
                nullif(trim(supplier_name), '') AS sanction_supplier_name,
                coacc_money(amount) AS sanction_amount,
                coacc_money(amount_paid) AS sanction_amount_paid,
                try_cast(event_date AS DATE) AS sanction_event_date,
                nullif(trim(applied_warranties), '') AS applied_warranties,
                CASE
                    WHEN lower(trim(coalesce(act_number, ''))) IN (
                        '',
                        'no definido',
                        'no definido.'
                    ) THEN NULL
                    ELSE nullif(trim(act_number), '')
                END AS sanction_act_number,
                CASE
                    WHEN lower(trim(coalesce(sanction_type, ''))) IN (
                        '',
                        'no definido',
                        'no definido.'
                    ) THEN NULL
                    ELSE nullif(trim(sanction_type), '')
                END AS sanction_type,
                nullif(trim(description_other_type), '') AS sanction_description_other_type,
                nullif(trim(status), '') AS sanction_status,
                lower(trim(coalesce(status, ''))) AS sanction_status_norm,
                nullif(trim(type), '') AS sanction_event_type,
                nullif(trim(version_number), '') AS sanction_version_number
            FROM src_secop_sanctions
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND (
                    try_cast(event_date AS DATE) IS NULL
                    OR try_cast(event_date AS DATE) <= current_date
                )
        ),
        sanction_rows AS (
            SELECT
                *,
                coalesce(
                    sanction_act_number,
                    sanction_process_id,
                    sanction_contract_id || ':' || cast(sanction_row_id AS VARCHAR)
                ) AS sanction_record_id
            FROM raw_sanctions
            WHERE sanction_status_norm NOT LIKE '%borrador%'
        ),
        sanctioned_contracts AS (
            SELECT
                s.sanction_row_id,
                s.sanction_record_id,
                s.sanction_contract_id,
                s.sanction_process_id,
                s.sanction_process_reference,
                s.sanction_entity_id,
                s.sanction_entity_name,
                s.sanction_supplier_code,
                s.sanction_supplier_name,
                s.sanction_amount,
                s.sanction_amount_paid,
                s.sanction_event_date,
                s.applied_warranties,
                s.sanction_act_number,
                s.sanction_type,
                s.sanction_description_other_type,
                s.sanction_status,
                s.sanction_event_type,
                s.sanction_version_number,
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_nit_canonical,
                a.supplier_doc_type,
                a.supplier_name,
                a.buyer_document_id AS sanction_buyer_document_id,
                a.buyer_document_digits AS sanction_buyer_document_digits,
                a.buyer_name AS sanction_buyer_name,
                a.department AS sanction_department,
                a.city AS sanction_city,
                a.procurement_modality AS sanction_procurement_modality,
                a.contract_type AS sanction_contract_type,
                a.contract_value AS sanction_contract_value,
                a.signing_date AS sanction_contract_signing_date,
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    AS sanction_contract_ref
            FROM sanction_rows s
            JOIN curated_contract_awards a
                ON a.contract_id = s.sanction_contract_id
            WHERE a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
        ),
        later_awards AS (
            SELECT
                s.sanction_row_id,
                s.sanction_record_id,
                s.sanction_contract_id,
                later.contract_id AS later_contract_id,
                later.contract_reference AS later_contract_reference,
                later.process_id AS later_process_id,
                coalesce(later.process_url, 'secop_ii_contracts:' || later.contract_id)
                    AS later_contract_ref,
                later.buyer_document_id AS later_buyer_document_id,
                later.buyer_document_digits AS later_buyer_document_digits,
                later.buyer_name AS later_buyer_name,
                later.department AS later_department,
                later.city AS later_city,
                later.procurement_modality AS later_procurement_modality,
                later.contract_type AS later_contract_type,
                later.contract_value AS later_contract_value,
                later.signing_date AS later_signing_date,
                later.contract_start_date AS later_contract_start_date,
                later.contract_end_date AS later_contract_end_date,
                later.buyer_document_id = s.sanction_buyer_document_id
                    AS same_buyer_later_award_flag,
                regexp_matches(
                    lower(coalesce(later.procurement_modality, '')),
                    'directa|urgencia|especial|minima|mínima'
                ) AS direct_or_exception_later_award_flag
            FROM sanctioned_contracts s
            JOIN curated_contract_awards later
                ON later.supplier_document_key = s.supplier_document_key
                AND later.contract_id <> s.sanction_contract_id
                AND later.contract_id IS NOT NULL
                AND later.signing_date IS NOT NULL
                AND later.contract_value IS NOT NULL
                AND later.contract_value > 0
                AND later.signing_date >= coalesce(
                    s.sanction_event_date,
                    s.sanction_contract_signing_date,
                    DATE '1900-01-01'
                )
        ),
        exposure_rollup AS (
            SELECT
                s.sanction_row_id,
                s.sanction_record_id,
                s.sanction_contract_id,
                any_value(s.sanction_process_id) AS sanction_process_id,
                any_value(s.sanction_process_reference) AS sanction_process_reference,
                any_value(s.sanction_entity_id) AS sanction_entity_id,
                any_value(s.sanction_entity_name) AS sanction_entity_name,
                any_value(s.sanction_supplier_code) AS sanction_supplier_code,
                any_value(s.sanction_supplier_name) AS sanction_supplier_name,
                any_value(s.sanction_amount) AS sanction_amount,
                any_value(s.sanction_amount_paid) AS sanction_amount_paid,
                any_value(s.sanction_event_date) AS sanction_event_date,
                any_value(s.applied_warranties) AS applied_warranties,
                any_value(s.sanction_act_number) AS sanction_act_number,
                any_value(s.sanction_type) AS sanction_type,
                any_value(s.sanction_description_other_type)
                    AS sanction_description_other_type,
                any_value(s.sanction_status) AS sanction_status,
                any_value(s.sanction_event_type) AS sanction_event_type,
                any_value(s.sanction_version_number) AS sanction_version_number,
                any_value(s.supplier_entity_id) AS supplier_entity_id,
                s.supplier_document_key,
                any_value(s.supplier_nit_canonical) AS supplier_nit_canonical,
                any_value(s.supplier_doc_type) AS supplier_doc_type,
                any_value(coalesce(s.supplier_name, s.sanction_supplier_name))
                    AS supplier_name,
                any_value(s.sanction_buyer_document_id) AS sanction_buyer_document_id,
                any_value(s.sanction_buyer_document_digits)
                    AS sanction_buyer_document_digits,
                any_value(s.sanction_buyer_name) AS sanction_buyer_name,
                any_value(s.sanction_department) AS sanction_department,
                any_value(s.sanction_city) AS sanction_city,
                any_value(s.sanction_procurement_modality)
                    AS sanction_procurement_modality,
                any_value(s.sanction_contract_type) AS sanction_contract_type,
                any_value(s.sanction_contract_value) AS sanction_contract_value,
                any_value(s.sanction_contract_signing_date)
                    AS sanction_contract_signing_date,
                any_value(s.sanction_contract_ref) AS sanction_contract_ref,
                count(DISTINCT l.later_contract_id) AS later_contract_count,
                count(DISTINCT l.later_buyer_document_id) AS later_buyer_count,
                sum(l.later_contract_value) AS later_contract_value,
                max(l.later_contract_value) AS max_later_contract_value,
                min(l.later_signing_date) AS first_later_signing_date,
                max(l.later_signing_date) AS last_later_signing_date,
                count(DISTINCT l.later_contract_id) FILTER (
                    WHERE l.same_buyer_later_award_flag
                ) AS same_buyer_later_contract_count,
                sum(l.later_contract_value) FILTER (
                    WHERE l.same_buyer_later_award_flag
                ) AS same_buyer_later_contract_value,
                count(DISTINCT l.later_contract_id) FILTER (
                    WHERE l.direct_or_exception_later_award_flag
                ) AS direct_or_exception_later_contract_count,
                bool_or(l.same_buyer_later_award_flag) AS same_buyer_later_award_flag,
                bool_or(l.direct_or_exception_later_award_flag)
                    AS direct_or_exception_later_award_flag
            FROM sanctioned_contracts s
            JOIN later_awards l
                ON l.sanction_row_id = s.sanction_row_id
                AND l.sanction_record_id = s.sanction_record_id
                AND l.sanction_contract_id = s.sanction_contract_id
            GROUP BY
                s.sanction_row_id,
                s.sanction_record_id,
                s.sanction_contract_id,
                s.supplier_document_key
        ),
        later_evidence_ranked AS (
            SELECT
                sanction_row_id,
                sanction_record_id,
                sanction_contract_id,
                later_contract_ref AS evidence_ref,
                later_contract_value,
                later_signing_date,
                later_contract_id,
                row_number() OVER (
                    PARTITION BY sanction_row_id, sanction_record_id, sanction_contract_id
                    ORDER BY later_contract_value DESC NULLS LAST,
                        later_signing_date DESC NULLS LAST,
                        later_contract_id
                ) AS evidence_rank
            FROM later_awards
        ),
        later_evidence AS (
            SELECT
                sanction_row_id,
                sanction_record_id,
                sanction_contract_id,
                list(evidence_ref ORDER BY later_contract_value DESC, later_signing_date DESC)
                    AS later_evidence_refs
            FROM later_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY sanction_row_id, sanction_record_id, sanction_contract_id
        )
        SELECT
            'procurement_secop_sanction_later_awards_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            'secop_sanction_later_awards:'
                || e.sanction_contract_id
                || ':'
                || supplier_document_key
                || ':'
                || cast(e.sanction_row_id AS VARCHAR) AS scope_key,
            'secop_sanction_later_awards' AS scope_type,
            CASE
                WHEN same_buyer_later_award_flag
                    OR coalesce(later_contract_value, 0.0) >= 5000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.95,
                0.55
                    + CASE WHEN same_buyer_later_award_flag THEN 0.15 ELSE 0.0 END
                    + CASE
                        WHEN lower(coalesce(sanction_status, '')) LIKE '%publicado%'
                            THEN 0.08
                        ELSE 0.0
                    END
                    + least(log10(greatest(coalesce(later_contract_value, 0.0), 1)) / 100.0, 0.12)
                    + least(later_contract_count / 50.0, 0.08)
                    + CASE
                        WHEN direct_or_exception_later_award_flag THEN 0.07
                        ELSE 0.0
                    END
                    + least(log10(greatest(coalesce(sanction_amount, 0.0), 1)) / 120.0, 0.07)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_nit_canonical,
            supplier_doc_type,
            e.sanction_record_id,
            e.sanction_contract_id,
            sanction_process_id,
            sanction_process_reference,
            sanction_entity_id,
            sanction_entity_name,
            sanction_supplier_code,
            sanction_supplier_name,
            sanction_amount,
            sanction_amount_paid,
            sanction_event_date,
            applied_warranties,
            sanction_act_number,
            sanction_type,
            sanction_description_other_type,
            sanction_status,
            sanction_event_type,
            sanction_version_number,
            sanction_buyer_document_id,
            sanction_buyer_document_digits,
            sanction_buyer_name,
            sanction_department,
            sanction_city,
            sanction_procurement_modality,
            sanction_contract_type,
            sanction_contract_value,
            sanction_contract_signing_date,
            later_contract_count,
            later_buyer_count,
            later_contract_value,
            max_later_contract_value,
            first_later_signing_date,
            last_later_signing_date,
            same_buyer_later_contract_count,
            coalesce(same_buyer_later_contract_value, 0.0)
                AS same_buyer_later_contract_value,
            direct_or_exception_later_contract_count,
            same_buyer_later_award_flag,
            direct_or_exception_later_award_flag,
            'exact SECOP sanction-contract to supplier match and later exact supplier awards; sanction finality, appeal status, legal ineligibility, and corrupt intent are not proven by this feature'
                AS what_is_unproven,
            list_concat(
                ['secop_sanctions:' || e.sanction_record_id, sanction_contract_ref],
                le.later_evidence_refs
            ) AS evidence_refs
        FROM exposure_rollup e
        JOIN later_evidence le
            ON le.sanction_row_id = e.sanction_row_id
            AND le.sanction_record_id = e.sanction_record_id
            AND le.sanction_contract_id = e.sanction_contract_id
        WHERE later_contract_count >= 1
            AND (
                later_contract_value >= 100000000
                OR same_buyer_later_contract_count > 0
            )
        QUALIFY row_number() OVER (
            ORDER BY later_contract_value DESC NULLS LAST,
                same_buyer_later_contract_count DESC NULLS LAST,
                later_contract_count DESC NULLS LAST,
                e.sanction_contract_id
        ) <= 1000
    """)


def _create_fiscal_procurement_chronology_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {"secop_ii_contracts", "fiscal_findings", "fiscal_responsibility"}
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_fiscal_procurement_chronology AS
        WITH finding_rows AS (
            SELECT
                row_number() OVER () AS fiscal_row_id,
                'fiscal_finding' AS fiscal_source_type,
                'fiscal_findings' AS fiscal_source_id,
                coalesce(nullif(trim(radicado), ''), 'sin_radicado')
                    || ':'
                    || cast(row_number() OVER () AS VARCHAR) AS fiscal_record_id,
                coacc_document_key(nit, 'NIT') AS fiscal_subject_document_key,
                coacc_nit_canonical(nit, 'NIT') AS fiscal_subject_nit_canonical,
                nullif(trim(nit), '') AS fiscal_subject_document_id,
                nullif(trim(entity_name), '') AS fiscal_subject_name,
                'Hallazgo fiscal' AS fiscal_record_type,
                nullif(trim(status), '') AS fiscal_status,
                nullif(trim(process), '') AS fiscal_topic,
                nullif(trim(audited_year), '') AS fiscal_period,
                try_cast(received_date AS DATE) AS fiscal_event_date,
                try_cast(report_date AS DATE) AS fiscal_report_date,
                try_cast(procedure_date AS DATE) AS fiscal_procedure_date,
                NULL::DATE AS fiscal_finality_date,
                coacc_money_decimal(amount) AS fiscal_amount,
                nullif(trim(facts), '') AS fiscal_description,
                nullif(trim(observations), '') AS fiscal_observations
            FROM src_fiscal_findings
            WHERE coacc_document_key(nit, 'NIT') IS NOT NULL
        ),
        responsibility_rows AS (
            SELECT
                row_number() OVER () AS fiscal_row_id,
                'fiscal_responsibility' AS fiscal_source_type,
                'fiscal_responsibility' AS fiscal_source_id,
                coalesce(nullif(trim(n_mero_de_resoluci_n_de_la), ''), 'sin_resolucion')
                    || ':'
                    || cast(row_number() OVER () AS VARCHAR) AS fiscal_record_id,
                coacc_document_key(nit, 'NIT') AS fiscal_subject_document_key,
                coacc_nit_canonical(nit, 'NIT') AS fiscal_subject_nit_canonical,
                nullif(trim(nit), '') AS fiscal_subject_document_id,
                nullif(trim(entity_name), '') AS fiscal_subject_name,
                coalesce(
                    nullif(trim(sanction_type), ''),
                    nullif(trim(topic), ''),
                    'Responsabilidad fiscal'
                ) AS fiscal_record_type,
                nullif(trim(appeal_info), '') AS fiscal_status,
                nullif(trim(topic), '') AS fiscal_topic,
                NULL::VARCHAR AS fiscal_period,
                try_cast(resolution_date AS DATE) AS fiscal_event_date,
                NULL::DATE AS fiscal_report_date,
                try_cast(appeal_resolution_date AS DATE) AS fiscal_procedure_date,
                try_cast(decision_finality_date AS DATE) AS fiscal_finality_date,
                coacc_money_decimal(amount) AS fiscal_amount,
                nullif(trim(description), '') AS fiscal_description,
                nullif(trim(source_system), '') AS fiscal_observations
            FROM src_fiscal_responsibility
            WHERE coacc_document_key(nit, 'NIT') IS NOT NULL
        ),
        fiscal_records AS (
            SELECT * FROM finding_rows
            UNION ALL
            SELECT * FROM responsibility_rows
        ),
        contract_matches AS (
            SELECT
                f.*,
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_nit_canonical,
                a.supplier_doc_type,
                a.supplier_name,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.buyer_document_id,
                a.buyer_document_digits,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_value,
                a.signing_date,
                a.contract_start_date,
                a.contract_end_date,
                date_diff('day', f.fiscal_event_date, a.signing_date)
                    AS days_after_fiscal_event,
                regexp_matches(
                    lower(coalesce(a.procurement_modality, '')),
                    'directa|urgencia|especial|minima|mínima'
                ) AS direct_or_exception_award_flag,
                regexp_matches(
                    lower(
                        coalesce(a.supplier_name, '') || ' ' || coalesce(f.fiscal_subject_name, '')
                    ),
                    'alcald|municipio|gobernaci|departamento|ese|hospital|instituto|universidad|empresa de servicios p|emcali|emcasa|emcaservicios'
                ) AS public_entity_subject_flag
            FROM fiscal_records f
            JOIN curated_contract_awards a
                ON a.supplier_document_key = f.fiscal_subject_document_key
            WHERE f.fiscal_event_date IS NOT NULL
                AND a.signing_date IS NOT NULL
                AND a.signing_date >= f.fiscal_event_date
                AND a.contract_id IS NOT NULL
                AND a.contract_value IS NOT NULL
                AND a.contract_value > 0
                AND (
                    a.contract_value >= 100000000
                    OR coalesce(f.fiscal_amount, 0.0) >= 100000000
                    OR f.fiscal_source_type = 'fiscal_responsibility'
                )
        ),
        evidence_ranked AS (
            SELECT
                fiscal_source_type,
                fiscal_source_id,
                fiscal_record_id,
                fiscal_subject_document_key,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id) AS evidence_ref,
                contract_value,
                signing_date,
                contract_id,
                row_number() OVER (
                    PARTITION BY
                        fiscal_source_type,
                        fiscal_record_id,
                        fiscal_subject_document_key
                    ORDER BY contract_value DESC NULLS LAST,
                        signing_date DESC NULLS LAST,
                        contract_id
                ) AS evidence_rank
            FROM contract_matches
        ),
        evidence AS (
            SELECT
                fiscal_source_type,
                fiscal_record_id,
                fiscal_subject_document_key,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS later_evidence_refs
            FROM evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY
                fiscal_source_type,
                fiscal_record_id,
                fiscal_subject_document_key
        ),
        rollup AS (
            SELECT
                fiscal_source_type,
                fiscal_source_id,
                fiscal_record_id,
                fiscal_subject_document_id,
                fiscal_subject_document_key,
                fiscal_subject_nit_canonical,
                fiscal_subject_name,
                fiscal_record_type,
                fiscal_status,
                fiscal_topic,
                fiscal_period,
                fiscal_event_date,
                fiscal_report_date,
                fiscal_procedure_date,
                fiscal_finality_date,
                fiscal_amount,
                fiscal_description,
                fiscal_observations,
                any_value(coalesce(supplier_entity_id, 'doc:' || supplier_document_key))
                    AS entity_id,
                any_value(supplier_document_key) AS entity_key,
                bool_or(supplier_nit_canonical IS NOT NULL) AS company_identity_flag,
                any_value(supplier_name) AS supplier_name,
                any_value(supplier_nit_canonical) AS supplier_nit_canonical,
                any_value(supplier_doc_type) AS supplier_doc_type,
                bool_or(public_entity_subject_flag) AS public_entity_subject_flag,
                count(DISTINCT contract_id) AS later_contract_count,
                count(DISTINCT buyer_document_id) AS later_buyer_count,
                sum(contract_value) AS later_contract_value,
                max(contract_value) AS max_later_contract_value,
                min(signing_date) AS first_later_signing_date,
                max(signing_date) AS last_later_signing_date,
                min(days_after_fiscal_event) AS min_days_after_fiscal_event,
                max(days_after_fiscal_event) AS max_days_after_fiscal_event,
                count(DISTINCT contract_id) FILTER (
                    WHERE direct_or_exception_award_flag
                ) AS direct_or_exception_later_contract_count,
                bool_or(direct_or_exception_award_flag)
                    AS direct_or_exception_award_flag
            FROM contract_matches
            GROUP BY
                fiscal_source_type,
                fiscal_source_id,
                fiscal_record_id,
                fiscal_subject_document_id,
                fiscal_subject_document_key,
                fiscal_subject_nit_canonical,
                fiscal_subject_name,
                fiscal_record_type,
                fiscal_status,
                fiscal_topic,
                fiscal_period,
                fiscal_event_date,
                fiscal_report_date,
                fiscal_procedure_date,
                fiscal_finality_date,
                fiscal_amount,
                fiscal_description,
                fiscal_observations
        )
        SELECT
            'fiscal_procurement_chronology_review_only' AS signal_id,
            r.entity_id,
            r.entity_key,
            CASE
                WHEN company_identity_flag THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            'fiscal_procurement:'
                || r.fiscal_source_type
                || ':'
                || r.fiscal_record_id
                || ':'
                || r.fiscal_subject_document_key AS scope_key,
            'fiscal_procurement_chronology' AS scope_type,
            CASE
                WHEN r.fiscal_source_type = 'fiscal_responsibility'
                    AND (
                        r.fiscal_finality_date IS NOT NULL
                        OR coalesce(r.fiscal_amount, 0.0) >= 1000000000
                        OR coalesce(r.later_contract_value, 0.0) >= 5000000000
                    )
                    THEN 'critical'
                WHEN coalesce(r.later_contract_value, 0.0) >= 1000000000
                    OR coalesce(r.fiscal_amount, 0.0) >= 100000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.97,
                0.58
                    + CASE
                        WHEN r.fiscal_source_type = 'fiscal_responsibility' THEN 0.12
                        ELSE 0.0
                    END
                    + CASE WHEN r.fiscal_finality_date IS NOT NULL THEN 0.08 ELSE 0.0 END
                    + least(log10(greatest(coalesce(r.fiscal_amount, 0.0), 1)) / 120.0, 0.08)
                    + least(log10(greatest(coalesce(r.later_contract_value, 0.0), 1)) / 100.0, 0.09)
                    + least(r.later_contract_count / 100.0, 0.05)
                    + CASE WHEN r.direct_or_exception_award_flag THEN 0.06 ELSE 0.0 END
                    + CASE
                        WHEN r.min_days_after_fiscal_event BETWEEN 0 AND 730 THEN 0.05
                        ELSE 0.0
                    END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN company_identity_flag THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            r.fiscal_source_type,
            r.fiscal_source_id,
            r.fiscal_record_id,
            r.fiscal_subject_document_id,
            r.fiscal_subject_document_key,
            r.fiscal_subject_nit_canonical,
            r.fiscal_subject_name,
            r.fiscal_record_type,
            r.fiscal_status,
            r.fiscal_topic,
            r.fiscal_period,
            r.fiscal_event_date,
            r.fiscal_report_date,
            r.fiscal_procedure_date,
            r.fiscal_finality_date,
            r.fiscal_amount,
            left(r.fiscal_description, 1000) AS fiscal_description,
            left(r.fiscal_observations, 500) AS fiscal_observations,
            r.supplier_name,
            r.supplier_nit_canonical,
            r.supplier_doc_type,
            r.public_entity_subject_flag,
            r.later_contract_count,
            r.later_buyer_count,
            r.later_contract_value,
            r.max_later_contract_value,
            r.first_later_signing_date,
            r.last_later_signing_date,
            r.min_days_after_fiscal_event,
            r.max_days_after_fiscal_event,
            r.direct_or_exception_later_contract_count,
            r.direct_or_exception_award_flag,
            'exact Contraloria fiscal record to SECOP supplier chronology; this does not prove current legal disability, final fiscal liability for findings, contract illegality, public-entity role misuse, or corrupt intent'
                AS what_is_unproven,
            list_concat(
                [r.fiscal_source_id || ':' || r.fiscal_record_id],
                e.later_evidence_refs
            ) AS evidence_refs
        FROM rollup r
        JOIN evidence e
            ON e.fiscal_source_type = r.fiscal_source_type
            AND e.fiscal_record_id = r.fiscal_record_id
            AND e.fiscal_subject_document_key = r.fiscal_subject_document_key
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN r.fiscal_source_type = 'fiscal_responsibility' THEN 1
                    ELSE 0
                END DESC,
                r.later_contract_value DESC NULLS LAST,
                r.fiscal_amount DESC NULLS LAST,
                r.later_contract_count DESC NULLS LAST,
                r.fiscal_record_id
        ) <= 1000
    """)


def _create_siri_antecedent_procurement_chronology_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "siri_antecedents",
        "secop_ii_contracts",
        "company_registry_c82u",
        "secop_suppliers",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_siri_antecedent_procurement_chronology AS
        WITH siri_raw AS (
            SELECT
                row_number() OVER () AS siri_row_id,
                nullif(trim(siri_number), '') AS siri_number,
                nullif(trim(ineligibility_type), '') AS ineligibility_type,
                nullif(trim(person_quality), '') AS person_quality,
                nullif(trim(identification_type_code), '') AS identification_type_code,
                nullif(trim(identification_type_name), '') AS identification_type_name,
                coacc_document_key(document_id, identification_type_name)
                    AS siri_document_key,
                coacc_nit_canonical(document_id, identification_type_name)
                    AS siri_nit_canonical,
                nullif(trim(document_id), '') AS raw_document_id,
                concat_ws(
                    ' ',
                    nullif(trim(first_name), ''),
                    nullif(trim(second_name), ''),
                    nullif(trim(first_last_name), ''),
                    nullif(trim(second_last_name), '')
                ) AS subject_name,
                nullif(trim(role_or_position), '') AS role_or_position,
                nullif(trim(facts_department), '') AS facts_department,
                nullif(trim(facts_municipality), '') AS facts_municipality,
                nullif(trim(sanctions), '') AS sanctions,
                lower(coalesce(sanctions, '')) AS sanctions_norm,
                coalesce(try_cast(duration_years AS INTEGER), 0) AS duration_years,
                coalesce(try_cast(duration_months AS INTEGER), 0) AS duration_months,
                coalesce(try_cast(duration_days AS INTEGER), 0) AS duration_days,
                nullif(trim(decision_instance), '') AS decision_instance,
                nullif(trim(authority), '') AS authority,
                try_cast(try_strptime(legal_effects_date, '%d/%m/%Y') AS DATE)
                    AS legal_effects_date,
                nullif(trim(process_number), '') AS process_number,
                nullif(trim(sanctioned_entity), '') AS sanctioned_entity,
                nullif(trim(sanctioned_entity_department), '')
                    AS sanctioned_entity_department,
                nullif(trim(sanctioned_entity_municipality), '')
                    AS sanctioned_entity_municipality
            FROM src_siri_antecedents
            WHERE coacc_document_key(document_id, identification_type_name) IS NOT NULL
        ),
        siri AS (
            SELECT
                *,
                coalesce(
                    siri_number,
                    process_number,
                    'row-' || cast(siri_row_id AS VARCHAR)
                ) AS siri_record_id,
                (
                    duration_years + duration_months + duration_days
                ) > 0 AS has_duration,
                CASE
                    WHEN legal_effects_date IS NOT NULL
                        AND (
                            duration_years + duration_months + duration_days
                        ) > 0
                        THEN legal_effects_date
                            + (duration_years * INTERVAL '1 year')
                            + (duration_months * INTERVAL '1 month')
                            + (duration_days * INTERVAL '1 day')
                    ELSE NULL
                END AS active_until_date,
                regexp_matches(
                    sanctions_norm,
                    'inhabilidad|destituci[oó]n|destitucion|suspensi[oó]n|'
                    || 'suspension|separaci[oó]n|separacion|exclusi[oó]n|exclusion'
                ) AS severe_sanction_keyword_flag
            FROM siri_raw
            WHERE siri_document_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
                AND NOT regexp_matches(siri_document_key, '^0+$')
                AND legal_effects_date IS NOT NULL
        ),
        direct_supplier_exposure AS (
            SELECT
                'direct_supplier' AS exposure_type,
                a.supplier_document_key AS subject_document_key,
                coalesce(a.supplier_entity_id, 'doc:' || a.supplier_document_key)
                    AS subject_entity_id,
                a.supplier_nit_canonical AS subject_nit_canonical,
                a.supplier_name,
                a.supplier_doc_type,
                NULL::VARCHAR AS represented_supplier_key,
                NULL::VARCHAR AS represented_supplier_name,
                NULL::VARCHAR AS representative_source,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.buyer_document_id,
                a.buyer_document_digits,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_value,
                a.signing_date AS exposure_date,
                a.contract_start_date,
                a.contract_end_date,
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    AS exposure_evidence_ref,
                NULL::VARCHAR AS representative_evidence_ref
            FROM curated_contract_awards a
            WHERE a.supplier_document_key IS NOT NULL
                AND a.signing_date IS NOT NULL
        ),
        role_exposure AS (
            SELECT
                'role_supervisor' AS exposure_type,
                coacc_document_key(supervisor_doc_number, supervisor_doc_type)
                    AS subject_document_key,
                'doc:' || coacc_document_key(supervisor_doc_number, supervisor_doc_type)
                    AS subject_entity_id,
                coacc_nit_canonical(supervisor_doc_number, supervisor_doc_type)
                    AS subject_nit_canonical,
                nullif(trim(supervisor_name), '') AS supplier_name,
                nullif(trim(supervisor_doc_type), '') AS supplier_doc_type,
                NULL::VARCHAR AS represented_supplier_key,
                NULL::VARCHAR AS represented_supplier_name,
                NULL::VARCHAR AS representative_source,
                nullif(trim(contract_id), '') AS contract_id,
                nullif(trim(contract_reference), '') AS contract_reference,
                nullif(trim(procurement_process), '') AS process_id,
                coacc_reference_url(process_url) AS process_url,
                nullif(trim(entity_nit), '') AS buyer_document_id,
                coacc_doc_digits(entity_nit) AS buyer_document_digits,
                nullif(trim(entity_name), '') AS buyer_name,
                nullif(trim(department), '') AS department,
                nullif(trim(city), '') AS city,
                nullif(trim(sector), '') AS sector,
                nullif(trim(procurement_modality), '') AS procurement_modality,
                nullif(trim(contract_type), '') AS contract_type,
                coacc_money(contract_value) AS contract_value,
                try_cast(signing_date AS DATE) AS exposure_date,
                try_cast(contract_start_date AS DATE) AS contract_start_date,
                try_cast(contract_end_date AS DATE) AS contract_end_date,
                'secop_ii_contracts:role:' || nullif(trim(contract_id), '')
                    || ':supervisor_doc_number' AS exposure_evidence_ref,
                NULL::VARCHAR AS representative_evidence_ref
            FROM src_secop_ii_contracts
            WHERE coacc_document_key(supervisor_doc_number, supervisor_doc_type)
                IS NOT NULL
                AND nullif(trim(contract_id), '') IS NOT NULL
            UNION ALL
            SELECT
                'role_spending_orderer' AS exposure_type,
                coacc_document_key(spending_orderer_doc_number, spending_orderer_doc_type),
                'doc:' || coacc_document_key(
                    spending_orderer_doc_number,
                    spending_orderer_doc_type
                ),
                coacc_nit_canonical(
                    spending_orderer_doc_number,
                    spending_orderer_doc_type
                ),
                nullif(trim(spending_orderer_name), ''),
                nullif(trim(spending_orderer_doc_type), ''),
                NULL::VARCHAR,
                NULL::VARCHAR,
                NULL::VARCHAR,
                nullif(trim(contract_id), ''),
                nullif(trim(contract_reference), ''),
                nullif(trim(procurement_process), ''),
                coacc_reference_url(process_url),
                nullif(trim(entity_nit), ''),
                coacc_doc_digits(entity_nit),
                nullif(trim(entity_name), ''),
                nullif(trim(department), ''),
                nullif(trim(city), ''),
                nullif(trim(sector), ''),
                nullif(trim(procurement_modality), ''),
                nullif(trim(contract_type), ''),
                coacc_money(contract_value),
                try_cast(signing_date AS DATE),
                try_cast(contract_start_date AS DATE),
                try_cast(contract_end_date AS DATE),
                'secop_ii_contracts:role:' || nullif(trim(contract_id), '')
                    || ':spending_orderer_doc_number',
                NULL::VARCHAR
            FROM src_secop_ii_contracts
            WHERE coacc_document_key(
                    spending_orderer_doc_number,
                    spending_orderer_doc_type
                ) IS NOT NULL
                AND nullif(trim(contract_id), '') IS NOT NULL
            UNION ALL
            SELECT
                'role_payment_orderer' AS exposure_type,
                coacc_document_key(payment_orderer_doc_number, payment_orderer_doc_type),
                'doc:' || coacc_document_key(
                    payment_orderer_doc_number,
                    payment_orderer_doc_type
                ),
                coacc_nit_canonical(
                    payment_orderer_doc_number,
                    payment_orderer_doc_type
                ),
                nullif(trim(payment_orderer_name), ''),
                nullif(trim(payment_orderer_doc_type), ''),
                NULL::VARCHAR,
                NULL::VARCHAR,
                NULL::VARCHAR,
                nullif(trim(contract_id), ''),
                nullif(trim(contract_reference), ''),
                nullif(trim(procurement_process), ''),
                coacc_reference_url(process_url),
                nullif(trim(entity_nit), ''),
                coacc_doc_digits(entity_nit),
                nullif(trim(entity_name), ''),
                nullif(trim(department), ''),
                nullif(trim(city), ''),
                nullif(trim(sector), ''),
                nullif(trim(procurement_modality), ''),
                nullif(trim(contract_type), ''),
                coacc_money(contract_value),
                try_cast(signing_date AS DATE),
                try_cast(contract_start_date AS DATE),
                try_cast(contract_end_date AS DATE),
                'secop_ii_contracts:role:' || nullif(trim(contract_id), '')
                    || ':payment_orderer_doc_number',
                NULL::VARCHAR
            FROM src_secop_ii_contracts
            WHERE coacc_document_key(
                    payment_orderer_doc_number,
                    payment_orderer_doc_type
                ) IS NOT NULL
                AND nullif(trim(contract_id), '') IS NOT NULL
        ),
        rues_representatives AS (
            SELECT
                coacc_document_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) AS representative_document_key,
                coacc_document_key(document_id, identification_class) AS company_key,
                nullif(trim(business_name), '') AS company_name,
                'company_registry_c82u:' || coalesce(
                    nullif(trim(cast(":id" AS VARCHAR)), ''),
                    coacc_document_key(document_id, identification_class) || ':'
                    || coacc_document_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    )
                ) AS representative_evidence_ref
            FROM src_company_registry_c82u
            WHERE coacc_document_key(
                    num_identificacion_representante_legal,
                    clase_identificacion_rl
                ) IS NOT NULL
                AND coacc_document_key(document_id, identification_class) IS NOT NULL
        ),
        supplier_registry_representatives AS (
            SELECT
                coacc_document_key(
                    n_mero_doc_representante_legal,
                    tipo_doc_representante_legal
                ) AS representative_document_key,
                coacc_document_key(nit, 'NIT') AS company_key,
                nullif(trim(nombre), '') AS company_name,
                'secop_suppliers:' || coalesce(
                    nullif(trim(codigo), ''),
                    nullif(trim(nit), ''),
                    coacc_document_key(nit, 'NIT') || ':'
                    || coacc_document_key(
                        n_mero_doc_representante_legal,
                        tipo_doc_representante_legal
                    )
                ) AS representative_evidence_ref
            FROM src_secop_suppliers
            WHERE coacc_document_key(
                    n_mero_doc_representante_legal,
                    tipo_doc_representante_legal
                ) IS NOT NULL
                AND coacc_document_key(nit, 'NIT') IS NOT NULL
        ),
        representative_exposure AS (
            SELECT
                'rues_legal_representative_supplier' AS exposure_type,
                r.representative_document_key AS subject_document_key,
                'doc:' || r.representative_document_key AS subject_entity_id,
                NULL::VARCHAR AS subject_nit_canonical,
                r.company_name AS supplier_name,
                'CC' AS supplier_doc_type,
                a.supplier_document_key AS represented_supplier_key,
                a.supplier_name AS represented_supplier_name,
                'company_registry_c82u' AS representative_source,
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.buyer_document_id,
                a.buyer_document_digits,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_value,
                a.signing_date AS exposure_date,
                a.contract_start_date,
                a.contract_end_date,
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                    AS exposure_evidence_ref,
                r.representative_evidence_ref
            FROM rues_representatives r
            JOIN curated_contract_awards a
                ON a.supplier_document_key = r.company_key
                AND a.signing_date IS NOT NULL
            UNION ALL
            SELECT
                'secop_supplier_legal_representative',
                r.representative_document_key,
                'doc:' || r.representative_document_key,
                NULL::VARCHAR,
                r.company_name,
                'CC',
                a.supplier_document_key,
                a.supplier_name,
                'secop_suppliers',
                a.contract_id,
                a.contract_reference,
                a.process_id,
                a.process_url,
                a.buyer_document_id,
                a.buyer_document_digits,
                a.buyer_name,
                a.department,
                a.city,
                a.sector,
                a.procurement_modality,
                a.contract_type,
                a.contract_value,
                a.signing_date,
                a.contract_start_date,
                a.contract_end_date,
                coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id),
                r.representative_evidence_ref
            FROM supplier_registry_representatives r
            JOIN curated_contract_awards a
                ON a.supplier_document_key = r.company_key
                AND a.signing_date IS NOT NULL
        ),
        exposures AS (
            SELECT * FROM direct_supplier_exposure
            UNION ALL
            SELECT * FROM role_exposure
            UNION ALL
            SELECT * FROM representative_exposure
        ),
        joined AS (
            SELECT
                s.*,
                e.*,
                e.exposure_date BETWEEN s.legal_effects_date
                    AND s.active_until_date AS active_period_overlap_flag,
                (
                    NOT s.has_duration
                    AND e.exposure_date >= s.legal_effects_date
                    AND e.exposure_date >= DATE '2018-01-01'
                ) AS duration_missing_post_effect_flag
            FROM siri s
            JOIN exposures e
                ON e.subject_document_key = s.siri_document_key
                AND e.exposure_date >= s.legal_effects_date
            WHERE e.contract_id IS NOT NULL
                AND e.exposure_date IS NOT NULL
                AND (
                    e.exposure_date BETWEEN s.legal_effects_date
                        AND s.active_until_date
                    OR (
                        NOT s.has_duration
                        AND s.severe_sanction_keyword_flag
                        AND e.exposure_date >= DATE '2018-01-01'
                        AND (
                            coalesce(e.contract_value, 0.0) >= 100000000
                            OR e.exposure_type LIKE 'role_%'
                        )
                    )
                )
        ),
        scored AS (
            SELECT
                *,
                CASE
                    WHEN active_period_overlap_flag THEN 'active_ineligibility_inferred'
                    WHEN duration_missing_post_effect_flag THEN
                        'duration_missing_post_effect'
                    ELSE 'expired_or_unknown_antecedent'
                END AS status_bucket,
                CASE
                    WHEN exposure_type LIKE 'role_%' THEN 'contract_role'
                    WHEN exposure_type LIKE '%representative%' THEN 'legal_representative'
                    ELSE 'supplier_award'
                END AS exposure_group
            FROM joined
        ),
        deduped_scored AS (
            SELECT *
            FROM scored
            QUALIFY row_number() OVER (
                PARTITION BY
                    siri_document_key,
                    exposure_type,
                    contract_id,
                    siri_record_id
                ORDER BY contract_value DESC NULLS LAST,
                    exposure_date DESC,
                    representative_evidence_ref NULLS LAST,
                    exposure_evidence_ref
            ) = 1
        )
        SELECT
            'siri_antecedent_procurement_chronology_review_only' AS signal_id,
            CASE
                WHEN siri_nit_canonical IS NOT NULL THEN 'doc:' || siri_document_key
                ELSE 'person:' || siri_document_key
            END AS entity_id,
            siri_document_key AS entity_key,
            CASE
                WHEN siri_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            'siri_procurement:' || siri_document_key || ':' || exposure_type || ':'
                || contract_id || ':' || siri_record_id AS scope_key,
            'siri_procurement_chronology' AS scope_type,
            CASE
                WHEN active_period_overlap_flag
                    AND (
                        coalesce(contract_value, 0.0) >= 1000000000
                        OR exposure_group IN ('legal_representative', 'contract_role')
                    )
                    THEN 'critical'
                WHEN active_period_overlap_flag THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.98,
                0.54
                    + CASE WHEN active_period_overlap_flag THEN 0.22 ELSE 0.08 END
                    + CASE WHEN exposure_group = 'legal_representative' THEN 0.08 ELSE 0.0 END
                    + CASE WHEN exposure_group = 'contract_role' THEN 0.08 ELSE 0.0 END
                    + CASE WHEN severe_sanction_keyword_flag THEN 0.05 ELSE 0.0 END
                    + least(log10(greatest(coalesce(contract_value, 0.0), 1)) / 140.0, 0.10)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN siri_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            subject_name,
            siri_number,
            ineligibility_type,
            person_quality,
            identification_type_name,
            raw_document_id,
            role_or_position,
            facts_department,
            facts_municipality,
            sanctions,
            decision_instance,
            authority,
            legal_effects_date,
            duration_years,
            duration_months,
            duration_days,
            active_until_date,
            has_duration,
            active_period_overlap_flag,
            duration_missing_post_effect_flag,
            status_bucket,
            process_number,
            sanctioned_entity,
            sanctioned_entity_department,
            sanctioned_entity_municipality,
            exposure_type,
            exposure_group,
            supplier_name,
            supplier_doc_type,
            represented_supplier_key,
            represented_supplier_name,
            representative_source,
            contract_id,
            contract_reference,
            process_id,
            process_url,
            buyer_document_id,
            buyer_document_digits,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            contract_value,
            exposure_date,
            contract_start_date,
            contract_end_date,
            'exact SIRI document to procurement exposure chronology; active legal disability is inferred only from effect date and duration fields, and legal finality, exceptions, employment status, representation validity, and corrupt intent are not proven by this feature'
                AS what_is_unproven,
            CASE
                WHEN representative_evidence_ref IS NOT NULL THEN [
                    'siri_antecedents:' || siri_record_id,
                    exposure_evidence_ref,
                    representative_evidence_ref
                ]
                ELSE [
                    'siri_antecedents:' || siri_record_id,
                    exposure_evidence_ref
                ]
            END AS evidence_refs
        FROM deduped_scored
        WHERE active_period_overlap_flag
            OR duration_missing_post_effect_flag
        QUALIFY row_number() OVER (
            ORDER BY active_period_overlap_flag DESC,
                CASE
                    WHEN exposure_group = 'legal_representative' THEN 0
                    WHEN exposure_group = 'contract_role' THEN 1
                    ELSE 2
                END,
                contract_value DESC NULLS LAST,
                legal_effects_date DESC,
                siri_record_id,
                contract_id
        ) <= 2000
    """)


def _create_guarantee_advance_execution_chain_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_guarantee_advance_execution_chain AS
        WITH contract_financials AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                any_value(nullif(trim(contract_status), '')) AS contract_status,
                any_value(nullif(trim(enables_advance_payment), ''))
                    AS enables_advance_payment,
                any_value(nullif(trim(liquidation), '')) AS liquidation,
                max(coacc_money_decimal(advance_payment_value)) AS advance_payment_value,
                max(coacc_money_decimal(invoiced_value)) AS invoiced_value,
                max(coacc_money_decimal(pending_payment_value)) AS pending_payment_value,
                max(coacc_money_decimal(paid_value)) AS paid_value,
                max(coacc_money_decimal(amortized_value)) AS amortized_value,
                max(coacc_money_decimal(pending_value)) AS pending_value,
                max(coacc_money_decimal(pending_execution_value))
                    AS pending_execution_value
            FROM src_secop_ii_contracts
            WHERE nullif(trim(contract_id), '') IS NOT NULL
            GROUP BY nullif(trim(contract_id), '')
        ),
        base_contracts AS (
            SELECT
                a.*,
                f.contract_status,
                f.enables_advance_payment,
                f.liquidation,
                f.advance_payment_value,
                f.invoiced_value,
                f.pending_payment_value,
                f.paid_value,
                f.amortized_value,
                f.pending_value,
                f.pending_execution_value,
                coalesce(f.advance_payment_value, 0.0) / nullif(a.contract_value, 0)
                    AS advance_payment_share,
                coalesce(f.pending_execution_value, 0.0) / nullif(a.contract_value, 0)
                    AS pending_execution_share,
                coalesce(f.paid_value, 0.0) / nullif(a.contract_value, 0)
                    AS paid_value_share,
                coalesce(f.invoiced_value, 0.0) / nullif(a.contract_value, 0)
                    AS invoiced_value_share
            FROM curated_contract_awards a
            JOIN contract_financials f
                ON f.contract_id = a.contract_id
            WHERE a.contract_id IS NOT NULL
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 500000000
                AND a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
        ),
        raw_execution AS (
            SELECT
                nullif(trim(identificadorcontrato), '') AS contract_id,
                coalesce(
                    nullif(trim(referencia_de_articulos), ''),
                    nullif(trim(nombreplan), ''),
                    nullif(trim(identificadorcontrato), '') || ':' ||
                        cast(row_number() OVER () AS VARCHAR)
                ) AS execution_item_id,
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
                ) AS actual_progress_pct
            FROM src_secop_contract_execution
            WHERE nullif(trim(identificadorcontrato), '') IS NOT NULL
        ),
        execution_scored AS (
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
                expected_progress_pct - actual_progress_pct AS progress_gap_pct
            FROM raw_execution
        ),
        execution_lagged AS (
            SELECT
                *,
                greatest(
                    coalesce(delivery_delay_days, 0),
                    coalesce(unresolved_delay_days, 0)
                ) AS effective_delay_days,
                (
                    coalesce(delivery_delay_days, unresolved_delay_days, 0) >= 30
                    OR coalesce(progress_gap_pct, 0) >= 30
                ) AS lag_flag
            FROM execution_scored
        ),
        execution_rollup AS (
            SELECT
                contract_id,
                count(*) AS execution_item_count,
                count(*) FILTER (WHERE lag_flag) AS delayed_item_count,
                max(effective_delay_days) AS max_delay_days,
                max(progress_gap_pct) AS max_progress_gap_pct,
                max(observed_at) AS last_execution_observed_at
            FROM execution_lagged
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
            FROM execution_lagged
            WHERE lag_flag
        ),
        execution_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY effective_delay_days DESC, progress_gap_pct DESC)
                    AS execution_evidence_refs
            FROM execution_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY contract_id
        ),
        suspension_events AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                lower(nullif(trim(tipo), '')) AS event_type_norm,
                coalesce(
                    try_cast(fecha_de_aprobacion AS TIMESTAMP),
                    try_cast(fecha_de_creacion AS TIMESTAMP)
                ) AS event_at,
                nullif(trim(proposito_de_la_modificacion), '') AS event_purpose
            FROM src_secop_contract_suspensions
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        suspension_rollup AS (
            SELECT
                contract_id,
                count(*) FILTER (
                    WHERE event_type_norm LIKE '%suspension%'
                ) AS suspension_event_count,
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
            FROM suspension_events
            GROUP BY contract_id
        ),
        suspension_evidence_ranked AS (
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
            FROM suspension_events
            WHERE event_type_norm LIKE '%suspension%'
        ),
        suspension_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY event_at DESC, evidence_ref)
                    AS suspension_evidence_refs
            FROM suspension_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY contract_id
        ),
        modification_events AS (
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
                coalesce(
                    try_cast(fecha_de_aprobacion AS TIMESTAMP),
                    try_cast(fecha_version AS TIMESTAMP),
                    try_cast(fecha_de_carga AS TIMESTAMP),
                    try_cast(fecha_creacion AS TIMESTAMP)
                ) AS modification_at
            FROM src_secop_contract_modifications
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        modification_rollup AS (
            SELECT
                contract_id,
                count(*) AS modification_event_count,
                sum(coalesce(modification_value, 0.0)) AS total_modification_value,
                max(modification_value) AS max_modification_value,
                sum(coalesce(extended_days, 0)) AS total_extended_days,
                max(extended_days) AS max_extended_days,
                max(modification_at) AS last_modification_at
            FROM modification_events
            GROUP BY contract_id
        ),
        modification_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_contract_modifications:' || modification_id AS evidence_ref,
                modification_value,
                modification_at,
                modification_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY modification_value DESC NULLS LAST,
                        modification_at DESC NULLS LAST,
                        modification_id
                ) AS evidence_rank
            FROM modification_events
            WHERE modification_value IS NOT NULL
                AND modification_value > 0
        ),
        modification_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY modification_value DESC, modification_at DESC)
                    AS modification_evidence_refs
            FROM modification_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY contract_id
        ),
        sanction_events AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                coalesce(
                    nullif(trim(act_number), ''),
                    nullif(trim(process_id), ''),
                    nullif(trim(contract_id), '') || ':' || cast(row_number() OVER () AS VARCHAR)
                ) AS sanction_record_id,
                coacc_money(amount) AS sanction_amount,
                try_cast(event_date AS DATE) AS sanction_event_date,
                nullif(trim(sanction_type), '') AS sanction_type,
                nullif(trim(status), '') AS sanction_status
            FROM src_secop_sanctions
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND (
                    try_cast(event_date AS DATE) <= current_date
                    OR try_cast(event_date AS DATE) IS NULL
                )
                AND lower(coalesce(status, '')) NOT LIKE '%borrador%'
        ),
        sanction_rollup AS (
            SELECT
                contract_id,
                count(*) AS secop_sanction_event_count,
                sum(coalesce(sanction_amount, 0.0)) AS secop_sanction_amount,
                max(sanction_event_date) AS last_secop_sanction_date
            FROM sanction_events
            GROUP BY contract_id
        ),
        sanction_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_sanctions:' || sanction_record_id AS evidence_ref,
                sanction_amount,
                sanction_event_date,
                sanction_record_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY sanction_amount DESC NULLS LAST,
                        sanction_event_date DESC NULLS LAST,
                        sanction_record_id
                ) AS evidence_rank
            FROM sanction_events
        ),
        sanction_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY sanction_amount DESC, sanction_event_date DESC)
                    AS sanction_evidence_refs
            FROM sanction_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY contract_id
        ),
        guarantee_raw AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                coalesce(nullif(trim(policy_number), ''), 'unknown') AS policy_number,
                nullif(trim(insurer), '') AS insurer,
                nullif(trim(insured), '') AS insured,
                nullif(trim(beneficiary), '') AS beneficiary,
                nullif(trim(policy_side), '') AS policy_side,
                nullif(trim(policy_type), '') AS policy_type,
                nullif(trim(policy_subtype), '') AS policy_subtype,
                nullif(trim(status), '') AS guarantee_status,
                lower(coalesce(nullif(trim(status), ''), '')) AS status_norm,
                coacc_money(policy_value) AS policy_value,
                try_cast(substr(policy_created_at, 1, 19) AS TIMESTAMP)
                    AS policy_created_at,
                try_cast(substr(policy_sent_at, 1, 19) AS TIMESTAMP) AS policy_sent_at,
                try_cast(substr(policy_end_at, 1, 19) AS TIMESTAMP) AS policy_end_at
            FROM src_secop_guarantees
            WHERE nullif(trim(contract_id), '') IS NOT NULL
        ),
        guarantee_events AS (
            SELECT
                *,
                concat_ws(
                    ':',
                    contract_id,
                    policy_number,
                    cast(
                        row_number() OVER (
                            PARTITION BY contract_id, policy_number
                            ORDER BY policy_value DESC NULLS LAST,
                                policy_end_at DESC NULLS LAST,
                                guarantee_status,
                                policy_created_at DESC NULLS LAST
                        ) AS VARCHAR
                    )
                ) AS guarantee_record_id
            FROM guarantee_raw
        ),
        guarantee_rollup AS (
            SELECT
                contract_id,
                count(*) AS guarantee_row_count,
                count(DISTINCT policy_number) AS guarantee_policy_count,
                count(*) FILTER (WHERE status_norm = 'aceptada')
                    AS guarantee_accepted_count,
                count(*) FILTER (WHERE status_norm = 'expirada')
                    AS guarantee_expired_count,
                count(*) FILTER (WHERE status_norm LIKE '%pendiente%')
                    AS guarantee_pending_count,
                count(*) FILTER (WHERE status_norm LIKE '%rechazada%')
                    AS guarantee_rejected_count,
                count(*) FILTER (WHERE status_norm LIKE '%borrador%')
                    AS guarantee_draft_count,
                count(*) FILTER (WHERE status_norm LIKE '%cancel%')
                    AS guarantee_cancelled_count,
                sum(coalesce(policy_value, 0.0)) AS total_guarantee_value,
                sum(coalesce(policy_value, 0.0)) FILTER (
                    WHERE status_norm IN ('aceptada', 'expirada')
                ) AS accepted_or_expired_guarantee_value,
                max(policy_value) AS max_guarantee_value,
                min(policy_created_at) AS first_policy_created_at,
                min(policy_sent_at) AS first_policy_sent_at,
                max(policy_end_at) AS max_policy_end_at
            FROM guarantee_events
            GROUP BY contract_id
        ),
        guarantee_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_guarantees:' || guarantee_record_id AS evidence_ref,
                policy_value,
                policy_end_at,
                policy_sent_at,
                status_norm,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        CASE
                            WHEN status_norm LIKE '%rechazada%' THEN 0
                            WHEN status_norm LIKE '%pendiente%' THEN 1
                            WHEN status_norm LIKE '%borrador%' THEN 2
                            WHEN status_norm LIKE '%cancel%' THEN 3
                            ELSE 4
                        END,
                        policy_value DESC NULLS LAST,
                        policy_end_at DESC NULLS LAST,
                        policy_sent_at DESC NULLS LAST,
                        guarantee_record_id
                ) AS evidence_rank
            FROM guarantee_events
        ),
        guarantee_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS guarantee_evidence_refs
            FROM guarantee_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        scored AS (
            SELECT
                b.*,
                coalesce(er.execution_item_count, 0) AS execution_item_count,
                coalesce(er.delayed_item_count, 0) AS delayed_item_count,
                coalesce(er.max_delay_days, 0) AS max_delay_days,
                coalesce(er.max_progress_gap_pct, 0) AS max_progress_gap_pct,
                er.last_execution_observed_at,
                coalesce(sr.suspension_event_count, 0) AS suspension_event_count,
                coalesce(sr.distinct_suspension_dates, 0) AS distinct_suspension_dates,
                sr.first_suspension_at,
                sr.last_suspension_at,
                coalesce(mr.modification_event_count, 0) AS modification_event_count,
                coalesce(mr.total_modification_value, 0.0) AS total_modification_value,
                coalesce(mr.max_modification_value, 0.0) AS max_modification_value,
                coalesce(mr.total_extended_days, 0) AS total_extended_days,
                mr.max_extended_days,
                mr.last_modification_at,
                coalesce(mr.total_modification_value, 0.0) / nullif(b.contract_value, 0)
                    AS modification_value_share,
                coalesce(sar.secop_sanction_event_count, 0)
                    AS secop_sanction_event_count,
                coalesce(sar.secop_sanction_amount, 0.0) AS secop_sanction_amount,
                sar.last_secop_sanction_date,
                coalesce(gr.guarantee_row_count, 0) AS guarantee_row_count,
                coalesce(gr.guarantee_policy_count, 0) AS guarantee_policy_count,
                coalesce(gr.guarantee_accepted_count, 0) AS guarantee_accepted_count,
                coalesce(gr.guarantee_expired_count, 0) AS guarantee_expired_count,
                coalesce(gr.guarantee_pending_count, 0) AS guarantee_pending_count,
                coalesce(gr.guarantee_rejected_count, 0) AS guarantee_rejected_count,
                coalesce(gr.guarantee_draft_count, 0) AS guarantee_draft_count,
                coalesce(gr.guarantee_cancelled_count, 0) AS guarantee_cancelled_count,
                coalesce(gr.total_guarantee_value, 0.0) AS total_guarantee_value,
                coalesce(gr.accepted_or_expired_guarantee_value, 0.0)
                    AS accepted_or_expired_guarantee_value,
                coalesce(gr.max_guarantee_value, 0.0) AS max_guarantee_value,
                gr.first_policy_created_at,
                gr.first_policy_sent_at,
                gr.max_policy_end_at,
                (
                    coalesce(b.advance_payment_value, 0.0) >= 100000000
                    AND coalesce(b.advance_payment_share, 0.0) >= 0.20
                ) AS high_advance_payment_flag,
                (
                    b.contract_end_date <= current_date
                    AND coalesce(b.pending_execution_value, 0.0) >= 100000000
                    AND coalesce(b.pending_execution_share, 0.0) >= 0.25
                ) AS ended_pending_execution_flag,
                (
                    coalesce(er.delayed_item_count, 0) > 0
                    OR coalesce(er.max_delay_days, 0) >= 30
                    OR coalesce(er.max_progress_gap_pct, 0) >= 30
                ) AS execution_delay_flag,
                coalesce(sr.suspension_event_count, 0) > 0 AS suspension_flag,
                (
                    coalesce(mr.total_modification_value, 0.0) >= 100000000
                    OR coalesce(mr.total_modification_value, 0.0)
                        / nullif(b.contract_value, 0) >= 0.20
                ) AS large_modification_flag,
                coalesce(sar.secop_sanction_event_count, 0) > 0
                    AS secop_sanction_flag,
                gr.contract_id IS NULL AS guarantee_missing_flag,
                (
                    gr.contract_id IS NOT NULL
                    AND coalesce(gr.guarantee_accepted_count, 0) = 0
                ) AS guarantee_no_accepted_flag,
                (
                    coalesce(gr.guarantee_rejected_count, 0) > 0
                    OR coalesce(gr.guarantee_draft_count, 0) > 0
                    OR coalesce(gr.guarantee_cancelled_count, 0) > 0
                ) AS guarantee_rejected_draft_cancelled_flag,
                (
                    gr.first_policy_sent_at IS NOT NULL
                    AND b.contract_start_date IS NOT NULL
                    AND gr.first_policy_sent_at::DATE > b.contract_start_date
                ) AS guarantee_sent_after_start_flag,
                (
                    gr.max_policy_end_at IS NOT NULL
                    AND b.contract_end_date IS NOT NULL
                    AND gr.max_policy_end_at::DATE < b.contract_end_date
                ) AS guarantee_ends_before_contract_end_flag,
                (
                    coalesce(b.advance_payment_value, 0.0) > 0
                    AND coalesce(gr.accepted_or_expired_guarantee_value, 0.0) > 0
                    AND coalesce(gr.accepted_or_expired_guarantee_value, 0.0)
                        < coalesce(b.advance_payment_value, 0.0)
                ) AS guarantee_value_below_advance_flag,
                list_concat(
                    list_concat(
                        list_concat(
                            list_concat(
                                list_concat(
                                    [
                                        coalesce(
                                            b.process_url,
                                            'secop_ii_contracts:' || b.contract_id
                                        )
                                    ],
                                    coalesce(ge.guarantee_evidence_refs, []::VARCHAR[])
                                ),
                                coalesce(ee.execution_evidence_refs, []::VARCHAR[])
                            ),
                            coalesce(se.suspension_evidence_refs, []::VARCHAR[])
                        ),
                        coalesce(me.modification_evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(sae.sanction_evidence_refs, []::VARCHAR[])
                ) AS evidence_refs
            FROM base_contracts b
            LEFT JOIN execution_rollup er
                ON er.contract_id = b.contract_id
            LEFT JOIN execution_evidence ee
                ON ee.contract_id = b.contract_id
            LEFT JOIN suspension_rollup sr
                ON sr.contract_id = b.contract_id
            LEFT JOIN suspension_evidence se
                ON se.contract_id = b.contract_id
            LEFT JOIN modification_rollup mr
                ON mr.contract_id = b.contract_id
            LEFT JOIN modification_evidence me
                ON me.contract_id = b.contract_id
            LEFT JOIN sanction_rollup sar
                ON sar.contract_id = b.contract_id
            LEFT JOIN sanction_evidence sae
                ON sae.contract_id = b.contract_id
            LEFT JOIN guarantee_rollup gr
                ON gr.contract_id = b.contract_id
            LEFT JOIN guarantee_evidence ge
                ON ge.contract_id = b.contract_id
        ),
        eligible AS (
            SELECT
                *,
                cast(high_advance_payment_flag AS INTEGER)
                    + cast(ended_pending_execution_flag AS INTEGER)
                    + cast(execution_delay_flag AS INTEGER)
                    + cast(suspension_flag AS INTEGER)
                    + cast(large_modification_flag AS INTEGER)
                    + cast(secop_sanction_flag AS INTEGER)
                    AS chain_flag_count,
                cast(guarantee_missing_flag AS INTEGER)
                    + cast(guarantee_no_accepted_flag AS INTEGER)
                    + cast(guarantee_rejected_draft_cancelled_flag AS INTEGER)
                    + cast(guarantee_sent_after_start_flag AS INTEGER)
                    + cast(guarantee_ends_before_contract_end_flag AS INTEGER)
                    + cast(guarantee_value_below_advance_flag AS INTEGER)
                    AS guarantee_issue_flag_count
            FROM scored
            WHERE (
                    high_advance_payment_flag
                    OR ended_pending_execution_flag
                )
                AND (
                    execution_delay_flag
                    OR suspension_flag
                    OR large_modification_flag
                    OR secop_sanction_flag
                )
        )
        SELECT
            'procurement_guarantee_advance_execution_chain' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'contract_execution_chain' AS scope_type,
            CASE
                WHEN chain_flag_count >= 4
                    OR secop_sanction_flag
                    OR contract_value >= 50000000000
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.58
                    + least(chain_flag_count / 10.0, 0.18)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.10)
                    + least(coalesce(advance_payment_share, 0.0) / 2.0, 0.06)
                    + least(coalesce(pending_execution_share, 0.0) / 2.0, 0.06)
                    + CASE WHEN suspension_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN large_modification_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN secop_sanction_flag THEN 0.06 ELSE 0.0 END
                    + CASE WHEN guarantee_issue_flag_count > 0 THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
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
            enables_advance_payment,
            liquidation,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            advance_payment_value,
            advance_payment_share,
            pending_execution_value,
            pending_execution_share,
            pending_payment_value,
            paid_value,
            paid_value_share,
            invoiced_value,
            invoiced_value_share,
            high_advance_payment_flag,
            ended_pending_execution_flag,
            execution_delay_flag,
            suspension_flag,
            large_modification_flag,
            secop_sanction_flag,
            chain_flag_count,
            execution_item_count,
            delayed_item_count,
            max_delay_days,
            max_progress_gap_pct,
            last_execution_observed_at,
            suspension_event_count,
            distinct_suspension_dates,
            first_suspension_at,
            last_suspension_at,
            modification_event_count,
            total_modification_value,
            max_modification_value,
            modification_value_share,
            total_extended_days,
            max_extended_days,
            last_modification_at,
            secop_sanction_event_count,
            secop_sanction_amount,
            last_secop_sanction_date,
            guarantee_row_count,
            guarantee_policy_count,
            guarantee_accepted_count,
            guarantee_expired_count,
            guarantee_pending_count,
            guarantee_rejected_count,
            guarantee_draft_count,
            guarantee_cancelled_count,
            total_guarantee_value,
            accepted_or_expired_guarantee_value,
            max_guarantee_value,
            first_policy_created_at,
            first_policy_sent_at,
            max_policy_end_at,
            guarantee_missing_flag,
            guarantee_no_accepted_flag,
            guarantee_rejected_draft_cancelled_flag,
            guarantee_sent_after_start_flag,
            guarantee_ends_before_contract_end_flag,
            guarantee_value_below_advance_flag,
            guarantee_issue_flag_count,
            CASE
                WHEN guarantee_missing_flag THEN 'no_secop_guarantee_row'
                WHEN guarantee_no_accepted_flag AND guarantee_pending_count > 0
                    THEN 'guarantee_rows_without_accepted_pending'
                WHEN guarantee_no_accepted_flag AND guarantee_rejected_count > 0
                    THEN 'guarantee_rows_without_accepted_rejected'
                WHEN guarantee_no_accepted_flag AND guarantee_draft_count > 0
                    THEN 'guarantee_rows_without_accepted_draft'
                WHEN guarantee_no_accepted_flag
                    THEN 'guarantee_rows_without_accepted'
                WHEN guarantee_sent_after_start_flag
                    THEN 'accepted_guarantee_present_but_sent_after_start'
                ELSE 'accepted_or_expired_guarantee_present'
            END AS guarantee_source_status,
            'high advance or ended pending execution plus execution/suspension/modification/sanction chain; guarantee status/date/value evidence is reviewer-only and does not prove guarantee validity, delivery failure causality, legal breach, or corrupt intent'
                AS what_is_unproven,
            evidence_refs
        FROM eligible
        WHERE chain_flag_count >= 3
        QUALIFY row_number() OVER (
            ORDER BY contract_value DESC NULLS LAST,
                chain_flag_count DESC,
                pending_execution_value DESC NULLS LAST,
                advance_payment_value DESC NULLS LAST,
                contract_id
        ) <= 1000
    """)


def _create_guarantee_policy_reuse_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_guarantees",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_guarantee_policy_reuse AS
        WITH guarantee_raw AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                nullif(trim(policy_number), '') AS policy_number,
                nullif(trim(insurer), '') AS insurer,
                nullif(trim(insured), '') AS insured,
                nullif(trim(beneficiary), '') AS beneficiary,
                nullif(trim(policy_side), '') AS policy_side,
                nullif(trim(policy_type), '') AS policy_type,
                nullif(trim(policy_subtype), '') AS policy_subtype,
                nullif(trim(status), '') AS guarantee_status,
                lower(coalesce(nullif(trim(status), ''), '')) AS status_norm,
                coacc_money(policy_value) AS policy_value,
                try_cast(substr(policy_created_at, 1, 19) AS TIMESTAMP)
                    AS policy_created_at,
                try_cast(substr(policy_sent_at, 1, 19) AS TIMESTAMP)
                    AS policy_sent_at,
                try_cast(substr(policy_end_at, 1, 19) AS TIMESTAMP)
                    AS policy_end_at
            FROM src_secop_guarantees
            WHERE nullif(trim(contract_id), '') IS NOT NULL
        ),
        policy_keyed AS (
            SELECT
                *,
                lower(
                    regexp_replace(
                        coalesce(policy_number, ''),
                        '[^0-9A-Za-z]',
                        '',
                        'g'
                    )
                ) AS policy_key,
                lower(
                    regexp_replace(
                        coalesce(insurer, ''),
                        '[^0-9A-Za-z]',
                        '',
                        'g'
                    )
                ) AS insurer_key,
                length(
                    regexp_replace(coalesce(policy_number, ''), '[^0-9]', '', 'g')
                ) AS policy_digit_count,
                lower(coalesce(policy_number, '')) AS policy_number_norm
            FROM guarantee_raw
        ),
        policy_like_rows AS (
            SELECT *
            FROM policy_keyed
            WHERE length(policy_key) >= 8
                AND length(insurer_key) >= 4
                AND policy_digit_count >= 8
                AND policy_key NOT IN (
                    'noaplica',
                    'noaplic',
                    'na',
                    'noregistra',
                    'noreporta',
                    'sinpoliza',
                    'sininformacion',
                    'pendiente',
                    'porregistrar',
                    'unknown'
                )
                AND insurer_key NOT IN (
                    'noaplica',
                    'noaplic',
                    'na',
                    'noregistra',
                    'noreporta',
                    'sininformacion',
                    'pendiente',
                    'unknown'
                )
                AND NOT regexp_matches(policy_key, '^(0+|1+|9+|x+|n+|a+)$')
                AND NOT regexp_matches(
                    policy_number_norm,
                    '(no[ ]*defin|cumplim|poliza|p.liza|seguro|garant|'
                    || 'responsabilidad|extracontract|anexo|entidad estatal|'
                    || 'contrato|aseguradora|solidaria|estado|mundial|'
                    || 'suramericana|previsora|chubb|mapfre|allianz|bolivar|sbs)'
                )
        ),
        guarantee_events AS (
            SELECT
                *,
                concat_ws(
                    ':',
                    contract_id,
                    policy_key,
                    cast(
                        row_number() OVER (
                            PARTITION BY contract_id, insurer_key, policy_key
                            ORDER BY
                                CASE
                                    WHEN status_norm IN ('aceptada', 'expirada')
                                        THEN 0
                                    WHEN status_norm LIKE '%pendiente%' THEN 1
                                    WHEN status_norm LIKE '%rechazada%' THEN 2
                                    ELSE 3
                                END,
                                policy_value DESC NULLS LAST,
                                policy_end_at DESC NULLS LAST,
                                policy_sent_at DESC NULLS LAST,
                                policy_number
                        ) AS VARCHAR
                    )
                ) AS guarantee_record_id
            FROM policy_like_rows
        ),
        joined_contracts AS (
            SELECT *
            FROM (
                SELECT
                    g.*,
                    'secop_guarantees:' || g.guarantee_record_id
                        AS guarantee_evidence_ref,
                    a.supplier_entity_id,
                    a.supplier_document_key,
                    a.supplier_nit_canonical,
                    a.supplier_name,
                    a.supplier_doc_type,
                    a.buyer_document_id,
                    a.buyer_document_digits,
                    a.buyer_name,
                    a.department,
                    a.city,
                    a.sector,
                    a.procurement_modality,
                    a.contract_type,
                    a.contract_reference,
                    a.process_id,
                    a.process_url,
                    a.contract_value,
                    a.signing_date,
                    a.contract_start_date,
                    a.contract_end_date,
                    coalesce(a.process_url, 'secop_ii_contracts:' || a.contract_id)
                        AS contract_evidence_ref,
                    row_number() OVER (
                        PARTITION BY g.insurer_key, g.policy_key, g.contract_id
                        ORDER BY
                            CASE
                                WHEN g.status_norm IN ('aceptada', 'expirada')
                                    THEN 0
                                WHEN g.status_norm LIKE '%pendiente%' THEN 1
                                WHEN g.status_norm LIKE '%rechazada%' THEN 2
                                ELSE 3
                            END,
                            g.policy_value DESC NULLS LAST,
                            g.policy_end_at DESC NULLS LAST,
                            g.policy_sent_at DESC NULLS LAST,
                            g.policy_number
                    ) AS contract_policy_rank
                FROM guarantee_events g
                JOIN curated_contract_awards a
                    ON a.contract_id = g.contract_id
                WHERE a.contract_id IS NOT NULL
                    AND a.supplier_document_key IS NOT NULL
                    AND a.buyer_document_digits IS NOT NULL
                    AND NOT regexp_matches(a.supplier_document_key, '^0+$')
                    AND coalesce(a.contract_value, 0.0) > 0
            )
            WHERE contract_policy_rank = 1
        ),
        policy_clusters AS (
            SELECT
                insurer_key,
                policy_key,
                any_value(insurer) AS insurer,
                any_value(policy_number) AS policy_number,
                count(*) AS cluster_contract_count,
                count(DISTINCT supplier_document_key) AS cluster_supplier_count,
                count(DISTINCT buyer_document_digits) AS cluster_buyer_count,
                count(*) FILTER (
                    WHERE status_norm IN ('aceptada', 'expirada')
                ) AS cluster_accepted_or_expired_contract_count,
                count(*) FILTER (WHERE status_norm = 'aceptada')
                    AS cluster_accepted_contract_count,
                count(*) FILTER (WHERE status_norm = 'expirada')
                    AS cluster_expired_contract_count,
                sum(coalesce(contract_value, 0.0)) AS cluster_total_contract_value,
                max(coalesce(contract_value, 0.0)) AS cluster_max_contract_value,
                min(signing_date) AS cluster_first_signing_date,
                max(signing_date) AS cluster_last_signing_date,
                count(DISTINCT department) AS cluster_department_count,
                count(DISTINCT sector) AS cluster_sector_count
            FROM joined_contracts
            GROUP BY insurer_key, policy_key
        ),
        eligible_clusters AS (
            SELECT
                *,
                cluster_supplier_count >= 2 AS different_supplier_policy_reuse_flag,
                cluster_buyer_count >= 2 AS different_buyer_policy_reuse_flag,
                cluster_contract_count BETWEEN 2 AND 5 AS small_policy_cluster_flag,
                (
                    cluster_accepted_or_expired_contract_count
                        = cluster_contract_count
                ) AS all_cluster_contracts_accepted_or_expired_flag,
                cluster_total_contract_value >= 1000000000
                    AS high_cluster_value_flag,
                cluster_total_contract_value >= 50000000000
                    OR cluster_max_contract_value >= 20000000000
                    AS very_high_cluster_value_flag
            FROM policy_clusters
            WHERE cluster_supplier_count >= 2
                AND cluster_buyer_count >= 2
                AND cluster_contract_count BETWEEN 2 AND 5
                AND cluster_accepted_or_expired_contract_count
                    = cluster_contract_count
                AND cluster_total_contract_value >= 1000000000
        ),
        cluster_members AS (
            SELECT
                j.*,
                c.insurer AS cluster_insurer,
                c.policy_number AS cluster_policy_number,
                c.cluster_contract_count,
                c.cluster_supplier_count,
                c.cluster_buyer_count,
                c.cluster_accepted_or_expired_contract_count,
                c.cluster_accepted_contract_count,
                c.cluster_expired_contract_count,
                c.cluster_total_contract_value,
                c.cluster_max_contract_value,
                c.cluster_first_signing_date,
                c.cluster_last_signing_date,
                c.cluster_department_count,
                c.cluster_sector_count,
                c.different_supplier_policy_reuse_flag,
                c.different_buyer_policy_reuse_flag,
                c.small_policy_cluster_flag,
                c.all_cluster_contracts_accepted_or_expired_flag,
                c.high_cluster_value_flag,
                c.very_high_cluster_value_flag
            FROM joined_contracts j
            JOIN eligible_clusters c
                ON c.insurer_key = j.insurer_key
                AND c.policy_key = j.policy_key
        ),
        other_member_evidence AS (
            SELECT
                m.insurer_key,
                m.policy_key,
                m.contract_id,
                list(
                    o.contract_evidence_ref
                    ORDER BY o.contract_value DESC NULLS LAST, o.contract_id
                ) AS other_contract_evidence_refs,
                list(
                    o.guarantee_evidence_ref
                    ORDER BY o.contract_value DESC NULLS LAST, o.contract_id
                ) AS other_guarantee_evidence_refs
            FROM cluster_members m
            JOIN cluster_members o
                ON o.insurer_key = m.insurer_key
                AND o.policy_key = m.policy_key
                AND o.contract_id <> m.contract_id
            GROUP BY m.insurer_key, m.policy_key, m.contract_id
        )
        SELECT
            'procurement_guarantee_policy_reuse_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            'guarantee_policy_reuse:' || m.insurer_key || ':' || m.policy_key || ':'
                || m.contract_id AS scope_key,
            'guarantee_policy_cluster' AS scope_type,
            CASE
                WHEN very_high_cluster_value_flag
                    OR cluster_contract_count >= 4
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.97,
                0.62
                    + least(cluster_contract_count / 25.0, 0.12)
                    + least(
                        log10(greatest(cluster_total_contract_value, 1)) / 130.0,
                        0.10
                    )
                    + CASE
                        WHEN different_supplier_policy_reuse_flag THEN 0.05
                        ELSE 0.0
                    END
                    + CASE
                        WHEN different_buyer_policy_reuse_flag THEN 0.05
                        ELSE 0.0
                    END
                    + CASE
                        WHEN all_cluster_contracts_accepted_or_expired_flag THEN 0.04
                        ELSE 0.0
                    END
                    + CASE WHEN very_high_cluster_value_flag THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
            buyer_name,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            m.contract_id,
            contract_reference,
            process_id,
            process_url,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            insurer,
            policy_number,
            m.insurer_key,
            m.policy_key,
            policy_digit_count,
            guarantee_status,
            policy_side,
            policy_type,
            policy_subtype,
            policy_value,
            policy_created_at,
            policy_sent_at,
            policy_end_at,
            cluster_contract_count,
            cluster_supplier_count,
            cluster_buyer_count,
            cluster_accepted_or_expired_contract_count,
            cluster_accepted_contract_count,
            cluster_expired_contract_count,
            cluster_total_contract_value,
            cluster_max_contract_value,
            cluster_first_signing_date,
            cluster_last_signing_date,
            cluster_department_count,
            cluster_sector_count,
            true AS policy_like_number_flag,
            different_supplier_policy_reuse_flag,
            different_buyer_policy_reuse_flag,
            small_policy_cluster_flag,
            all_cluster_contracts_accepted_or_expired_flag,
            high_cluster_value_flag,
            very_high_cluster_value_flag,
            CASE
                WHEN cluster_contract_count = 2
                    THEN 'same_policy_two_contracts_different_supplier_buyer'
                ELSE 'same_policy_small_cluster_different_supplier_buyer'
            END AS policy_reuse_status,
            'exact insurer and normalized policy number reuse across different suppliers and buyers; reviewer-only queue does not prove the policy is false, invalid, insurer-denied, unauthorized, a legal breach, or corrupt intent without insurer confirmation and SECOP contract-file review'
                AS what_is_unproven,
            list_concat(
                [contract_evidence_ref, guarantee_evidence_ref],
                list_concat(
                    coalesce(ome.other_contract_evidence_refs, []::VARCHAR[]),
                    coalesce(ome.other_guarantee_evidence_refs, []::VARCHAR[])
                )
            ) AS evidence_refs
        FROM cluster_members m
        LEFT JOIN other_member_evidence ome
            ON ome.insurer_key = m.insurer_key
            AND ome.policy_key = m.policy_key
            AND ome.contract_id = m.contract_id
        QUALIFY row_number() OVER (
                ORDER BY cluster_total_contract_value DESC,
                    cluster_contract_count DESC,
                    contract_value DESC,
                    m.insurer_key,
                    m.policy_key,
                    m.contract_id
        ) <= 1000
    """)


def _create_budget_chain_reconciliation_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_budget_chain_reconciliation AS
        WITH budget_base_contracts AS (
            SELECT
                a.*
            FROM curated_contract_awards a
            WHERE a.contract_id IS NOT NULL
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 500000000
                AND a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
        ),
        cdp_raw AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                coalesce(nullif(trim(c_digo_cdp), ''), 'unknown') AS cdp_code,
                nullif(trim(id_siif), '') AS siif_id,
                lower(coalesce(nullif(trim(estado_siif), ''), '')) AS siif_status_norm,
                coalesce(try_cast(registrado_en_siif AS BOOLEAN), false)
                    AS registered_in_siif,
                coacc_money(valor_utilizado) AS cdp_used_value,
                coacc_money(saldo_total_a_comprometer) AS cdp_commit_balance_value,
                coacc_money(saldo_cdp) AS cdp_balance_value,
                nullif(trim(fuente_de_los_recursos), '') AS resource_source,
                nullif(trim(destino_del_gasto), '') AS expense_destination,
                nullif(trim(bpin_codigo), '') AS bpin_code,
                nullif(trim(ultima_consulta_siif), '') AS last_siif_query_status
            FROM src_secop_cdp_requests
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        cdp_rollup AS (
            SELECT
                contract_id,
                count(*) AS cdp_row_count,
                count(DISTINCT cdp_code) AS cdp_code_count,
                sum(coalesce(cdp_used_value, 0.0)) AS cdp_used_value,
                sum(coalesce(cdp_commit_balance_value, 0.0))
                    AS cdp_commit_balance_value,
                sum(coalesce(cdp_balance_value, 0.0)) AS cdp_balance_value,
                count(*) FILTER (
                    WHERE siif_status_norm IN ('generado', 'con compromiso', 'realizado')
                ) AS cdp_siif_ok_row_count,
                count(*) FILTER (WHERE registered_in_siif) AS cdp_registered_row_count,
                count(*) FILTER (
                    WHERE siif_status_norm IN ('no iniciado', 'no definido', 'cancelado')
                ) AS cdp_weak_status_row_count,
                list(DISTINCT resource_source ORDER BY resource_source)
                    FILTER (WHERE resource_source IS NOT NULL) AS cdp_resource_sources,
                list(DISTINCT expense_destination ORDER BY expense_destination)
                    FILTER (WHERE expense_destination IS NOT NULL)
                    AS cdp_expense_destinations
            FROM cdp_raw
            GROUP BY contract_id
        ),
        cdp_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_cdp_requests:' || contract_id || ':' || cdp_code
                    AS evidence_ref,
                cdp_used_value,
                siif_status_norm,
                registered_in_siif,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        CASE
                            WHEN siif_status_norm IN ('no iniciado', 'no definido', 'cancelado')
                                THEN 0
                            ELSE 1
                        END,
                        cdp_used_value DESC NULLS LAST,
                        cdp_code
                ) AS evidence_rank
            FROM cdp_raw
        ),
        cdp_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS cdp_evidence_refs
            FROM cdp_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        commitment_raw AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                coalesce(nullif(trim(identificador_nico), ''), 'unknown')
                    AS commitment_id,
                coalesce(nullif(trim(identificador_item), ''), 'unknown')
                    AS commitment_item_id,
                lower(coalesce(nullif(trim(estado_integraci_n), ''), ''))
                    AS integration_status_norm,
                lower(coalesce(nullif(trim(estado_integraci_n_item), ''), ''))
                    AS item_integration_status_norm,
                nullif(trim(tipo_de_compromiso), '') AS commitment_type,
                nullif(trim(c_digo_item), '') AS budget_item_code,
                coacc_money(valor_item) AS commitment_value,
                coacc_money(balance_compromiso) AS commitment_balance_value,
                coacc_money(balance_vigencia_futura) AS future_term_balance_value,
                coacc_money(valor_a_liberar) AS release_value
            FROM src_secop_budget_commitments
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        commitment_rollup AS (
            SELECT
                contract_id,
                count(*) AS commitment_row_count,
                count(DISTINCT commitment_id) AS commitment_count,
                count(DISTINCT commitment_item_id) AS commitment_item_count,
                sum(coalesce(commitment_value, 0.0)) AS commitment_value,
                sum(coalesce(commitment_balance_value, 0.0)) AS commitment_balance_value,
                sum(coalesce(future_term_balance_value, 0.0))
                    AS future_term_balance_value,
                sum(coalesce(release_value, 0.0)) AS release_value,
                count(*) FILTER (WHERE integration_status_norm = 'exito')
                    AS commitment_success_row_count,
                count(*) FILTER (WHERE integration_status_norm IN ('fallido', 'error'))
                    AS commitment_failed_row_count,
                count(*) FILTER (WHERE item_integration_status_norm = 'con obligación')
                    AS obligation_row_count,
                count(*) FILTER (
                    WHERE item_integration_status_norm IN ('no iniciado', 'no definido')
                ) AS commitment_item_weak_row_count
            FROM commitment_raw
            GROUP BY contract_id
        ),
        commitment_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_budget_commitments:' || contract_id || ':' || commitment_id
                    AS evidence_ref,
                commitment_value,
                integration_status_norm,
                item_integration_status_norm,
                commitment_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        CASE
                            WHEN integration_status_norm IN ('fallido', 'error') THEN 0
                            WHEN item_integration_status_norm IN ('no iniciado', 'no definido')
                                THEN 1
                            ELSE 2
                        END,
                        commitment_value DESC NULLS LAST,
                        commitment_id
                ) AS evidence_rank
            FROM commitment_raw
        ),
        commitment_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS commitment_evidence_refs
            FROM commitment_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        rubro_raw AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                coalesce(nullif(trim(identificador_unico), ''), 'unknown')
                    AS rubro_record_id,
                coalesce(nullif(trim(identificador_compromiso), ''), 'unknown')
                    AS commitment_id,
                nullif(trim(codigo), '') AS rubro_code,
                nullif(trim(nombre), '') AS rubro_name,
                coacc_money(valor_actual) AS rubro_value,
                nullif(trim(anno), '') AS rubro_year
            FROM src_secop_budget_items
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        rubro_rollup AS (
            SELECT
                contract_id,
                count(*) AS rubro_row_count,
                count(DISTINCT rubro_code) AS rubro_code_count,
                sum(coalesce(rubro_value, 0.0)) AS rubro_value,
                count(*) FILTER (
                    WHERE lower(coalesce(rubro_code, '')) LIKE '%no definido%'
                        OR lower(coalesce(rubro_name, '')) LIKE '%no definido%'
                ) AS undefined_rubro_row_count
            FROM rubro_raw
            GROUP BY contract_id
        ),
        rubro_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_budget_items:' || contract_id || ':' || rubro_record_id
                    AS evidence_ref,
                rubro_value,
                rubro_record_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY rubro_value DESC NULLS LAST, rubro_record_id
                ) AS evidence_rank
            FROM rubro_raw
        ),
        rubro_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS rubro_evidence_refs
            FROM rubro_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        guarantee_context AS (
            SELECT
                contract_id,
                severity AS guarantee_chain_severity,
                risk_signal AS guarantee_chain_risk_signal,
                guarantee_source_status,
                chain_flag_count AS guarantee_chain_flag_count,
                guarantee_issue_flag_count,
                'signal_feature_procurement_guarantee_advance_execution_chain:'
                    || contract_id AS guarantee_chain_evidence_ref
            FROM curated_guarantee_advance_execution_chain
        ),
        scored AS (
            SELECT
                b.*,
                c.cdp_row_count,
                c.cdp_code_count,
                c.cdp_used_value,
                c.cdp_commit_balance_value,
                c.cdp_balance_value,
                c.cdp_siif_ok_row_count,
                c.cdp_registered_row_count,
                c.cdp_weak_status_row_count,
                c.cdp_resource_sources,
                c.cdp_expense_destinations,
                cr.commitment_row_count,
                cr.commitment_count,
                cr.commitment_item_count,
                cr.commitment_value,
                cr.commitment_balance_value,
                cr.future_term_balance_value,
                cr.release_value,
                cr.commitment_success_row_count,
                cr.commitment_failed_row_count,
                cr.obligation_row_count,
                cr.commitment_item_weak_row_count,
                r.rubro_row_count,
                r.rubro_code_count,
                r.rubro_value,
                r.undefined_rubro_row_count,
                g.guarantee_chain_severity,
                g.guarantee_chain_risk_signal,
                g.guarantee_source_status,
                g.guarantee_chain_flag_count,
                g.guarantee_issue_flag_count,
                c.contract_id IS NULL AS missing_cdp_flag,
                cr.contract_id IS NULL AS missing_commitment_flag,
                r.contract_id IS NULL AS missing_rubro_flag,
                (
                    coalesce(c.cdp_weak_status_row_count, 0) > 0
                    AND coalesce(c.cdp_siif_ok_row_count, 0) = 0
                ) AS cdp_only_weak_siif_flag,
                (
                    coalesce(cr.commitment_failed_row_count, 0) > 0
                    AND coalesce(cr.commitment_success_row_count, 0) = 0
                ) AS commitment_only_failed_flag,
                (
                    coalesce(c.cdp_used_value, 0.0) < b.contract_value * 0.50
                    AND b.contract_value - coalesce(c.cdp_used_value, 0.0) >= 500000000
                ) AS cdp_under_contract_flag,
                (
                    coalesce(cr.commitment_value, 0.0) < b.contract_value * 0.50
                    AND b.contract_value - coalesce(cr.commitment_value, 0.0)
                        >= 500000000
                ) AS commitment_under_contract_flag,
                (
                    coalesce(cr.commitment_value, 0.0) > b.contract_value * 1.50
                    AND coalesce(cr.commitment_value, 0.0) - b.contract_value
                        >= 500000000
                ) AS commitment_over_contract_flag,
                (
                    coalesce(r.undefined_rubro_row_count, 0) > 0
                    AND coalesce(r.rubro_value, 0.0) = 0.0
                ) AS rubro_zero_or_undefined_flag,
                g.contract_id IS NOT NULL AS guarantee_chain_flag,
                list_concat(
                    list_concat(
                        list_concat(
                            list_concat(
                                [
                                    coalesce(
                                        b.process_url,
                                        'secop_ii_contracts:' || b.contract_id
                                    )
                                ],
                                coalesce(ce.cdp_evidence_refs, []::VARCHAR[])
                            ),
                            coalesce(cme.commitment_evidence_refs, []::VARCHAR[])
                        ),
                        coalesce(re.rubro_evidence_refs, []::VARCHAR[])
                    ),
                    CASE
                        WHEN g.guarantee_chain_evidence_ref IS NOT NULL
                            THEN [g.guarantee_chain_evidence_ref]
                        ELSE []::VARCHAR[]
                    END
                ) AS evidence_refs
            FROM budget_base_contracts b
            LEFT JOIN cdp_rollup c
                ON c.contract_id = b.contract_id
            LEFT JOIN cdp_evidence ce
                ON ce.contract_id = b.contract_id
            LEFT JOIN commitment_rollup cr
                ON cr.contract_id = b.contract_id
            LEFT JOIN commitment_evidence cme
                ON cme.contract_id = b.contract_id
            LEFT JOIN rubro_rollup r
                ON r.contract_id = b.contract_id
            LEFT JOIN rubro_evidence re
                ON re.contract_id = b.contract_id
            LEFT JOIN guarantee_context g
                ON g.contract_id = b.contract_id
        ),
        eligible AS (
            SELECT
                *,
                cast(missing_cdp_flag AS INTEGER)
                    + cast(missing_commitment_flag AS INTEGER)
                    + cast(missing_rubro_flag AS INTEGER)
                    + cast(cdp_only_weak_siif_flag AS INTEGER)
                    + cast(commitment_only_failed_flag AS INTEGER)
                    + cast(cdp_under_contract_flag AS INTEGER)
                    + cast(commitment_under_contract_flag AS INTEGER)
                    + cast(commitment_over_contract_flag AS INTEGER)
                    + cast(rubro_zero_or_undefined_flag AS INTEGER)
                    AS budget_issue_flag_count
            FROM scored
            WHERE (
                    guarantee_chain_flag
                    AND (
                        missing_cdp_flag
                        OR missing_commitment_flag
                        OR missing_rubro_flag
                        OR cdp_only_weak_siif_flag
                        OR commitment_only_failed_flag
                        OR cdp_under_contract_flag
                        OR commitment_under_contract_flag
                        OR commitment_over_contract_flag
                        OR rubro_zero_or_undefined_flag
                    )
                )
                OR (
                    contract_value >= 5000000000
                    AND (
                        cast(missing_cdp_flag AS INTEGER)
                        + cast(missing_commitment_flag AS INTEGER)
                        + cast(missing_rubro_flag AS INTEGER)
                        + cast(cdp_only_weak_siif_flag AS INTEGER)
                        + cast(commitment_only_failed_flag AS INTEGER)
                        + cast(cdp_under_contract_flag AS INTEGER)
                        + cast(commitment_under_contract_flag AS INTEGER)
                        + cast(commitment_over_contract_flag AS INTEGER)
                        + cast(rubro_zero_or_undefined_flag AS INTEGER)
                    ) >= 2
                )
                OR (
                    contract_value >= 1000000000
                    AND (
                        cast(missing_cdp_flag AS INTEGER)
                        + cast(missing_commitment_flag AS INTEGER)
                        + cast(missing_rubro_flag AS INTEGER)
                        + cast(cdp_only_weak_siif_flag AS INTEGER)
                        + cast(commitment_only_failed_flag AS INTEGER)
                        + cast(cdp_under_contract_flag AS INTEGER)
                        + cast(commitment_under_contract_flag AS INTEGER)
                        + cast(commitment_over_contract_flag AS INTEGER)
                        + cast(rubro_zero_or_undefined_flag AS INTEGER)
                    ) >= 3
                )
        )
        SELECT
            'procurement_budget_chain_reconciliation_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'contract_budget_chain' AS scope_type,
            CASE
                WHEN guarantee_chain_flag AND budget_issue_flag_count >= 3
                    THEN 'critical'
                WHEN contract_value >= 50000000000 AND budget_issue_flag_count >= 2
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.55
                    + least(budget_issue_flag_count * 0.055, 0.24)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.10)
                    + CASE WHEN guarantee_chain_flag THEN 0.08 ELSE 0.0 END
                    + CASE WHEN cdp_only_weak_siif_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN commitment_only_failed_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN commitment_over_contract_flag THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
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
            cdp_row_count,
            cdp_code_count,
            cdp_used_value,
            cdp_commit_balance_value,
            cdp_balance_value,
            cdp_siif_ok_row_count,
            cdp_registered_row_count,
            cdp_weak_status_row_count,
            cdp_resource_sources,
            cdp_expense_destinations,
            commitment_row_count,
            commitment_count,
            commitment_item_count,
            commitment_value,
            commitment_balance_value,
            future_term_balance_value,
            release_value,
            commitment_success_row_count,
            commitment_failed_row_count,
            obligation_row_count,
            commitment_item_weak_row_count,
            rubro_row_count,
            rubro_code_count,
            rubro_value,
            undefined_rubro_row_count,
            guarantee_chain_flag,
            guarantee_chain_severity,
            guarantee_source_status,
            guarantee_issue_flag_count,
            guarantee_chain_flag_count,
            missing_cdp_flag,
            missing_commitment_flag,
            missing_rubro_flag,
            cdp_only_weak_siif_flag,
            commitment_only_failed_flag,
            cdp_under_contract_flag,
            commitment_under_contract_flag,
            commitment_over_contract_flag,
            rubro_zero_or_undefined_flag,
            budget_issue_flag_count,
            'budget-chain reconciliation flags use exact contract IDs and source status/value fields; they do not prove illegal payment, budget availability, accounting breach, or corrupt intent without SIIF/source-document review'
                AS what_is_unproven,
            evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY guarantee_chain_flag DESC,
                budget_issue_flag_count DESC,
                contract_value DESC NULLS LAST,
                risk_signal DESC,
                contract_id
        ) <= 1000
    """)


def _create_invoice_budget_reconciliation_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_invoices",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_invoice_budget_reconciliation AS
        WITH contract_financials AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                any_value(nullif(trim(contract_status), '')) AS contract_status,
                any_value(nullif(trim(enables_advance_payment), ''))
                    AS enables_advance_payment,
                any_value(nullif(trim(liquidation), '')) AS liquidation,
                max(coacc_money_decimal(advance_payment_value)) AS advance_payment_value,
                max(coacc_money_decimal(invoiced_value)) AS invoiced_value,
                max(coacc_money_decimal(pending_payment_value)) AS pending_payment_value,
                max(coacc_money_decimal(paid_value)) AS paid_value,
                max(coacc_money_decimal(amortized_value)) AS amortized_value,
                max(coacc_money_decimal(pending_value)) AS pending_value,
                max(coacc_money_decimal(pending_execution_value))
                    AS pending_execution_value
            FROM src_secop_ii_contracts
            WHERE nullif(trim(contract_id), '') IS NOT NULL
            GROUP BY nullif(trim(contract_id), '')
        ),
        invoice_base_contracts AS (
            SELECT
                a.*,
                f.contract_status,
                f.enables_advance_payment,
                f.liquidation,
                f.advance_payment_value,
                f.invoiced_value,
                f.pending_payment_value,
                f.paid_value,
                f.amortized_value,
                f.pending_value,
                f.pending_execution_value
            FROM curated_contract_awards a
            JOIN contract_financials f
                ON f.contract_id = a.contract_id
            WHERE a.contract_id IS NOT NULL
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
            QUALIFY row_number() OVER (
                PARTITION BY a.contract_id
                ORDER BY a.contract_value DESC NULLS LAST,
                    a.signing_date DESC NULLS LAST,
                    a.process_url DESC NULLS LAST
            ) = 1
        ),
        invoice_raw AS (
            SELECT
                nullif(trim(id_contrato), '') AS contract_id,
                coalesce(
                    nullif(trim(id_pago), ''),
                    nullif(trim(numero_de_factura), ''),
                    'unknown'
                ) AS invoice_id,
                nullif(trim(numero_de_factura), '') AS invoice_number,
                lower(coalesce(nullif(trim(estado), ''), '')) AS invoice_status_norm,
                CASE
                    WHEN lower(coalesce(cast(pago_confirmado AS VARCHAR), ''))
                        IN ('true', 't', '1', 'si', 'sí', 'yes', 'y')
                        THEN true
                    ELSE coalesce(try_cast(pago_confirmado AS BOOLEAN), false)
                END AS payment_confirmed,
                coacc_money_decimal(valor_total) AS invoice_total_line_value,
                coacc_money_decimal(valor_a_pagar) AS invoice_payable_line_value,
                coacc_money_decimal(valor_neto) AS invoice_net_line_value,
                try_cast(substr(cast(fecha_factura AS VARCHAR), 1, 10) AS DATE)
                    AS invoice_date,
                try_cast(substr(cast(fecha_de_entrega AS VARCHAR), 1, 10) AS DATE)
                    AS delivery_date,
                try_cast(substr(cast(fecha_estimada_de_pago AS VARCHAR), 1, 10) AS DATE)
                    AS estimated_payment_date
            FROM src_secop_invoices
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        invoice_scoped AS (
            SELECT
                b.contract_id,
                b.contract_end_date,
                i.invoice_id,
                i.invoice_number,
                i.invoice_status_norm,
                i.payment_confirmed,
                coalesce(
                    i.invoice_payable_line_value,
                    i.invoice_total_line_value,
                    i.invoice_net_line_value,
                    0.0
                ) AS invoice_line_value,
                coalesce(i.invoice_total_line_value, 0.0) AS invoice_total_line_value,
                coalesce(i.invoice_payable_line_value, 0.0) AS invoice_payable_line_value,
                coalesce(i.invoice_net_line_value, 0.0) AS invoice_net_line_value,
                i.invoice_date,
                i.delivery_date,
                i.estimated_payment_date,
                (
                    i.invoice_status_norm NOT LIKE '%rechaz%'
                    AND i.invoice_status_norm NOT LIKE '%anulad%'
                    AND i.invoice_status_norm NOT LIKE '%cancel%'
                    AND i.invoice_status_norm NOT LIKE '%borrador%'
                ) AS invoice_status_countable
            FROM invoice_base_contracts b
            JOIN invoice_raw i
                ON i.contract_id = b.contract_id
            WHERE coalesce(
                    i.invoice_payable_line_value,
                    i.invoice_total_line_value,
                    i.invoice_net_line_value,
                    0.0
                ) > 0
        ),
        invoice_rollup AS (
            SELECT
                contract_id,
                count(*) AS invoice_row_count,
                count(DISTINCT invoice_id) AS invoice_document_count,
                count(*) FILTER (WHERE payment_confirmed) AS confirmed_invoice_row_count,
                count(*) FILTER (
                    WHERE invoice_status_countable
                        AND contract_end_date IS NOT NULL
                        AND coalesce(invoice_date, delivery_date) > contract_end_date
                ) AS post_end_invoice_row_count,
                count(*) FILTER (
                    WHERE invoice_status_countable
                        AND estimated_payment_date IS NOT NULL
                        AND invoice_date IS NOT NULL
                        AND estimated_payment_date < invoice_date
                ) AS estimated_payment_before_invoice_count,
                sum(invoice_total_line_value) FILTER (WHERE invoice_status_countable)
                    AS invoice_total_value,
                sum(invoice_payable_line_value) FILTER (WHERE invoice_status_countable)
                    AS invoice_payable_value,
                sum(invoice_net_line_value) FILTER (WHERE invoice_status_countable)
                    AS invoice_net_value,
                sum(invoice_line_value) FILTER (WHERE invoice_status_countable)
                    AS invoice_source_value,
                sum(invoice_line_value) FILTER (
                    WHERE invoice_status_countable AND payment_confirmed
                ) AS confirmed_invoice_value,
                sum(invoice_line_value) FILTER (
                    WHERE invoice_status_countable
                        AND contract_end_date IS NOT NULL
                        AND coalesce(invoice_date, delivery_date) > contract_end_date
                ) AS post_end_invoice_value,
                min(invoice_date) FILTER (WHERE invoice_status_countable)
                    AS first_invoice_date,
                max(invoice_date) FILTER (WHERE invoice_status_countable)
                    AS last_invoice_date,
                max(estimated_payment_date) FILTER (WHERE invoice_status_countable)
                    AS latest_estimated_payment_date,
                list(DISTINCT invoice_status_norm ORDER BY invoice_status_norm)
                    FILTER (WHERE invoice_status_norm IS NOT NULL)
                    AS invoice_statuses
            FROM invoice_scoped
            GROUP BY contract_id
        ),
        invoice_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_invoices:' || contract_id || ':' || invoice_id AS evidence_ref,
                invoice_line_value,
                payment_confirmed,
                invoice_date,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY payment_confirmed DESC,
                        invoice_line_value DESC NULLS LAST,
                        invoice_date DESC NULLS LAST,
                        invoice_id
                ) AS evidence_rank
            FROM invoice_scoped
            WHERE invoice_status_countable
        ),
        invoice_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS invoice_evidence_refs
            FROM invoice_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        budget_context AS (
            SELECT
                contract_id,
                severity AS budget_chain_severity,
                risk_signal AS budget_chain_risk_signal,
                budget_issue_flag_count,
                missing_cdp_flag,
                missing_commitment_flag,
                missing_rubro_flag,
                cdp_only_weak_siif_flag,
                commitment_only_failed_flag,
                cdp_under_contract_flag,
                commitment_under_contract_flag,
                commitment_over_contract_flag,
                rubro_zero_or_undefined_flag,
                'signal_feature_procurement_budget_chain_reconciliation_review_only:'
                    || contract_id AS budget_chain_evidence_ref
            FROM curated_budget_chain_reconciliation
        ),
        guarantee_context AS (
            SELECT
                contract_id,
                severity AS guarantee_chain_severity,
                risk_signal AS guarantee_chain_risk_signal,
                guarantee_source_status,
                chain_flag_count AS guarantee_chain_flag_count,
                guarantee_issue_flag_count,
                'signal_feature_procurement_guarantee_advance_execution_chain:'
                    || contract_id AS guarantee_chain_evidence_ref
            FROM curated_guarantee_advance_execution_chain
        ),
        scored AS (
            SELECT
                b.*,
                coalesce(ir.invoice_row_count, 0) AS invoice_row_count,
                coalesce(ir.invoice_document_count, 0) AS invoice_document_count,
                coalesce(ir.confirmed_invoice_row_count, 0)
                    AS confirmed_invoice_row_count,
                coalesce(ir.post_end_invoice_row_count, 0) AS post_end_invoice_row_count,
                coalesce(ir.estimated_payment_before_invoice_count, 0)
                    AS estimated_payment_before_invoice_count,
                coalesce(ir.invoice_total_value, 0.0) AS invoice_total_value,
                coalesce(ir.invoice_payable_value, 0.0) AS invoice_payable_value,
                coalesce(ir.invoice_net_value, 0.0) AS invoice_net_value,
                coalesce(ir.invoice_source_value, 0.0) AS invoice_source_value,
                coalesce(ir.confirmed_invoice_value, 0.0) AS confirmed_invoice_value,
                coalesce(ir.post_end_invoice_value, 0.0) AS post_end_invoice_value,
                greatest(coalesce(ir.invoice_source_value, 0.0) - b.contract_value, 0.0)
                    AS invoice_excess_value,
                greatest(
                    coalesce(ir.confirmed_invoice_value, 0.0) - b.contract_value,
                    0.0
                ) AS confirmed_invoice_excess_value,
                ir.first_invoice_date,
                ir.last_invoice_date,
                ir.latest_estimated_payment_date,
                ir.invoice_statuses,
                bc.budget_chain_severity,
                bc.budget_chain_risk_signal,
                bc.budget_issue_flag_count,
                bc.missing_cdp_flag,
                bc.missing_commitment_flag,
                bc.missing_rubro_flag,
                bc.cdp_only_weak_siif_flag,
                bc.commitment_only_failed_flag,
                bc.cdp_under_contract_flag,
                bc.commitment_under_contract_flag,
                bc.commitment_over_contract_flag,
                bc.rubro_zero_or_undefined_flag,
                gc.guarantee_chain_severity,
                gc.guarantee_chain_risk_signal,
                gc.guarantee_source_status,
                gc.guarantee_chain_flag_count,
                gc.guarantee_issue_flag_count,
                bc.contract_id IS NOT NULL AS budget_chain_flag,
                gc.contract_id IS NOT NULL AS guarantee_chain_flag,
                (
                    coalesce(ir.invoice_source_value, 0.0) > b.contract_value * 1.25
                    AND coalesce(ir.invoice_source_value, 0.0) - b.contract_value
                        >= 100000000
                ) AS invoice_value_over_contract_flag,
                (
                    coalesce(ir.confirmed_invoice_value, 0.0) > b.contract_value * 1.25
                    AND coalesce(ir.confirmed_invoice_value, 0.0) - b.contract_value
                        >= 100000000
                ) AS confirmed_invoice_over_contract_flag,
                (
                    b.contract_end_date IS NOT NULL
                    AND b.contract_end_date <= current_date
                    AND coalesce(ir.post_end_invoice_value, 0.0) >= 100000000
                    AND coalesce(ir.post_end_invoice_value, 0.0)
                        / nullif(b.contract_value, 0) >= 0.10
                ) AS invoice_after_contract_end_flag,
                (
                    coalesce(ir.invoice_source_value, 0.0) >= 100000000
                    AND coalesce(b.invoiced_value, 0.0) = 0.0
                    AND coalesce(b.paid_value, 0.0) = 0.0
                ) AS invoice_summary_zero_gap_flag,
                coalesce(ir.estimated_payment_before_invoice_count, 0) > 0
                    AS estimated_payment_before_invoice_flag,
                list_concat(
                    list_concat(
                        list_concat(
                            [
                                coalesce(
                                    b.process_url,
                                    'secop_ii_contracts:' || b.contract_id
                                )
                            ],
                            coalesce(ie.invoice_evidence_refs, []::VARCHAR[])
                        ),
                        CASE
                            WHEN bc.budget_chain_evidence_ref IS NOT NULL
                                THEN [bc.budget_chain_evidence_ref]
                            ELSE []::VARCHAR[]
                        END
                    ),
                    CASE
                        WHEN gc.guarantee_chain_evidence_ref IS NOT NULL
                            THEN [gc.guarantee_chain_evidence_ref]
                        ELSE []::VARCHAR[]
                    END
                ) AS evidence_refs
            FROM invoice_base_contracts b
            LEFT JOIN invoice_rollup ir
                ON ir.contract_id = b.contract_id
            LEFT JOIN invoice_evidence ie
                ON ie.contract_id = b.contract_id
            LEFT JOIN budget_context bc
                ON bc.contract_id = b.contract_id
            LEFT JOIN guarantee_context gc
                ON gc.contract_id = b.contract_id
        ),
        eligible AS (
            SELECT
                *,
                cast(invoice_value_over_contract_flag AS INTEGER)
                    + cast(confirmed_invoice_over_contract_flag AS INTEGER)
                    + cast(invoice_after_contract_end_flag AS INTEGER)
                    + cast(invoice_summary_zero_gap_flag AS INTEGER)
                    + cast(estimated_payment_before_invoice_flag AS INTEGER)
                    AS invoice_issue_flag_count
            FROM scored
            WHERE invoice_row_count > 0
                AND (
                    (
                        (
                            invoice_value_over_contract_flag
                            OR confirmed_invoice_over_contract_flag
                        )
                        AND (
                            budget_chain_flag
                            OR guarantee_chain_flag
                            OR invoice_excess_value >= 500000000
                            OR confirmed_invoice_excess_value >= 500000000
                        )
                    )
                    OR (
                        invoice_after_contract_end_flag
                        AND (budget_chain_flag OR guarantee_chain_flag)
                    )
                    OR (
                        invoice_summary_zero_gap_flag
                        AND budget_chain_flag
                        AND guarantee_chain_flag
                    )
                )
        )
        SELECT
            'procurement_invoice_budget_reconciliation_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'contract_invoice_budget_reconciliation' AS scope_type,
            CASE
                WHEN confirmed_invoice_over_contract_flag
                    AND budget_chain_flag
                    AND guarantee_chain_flag
                    THEN 'critical'
                WHEN invoice_issue_flag_count >= 4
                    THEN 'critical'
                WHEN invoice_excess_value >= 1000000000
                    OR confirmed_invoice_excess_value >= 1000000000
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.56
                    + least(invoice_issue_flag_count * 0.055, 0.24)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.10)
                    + CASE WHEN budget_chain_flag THEN 0.06 ELSE 0.0 END
                    + CASE WHEN guarantee_chain_flag THEN 0.06 ELSE 0.0 END
                    + CASE WHEN confirmed_invoice_over_contract_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN invoice_after_contract_end_flag THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            CASE
                WHEN confirmed_invoice_over_contract_flag
                    THEN 'confirmed_invoice_value_exceeds_contract'
                WHEN invoice_value_over_contract_flag
                    THEN 'invoice_value_exceeds_contract'
                WHEN invoice_after_contract_end_flag
                    THEN 'invoice_after_contract_end'
                WHEN invoice_summary_zero_gap_flag
                    THEN 'invoice_rows_contradict_zero_contract_summary'
                ELSE 'invoice_payment_date_sequence_anomaly'
            END AS anomaly_type,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
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
            enables_advance_payment,
            liquidation,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            advance_payment_value,
            pending_payment_value,
            paid_value,
            invoiced_value,
            pending_value,
            pending_execution_value,
            invoice_row_count,
            invoice_document_count,
            confirmed_invoice_row_count,
            post_end_invoice_row_count,
            estimated_payment_before_invoice_count,
            invoice_total_value,
            invoice_payable_value,
            invoice_net_value,
            invoice_source_value,
            confirmed_invoice_value,
            post_end_invoice_value,
            invoice_excess_value,
            confirmed_invoice_excess_value,
            invoice_source_value / nullif(contract_value, 0) AS invoice_value_share,
            confirmed_invoice_value / nullif(contract_value, 0)
                AS confirmed_invoice_value_share,
            post_end_invoice_value / nullif(contract_value, 0)
                AS post_end_invoice_value_share,
            first_invoice_date,
            last_invoice_date,
            latest_estimated_payment_date,
            invoice_statuses,
            budget_chain_flag,
            budget_chain_severity,
            budget_issue_flag_count,
            missing_cdp_flag,
            missing_commitment_flag,
            missing_rubro_flag,
            cdp_only_weak_siif_flag,
            commitment_only_failed_flag,
            cdp_under_contract_flag,
            commitment_under_contract_flag,
            commitment_over_contract_flag,
            rubro_zero_or_undefined_flag,
            guarantee_chain_flag,
            guarantee_chain_severity,
            guarantee_source_status,
            guarantee_chain_flag_count,
            guarantee_issue_flag_count,
            invoice_value_over_contract_flag,
            confirmed_invoice_over_contract_flag,
            invoice_after_contract_end_flag,
            invoice_summary_zero_gap_flag,
            estimated_payment_before_invoice_flag,
            invoice_issue_flag_count,
            'invoice/budget reconciliation uses exact contract IDs, SECOP invoice rows, and SECOP budget/guarantee feature context; it does not prove illegal payment, duplicate disbursement, accounting breach, goods delivery failure, or corrupt intent without SIIF, treasury, and contract-file review'
                AS what_is_unproven,
            evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY severity,
                budget_chain_flag DESC,
                guarantee_chain_flag DESC,
                invoice_issue_flag_count DESC,
                invoice_excess_value DESC NULLS LAST,
                confirmed_invoice_excess_value DESC NULLS LAST,
                contract_value DESC NULLS LAST,
                contract_id
        ) <= 1000
    """)


def _create_payment_plan_reconciliation_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_payment_plans",
        "secop_invoices",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_payment_plan_reconciliation AS
        WITH contract_financials AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                any_value(nullif(trim(contract_status), '')) AS contract_status,
                any_value(nullif(trim(enables_advance_payment), ''))
                    AS enables_advance_payment,
                any_value(nullif(trim(liquidation), '')) AS liquidation,
                max(coacc_money_decimal(advance_payment_value)) AS advance_payment_value,
                max(coacc_money_decimal(invoiced_value)) AS invoiced_value,
                max(coacc_money_decimal(pending_payment_value)) AS pending_payment_value,
                max(coacc_money_decimal(paid_value)) AS paid_value,
                max(coacc_money_decimal(amortized_value)) AS amortized_value,
                max(coacc_money_decimal(pending_value)) AS pending_value,
                max(coacc_money_decimal(pending_execution_value))
                    AS pending_execution_value
            FROM src_secop_ii_contracts
            WHERE nullif(trim(contract_id), '') IS NOT NULL
            GROUP BY nullif(trim(contract_id), '')
        ),
        payment_base_contracts AS (
            SELECT
                a.*,
                f.contract_status,
                f.enables_advance_payment,
                f.liquidation,
                f.advance_payment_value,
                f.invoiced_value,
                f.pending_payment_value,
                f.paid_value,
                f.amortized_value,
                f.pending_value,
                f.pending_execution_value
            FROM curated_contract_awards a
            JOIN contract_financials f
                ON f.contract_id = a.contract_id
            WHERE a.contract_id IS NOT NULL
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
            QUALIFY row_number() OVER (
                PARTITION BY a.contract_id
                ORDER BY a.contract_value DESC NULLS LAST,
                    a.signing_date DESC NULLS LAST,
                    a.process_url DESC NULLS LAST
            ) = 1
        ),
        payment_raw_input AS (
            SELECT
                nullif(trim(id_del_contrato), '') AS contract_id,
                coalesce(
                    nullif(trim(id_de_pago), ''),
                    nullif(trim(cufe), ''),
                    nullif(trim(numero_de_factura), ''),
                    'unknown'
                ) AS payment_id,
                nullif(trim(id_de_pago), '') AS source_payment_id,
                nullif(trim(numero_de_factura), '') AS invoice_number,
                nullif(trim(cufe), '') AS cufe_raw,
                CASE
                    WHEN length(nullif(trim(cufe), '')) >= 20
                        AND lower(nullif(trim(cufe), '')) NOT LIKE '%no aplica%'
                        AND lower(nullif(trim(cufe), '')) NOT LIKE '%no obligado%'
                        AND lower(nullif(trim(cufe), '')) NOT LIKE '%sin cufe%'
                        THEN nullif(trim(cufe), '')
                    ELSE NULL
                END AS cufe_key,
                nullif(trim(compromiso_presupuestal), '') AS budget_commitment_ref,
                nullif(trim(documento_proveedor), '') AS payment_supplier_document_id,
                coacc_doc_digits(documento_proveedor)
                    AS payment_supplier_document_digits,
                nullif(trim(nombre_proveedor), '') AS payment_supplier_name,
                nullif(trim(documento_supervisor), '') AS supervisor_document_id,
                coacc_doc_digits(documento_supervisor) AS supervisor_document_digits,
                nullif(trim(nombre_supervisor), '') AS supervisor_name,
                lower(coalesce(nullif(trim(estado), ''), '')) AS payment_status_norm,
                coacc_money_decimal(valor_a_pagar) AS payment_payable_line_value,
                coacc_money_decimal(valor_total) AS payment_total_line_value,
                coacc_money_decimal(valor_total_de_la_factura)
                    AS invoice_total_line_value,
                coacc_money_decimal(valor_neto) AS payment_net_line_value,
                coacc_money_decimal(valor_neto_de_la_factura)
                    AS invoice_net_line_value,
                try_cast(substr(cast(fecha_de_emision AS VARCHAR), 1, 10) AS DATE)
                    AS invoice_issue_date,
                try_cast(substr(cast(fecha_de_recepcion AS VARCHAR), 1, 10) AS DATE)
                    AS invoice_receipt_date,
                try_cast(substr(cast(fecha_estimada_de_pago AS VARCHAR), 1, 10) AS DATE)
                    AS estimated_payment_date,
                try_cast(substr(cast(fecha_real_de_pago AS VARCHAR), 1, 10) AS DATE)
                    AS real_payment_date,
                try_cast(substr(cast(fecha_inicio_contrato AS VARCHAR), 1, 10) AS DATE)
                    AS source_contract_start_date
            FROM src_secop_payment_plans
            WHERE nullif(trim(id_del_contrato), '') IS NOT NULL
        ),
        payment_raw AS (
            SELECT
                *,
                CASE
                    WHEN length(payment_supplier_document_digits) = 10
                        THEN left(payment_supplier_document_digits, 9)
                    ELSE payment_supplier_document_digits
                END AS payment_supplier_document_key,
                coalesce(
                    payment_payable_line_value,
                    payment_total_line_value,
                    invoice_total_line_value,
                    payment_net_line_value,
                    invoice_net_line_value,
                    0.0
                ) AS payment_line_value,
                (
                    payment_status_norm NOT LIKE '%rechaz%'
                    AND payment_status_norm NOT LIKE '%anulad%'
                    AND payment_status_norm NOT LIKE '%cancel%'
                    AND payment_status_norm NOT LIKE '%borrador%'
                ) AS payment_status_countable,
                (
                    payment_status_norm LIKE '%pagad%'
                    OR real_payment_date IS NOT NULL
                ) AS paid_status_flag
            FROM payment_raw_input
        ),
        duplicate_cufe AS (
            SELECT
                cufe_key,
                count(DISTINCT contract_id) AS duplicate_cufe_contract_count,
                count(DISTINCT payment_supplier_document_key)
                    AS duplicate_cufe_supplier_count,
                list(DISTINCT contract_id ORDER BY contract_id) AS duplicate_cufe_contracts
            FROM payment_raw
            WHERE cufe_key IS NOT NULL
            GROUP BY cufe_key
            HAVING count(DISTINCT contract_id) > 1
        ),
        payment_scoped AS (
            SELECT
                b.contract_id,
                b.contract_value,
                b.contract_end_date,
                b.supplier_document_key,
                p.payment_id,
                p.source_payment_id,
                p.invoice_number,
                p.cufe_key,
                p.budget_commitment_ref,
                p.payment_supplier_document_id,
                p.payment_supplier_document_key,
                p.payment_supplier_name,
                p.supervisor_document_id,
                p.supervisor_document_digits,
                p.supervisor_name,
                p.payment_status_norm,
                p.payment_status_countable,
                p.paid_status_flag,
                p.payment_line_value,
                p.payment_payable_line_value,
                p.payment_total_line_value,
                p.invoice_total_line_value,
                p.payment_net_line_value,
                p.invoice_net_line_value,
                p.invoice_issue_date,
                p.invoice_receipt_date,
                p.estimated_payment_date,
                p.real_payment_date,
                p.source_contract_start_date,
                coalesce(dc.duplicate_cufe_contract_count, 0)
                    AS duplicate_cufe_contract_count,
                coalesce(dc.duplicate_cufe_supplier_count, 0)
                    AS duplicate_cufe_supplier_count,
                dc.duplicate_cufe_contracts,
                (
                    p.payment_supplier_document_key IS NOT NULL
                    AND b.supplier_document_key IS NOT NULL
                    AND p.payment_supplier_document_key != b.supplier_document_key
                ) AS supplier_document_mismatch
            FROM payment_base_contracts b
            JOIN payment_raw p
                ON p.contract_id = b.contract_id
            LEFT JOIN duplicate_cufe dc
                ON dc.cufe_key = p.cufe_key
            WHERE p.payment_line_value > 0
        ),
        payment_rollup AS (
            SELECT
                contract_id,
                count(*) AS payment_row_count,
                count(DISTINCT payment_id) AS payment_document_count,
                count(*) FILTER (
                    WHERE payment_status_countable AND paid_status_flag
                ) AS paid_payment_row_count,
                count(*) FILTER (
                    WHERE payment_status_countable AND real_payment_date IS NOT NULL
                ) AS real_payment_row_count,
                count(DISTINCT cufe_key) FILTER (WHERE cufe_key IS NOT NULL)
                    AS cufe_count,
                count(DISTINCT budget_commitment_ref)
                    FILTER (WHERE budget_commitment_ref IS NOT NULL)
                    AS payment_budget_commitment_count,
                sum(payment_line_value) FILTER (WHERE payment_status_countable)
                    AS payment_plan_source_value,
                sum(payment_payable_line_value) FILTER (WHERE payment_status_countable)
                    AS payment_payable_value,
                sum(invoice_total_line_value) FILTER (WHERE payment_status_countable)
                    AS payment_invoice_total_value,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable AND paid_status_flag
                ) AS actual_paid_value,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND contract_end_date IS NOT NULL
                        AND real_payment_date > contract_end_date
                ) AS post_end_actual_paid_value,
                count(*) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND contract_end_date IS NOT NULL
                        AND real_payment_date > contract_end_date
                ) AS post_end_paid_payment_count,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND real_payment_date IS NOT NULL
                        AND invoice_issue_date IS NOT NULL
                        AND real_payment_date < invoice_issue_date
                ) AS payment_before_invoice_issue_value,
                count(*) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND real_payment_date IS NOT NULL
                        AND invoice_issue_date IS NOT NULL
                        AND real_payment_date < invoice_issue_date
                ) AS payment_before_invoice_issue_count,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND real_payment_date IS NOT NULL
                        AND invoice_receipt_date IS NOT NULL
                        AND real_payment_date < invoice_receipt_date
                ) AS payment_before_invoice_receipt_value,
                count(*) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND real_payment_date IS NOT NULL
                        AND invoice_receipt_date IS NOT NULL
                        AND real_payment_date < invoice_receipt_date
                ) AS payment_before_invoice_receipt_count,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND supplier_document_mismatch
                ) AS supplier_mismatch_paid_value,
                count(*) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND supplier_document_mismatch
                ) AS supplier_mismatch_paid_count,
                sum(payment_line_value) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND duplicate_cufe_contract_count > 1
                ) AS duplicate_cufe_paid_value,
                count(DISTINCT cufe_key) FILTER (
                    WHERE payment_status_countable
                        AND paid_status_flag
                        AND duplicate_cufe_contract_count > 1
                ) AS duplicate_cufe_count,
                max(duplicate_cufe_contract_count) AS max_duplicate_cufe_contract_count,
                min(real_payment_date) FILTER (
                    WHERE payment_status_countable AND paid_status_flag
                ) AS first_real_payment_date,
                max(real_payment_date) FILTER (
                    WHERE payment_status_countable AND paid_status_flag
                ) AS last_real_payment_date,
                min(invoice_issue_date) FILTER (WHERE payment_status_countable)
                    AS first_invoice_issue_date,
                max(invoice_receipt_date) FILTER (WHERE payment_status_countable)
                    AS last_invoice_receipt_date,
                list(DISTINCT payment_status_norm ORDER BY payment_status_norm)
                    FILTER (WHERE payment_status_norm IS NOT NULL)
                    AS payment_statuses,
                list(DISTINCT payment_supplier_document_key
                    ORDER BY payment_supplier_document_key)
                    FILTER (WHERE payment_supplier_document_key IS NOT NULL)
                    AS payment_supplier_document_keys,
                list(DISTINCT supervisor_document_digits
                    ORDER BY supervisor_document_digits)
                    FILTER (WHERE supervisor_document_digits IS NOT NULL)
                    AS supervisor_document_keys
            FROM payment_scoped
            GROUP BY contract_id
        ),
        payment_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_payment_plans:' || contract_id || ':' || payment_id
                    AS evidence_ref,
                payment_line_value,
                paid_status_flag,
                real_payment_date,
                supplier_document_mismatch,
                duplicate_cufe_contract_count,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY supplier_document_mismatch DESC,
                        (duplicate_cufe_contract_count > 1) DESC,
                        paid_status_flag DESC,
                        payment_line_value DESC NULLS LAST,
                        real_payment_date DESC NULLS LAST,
                        payment_id
                ) AS evidence_rank
            FROM payment_scoped
            WHERE payment_status_countable
        ),
        payment_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank)
                    AS payment_evidence_refs
            FROM payment_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY contract_id
        ),
        budget_context AS (
            SELECT
                contract_id,
                severity AS budget_chain_severity,
                risk_signal AS budget_chain_risk_signal,
                budget_issue_flag_count,
                missing_commitment_flag,
                commitment_only_failed_flag,
                commitment_under_contract_flag,
                commitment_over_contract_flag,
                'signal_feature_procurement_budget_chain_reconciliation_review_only:'
                    || contract_id AS budget_chain_evidence_ref
            FROM curated_budget_chain_reconciliation
        ),
        guarantee_context AS (
            SELECT
                contract_id,
                severity AS guarantee_chain_severity,
                risk_signal AS guarantee_chain_risk_signal,
                guarantee_source_status,
                chain_flag_count AS guarantee_chain_flag_count,
                guarantee_issue_flag_count,
                'signal_feature_procurement_guarantee_advance_execution_chain:'
                    || contract_id AS guarantee_chain_evidence_ref
            FROM curated_guarantee_advance_execution_chain
        ),
        invoice_context AS (
            SELECT
                contract_id,
                severity AS invoice_chain_severity,
                risk_signal AS invoice_chain_risk_signal,
                anomaly_type AS invoice_chain_anomaly_type,
                invoice_issue_flag_count,
                'signal_feature_procurement_invoice_budget_reconciliation_review_only:'
                    || contract_id AS invoice_chain_evidence_ref
            FROM curated_invoice_budget_reconciliation
        ),
        scored AS (
            SELECT
                b.*,
                coalesce(pr.payment_row_count, 0) AS payment_row_count,
                coalesce(pr.payment_document_count, 0) AS payment_document_count,
                coalesce(pr.paid_payment_row_count, 0) AS paid_payment_row_count,
                coalesce(pr.real_payment_row_count, 0) AS real_payment_row_count,
                coalesce(pr.cufe_count, 0) AS cufe_count,
                coalesce(pr.payment_budget_commitment_count, 0)
                    AS payment_budget_commitment_count,
                coalesce(pr.payment_plan_source_value, 0.0)
                    AS payment_plan_source_value,
                coalesce(pr.payment_payable_value, 0.0) AS payment_payable_value,
                coalesce(pr.payment_invoice_total_value, 0.0)
                    AS payment_invoice_total_value,
                coalesce(pr.actual_paid_value, 0.0) AS actual_paid_value,
                coalesce(pr.post_end_actual_paid_value, 0.0)
                    AS post_end_actual_paid_value,
                coalesce(pr.post_end_paid_payment_count, 0)
                    AS post_end_paid_payment_count,
                coalesce(pr.payment_before_invoice_issue_value, 0.0)
                    AS payment_before_invoice_issue_value,
                coalesce(pr.payment_before_invoice_issue_count, 0)
                    AS payment_before_invoice_issue_count,
                coalesce(pr.payment_before_invoice_receipt_value, 0.0)
                    AS payment_before_invoice_receipt_value,
                coalesce(pr.payment_before_invoice_receipt_count, 0)
                    AS payment_before_invoice_receipt_count,
                coalesce(pr.supplier_mismatch_paid_value, 0.0)
                    AS supplier_mismatch_paid_value,
                coalesce(pr.supplier_mismatch_paid_count, 0)
                    AS supplier_mismatch_paid_count,
                coalesce(pr.duplicate_cufe_paid_value, 0.0)
                    AS duplicate_cufe_paid_value,
                coalesce(pr.duplicate_cufe_count, 0) AS duplicate_cufe_count,
                coalesce(pr.max_duplicate_cufe_contract_count, 0)
                    AS max_duplicate_cufe_contract_count,
                greatest(coalesce(pr.actual_paid_value, 0.0) - b.contract_value, 0.0)
                    AS actual_paid_excess_value,
                pr.first_real_payment_date,
                pr.last_real_payment_date,
                pr.first_invoice_issue_date,
                pr.last_invoice_receipt_date,
                pr.payment_statuses,
                pr.payment_supplier_document_keys,
                pr.supervisor_document_keys,
                bc.budget_chain_severity,
                bc.budget_chain_risk_signal,
                bc.budget_issue_flag_count,
                bc.missing_commitment_flag,
                bc.commitment_only_failed_flag,
                bc.commitment_under_contract_flag,
                bc.commitment_over_contract_flag,
                gc.guarantee_chain_severity,
                gc.guarantee_chain_risk_signal,
                gc.guarantee_source_status,
                gc.guarantee_chain_flag_count,
                gc.guarantee_issue_flag_count,
                ic.invoice_chain_severity,
                ic.invoice_chain_risk_signal,
                ic.invoice_chain_anomaly_type,
                ic.invoice_issue_flag_count,
                bc.contract_id IS NOT NULL AS budget_chain_flag,
                gc.contract_id IS NOT NULL AS guarantee_chain_flag,
                ic.contract_id IS NOT NULL AS invoice_chain_flag,
                (
                    coalesce(pr.actual_paid_value, 0.0) > b.contract_value * 1.25
                    AND coalesce(pr.actual_paid_value, 0.0) - b.contract_value
                        >= 100000000
                ) AS actual_paid_over_contract_flag,
                (
                    b.contract_end_date IS NOT NULL
                    AND b.contract_end_date <= current_date
                    AND coalesce(pr.post_end_actual_paid_value, 0.0) >= 100000000
                    AND coalesce(pr.post_end_actual_paid_value, 0.0)
                        / nullif(b.contract_value, 0) >= 0.10
                ) AS real_payment_after_contract_end_flag,
                (
                    coalesce(pr.payment_before_invoice_issue_value, 0.0) >= 100000000
                    OR coalesce(pr.payment_before_invoice_receipt_value, 0.0)
                        >= 100000000
                ) AS payment_before_invoice_sequence_flag,
                (
                    coalesce(pr.supplier_mismatch_paid_value, 0.0) >= 100000000
                    AND coalesce(pr.supplier_mismatch_paid_value, 0.0)
                        / nullif(b.contract_value, 0) >= 0.05
                ) AS payment_supplier_document_mismatch_flag,
                coalesce(pr.duplicate_cufe_paid_value, 0.0) >= 100000000
                    AS duplicate_cufe_cross_contract_flag,
                (
                    coalesce(pr.actual_paid_value, 0.0) >= 100000000
                    AND coalesce(b.invoiced_value, 0.0) = 0.0
                    AND coalesce(b.paid_value, 0.0) = 0.0
                ) AS payment_summary_zero_gap_flag,
                list_concat(
                    list_concat(
                        list_concat(
                            list_concat(
                                [
                                    coalesce(
                                        b.process_url,
                                        'secop_ii_contracts:' || b.contract_id
                                    )
                                ],
                                coalesce(pe.payment_evidence_refs, []::VARCHAR[])
                            ),
                            CASE
                                WHEN bc.budget_chain_evidence_ref IS NOT NULL
                                    THEN [bc.budget_chain_evidence_ref]
                                ELSE []::VARCHAR[]
                            END
                        ),
                        CASE
                            WHEN gc.guarantee_chain_evidence_ref IS NOT NULL
                                THEN [gc.guarantee_chain_evidence_ref]
                            ELSE []::VARCHAR[]
                        END
                    ),
                    CASE
                        WHEN ic.invoice_chain_evidence_ref IS NOT NULL
                            THEN [ic.invoice_chain_evidence_ref]
                        ELSE []::VARCHAR[]
                    END
                ) AS evidence_refs
            FROM payment_base_contracts b
            LEFT JOIN payment_rollup pr
                ON pr.contract_id = b.contract_id
            LEFT JOIN payment_evidence pe
                ON pe.contract_id = b.contract_id
            LEFT JOIN budget_context bc
                ON bc.contract_id = b.contract_id
            LEFT JOIN guarantee_context gc
                ON gc.contract_id = b.contract_id
            LEFT JOIN invoice_context ic
                ON ic.contract_id = b.contract_id
        ),
        eligible AS (
            SELECT
                *,
                cast(actual_paid_over_contract_flag AS INTEGER)
                    + cast(real_payment_after_contract_end_flag AS INTEGER)
                    + cast(payment_before_invoice_sequence_flag AS INTEGER)
                    + cast(payment_supplier_document_mismatch_flag AS INTEGER)
                    + cast(duplicate_cufe_cross_contract_flag AS INTEGER)
                    + cast(payment_summary_zero_gap_flag AS INTEGER)
                    AS payment_issue_flag_count
            FROM scored
            WHERE payment_row_count > 0
                AND (
                    (
                        actual_paid_over_contract_flag
                        AND (
                            budget_chain_flag
                            OR guarantee_chain_flag
                            OR invoice_chain_flag
                            OR actual_paid_excess_value >= 500000000
                        )
                    )
                    OR (
                        real_payment_after_contract_end_flag
                        AND (budget_chain_flag OR guarantee_chain_flag OR invoice_chain_flag)
                    )
                    OR (
                        payment_before_invoice_sequence_flag
                        AND (budget_chain_flag OR guarantee_chain_flag OR invoice_chain_flag)
                    )
                    OR (
                        payment_supplier_document_mismatch_flag
                        AND (budget_chain_flag OR guarantee_chain_flag OR invoice_chain_flag)
                    )
                    OR duplicate_cufe_cross_contract_flag
                    OR (
                        payment_summary_zero_gap_flag
                        AND (budget_chain_flag OR guarantee_chain_flag OR invoice_chain_flag)
                    )
                )
        )
        SELECT
            'procurement_payment_plan_reconciliation_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'contract_payment_plan_reconciliation' AS scope_type,
            CASE
                WHEN actual_paid_over_contract_flag
                    AND budget_chain_flag
                    AND guarantee_chain_flag
                    THEN 'critical'
                WHEN duplicate_cufe_cross_contract_flag
                    AND (budget_chain_flag OR guarantee_chain_flag OR invoice_chain_flag)
                    THEN 'critical'
                WHEN payment_issue_flag_count >= 4 THEN 'critical'
                WHEN actual_paid_excess_value >= 1000000000 THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.56
                    + least(payment_issue_flag_count * 0.05, 0.24)
                    + least(log10(greatest(contract_value, 1)) / 120.0, 0.10)
                    + CASE WHEN budget_chain_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN guarantee_chain_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN invoice_chain_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN actual_paid_over_contract_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN duplicate_cufe_cross_contract_flag THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            CASE
                WHEN duplicate_cufe_cross_contract_flag
                    THEN 'duplicate_cufe_paid_across_contracts'
                WHEN actual_paid_over_contract_flag
                    THEN 'actual_payment_value_exceeds_contract'
                WHEN payment_supplier_document_mismatch_flag
                    THEN 'payment_provider_document_mismatch'
                WHEN payment_before_invoice_sequence_flag
                    THEN 'actual_payment_before_invoice_date'
                WHEN real_payment_after_contract_end_flag
                    THEN 'actual_payment_after_contract_end'
                WHEN payment_summary_zero_gap_flag
                    THEN 'payment_plan_rows_contradict_zero_contract_summary'
                ELSE 'payment_plan_reconciliation_anomaly'
            END AS anomaly_type,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
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
            enables_advance_payment,
            liquidation,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            advance_payment_value,
            pending_payment_value,
            paid_value,
            invoiced_value,
            pending_value,
            pending_execution_value,
            payment_row_count,
            payment_document_count,
            paid_payment_row_count,
            real_payment_row_count,
            cufe_count,
            payment_budget_commitment_count,
            payment_plan_source_value,
            payment_payable_value,
            payment_invoice_total_value,
            actual_paid_value,
            post_end_actual_paid_value,
            actual_paid_excess_value,
            post_end_paid_payment_count,
            payment_before_invoice_issue_value,
            payment_before_invoice_issue_count,
            payment_before_invoice_receipt_value,
            payment_before_invoice_receipt_count,
            supplier_mismatch_paid_value,
            supplier_mismatch_paid_count,
            duplicate_cufe_paid_value,
            duplicate_cufe_count,
            max_duplicate_cufe_contract_count,
            first_real_payment_date,
            last_real_payment_date,
            first_invoice_issue_date,
            last_invoice_receipt_date,
            payment_statuses,
            payment_supplier_document_keys,
            supervisor_document_keys,
            budget_chain_flag,
            budget_chain_severity,
            budget_issue_flag_count,
            missing_commitment_flag,
            commitment_only_failed_flag,
            commitment_under_contract_flag,
            commitment_over_contract_flag,
            guarantee_chain_flag,
            guarantee_chain_severity,
            guarantee_source_status,
            guarantee_chain_flag_count,
            guarantee_issue_flag_count,
            invoice_chain_flag,
            invoice_chain_severity,
            invoice_chain_anomaly_type,
            invoice_issue_flag_count,
            actual_paid_over_contract_flag,
            real_payment_after_contract_end_flag,
            payment_before_invoice_sequence_flag,
            payment_supplier_document_mismatch_flag,
            duplicate_cufe_cross_contract_flag,
            payment_summary_zero_gap_flag,
            payment_issue_flag_count,
            (
                'payment-plan reconciliation uses exact contract IDs, SECOP '
                || 'payment-plan rows, real payment dates, CUFE, supplier/supervisor '
                || 'documents, and budget/invoice/guarantee context; it does not '
                || 'prove illegal payment, duplicate disbursement, accounting breach, '
                || 'delivery failure, or corrupt intent without SIIF, treasury, '
                || 'CUFE validation, and contract-file review'
            )
                AS what_is_unproven,
            evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY severity,
                budget_chain_flag DESC,
                guarantee_chain_flag DESC,
                invoice_chain_flag DESC,
                payment_issue_flag_count DESC,
                actual_paid_excess_value DESC NULLS LAST,
                duplicate_cufe_paid_value DESC NULLS LAST,
                contract_value DESC NULLS LAST,
                contract_id
        ) <= 1000
    """)


def _create_health_pae_service_delivery_gap_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "health_providers",
        "secop_ii_contracts",
        "secop_invoices",
        "secop_payment_plans",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    support_sources = {
        "large_modifications": (
            "signal_feature_procurement_large_modifications",
            "curated_large_modifications",
        ),
        "contract_suspensions": (
            "signal_feature_procurement_contract_suspensions",
            "curated_contract_suspensions",
        ),
        "contract_execution_delay": (
            "signal_feature_procurement_contract_execution_delay",
            "curated_contract_execution_delay",
        ),
        "guarantee_chain": (
            "signal_feature_procurement_guarantee_advance_execution_chain",
            "curated_guarantee_advance_execution_chain",
        ),
        "budget_chain": (
            "signal_feature_procurement_budget_chain_reconciliation_review_only",
            "curated_budget_chain_reconciliation",
        ),
        "invoice_chain": (
            "signal_feature_procurement_invoice_budget_reconciliation_review_only",
            "curated_invoice_budget_reconciliation",
        ),
        "payment_chain": (
            "signal_feature_procurement_payment_plan_reconciliation_review_only",
            "curated_payment_plan_reconciliation",
        ),
        "single_bidder": (
            "signal_feature_procurement_single_bidder_high_value",
            "curated_single_bidder_high_value",
        ),
    }

    def support_relation(key: str) -> str:
        table, view_name = support_sources[key]
        table_path = curated_path(table)
        if any(table_path.glob("*.parquet")):
            return f"read_parquet({_sql_string(str(table_path / '*.parquet'))})"
        return view_name

    secop_columns = {
        str(row[0])
        for row in con.execute("DESCRIBE SELECT * FROM src_secop_ii_contracts").fetchall()
    }

    def secop_col(column: str, sql_type: str = "VARCHAR") -> str:
        if column in secop_columns:
            return column
        return f"NULL::{sql_type}"

    con.execute(fr"""
        CREATE OR REPLACE TEMP VIEW curated_health_pae_service_delivery_gap AS
        WITH provider_rows AS (
            SELECT
                row_number() OVER () AS provider_row_id,
                coacc_document_key(
                    numeroidentificacion,
                    CASE
                        WHEN upper(nullif(trim(tipoid), '')) IN ('NI', 'NIT') THEN 'NIT'
                        ELSE nullif(trim(tipoid), '')
                    END
                ) AS provider_document_key,
                coacc_doc_digits(numeroidentificacion) AS provider_document_digits,
                coacc_nit_canonical(
                    numeroidentificacion,
                    CASE
                        WHEN upper(nullif(trim(tipoid), '')) IN ('NI', 'NIT') THEN 'NIT'
                        ELSE nullif(trim(tipoid), '')
                    END
                ) AS provider_nit_canonical,
                nullif(trim(tipoid), '') AS provider_doc_type,
                nullif(trim(numeroidentificacion), '') AS provider_document_id,
                nullif(trim(codigoprestador), '') AS provider_code,
                nullif(trim(codigohabilitacionsede), '') AS provider_site_code,
                nullif(trim(nombreprestador), '') AS provider_name,
                nullif(trim(nombresede), '') AS provider_site_name,
                nullif(trim(claseprestador), '') AS provider_class,
                nullif(trim(naturalezajuridica), '') AS provider_legal_nature,
                nullif(trim(ese), '') AS provider_ese_flag,
                nullif(trim(departamentoprestadordesc), '') AS provider_department,
                nullif(trim(municipioprestadordesc), '') AS provider_municipality,
                nullif(trim(departamentodededesc), '') AS site_department,
                nullif(trim(municipiosededesc), '') AS site_municipality,
                nullif(trim(fecha_corte_reps), '') AS reps_cutoff,
                'health_providers:' || coalesce(
                    nullif(trim(codigohabilitacionsede), ''),
                    nullif(trim(codigoprestador), ''),
                    coacc_document_key(
                        numeroidentificacion,
                        CASE
                            WHEN upper(nullif(trim(tipoid), '')) IN ('NI', 'NIT') THEN 'NIT'
                            ELSE nullif(trim(tipoid), '')
                        END
                    ) || ':' || cast(row_number() OVER () AS VARCHAR)
                ) AS provider_evidence_ref
            FROM src_health_providers
            WHERE coacc_document_key(
                    numeroidentificacion,
                    CASE
                        WHEN upper(nullif(trim(tipoid), '')) IN ('NI', 'NIT') THEN 'NIT'
                        ELSE nullif(trim(tipoid), '')
                    END
                ) IS NOT NULL
                AND NOT regexp_matches(
                    coacc_document_key(
                        numeroidentificacion,
                        CASE
                            WHEN upper(nullif(trim(tipoid), '')) IN ('NI', 'NIT') THEN 'NIT'
                            ELSE nullif(trim(tipoid), '')
                        END
                    ),
                    '^0+$'
                )
        ),
        provider_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY provider_document_key
                    ORDER BY
                        CASE
                            WHEN provider_class ILIKE '%IPS%' THEN 0
                            WHEN provider_class ILIKE '%Transporte Especial%' THEN 1
                            ELSE 2
                        END,
                        provider_site_code NULLS LAST,
                        provider_code NULLS LAST
                ) AS provider_rank
            FROM provider_rows
        ),
        provider_rollup AS (
            SELECT
                provider_document_key,
                max(provider_document_digits) AS provider_document_digits,
                max(provider_nit_canonical) AS provider_nit_canonical,
                any_value(provider_doc_type) AS provider_doc_type,
                any_value(provider_document_id) AS provider_document_id,
                any_value(provider_code) AS provider_code,
                min(provider_name) FILTER (WHERE provider_name IS NOT NULL)
                    AS provider_name,
                min(provider_class) FILTER (WHERE provider_class IS NOT NULL)
                    AS provider_class,
                min(provider_legal_nature) FILTER (WHERE provider_legal_nature IS NOT NULL)
                    AS provider_legal_nature,
                bool_or(provider_class ILIKE '%IPS%') AS provider_ips_flag,
                bool_or(provider_class ILIKE '%Transporte Especial%') AS provider_transport_flag,
                bool_or(provider_legal_nature ILIKE '%Pública%') AS provider_public_flag,
                bool_or(provider_ese_flag ILIKE '%si%') AS provider_ese_public_flag,
                count(*) AS provider_site_row_count,
                count(DISTINCT provider_code) AS provider_code_count,
                count(DISTINCT provider_site_code) AS provider_site_count,
                list(DISTINCT provider_department ORDER BY provider_department)
                    FILTER (WHERE provider_department IS NOT NULL)
                    AS provider_departments,
                list(DISTINCT provider_municipality ORDER BY provider_municipality)
                    FILTER (WHERE provider_municipality IS NOT NULL)
                    AS provider_municipalities,
                max(reps_cutoff) AS reps_cutoff,
                list(provider_evidence_ref ORDER BY provider_rank, provider_evidence_ref)
                    FILTER (WHERE provider_rank <= 3) AS provider_evidence_refs
            FROM provider_ranked
            GROUP BY provider_document_key
        ),
        raw_contract_domain AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                nullif(trim({secop_col("contract_status")}), '') AS contract_status,
                nullif(trim({secop_col("contract_object")}), '') AS contract_object,
                nullif(trim({secop_col("process_description")}), '')
                    AS process_description,
                nullif(trim({secop_col("enables_advance_payment")}), '')
                    AS enables_advance_payment,
                nullif(trim({secop_col("liquidation")}), '') AS liquidation,
                coacc_money_decimal({secop_col("advance_payment_value")})
                    AS advance_payment_value,
                coacc_money_decimal({secop_col("invoiced_value")}) AS invoiced_value,
                coacc_money_decimal({secop_col("pending_payment_value")})
                    AS pending_payment_value,
                coacc_money_decimal({secop_col("paid_value")}) AS paid_value,
                coacc_money_decimal({secop_col("pending_value")}) AS pending_value,
                coacc_money_decimal({secop_col("pending_execution_value")})
                    AS pending_execution_value,
                lower(
                    coalesce({secop_col("contract_object")}, '') || ' ' ||
                    coalesce({secop_col("process_description")}, '') || ' ' ||
                    coalesce(sector, '') || ' ' ||
                    coalesce(procurement_modality, '') || ' ' ||
                    coalesce(contract_type, '')
                ) AS domain_text,
                regexp_matches(
                    lower(
                        coalesce({secop_col("contract_object")}, '') || ' ' ||
                        coalesce({secop_col("process_description")}, '') || ' ' ||
                        coalesce(sector, '') || ' ' ||
                        coalesce(procurement_modality, '') || ' ' ||
                        coalesce(contract_type, '')
                    ),
                    'alimentaci[oó]n escolar|\bpae\b|programa de alimentaci[oó]n escolar|'
                    || 'complemento alimentario|restaurante escolar|raci[oó]n alimentaria'
                ) AS pae_keyword_flag,
                regexp_matches(
                    lower(
                        coalesce({secop_col("contract_object")}, '') || ' ' ||
                        coalesce({secop_col("process_description")}, '') || ' ' ||
                        coalesce(sector, '') || ' ' ||
                        coalesce(procurement_modality, '') || ' ' ||
                        coalesce(contract_type, '')
                    ),
                    'hospital|salud|m[eé]dic|medicamento|ips|servicio(s)? de salud|'
                    || 'hemofilia|ambulancia|vacuna|biom[eé]dic|urgencia|tratamiento|paciente'
                ) AS health_keyword_flag,
                row_number() OVER (
                    PARTITION BY nullif(trim(contract_id), '')
                    ORDER BY try_cast({secop_col("last_update")} AS TIMESTAMP)
                        DESC NULLS LAST
                ) AS contract_rank
            FROM src_secop_ii_contracts
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND regexp_matches(
                    lower(
                        coalesce({secop_col("contract_object")}, '') || ' ' ||
                        coalesce({secop_col("process_description")}, '') || ' ' ||
                        coalesce(sector, '') || ' ' ||
                        coalesce(procurement_modality, '') || ' ' ||
                        coalesce(contract_type, '')
                    ),
                    'alimentaci[oó]n escolar|\bpae\b|programa de alimentaci[oó]n escolar|'
                    || 'complemento alimentario|restaurante escolar|raci[oó]n alimentaria|'
                    || 'hospital|salud|m[eé]dic|medicamento|ips|servicio(s)? de salud|'
                    || 'hemofilia|ambulancia|vacuna|biom[eé]dic|urgencia|tratamiento|paciente'
                )
            QUALIFY contract_rank = 1
        ),
        provider_contract_candidates AS (
            SELECT
                a.award_row_id,
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_document_digits,
                a.supplier_nit_base,
                a.supplier_nit_canonical,
                a.supplier_name,
                a.supplier_doc_type,
                a.buyer_document_id,
                a.buyer_document_digits,
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
                r.contract_status,
                r.contract_object,
                r.process_description,
                r.enables_advance_payment,
                r.liquidation,
                r.advance_payment_value,
                r.invoiced_value,
                r.pending_payment_value,
                r.paid_value,
                r.pending_value,
                r.pending_execution_value,
                p.provider_document_key,
                p.provider_document_id,
                p.provider_doc_type,
                p.provider_code,
                p.provider_name,
                p.provider_class,
                p.provider_legal_nature,
                p.provider_ips_flag,
                p.provider_transport_flag,
                p.provider_public_flag,
                p.provider_ese_public_flag,
                p.provider_site_row_count,
                p.provider_code_count,
                p.provider_site_count,
                p.provider_departments,
                p.provider_municipalities,
                p.reps_cutoff,
                p.provider_evidence_refs,
                CASE
                    WHEN p.provider_document_key = a.supplier_document_key THEN 1
                    WHEN p.provider_document_key = a.supplier_nit_base THEN 2
                    WHEN p.provider_document_key = a.supplier_document_digits THEN 3
                    ELSE 99
                END AS provider_match_rank,
                1 AS candidate_source_rank,
                coalesce(
                    r.domain_text,
                    lower(
                        coalesce(a.buyer_name, '') || ' ' ||
                        coalesce(a.sector, '') || ' ' ||
                        coalesce(a.procurement_modality, '') || ' ' ||
                        coalesce(a.contract_type, '')
                    )
                ) AS domain_text,
                coalesce(
                    r.pae_keyword_flag,
                    regexp_matches(
                        lower(
                            coalesce(a.buyer_name, '') || ' ' ||
                            coalesce(a.sector, '') || ' ' ||
                            coalesce(a.procurement_modality, '') || ' ' ||
                            coalesce(a.contract_type, '')
                        ),
                        'alimentaci[oó]n escolar|\bpae\b|programa de alimentaci[oó]n escolar|'
                        || 'complemento alimentario|restaurante escolar|raci[oó]n alimentaria'
                    )
                ) AS pae_keyword_flag,
                coalesce(
                    r.health_keyword_flag,
                    regexp_matches(
                        lower(
                            coalesce(a.buyer_name, '') || ' ' ||
                            coalesce(a.sector, '') || ' ' ||
                            coalesce(a.procurement_modality, '') || ' ' ||
                            coalesce(a.contract_type, '')
                        ),
                        'hospital|salud|m[eé]dic|medicamento|ips|servicio(s)? de salud|'
                        || 'hemofilia|ambulancia|vacuna|biom[eé]dic|urgencia|tratamiento|paciente'
                    )
                ) AS health_keyword_flag
            FROM curated_contract_awards a
            JOIN provider_rollup p
                ON p.provider_document_key IN (
                    a.supplier_document_key,
                    a.supplier_nit_base,
                    a.supplier_document_digits
                )
            LEFT JOIN raw_contract_domain r
                ON r.contract_id = a.contract_id
            WHERE a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND a.contract_id IS NOT NULL
                AND a.supplier_document_key IS NOT NULL
        ),
        text_contract_candidates AS (
            SELECT
                a.award_row_id,
                a.supplier_entity_id,
                a.supplier_document_key,
                a.supplier_document_digits,
                a.supplier_nit_base,
                a.supplier_nit_canonical,
                a.supplier_name,
                a.supplier_doc_type,
                a.buyer_document_id,
                a.buyer_document_digits,
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
                r.contract_status,
                r.contract_object,
                r.process_description,
                r.enables_advance_payment,
                r.liquidation,
                r.advance_payment_value,
                r.invoiced_value,
                r.pending_payment_value,
                r.paid_value,
                r.pending_value,
                r.pending_execution_value,
                p.provider_document_key,
                p.provider_document_id,
                p.provider_doc_type,
                p.provider_code,
                p.provider_name,
                p.provider_class,
                p.provider_legal_nature,
                p.provider_ips_flag,
                p.provider_transport_flag,
                p.provider_public_flag,
                p.provider_ese_public_flag,
                p.provider_site_row_count,
                p.provider_code_count,
                p.provider_site_count,
                p.provider_departments,
                p.provider_municipalities,
                p.reps_cutoff,
                p.provider_evidence_refs,
                CASE
                    WHEN p.provider_document_key = a.supplier_document_key THEN 1
                    WHEN p.provider_document_key = a.supplier_nit_base THEN 2
                    WHEN p.provider_document_key = a.supplier_document_digits THEN 3
                    ELSE 99
                END AS provider_match_rank,
                2 AS candidate_source_rank,
                r.domain_text,
                r.pae_keyword_flag,
                r.health_keyword_flag
            FROM raw_contract_domain r
            JOIN curated_contract_awards a
                ON a.contract_id = r.contract_id
            LEFT JOIN provider_rollup p
                ON p.provider_document_key IN (
                    a.supplier_document_key,
                    a.supplier_nit_base,
                    a.supplier_document_digits
                )
            WHERE a.contract_value IS NOT NULL
                AND a.contract_value >= 100000000
                AND a.supplier_document_key IS NOT NULL
        ),
        contract_domain_candidates AS (
            SELECT * FROM provider_contract_candidates
            UNION ALL
            SELECT * FROM text_contract_candidates
        ),
        contract_domain AS (
            SELECT
                *,
                provider_document_key IS NOT NULL AS provider_match_flag
            FROM contract_domain_candidates
            QUALIFY row_number() OVER (
                PARTITION BY award_row_id
                ORDER BY
                    provider_match_rank ASC,
                    candidate_source_rank ASC,
                    provider_site_count DESC NULLS LAST
            ) = 1
        ),
        support_rows AS (
            SELECT
                contract_id,
                signal_id,
                'execution_failure' AS support_family,
                severity,
                risk_signal,
                'signal_feature_procurement_large_modifications:' || scope_key
                    AS evidence_ref
            FROM {support_relation("large_modifications")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'execution_failure',
                severity,
                risk_signal,
                'signal_feature_procurement_contract_suspensions:' || scope_key
            FROM {support_relation("contract_suspensions")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'execution_failure',
                severity,
                risk_signal,
                'signal_feature_procurement_contract_execution_delay:' || scope_key
            FROM {support_relation("contract_execution_delay")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'execution_failure',
                severity,
                risk_signal,
                'signal_feature_procurement_guarantee_advance_execution_chain:' || scope_key
            FROM {support_relation("guarantee_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'payment_budget',
                severity,
                risk_signal,
                'signal_feature_procurement_budget_chain_reconciliation_review_only:'
                    || scope_key
            FROM {support_relation("budget_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'payment_budget',
                severity,
                risk_signal,
                'signal_feature_procurement_invoice_budget_reconciliation_review_only:'
                    || scope_key
            FROM {support_relation("invoice_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'payment_budget',
                severity,
                risk_signal,
                'signal_feature_procurement_payment_plan_reconciliation_review_only:'
                    || scope_key
            FROM {support_relation("payment_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                'procurement_competition',
                severity,
                risk_signal,
                'signal_feature_procurement_single_bidder_high_value:' || scope_key
            FROM {support_relation("single_bidder")}
            WHERE contract_id IS NOT NULL
        ),
        support_ranked AS (
            SELECT
                *,
                CASE
                    WHEN severity = 'critical' THEN 3
                    WHEN severity = 'high' THEN 2
                    WHEN severity = 'medium' THEN 1
                    ELSE 0
                END AS severity_rank,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        CASE
                            WHEN severity = 'critical' THEN 3
                            WHEN severity = 'high' THEN 2
                            WHEN severity = 'medium' THEN 1
                            ELSE 0
                        END DESC,
                        risk_signal DESC NULLS LAST,
                        signal_id
                ) AS support_rank
            FROM support_rows
        ),
        support_rollup AS (
            SELECT
                contract_id,
                count(*) AS support_feature_row_count,
                count(DISTINCT signal_id) AS support_signal_count,
                count(DISTINCT support_family) AS support_family_count,
                list(DISTINCT signal_id ORDER BY signal_id) AS support_signal_ids,
                list(DISTINCT support_family ORDER BY support_family)
                    AS support_signal_families,
                max(severity_rank) AS max_support_severity_rank,
                max(risk_signal) AS max_support_risk_signal,
                sum(CASE WHEN support_family = 'payment_budget' THEN 1 ELSE 0 END)
                    AS payment_budget_support_count,
                sum(CASE WHEN support_family = 'execution_failure' THEN 1 ELSE 0 END)
                    AS execution_support_count,
                sum(CASE WHEN support_family = 'procurement_competition' THEN 1 ELSE 0 END)
                    AS competition_support_count,
                sum(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END)
                    AS critical_support_count,
                list(evidence_ref ORDER BY severity_rank DESC, risk_signal DESC, evidence_ref)
                    FILTER (WHERE support_rank <= 12) AS support_evidence_refs
            FROM support_ranked
            GROUP BY contract_id
        ),
        eligible AS (
            SELECT
                c.*,
                s.support_feature_row_count,
                s.support_signal_count,
                s.support_family_count,
                s.support_signal_ids,
                s.support_signal_families,
                s.max_support_severity_rank,
                s.max_support_risk_signal,
                s.payment_budget_support_count,
                s.execution_support_count,
                s.competition_support_count,
                s.critical_support_count,
                s.support_evidence_refs,
                CASE
                    WHEN c.pae_keyword_flag
                        AND (c.provider_match_flag OR c.health_keyword_flag)
                        THEN 'health_provider_and_pae'
                    WHEN c.pae_keyword_flag THEN 'pae_school_feeding'
                    WHEN c.provider_match_flag AND c.health_keyword_flag
                        THEN 'reps_provider_health_contract'
                    WHEN c.provider_match_flag THEN 'reps_provider_contract'
                    WHEN c.health_keyword_flag THEN 'health_keyword_contract'
                    ELSE 'health_pae_context'
                END AS domain_type
            FROM contract_domain c
            JOIN support_rollup s
                ON s.contract_id = c.contract_id
            WHERE s.support_signal_count >= 2
                AND s.support_family_count >= 2
                AND (
                    c.pae_keyword_flag
                    OR c.health_keyword_flag
                    OR (
                        c.provider_match_flag
                        AND (
                            c.provider_ips_flag
                            OR c.provider_transport_flag
                            OR c.provider_ese_public_flag
                        )
                    )
                )
        )
        SELECT
            'health_pae_service_delivery_gap_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL
                    OR upper(coalesce(provider_doc_type, '')) IN ('NI', 'NIT')
                    THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'health_pae_service_delivery' AS scope_type,
            CASE
                WHEN support_signal_count >= 3 THEN 'critical'
                WHEN payment_budget_support_count > 0 AND execution_support_count > 0
                    THEN 'critical'
                WHEN critical_support_count > 0 AND contract_value >= 1000000000
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.57
                    + least(support_signal_count * 0.04, 0.18)
                    + least(support_family_count * 0.05, 0.15)
                    + CASE WHEN provider_match_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN pae_keyword_flag THEN 0.05 ELSE 0.0 END
                    + CASE WHEN health_keyword_flag THEN 0.03 ELSE 0.0 END
                    + CASE WHEN payment_budget_support_count > 0 THEN 0.04 ELSE 0.0 END
                    + CASE WHEN execution_support_count > 0 THEN 0.04 ELSE 0.0 END
                    + CASE WHEN critical_support_count > 0 THEN 0.03 ELSE 0.0 END
                    + least(log10(greatest(contract_value, 1)) / 140.0, 0.08)
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL
                    OR upper(coalesce(provider_doc_type, '')) IN ('NI', 'NIT')
                    THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            domain_type,
            supplier_name,
            supplier_doc_type,
            provider_match_flag,
            provider_document_key,
            provider_document_id,
            provider_doc_type,
            provider_code,
            provider_name,
            provider_class,
            provider_legal_nature,
            provider_ips_flag,
            provider_transport_flag,
            provider_public_flag,
            provider_ese_public_flag,
            provider_site_row_count,
            provider_code_count,
            provider_site_count,
            provider_departments,
            provider_municipalities,
            reps_cutoff,
            pae_keyword_flag,
            health_keyword_flag,
            buyer_document_id,
            buyer_document_digits,
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
            contract_object,
            process_description,
            enables_advance_payment,
            liquidation,
            contract_value,
            signing_date,
            contract_start_date,
            contract_end_date,
            advance_payment_value,
            invoiced_value,
            pending_payment_value,
            paid_value,
            pending_value,
            pending_execution_value,
            support_feature_row_count,
            support_signal_count,
            support_family_count,
            support_signal_ids,
            support_signal_families,
            max_support_severity_rank,
            max_support_risk_signal,
            payment_budget_support_count,
            execution_support_count,
            competition_support_count,
            critical_support_count,
            (
                'health/PAE service-delivery review combines exact REPS provider '
                || 'identity or SECOP health/PAE object text with independent '
                || 'payment, budget, execution, modification, suspension, guarantee, '
                || 'or competition support signals; it does not prove false services, '
                || 'overbilling, patient/beneficiary mismatch, delivery failure, '
                || 'provider ineligibility, or corrupt intent without REPS service-line, '
                || 'beneficiary, clinical/service, audit, and contract-file validation'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    [
                        coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    ],
                    CASE
                        WHEN provider_match_flag
                            THEN coalesce(provider_evidence_refs, []::VARCHAR[])
                        ELSE []::VARCHAR[]
                    END
                ),
                support_evidence_refs
            ) AS evidence_refs
        FROM eligible
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN support_signal_count >= 3 THEN 1
                    WHEN payment_budget_support_count > 0 AND execution_support_count > 0 THEN 1
                    WHEN critical_support_count > 0 AND contract_value >= 1000000000 THEN 1
                    ELSE 0
                END DESC,
                support_signal_count DESC,
                support_family_count DESC,
                critical_support_count DESC,
                contract_value DESC NULLS LAST,
                contract_id
        ) <= 1000
    """)


def _create_pae_beneficiary_territory_delivery_gap_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "health_providers",
        "pae_indicators",
        "secop_ii_contracts",
        "secop_invoices",
        "secop_payment_plans",
        "secop_cdp_requests",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_guarantees",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_sanctions",
    }
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_pae_beneficiary_territory_delivery_gap AS
        WITH pae_raw AS (
            SELECT
                row_number() OVER () AS pae_source_row_id,
                nullif(coacc_text_key(departamento), '') AS department_key,
                nullif(coacc_text_key(municipio), '') AS municipality_key,
                nullif(trim(codigo_departamento), '') AS department_code,
                nullif(trim(codigo_municipio), '') AS municipality_code,
                nullif(trim(departamento), '') AS pae_department,
                nullif(trim(municipio), '') AS pae_municipality,
                coalesce(
                    try_cast(fecha AS DATE),
                    try_strptime(nullif(trim(cast(fecha AS VARCHAR)), ''), '%d/%m/%Y')::DATE,
                    try_strptime(nullif(trim(cast(fecha AS VARCHAR)), ''), '%Y-%m-%d')::DATE
                ) AS pae_date,
                coalesce(
                    try_cast(fecha_corte AS DATE),
                    try_strptime(
                        nullif(trim(cast(fecha_corte AS VARCHAR)), ''),
                        '%d/%m/%Y'
                    )::DATE,
                    try_strptime(
                        nullif(trim(cast(fecha_corte AS VARCHAR)), ''),
                        '%Y-%m-%d'
                    )::DATE
                ) AS pae_cutoff_date,
                nullif(trim(zona_sede), '') AS zone,
                nullif(trim(jornada), '') AS school_day,
                nullif(trim(grupo_poblacional), '') AS population_group,
                try_cast(
                    replace(cast(cantidad_beneficiarios_pae AS VARCHAR), ',', '.')
                    AS DOUBLE
                ) AS beneficiary_count
            FROM src_pae_indicators
            WHERE nullif(coacc_text_key(departamento), '') IS NOT NULL
                AND nullif(coacc_text_key(municipio), '') IS NOT NULL
        ),
        pae_flagged AS (
            SELECT
                *,
                extract(year FROM pae_date)::INTEGER AS pae_year,
                lower(coalesce(population_group, '')) NOT LIKE 'no pertenece%'
                    AND lower(coalesce(population_group, '')) NOT LIKE 'no aplica%'
                    AND population_group IS NOT NULL AS group_population_flag,
                regexp_matches(
                    lower(coalesce(population_group, '')),
                    'ind[ií]gen|afro|negritud|raizal|\\brom\\b|palenquer|'
                    || 'etnia|[eé]tnia|si perten'
                ) AS vulnerable_or_ethnic_flag,
                lower(coalesce(zone, '')) LIKE '%rural%' AS rural_zone_flag,
                'pae_indicators:' || coalesce(municipality_code, municipality_key)
                    || ':' || cast(extract(year FROM pae_date)::INTEGER AS VARCHAR)
                    || ':' || coalesce(population_group, 'sin_grupo')
                    || ':' || cast(pae_source_row_id AS VARCHAR) AS pae_evidence_ref
            FROM pae_raw
            WHERE pae_date IS NOT NULL
                AND beneficiary_count IS NOT NULL
                AND beneficiary_count > 0
        ),
        pae_rollup AS (
            SELECT
                department_key,
                municipality_key,
                pae_year,
                any_value(department_code) AS department_code,
                any_value(municipality_code) AS municipality_code,
                min(pae_department) FILTER (WHERE pae_department IS NOT NULL)
                    AS pae_department,
                min(pae_municipality) FILTER (WHERE pae_municipality IS NOT NULL)
                    AS pae_municipality,
                max(pae_cutoff_date) AS pae_cutoff_date,
                count(*) AS pae_row_count,
                sum(beneficiary_count) AS pae_beneficiary_count,
                sum(
                    CASE WHEN group_population_flag THEN beneficiary_count ELSE 0 END
                ) AS pae_group_beneficiary_count,
                sum(
                    CASE WHEN vulnerable_or_ethnic_flag THEN beneficiary_count ELSE 0 END
                ) AS pae_vulnerable_beneficiary_count,
                sum(
                    CASE WHEN rural_zone_flag THEN beneficiary_count ELSE 0 END
                ) AS pae_rural_beneficiary_count,
                count(DISTINCT population_group)
                    FILTER (WHERE population_group IS NOT NULL)
                    AS pae_population_group_count,
                list(DISTINCT population_group ORDER BY population_group)
                    FILTER (WHERE population_group IS NOT NULL)
                    AS pae_population_groups,
                list(DISTINCT zone ORDER BY zone)
                    FILTER (WHERE zone IS NOT NULL) AS pae_zones,
                list(DISTINCT school_day ORDER BY school_day)
                    FILTER (WHERE school_day IS NOT NULL) AS pae_school_days
            FROM pae_flagged
            WHERE pae_year IS NOT NULL
            GROUP BY department_key, municipality_key, pae_year
        ),
        pae_evidence_ranked AS (
            SELECT
                department_key,
                municipality_key,
                pae_year,
                pae_evidence_ref,
                row_number() OVER (
                    PARTITION BY department_key, municipality_key, pae_year
                    ORDER BY
                        group_population_flag DESC,
                        vulnerable_or_ethnic_flag DESC,
                        beneficiary_count DESC NULLS LAST,
                        pae_evidence_ref
                ) AS evidence_rank
            FROM pae_flagged
            WHERE pae_year IS NOT NULL
                AND pae_evidence_ref IS NOT NULL
        ),
        pae_evidence AS (
            SELECT
                department_key,
                municipality_key,
                pae_year,
                list(pae_evidence_ref ORDER BY evidence_rank) AS pae_evidence_refs
            FROM pae_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY department_key, municipality_key, pae_year
        ),
        health_pae AS (
            SELECT
                *,
                nullif(coacc_text_key(department), '') AS department_key,
                nullif(coacc_text_key(city), '') AS municipality_key,
                extract(year FROM signing_date)::INTEGER AS contract_year
            FROM curated_health_pae_service_delivery_gap
            WHERE pae_keyword_flag
                AND city IS NOT NULL
                AND nullif(coacc_text_key(city), '') IS NOT NULL
                AND nullif(coacc_text_key(city), '') <> coacc_text_key('No Definido')
        ),
        candidates AS (
            SELECT
                h.*,
                p.department_code,
                p.municipality_code,
                p.pae_department,
                p.pae_municipality,
                p.pae_year,
                p.pae_cutoff_date,
                p.pae_row_count,
                p.pae_beneficiary_count,
                p.pae_group_beneficiary_count,
                p.pae_vulnerable_beneficiary_count,
                p.pae_rural_beneficiary_count,
                p.pae_population_group_count,
                p.pae_population_groups,
                p.pae_zones,
                p.pae_school_days,
                e.pae_evidence_refs,
                h.contract_year IS NOT NULL
                    AND p.pae_year = h.contract_year AS pae_same_year_flag,
                CASE
                    WHEN h.contract_year IS NULL THEN NULL
                    ELSE h.contract_year - p.pae_year
                END AS pae_year_lag
            FROM health_pae h
            JOIN pae_rollup p
                ON p.department_key = h.department_key
                AND p.municipality_key = h.municipality_key
            LEFT JOIN pae_evidence e
                ON e.department_key = p.department_key
                AND e.municipality_key = p.municipality_key
                AND e.pae_year = p.pae_year
            WHERE h.contract_year IS NULL
                OR p.pae_year <= h.contract_year
            QUALIFY row_number() OVER (
                PARTITION BY h.entity_key, h.contract_id
                ORDER BY
                    CASE
                        WHEN h.contract_year IS NOT NULL
                            AND p.pae_year = h.contract_year THEN 0
                        ELSE 1
                    END,
                    p.pae_year DESC,
                    p.pae_beneficiary_count DESC NULLS LAST
            ) = 1
        )
        SELECT
            'pae_beneficiary_territory_delivery_gap_review_only' AS signal_id,
            entity_id,
            entity_key,
            entity_label,
            'pae_beneficiary_territory:' || contract_id AS scope_key,
            'pae_beneficiary_territory_delivery_gap' AS scope_type,
            CASE
                WHEN severity = 'critical'
                    AND (
                        pae_group_beneficiary_count > 0
                        OR pae_vulnerable_beneficiary_count > 0
                        OR pae_beneficiary_count >= 10000
                    )
                    THEN 'critical'
                WHEN support_family_count >= 2 AND contract_value >= 1000000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                0.99,
                coalesce(risk_signal, 0.65)
                    + CASE WHEN pae_same_year_flag THEN 0.04 ELSE 0.02 END
                    + CASE WHEN pae_group_beneficiary_count > 0 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN pae_vulnerable_beneficiary_count > 0 THEN 0.02 ELSE 0.0 END
                    + CASE WHEN pae_rural_beneficiary_count > 0 THEN 0.02 ELSE 0.0 END
                    + least(log10(greatest(pae_beneficiary_count, 1)) / 150.0, 0.05)
            ) AS risk_signal,
            identity_confidence,
            identity_match_type,
            identity_quality,
            severity AS base_health_pae_severity,
            risk_signal AS base_health_pae_risk_signal,
            domain_type,
            supplier_name,
            supplier_doc_type,
            provider_match_flag,
            provider_document_key,
            provider_document_id,
            provider_doc_type,
            provider_code,
            provider_name,
            provider_class,
            provider_legal_nature,
            provider_ips_flag,
            provider_transport_flag,
            provider_public_flag,
            provider_ese_public_flag,
            provider_site_row_count,
            provider_code_count,
            provider_site_count,
            provider_departments,
            provider_municipalities,
            reps_cutoff,
            pae_keyword_flag,
            health_keyword_flag,
            buyer_document_id,
            buyer_document_digits,
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
            contract_object,
            process_description,
            enables_advance_payment,
            liquidation,
            contract_value,
            signing_date,
            contract_year,
            contract_start_date,
            contract_end_date,
            advance_payment_value,
            invoiced_value,
            pending_payment_value,
            paid_value,
            pending_value,
            pending_execution_value,
            department_code,
            municipality_code,
            pae_department,
            pae_municipality,
            pae_year,
            pae_same_year_flag,
            pae_year_lag,
            pae_cutoff_date,
            pae_row_count,
            pae_beneficiary_count,
            pae_group_beneficiary_count,
            pae_vulnerable_beneficiary_count,
            pae_rural_beneficiary_count,
            pae_population_group_count,
            pae_population_groups,
            pae_zones,
            pae_school_days,
            support_feature_row_count,
            support_signal_count,
            support_family_count,
            support_signal_ids,
            support_signal_families,
            max_support_severity_rank,
            max_support_risk_signal,
            payment_budget_support_count,
            execution_support_count,
            competition_support_count,
            critical_support_count,
            (
                'PAE beneficiary-territory review combines MEN municipal beneficiary '
                || 'counts with an exact supplier in an existing SECOP health/PAE '
                || 'service-delivery risk queue; it does not prove beneficiary '
                || 'mismatch, non-delivery, ration quality failure, overbilling, '
                || 'service failure, legal breach, corrupt intent, or a per-contract '
                || 'beneficiary obligation without MEN microdata and contract-file review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    [
                        'signal_feature_health_pae_service_delivery_gap_review_only:'
                            || contract_id
                    ],
                    coalesce(evidence_refs, []::VARCHAR[])
                ),
                coalesce(pae_evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN severity = 'critical'
                        AND (
                            pae_group_beneficiary_count > 0
                            OR pae_vulnerable_beneficiary_count > 0
                            OR pae_beneficiary_count >= 10000
                        )
                        THEN 1
                    ELSE 0
                END DESC,
                pae_same_year_flag DESC,
                support_signal_count DESC,
                support_family_count DESC,
                pae_beneficiary_count DESC NULLS LAST,
                contract_value DESC NULLS LAST,
                contract_id
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


def _create_contract_modification_ladder_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_ii_contracts",
        "secop_contract_modifications",
        "secop_contract_suspensions",
        "secop_contract_execution",
    }
    if not (required <= set(required_sources)):
        return

    contract_support_sources = {
        "large_modifications": (
            "signal_feature_procurement_large_modifications",
            "curated_large_modifications",
            {"secop_ii_contracts", "secop_contract_modifications"},
        ),
        "contract_suspensions": (
            "signal_feature_procurement_contract_suspensions",
            "curated_contract_suspensions",
            {"secop_ii_contracts", "secop_contract_suspensions"},
        ),
        "contract_execution_delay": (
            "signal_feature_procurement_contract_execution_delay",
            "curated_contract_execution_delay",
            {"secop_ii_contracts", "secop_contract_execution"},
        ),
        "guarantee_chain": (
            "signal_feature_procurement_guarantee_advance_execution_chain",
            "curated_guarantee_advance_execution_chain",
            {
                "secop_ii_contracts",
                "secop_guarantees",
                "secop_contract_execution",
                "secop_contract_suspensions",
                "secop_contract_modifications",
                "secop_sanctions",
            },
        ),
        "budget_chain": (
            "signal_feature_procurement_budget_chain_reconciliation_review_only",
            "curated_budget_chain_reconciliation",
            {
                "secop_ii_contracts",
                "secop_cdp_requests",
                "secop_budget_commitments",
                "secop_budget_items",
                "secop_guarantees",
                "secop_contract_execution",
                "secop_contract_suspensions",
                "secop_contract_modifications",
                "secop_sanctions",
            },
        ),
        "invoice_chain": (
            "signal_feature_procurement_invoice_budget_reconciliation_review_only",
            "curated_invoice_budget_reconciliation",
            {
                "secop_ii_contracts",
                "secop_invoices",
                "secop_cdp_requests",
                "secop_budget_commitments",
                "secop_budget_items",
                "secop_guarantees",
                "secop_contract_execution",
                "secop_contract_suspensions",
                "secop_contract_modifications",
                "secop_sanctions",
            },
        ),
        "payment_chain": (
            "signal_feature_procurement_payment_plan_reconciliation_review_only",
            "curated_payment_plan_reconciliation",
            {
                "secop_ii_contracts",
                "secop_payment_plans",
                "secop_invoices",
                "secop_cdp_requests",
                "secop_budget_commitments",
                "secop_budget_items",
                "secop_guarantees",
                "secop_contract_execution",
                "secop_contract_suspensions",
                "secop_contract_modifications",
                "secop_sanctions",
            },
        ),
        "single_bidder": (
            "signal_feature_procurement_single_bidder_high_value",
            "curated_single_bidder_high_value",
            {"secop_ii_contracts", "secop_ii_processes", "secop_offers"},
        ),
    }

    def contract_support_relation(key: str) -> str:
        table, view_name, source_set = contract_support_sources[key]
        table_path = curated_path(table)
        if any(table_path.glob("*.parquet")):
            return f"read_parquet({_sql_string(str(table_path / '*.parquet'))})"
        if source_set <= set(required_sources):
            return view_name
        empty_view = f"empty_contract_modification_ladder_{key}"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW {empty_view} AS
            SELECT
                NULL::VARCHAR AS contract_id,
                NULL::VARCHAR AS signal_id,
                NULL::VARCHAR AS severity,
                NULL::DOUBLE AS risk_signal,
                NULL::VARCHAR AS scope_key
            WHERE false
        """)
        return empty_view

    def role_support_relation() -> str:
        table = "signal_feature_procurement_role_supplier_same_buyer_review_only"
        view_name = "curated_role_supplier_same_buyer"
        table_path = curated_path(table)
        if any(table_path.glob("*.parquet")):
            return f"read_parquet({_sql_string(str(table_path / '*.parquet'))})"
        if "secop_ii_contracts" in set(required_sources):
            return view_name
        empty_view = "empty_contract_modification_ladder_role_supplier"
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW {empty_view} AS
            SELECT
                NULL::VARCHAR AS entity_key,
                NULL::VARCHAR AS buyer_document_id,
                NULL::VARCHAR AS signal_id,
                NULL::VARCHAR AS severity,
                NULL::DOUBLE AS risk_signal,
                NULL::VARCHAR AS scope_key
            WHERE false
        """)
        return empty_view

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW curated_contract_modification_ladder AS
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
                lower(
                    coalesce(proposito_modificacion, '') || ' ' ||
                    coalesce(descripcion, '')
                ) AS modification_text,
                coalesce(
                    try_cast(fecha_de_aprobacion AS TIMESTAMP),
                    try_cast(fecha_version AS TIMESTAMP),
                    try_cast(fecha_de_carga AS TIMESTAMP),
                    try_cast(fecha_creacion AS TIMESTAMP)
                ) AS modification_at
            FROM src_secop_contract_modifications
            WHERE nullif(trim(id_contrato), '') IS NOT NULL
        ),
        modification_rollup AS (
            SELECT
                contract_id,
                count(*) AS modification_event_count,
                count(*) FILTER (WHERE coalesce(modification_value, 0.0) > 0)
                    AS value_modification_event_count,
                sum(coalesce(modification_value, 0.0)) AS total_modification_value,
                max(coalesce(modification_value, 0.0)) AS max_modification_value,
                sum(coalesce(extended_days, 0)) AS total_extended_days,
                max(coalesce(extended_days, 0)) AS max_extended_days,
                min(modification_at) AS first_modification_at,
                max(modification_at) AS last_modification_at,
                date_diff('day', min(modification_at), max(modification_at))
                    AS modification_sequence_span_days,
                count(*) FILTER (
                    WHERE regexp_matches(
                        modification_text,
                        'forma de pago|\\bpago\\b|desembolso|anticipo|amortiza|precio|valor|presupuesto'
                    )
                ) AS financial_terms_event_count,
                count(*) FILTER (
                    WHERE regexp_matches(
                        modification_text,
                        'adici|adici[oó]n|obra|alcance|objeto|actividad|producto|servicio|[íi]tem|item|cantid|mayor cantidad'
                    )
                ) AS scope_change_event_count,
                count(*) FILTER (
                    WHERE regexp_matches(
                        modification_text,
                        'plazo|pr[oó]rroga|tiempo|d[ií]as|fecha.*fin|ampli'
                    )
                ) AS term_extension_event_count,
                list(DISTINCT modification_purpose ORDER BY modification_purpose)
                    FILTER (WHERE modification_purpose IS NOT NULL)
                    AS modification_purpose_sample
            FROM raw_modifications
            GROUP BY contract_id
        ),
        modification_evidence_ranked AS (
            SELECT
                contract_id,
                'secop_contract_modifications:' || modification_id AS evidence_ref,
                modification_value,
                extended_days,
                modification_at,
                modification_id,
                row_number() OVER (
                    PARTITION BY contract_id
                    ORDER BY
                        coalesce(modification_value, 0.0) DESC,
                        coalesce(extended_days, 0) DESC,
                        modification_at DESC NULLS LAST,
                        modification_id
                ) AS evidence_rank
            FROM raw_modifications
        ),
        modification_evidence AS (
            SELECT
                contract_id,
                list(evidence_ref ORDER BY evidence_rank) AS modification_evidence_refs
            FROM modification_evidence_ranked
            WHERE evidence_rank <= 6
            GROUP BY contract_id
        ),
        contract_base AS (
            SELECT
                a.*
            FROM curated_contract_awards a
            WHERE a.contract_id IS NOT NULL
                AND a.supplier_document_key IS NOT NULL
                AND NOT regexp_matches(a.supplier_document_key, '^0+$')
                AND a.contract_value IS NOT NULL
                AND a.contract_value >= 500000000
            QUALIFY row_number() OVER (
                PARTITION BY a.contract_id
                ORDER BY a.contract_value DESC NULLS LAST,
                    a.signing_date DESC NULLS LAST,
                    a.process_url DESC NULLS LAST
            ) = 1
        ),
        contract_support_rows AS (
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'medium') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_large_modifications:' || scope_key
                    AS evidence_ref
            FROM {contract_support_relation("large_modifications")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'medium') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_contract_suspensions:' || scope_key
            FROM {contract_support_relation("contract_suspensions")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'low') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_contract_execution_delay:' || scope_key
            FROM {contract_support_relation("contract_execution_delay")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'high') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_guarantee_advance_execution_chain:' || scope_key
            FROM {contract_support_relation("guarantee_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'high') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_budget_chain_reconciliation_review_only:'
                    || scope_key
            FROM {contract_support_relation("budget_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'high') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_invoice_budget_reconciliation_review_only:'
                    || scope_key
            FROM {contract_support_relation("invoice_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'high') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_payment_plan_reconciliation_review_only:'
                    || scope_key
            FROM {contract_support_relation("payment_chain")}
            WHERE contract_id IS NOT NULL
            UNION ALL
            SELECT
                contract_id,
                signal_id,
                coalesce(severity, 'medium') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_single_bidder_high_value:' || scope_key
            FROM {contract_support_relation("single_bidder")}
            WHERE contract_id IS NOT NULL
        ),
        contract_support_ranked AS (
            SELECT
                *,
                CASE
                    WHEN severity = 'critical' THEN 3
                    WHEN severity = 'high' THEN 2
                    WHEN severity = 'medium' THEN 1
                    ELSE 0
                END AS severity_rank,
                row_number() OVER (
                    PARTITION BY contract_id, signal_id
                    ORDER BY
                        CASE
                            WHEN severity = 'critical' THEN 3
                            WHEN severity = 'high' THEN 2
                            WHEN severity = 'medium' THEN 1
                            ELSE 0
                        END DESC,
                        risk_signal DESC NULLS LAST,
                        evidence_ref
                ) AS signal_rank
            FROM contract_support_rows
        ),
        contract_support AS (
            SELECT
                contract_id,
                count(DISTINCT signal_id) AS contract_support_signal_count,
                list(DISTINCT signal_id ORDER BY signal_id)
                    FILTER (WHERE signal_id IS NOT NULL)
                    AS contract_support_signal_ids,
                max(severity_rank) AS max_contract_support_severity_rank,
                bool_or(signal_id = 'procurement_large_modifications')
                    AS large_modification_signal_flag,
                bool_or(signal_id = 'procurement_contract_suspensions')
                    AS suspension_signal_flag,
                bool_or(signal_id = 'procurement_contract_execution_delay')
                    AS execution_delay_signal_flag,
                bool_or(signal_id = 'procurement_guarantee_advance_execution_chain')
                    AS guarantee_chain_signal_flag,
                bool_or(signal_id = 'procurement_budget_chain_reconciliation_review_only')
                    AS budget_chain_signal_flag,
                bool_or(signal_id = 'procurement_invoice_budget_reconciliation_review_only')
                    AS invoice_chain_signal_flag,
                bool_or(signal_id = 'procurement_payment_plan_reconciliation_review_only')
                    AS payment_chain_signal_flag,
                bool_or(signal_id = 'procurement_single_bidder_high_value')
                    AS single_bidder_signal_flag,
                list(evidence_ref ORDER BY severity_rank DESC, risk_signal DESC NULLS LAST, evidence_ref)
                    FILTER (WHERE signal_rank = 1)
                    AS contract_support_evidence_refs
            FROM contract_support_ranked
            GROUP BY contract_id
        ),
        role_support_rows AS (
            SELECT
                entity_key,
                buyer_document_id,
                signal_id,
                coalesce(severity, 'high') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                'signal_feature_procurement_role_supplier_same_buyer_review_only:'
                    || scope_key AS evidence_ref
            FROM {role_support_relation()}
            WHERE entity_key IS NOT NULL
                AND buyer_document_id IS NOT NULL
        ),
        role_support_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY entity_key, buyer_document_id, signal_id
                    ORDER BY risk_signal DESC NULLS LAST, evidence_ref
                ) AS signal_rank
            FROM role_support_rows
        ),
        role_support AS (
            SELECT
                entity_key,
                buyer_document_id,
                count(DISTINCT signal_id) AS role_support_signal_count,
                list(DISTINCT signal_id ORDER BY signal_id)
                    FILTER (WHERE signal_id IS NOT NULL)
                    AS role_support_signal_ids,
                list(evidence_ref ORDER BY risk_signal DESC NULLS LAST, evidence_ref)
                    FILTER (WHERE signal_rank = 1)
                    AS role_support_evidence_refs
            FROM role_support_ranked
            GROUP BY entity_key, buyer_document_id
        ),
        scored AS (
            SELECT
                b.supplier_entity_id,
                b.supplier_document_key,
                b.supplier_nit_canonical,
                b.supplier_name,
                b.supplier_doc_type,
                b.buyer_document_id,
                b.buyer_document_digits,
                b.buyer_name,
                b.department,
                b.city,
                b.sector,
                b.procurement_modality,
                b.contract_type,
                b.contract_id,
                b.contract_reference,
                b.process_id,
                b.process_url,
                b.contract_value,
                b.signing_date,
                b.contract_start_date,
                b.contract_end_date,
                r.modification_event_count,
                r.value_modification_event_count,
                r.total_modification_value,
                r.max_modification_value,
                r.total_extended_days,
                r.max_extended_days,
                r.first_modification_at,
                r.last_modification_at,
                r.modification_sequence_span_days,
                r.financial_terms_event_count,
                r.scope_change_event_count,
                r.term_extension_event_count,
                r.modification_purpose_sample,
                r.total_modification_value / nullif(b.contract_value, 0)
                    AS modification_value_share,
                (
                    r.total_modification_value >= 500000000
                    OR r.total_modification_value / nullif(b.contract_value, 0) >= 0.20
                ) AS material_value_flag,
                (
                    r.total_extended_days >= 180
                    OR r.max_extended_days >= 90
                ) AS major_delay_flag,
                r.financial_terms_event_count > 0 AS financial_terms_flag,
                r.scope_change_event_count > 0 AS scope_change_flag,
                (
                    regexp_matches(lower(coalesce(b.procurement_modality, '')), 'direct|especial')
                    OR regexp_matches(lower(coalesce(b.contract_type, '')), 'concesi|obra|cr[eé]dito')
                ) AS modality_context_flag,
                coalesce(c.contract_support_signal_count, 0)
                    AS contract_support_signal_count,
                coalesce(rs.role_support_signal_count, 0) AS role_support_signal_count,
                coalesce(c.large_modification_signal_flag, false)
                    AS large_modification_signal_flag,
                coalesce(c.suspension_signal_flag, false) AS suspension_signal_flag,
                coalesce(c.execution_delay_signal_flag, false)
                    AS execution_delay_signal_flag,
                coalesce(c.guarantee_chain_signal_flag, false)
                    AS guarantee_chain_signal_flag,
                coalesce(c.budget_chain_signal_flag, false)
                    AS budget_chain_signal_flag,
                coalesce(c.invoice_chain_signal_flag, false)
                    AS invoice_chain_signal_flag,
                coalesce(c.payment_chain_signal_flag, false)
                    AS payment_chain_signal_flag,
                coalesce(c.single_bidder_signal_flag, false)
                    AS single_bidder_signal_flag,
                coalesce(rs.role_support_signal_count, 0) > 0
                    AS same_buyer_role_signal_flag,
                coalesce(c.contract_support_signal_ids, []::VARCHAR[])
                    AS contract_support_signal_ids,
                coalesce(rs.role_support_signal_ids, []::VARCHAR[])
                    AS role_support_signal_ids,
                me.modification_evidence_refs,
                c.contract_support_evidence_refs,
                rs.role_support_evidence_refs
            FROM contract_base b
            JOIN modification_rollup r
                ON r.contract_id = b.contract_id
            JOIN modification_evidence me
                ON me.contract_id = b.contract_id
            LEFT JOIN contract_support c
                ON c.contract_id = b.contract_id
            LEFT JOIN role_support rs
                ON rs.entity_key = b.supplier_document_key
                AND rs.buyer_document_id = b.buyer_document_id
        ),
        eligible AS (
            SELECT
                *,
                contract_support_signal_count + role_support_signal_count
                    AS support_signal_count,
                cast(material_value_flag AS INTEGER)
                    + cast(major_delay_flag AS INTEGER)
                    + cast(financial_terms_flag AS INTEGER)
                    + cast(scope_change_flag AS INTEGER)
                    + cast(modality_context_flag AS INTEGER)
                    + cast(large_modification_signal_flag AS INTEGER)
                    + cast(suspension_signal_flag AS INTEGER)
                    + cast(execution_delay_signal_flag AS INTEGER)
                    + cast(guarantee_chain_signal_flag AS INTEGER)
                    + cast(budget_chain_signal_flag AS INTEGER)
                    + cast(invoice_chain_signal_flag AS INTEGER)
                    + cast(payment_chain_signal_flag AS INTEGER)
                    + cast(single_bidder_signal_flag AS INTEGER)
                    + cast(same_buyer_role_signal_flag AS INTEGER)
                    AS chain_flag_count
            FROM scored
            WHERE modification_event_count >= 3
                AND (
                    material_value_flag
                    OR major_delay_flag
                    OR financial_terms_flag
                    OR scope_change_flag
                )
        ),
        final_candidates AS (
            SELECT *
            FROM eligible
            WHERE (
                    chain_flag_count >= 3
                    AND support_signal_count >= 1
                )
                OR (
                    chain_flag_count >= 4
                    AND (material_value_flag OR major_delay_flag)
                )
                OR (
                    modification_event_count >= 10
                    AND material_value_flag
                )
        )
        SELECT
            'procurement_contract_modification_ladder_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'doc:' || supplier_document_key) AS entity_id,
            supplier_document_key AS entity_key,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'Company'
                ELSE 'Person'
            END AS entity_label,
            contract_id AS scope_key,
            'contract_modification_ladder' AS scope_type,
            CASE
                WHEN chain_flag_count >= 5
                    OR (
                        material_value_flag
                        AND major_delay_flag
                        AND support_signal_count >= 2
                    )
                    OR (
                        modification_event_count >= 10
                        AND support_signal_count >= 1
                    )
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.99,
                0.58
                    + least(modification_event_count / 50.0, 0.12)
                    + least(coalesce(modification_value_share, 0.0) / 4.0, 0.12)
                    + least(coalesce(total_extended_days, 0) / 3000.0, 0.10)
                    + least(chain_flag_count * 0.035, 0.21)
                    + least(support_signal_count * 0.025, 0.10)
                    + CASE WHEN material_value_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN major_delay_flag THEN 0.04 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            CASE
                WHEN supplier_nit_canonical IS NOT NULL THEN 'EXACT_COMPANY_NIT'
                ELSE 'EXACT_PERSON_DOCUMENT'
            END AS identity_match_type,
            'exact' AS identity_quality,
            supplier_name,
            supplier_doc_type,
            buyer_document_id,
            buyer_document_digits,
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
            value_modification_event_count,
            total_modification_value,
            max_modification_value,
            modification_value_share,
            total_extended_days,
            max_extended_days,
            first_modification_at,
            last_modification_at,
            modification_sequence_span_days,
            financial_terms_event_count,
            scope_change_event_count,
            term_extension_event_count,
            modification_purpose_sample,
            material_value_flag,
            major_delay_flag,
            financial_terms_flag,
            scope_change_flag,
            modality_context_flag,
            large_modification_signal_flag,
            suspension_signal_flag,
            execution_delay_signal_flag,
            guarantee_chain_signal_flag,
            budget_chain_signal_flag,
            invoice_chain_signal_flag,
            payment_chain_signal_flag,
            single_bidder_signal_flag,
            same_buyer_role_signal_flag,
            support_signal_count,
            chain_flag_count,
            list_distinct(
                list_concat(contract_support_signal_ids, role_support_signal_ids)
            ) AS support_signal_ids,
            (
                'repeated SECOP modification sequence with value, delay, payment-term, '
                || 'scope, suspension, execution, budget, invoice, payment, competition, '
                || 'or same-buyer role support; this does not prove the modifications '
                || 'were illegal, avoided competition unlawfully, caused fiscal harm, '
                || 'or involved corrupt intent without source-file and legal review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    [
                        coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    ],
                    coalesce(modification_evidence_refs, []::VARCHAR[])
                ),
                list_concat(
                    coalesce(contract_support_evidence_refs, []::VARCHAR[]),
                    coalesce(role_support_evidence_refs, []::VARCHAR[])
                )
            ) AS evidence_refs
        FROM final_candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE WHEN severity = 'critical' THEN 1 ELSE 0 END DESC,
                chain_flag_count DESC,
                support_signal_count DESC,
                total_modification_value DESC NULLS LAST,
                total_extended_days DESC NULLS LAST,
                contract_value DESC NULLS LAST,
                contract_id
        ) <= 1000
    """)


def _create_role_supplier_same_buyer_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    if "secop_ii_contracts" not in set(required_sources):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW curated_role_supplier_same_buyer AS
        WITH raw_roles AS (
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                coacc_reference_url(process_url) AS process_url,
                coacc_doc_digits(entity_nit) AS buyer_document_digits,
                nullif(trim(entity_nit), '') AS buyer_document_id,
                nullif(trim(entity_name), '') AS buyer_name,
                coacc_money(contract_value) AS contract_value,
                try_cast(signing_date AS DATE) AS signing_date,
                try_cast(contract_start_date AS DATE) AS contract_start_date,
                try_cast(contract_end_date AS DATE) AS contract_end_date,
                'supervisor_doc_number' AS role_field,
                nullif(trim(supervisor_name), '') AS role_name,
                nullif(trim(supervisor_doc_type), '') AS role_doc_type,
                coacc_doc_digits(supervisor_doc_number) AS role_document_digits
            FROM src_secop_ii_contracts
            WHERE nullif(trim(supervisor_doc_number), '') IS NOT NULL
            UNION ALL
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                coacc_reference_url(process_url) AS process_url,
                coacc_doc_digits(entity_nit) AS buyer_document_digits,
                nullif(trim(entity_nit), '') AS buyer_document_id,
                nullif(trim(entity_name), '') AS buyer_name,
                coacc_money(contract_value) AS contract_value,
                try_cast(signing_date AS DATE) AS signing_date,
                try_cast(contract_start_date AS DATE) AS contract_start_date,
                try_cast(contract_end_date AS DATE) AS contract_end_date,
                'spending_orderer_doc_number' AS role_field,
                nullif(trim(spending_orderer_name), '') AS role_name,
                nullif(trim(spending_orderer_doc_type), '') AS role_doc_type,
                coacc_doc_digits(spending_orderer_doc_number) AS role_document_digits
            FROM src_secop_ii_contracts
            WHERE nullif(trim(spending_orderer_doc_number), '') IS NOT NULL
            UNION ALL
            SELECT
                nullif(trim(contract_id), '') AS contract_id,
                coacc_reference_url(process_url) AS process_url,
                coacc_doc_digits(entity_nit) AS buyer_document_digits,
                nullif(trim(entity_nit), '') AS buyer_document_id,
                nullif(trim(entity_name), '') AS buyer_name,
                coacc_money(contract_value) AS contract_value,
                try_cast(signing_date AS DATE) AS signing_date,
                try_cast(contract_start_date AS DATE) AS contract_start_date,
                try_cast(contract_end_date AS DATE) AS contract_end_date,
                'payment_orderer_doc_number' AS role_field,
                nullif(trim(payment_orderer_name), '') AS role_name,
                nullif(trim(payment_orderer_doc_type), '') AS role_doc_type,
                coacc_doc_digits(payment_orderer_doc_number) AS role_document_digits
            FROM src_secop_ii_contracts
            WHERE nullif(trim(payment_orderer_doc_number), '') IS NOT NULL
        ),
        buyer_documents AS (
            SELECT DISTINCT buyer_document_digits AS document_digits
            FROM curated_contract_awards
            WHERE buyer_document_digits IS NOT NULL
        ),
        valid_roles AS (
            SELECT r.*
            FROM raw_roles r
            LEFT JOIN buyer_documents b
                ON b.document_digits = r.role_document_digits
            WHERE r.contract_id IS NOT NULL
                AND r.buyer_document_digits IS NOT NULL
                AND r.role_document_digits IS NOT NULL
                AND length(r.role_document_digits) BETWEEN 6 AND 10
                AND NOT regexp_matches(r.role_document_digits, '^0+$')
                AND r.role_document_digits <> r.buyer_document_digits
                AND b.document_digits IS NULL
        ),
        supplier_awards AS (
            SELECT
                contract_id,
                coalesce(process_url, 'secop_ii_contracts:' || contract_id)
                    AS evidence_ref,
                supplier_document_digits,
                supplier_document_key,
                supplier_entity_id,
                supplier_name,
                supplier_doc_type,
                buyer_document_digits,
                buyer_document_id,
                buyer_name,
                department,
                city,
                sector,
                procurement_modality,
                contract_type,
                contract_value,
                signing_date,
                contract_start_date,
                contract_end_date
            FROM curated_contract_awards
            WHERE supplier_document_digits IS NOT NULL
                AND length(supplier_document_digits) BETWEEN 6 AND 10
                AND NOT regexp_matches(supplier_document_digits, '^0+$')
                AND buyer_document_digits IS NOT NULL
                AND contract_id IS NOT NULL
                AND contract_value IS NOT NULL
                AND contract_value > 0
                AND upper(coalesce(supplier_doc_type, '')) NOT IN ('NIT', 'RUT')
        ),
        pair_keys AS (
            SELECT DISTINCT
                v.role_document_digits,
                v.buyer_document_digits,
                v.role_field
            FROM valid_roles v
            JOIN supplier_awards s
                ON s.supplier_document_digits = v.role_document_digits
                AND s.buyer_document_digits = v.buyer_document_digits
                AND s.contract_id <> v.contract_id
        ),
        role_summary AS (
            SELECT
                v.role_document_digits,
                v.buyer_document_digits,
                v.role_field,
                any_value(v.role_name) AS role_name,
                any_value(v.role_doc_type) AS role_doc_type,
                any_value(v.buyer_document_id) AS buyer_document_id,
                any_value(v.buyer_name) AS buyer_name,
                count(DISTINCT v.contract_id) AS role_contract_count,
                min(v.signing_date) AS first_role_signing_date,
                max(v.signing_date) AS last_role_signing_date,
                max(v.contract_value) AS max_role_contract_value
            FROM valid_roles v
            JOIN pair_keys p
                ON p.role_document_digits = v.role_document_digits
                AND p.buyer_document_digits = v.buyer_document_digits
                AND p.role_field = v.role_field
            GROUP BY
                v.role_document_digits,
                v.buyer_document_digits,
                v.role_field
        ),
        supplier_summary AS (
            SELECT
                p.role_document_digits,
                p.buyer_document_digits,
                p.role_field,
                any_value(s.supplier_entity_id) AS supplier_entity_id,
                any_value(s.supplier_document_key) AS supplier_document_key,
                any_value(s.supplier_name) AS supplier_name,
                any_value(s.supplier_doc_type) AS supplier_doc_type,
                any_value(s.buyer_document_id) AS buyer_document_id,
                any_value(s.buyer_name) AS buyer_name,
                any_value(s.department) AS department,
                any_value(s.city) AS city,
                any_value(s.sector) AS sector,
                any_value(s.procurement_modality) AS procurement_modality,
                any_value(s.contract_type) AS contract_type,
                count(DISTINCT s.contract_id) AS supplier_contract_count,
                sum(s.contract_value) AS supplier_total_contract_value,
                max(s.contract_value) AS max_supplier_contract_value,
                min(s.signing_date) AS first_supplier_signing_date,
                max(s.signing_date) AS last_supplier_signing_date
            FROM pair_keys p
            JOIN supplier_awards s
                ON s.supplier_document_digits = p.role_document_digits
                AND s.buyer_document_digits = p.buyer_document_digits
            GROUP BY
                p.role_document_digits,
                p.buyer_document_digits,
                p.role_field
        ),
        role_evidence_ranked AS (
            SELECT
                v.role_document_digits,
                v.buyer_document_digits,
                v.role_field,
                'secop_ii_contracts:role:' || v.contract_id || ':' || v.role_field
                    AS evidence_ref,
                v.contract_value,
                v.signing_date,
                v.contract_id,
                row_number() OVER (
                    PARTITION BY
                        v.role_document_digits,
                        v.buyer_document_digits,
                        v.role_field
                    ORDER BY
                        v.contract_value DESC NULLS LAST,
                        v.signing_date DESC NULLS LAST,
                        v.contract_id
                ) AS evidence_rank
            FROM valid_roles v
            JOIN pair_keys p
                ON p.role_document_digits = v.role_document_digits
                AND p.buyer_document_digits = v.buyer_document_digits
                AND p.role_field = v.role_field
        ),
        role_evidence AS (
            SELECT
                role_document_digits,
                buyer_document_digits,
                role_field,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS role_evidence_refs
            FROM role_evidence_ranked
            WHERE evidence_rank <= 3
            GROUP BY role_document_digits, buyer_document_digits, role_field
        ),
        supplier_evidence_ranked AS (
            SELECT
                p.role_document_digits,
                p.buyer_document_digits,
                p.role_field,
                s.evidence_ref,
                s.contract_value,
                s.signing_date,
                s.contract_id,
                row_number() OVER (
                    PARTITION BY
                        p.role_document_digits,
                        p.buyer_document_digits,
                        p.role_field
                    ORDER BY
                        s.contract_value DESC NULLS LAST,
                        s.signing_date DESC NULLS LAST,
                        s.contract_id
                ) AS evidence_rank
            FROM pair_keys p
            JOIN supplier_awards s
                ON s.supplier_document_digits = p.role_document_digits
                AND s.buyer_document_digits = p.buyer_document_digits
        ),
        supplier_evidence AS (
            SELECT
                role_document_digits,
                buyer_document_digits,
                role_field,
                list(evidence_ref ORDER BY contract_value DESC, signing_date DESC)
                    AS supplier_evidence_refs
            FROM supplier_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY role_document_digits, buyer_document_digits, role_field
        ),
        joined AS (
            SELECT
                r.role_document_digits,
                r.buyer_document_digits,
                r.role_field,
                r.role_name,
                r.role_doc_type,
                coalesce(r.buyer_document_id, s.buyer_document_id) AS buyer_document_id,
                coalesce(r.buyer_name, s.buyer_name) AS buyer_name,
                r.role_contract_count,
                r.first_role_signing_date,
                r.last_role_signing_date,
                r.max_role_contract_value,
                s.supplier_entity_id,
                s.supplier_document_key,
                s.supplier_name,
                s.supplier_doc_type,
                s.department,
                s.city,
                s.sector,
                s.procurement_modality,
                s.contract_type,
                s.supplier_contract_count,
                s.supplier_total_contract_value,
                s.max_supplier_contract_value,
                s.first_supplier_signing_date,
                s.last_supplier_signing_date,
                list_concat(e.role_evidence_refs, se.supplier_evidence_refs)
                    AS evidence_refs
            FROM role_summary r
            JOIN supplier_summary s
                ON s.role_document_digits = r.role_document_digits
                AND s.buyer_document_digits = r.buyer_document_digits
                AND s.role_field = r.role_field
            JOIN role_evidence e
                ON e.role_document_digits = r.role_document_digits
                AND e.buyer_document_digits = r.buyer_document_digits
                AND e.role_field = r.role_field
            JOIN supplier_evidence se
                ON se.role_document_digits = r.role_document_digits
                AND se.buyer_document_digits = r.buyer_document_digits
                AND se.role_field = r.role_field
        )
        SELECT
            'procurement_role_supplier_same_buyer_review_only' AS signal_id,
            coalesce(supplier_entity_id, 'person:' || role_document_digits) AS entity_id,
            role_document_digits AS entity_key,
            'Person' AS entity_label,
            'role_supplier:' || buyer_document_digits || ':' || role_document_digits
                || ':' || role_field AS scope_key,
            'buyer_role_supplier' AS scope_type,
            CASE
                WHEN role_field IN (
                    'spending_orderer_doc_number',
                    'payment_orderer_doc_number'
                )
                    OR supplier_total_contract_value >= 500000000
                    THEN 'high'
                ELSE 'medium'
            END AS severity,
            least(
                1.0,
                0.55
                    + CASE
                        WHEN role_field IN (
                            'spending_orderer_doc_number',
                            'payment_orderer_doc_number'
                        )
                            THEN 0.15
                        ELSE 0.05
                    END
                    + least(log10(greatest(supplier_total_contract_value, 1)) / 120.0, 0.12)
                    + least(supplier_contract_count / 20.0, 0.08)
                    + least(role_contract_count / 20.0, 0.05)
                    + CASE
                        WHEN first_supplier_signing_date >= first_role_signing_date
                            THEN 0.10
                        ELSE 0.0
                    END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_PERSON_DOCUMENT' AS identity_match_type,
            'exact' AS identity_quality,
            role_name,
            role_doc_type,
            role_field,
            buyer_document_id,
            buyer_document_digits,
            buyer_name,
            supplier_document_key,
            supplier_name,
            supplier_doc_type,
            department,
            city,
            sector,
            procurement_modality,
            contract_type,
            role_contract_count,
            supplier_contract_count,
            supplier_total_contract_value,
            max_role_contract_value,
            max_supplier_contract_value,
            first_role_signing_date,
            last_role_signing_date,
            first_supplier_signing_date,
            last_supplier_signing_date,
            'exact person document overlap; does not prove employment status, conflict, or corrupt intent'
                AS what_is_unproven,
            evidence_refs
        FROM joined
        WHERE supplier_total_contract_value >= 100000000
            OR supplier_contract_count >= 2
    """)


def _create_interadmin_executor_network_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {
        "secop_interadmin_agreements",
        "secop_ii_contracts",
        "secop_ii_processes",
        "secop_offers",
        "paco_sanctions",
        "fiscal_findings",
        "fiscal_responsibility",
        "secop_budget_commitments",
        "secop_budget_items",
        "secop_cdp_requests",
        "secop_contract_execution",
        "secop_contract_suspensions",
        "secop_contract_modifications",
        "secop_guarantees",
        "secop_invoices",
        "secop_payment_plans",
        "health_providers",
        "secop_sanctions",
        "company_registry_c82u",
        "secop_suppliers",
    }
    if not (required <= set(required_sources)):
        return

    inputs = (
        ("curated_single_bidder_high_value", "procurement_competition", "medium", True),
        ("curated_large_modifications", "execution_failure", "high", True),
        ("curated_contract_modification_ladder", "execution_failure", "high", True),
        ("curated_sanctioned_awards", "sanctions", "high", False),
        ("curated_secop_sanction_later_awards", "sanctions", "high", True),
        ("curated_fiscal_procurement_chronology", "sanctions", "high", True),
        ("curated_supplier_concentration", "procurement_competition", "medium", False),
        ("curated_repeat_awards_same_supplier", "procurement_competition", "medium", False),
        (
            "curated_buyer_supplier_network_density",
            "procurement_competition",
            "medium",
            False,
        ),
        ("curated_cartel_risk_cobidding", "procurement_competition", "high", False),
        (
            "curated_related_bidders_same_process",
            "procurement_competition",
            "high",
            True,
        ),
        (
            "curated_shared_representative_same_buyer_cluster",
            "procurement_competition",
            "high",
            True,
        ),
        ("curated_payment_plan_anomalies", "execution_failure", "medium", True),
        (
            "curated_payment_plan_reconciliation",
            "execution_failure",
            "high",
            True,
        ),
        ("curated_guarantee_advance_execution_chain", "execution_failure", "high", True),
        ("curated_guarantee_policy_reuse", "execution_failure", "high", True),
        (
            "curated_invoice_budget_reconciliation",
            "execution_failure",
            "high",
            True,
        ),
        ("curated_contract_suspensions", "execution_failure", "medium", True),
        ("curated_contract_execution_delay", "execution_failure", "low", True),
        ("curated_short_bidding_window", "procurement_competition", "low", True),
        ("curated_related_companies_shared_officer", "corporate_capacity", "medium", False),
        (
            "curated_cross_source_identity_inconsistency",
            "corporate_capacity",
            "low",
            True,
        ),
        ("curated_rues_supplier_capacity_status", "corporate_capacity", "medium", True),
        ("curated_health_pae_service_delivery_gap", "service_delivery", "high", True),
    )

    feature_root = curated_path(
        "signal_feature_secop_interadmin_executor_network_review_only"
    ).parent
    existing_feature_files = [
        path
        for path in feature_root.glob("table=signal_feature_*/*.parquet")
        if "table=signal_feature_secop_interadmin_executor_network_review_only"
        not in str(path)
    ]
    if len(existing_feature_files) >= 20:
        feature_glob = feature_root / "table=signal_feature_*" / "*.parquet"
        feature_union = f"""
            SELECT
                signal_id,
                entity_key,
                entity_label,
                scope_key,
                coalesce(severity, 'medium') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                identity_quality,
                CASE
                    WHEN signal_id LIKE '%rues%'
                        OR signal_id LIKE '%related_companies%'
                        OR signal_id LIKE '%cross_source_identity%'
                        THEN 'corporate_capacity'
                    WHEN signal_id LIKE '%guarantee%'
                        OR signal_id LIKE '%suspension%'
                        OR signal_id LIKE '%execution_delay%'
                        OR signal_id LIKE '%invoice%'
                        OR signal_id LIKE '%large_modification%'
                        OR signal_id LIKE '%modification_ladder%'
                        OR signal_id LIKE '%payment_plan%'
                        THEN 'execution_failure'
                    WHEN signal_id LIKE '%sanction%'
                        OR signal_id LIKE '%fiscal%'
                        THEN 'sanctions'
                    WHEN signal_id LIKE '%single_bidder%'
                        OR signal_id LIKE '%repeat_awards%'
                        OR signal_id LIKE '%buyer_supplier_network%'
                        OR signal_id LIKE '%supplier_concentration%'
                        OR signal_id LIKE '%cartel%'
                        OR signal_id LIKE '%tvec%'
                        OR signal_id LIKE '%shared_representative_same_buyer%'
                        OR signal_id LIKE '%short_bidding%'
                        OR signal_id LIKE '%offers_competition%'
                        OR signal_id LIKE '%related_bidders%'
                        OR signal_id LIKE '%contract_value_outlier%'
                        THEN 'procurement_competition'
                    WHEN signal_id LIKE '%declaration%'
                        OR signal_id LIKE '%role_supplier%'
                        OR signal_id LIKE '%conflict%'
                        OR signal_id LIKE '%politically_exposed%'
                        THEN 'conflict_interest'
                    WHEN signal_id LIKE '%donor%'
                        OR signal_id LIKE '%cuentas%'
                        THEN 'campaign_finance'
                    WHEN signal_id LIKE '%bpin%'
                        OR signal_id LIKE '%regalias%'
                        OR signal_id LIKE '%sgr%'
                        OR signal_id LIKE '%pida%'
                        THEN 'project_royalties'
                    WHEN signal_id LIKE '%health_pae%'
                        OR signal_id LIKE '%pae_beneficiary%'
                        THEN 'service_delivery'
                    ELSE NULL
                END AS signal_family,
                evidence_refs
            FROM read_parquet(
                {_sql_string(str(feature_glob))},
                union_by_name = true,
                hive_partitioning = true
            )
            WHERE entity_label = 'Company'
                AND identity_quality = 'exact'
                AND entity_key IS NOT NULL
                AND signal_id NOT IN (
                    'cross_signal_compound_risk_review_only',
                    'secop_interadmin_executor_network_review_only'
                )
        """
    else:
        selects: list[str] = []
        for view_name, family, default_severity, has_severity in inputs:
            severity_expr = (
                f"coalesce(severity, {_sql_string(default_severity)})"
                if has_severity
                else _sql_string(default_severity)
            )
            selects.append(
                f"""
                SELECT
                    signal_id,
                    entity_key,
                    entity_label,
                    scope_key,
                    {severity_expr} AS severity,
                    try_cast(risk_signal AS DOUBLE) AS risk_signal,
                    identity_quality,
                    {_sql_string(family)} AS signal_family,
                    evidence_refs
                FROM {view_name}
                WHERE entity_label = 'Company'
                    AND identity_quality = 'exact'
                    AND entity_key IS NOT NULL
                """
            )
        feature_union = "\nUNION ALL\n".join(selects)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW curated_interadmin_executor_network AS
        WITH raw_agreements AS (
            SELECT
                nullif(trim(contract_id), '') AS agreement_id,
                coalesce(
                    nullif(trim(contract_number), ''),
                    nullif(trim(contract_id), '')
                ) AS agreement_number,
                nullif(trim(process_id), '') AS process_id,
                coacc_reference_url(link) AS agreement_url,
                coacc_document_key(contractor_id, contractor_type)
                    AS contractor_document_key,
                coacc_nit_canonical(contractor_id, contractor_type)
                    AS contractor_nit_canonical,
                nullif(trim(contractor_id), '') AS contractor_document_id,
                nullif(trim(contractor_name), '') AS contractor_name,
                nullif(trim(contractor_type), '') AS contractor_type,
                nullif(trim(entity_id), '') AS origin_entity_id,
                nullif(trim(entity_name), '') AS origin_entity_name,
                nullif(trim(department), '') AS department,
                nullif(trim(municipality), '') AS municipality,
                nullif(trim(government_order), '') AS government_order,
                nullif(trim(source_system), '') AS source_system,
                nullif(trim(resource_origin), '') AS resource_origin,
                nullif(trim(procurement_modality), '') AS procurement_modality,
                nullif(trim(contract_type), '') AS contract_type,
                nullif(trim(modality_justification), '') AS modality_justification,
                nullif(trim(unspsc_class_id), '') AS unspsc_class_id,
                nullif(trim(contractual_object), '') AS contractual_object,
                coacc_money(amount_with_additions) AS agreement_value,
                try_cast(signing_date AS DATE) AS signing_date,
                try_cast(contract_start_date AS DATE) AS contract_start_date,
                try_cast(contract_end_date AS DATE) AS contract_end_date,
                try_cast(load_date AS TIMESTAMP) AS load_date
            FROM src_secop_interadmin_agreements
            WHERE nullif(trim(contract_id), '') IS NOT NULL
                AND coacc_document_key(contractor_id, contractor_type) IS NOT NULL
                AND coacc_nit_canonical(contractor_id, contractor_type) IS NOT NULL
        ),
        agreements AS (
            SELECT
                agreement_id,
                agreement_number,
                contractor_document_key,
                any_value(contractor_nit_canonical) AS contractor_nit_canonical,
                any_value(contractor_document_id) AS contractor_document_id,
                any_value(contractor_name) AS contractor_name,
                any_value(contractor_type) AS contractor_type,
                any_value(process_id) AS process_id,
                any_value(agreement_url) AS agreement_url,
                any_value(origin_entity_id) AS origin_entity_id,
                any_value(origin_entity_name) AS origin_entity_name,
                any_value(department) AS department,
                any_value(municipality) AS municipality,
                any_value(government_order) AS government_order,
                any_value(source_system) AS source_system,
                list(DISTINCT resource_origin ORDER BY resource_origin)
                    FILTER (WHERE resource_origin IS NOT NULL) AS resource_origins,
                any_value(procurement_modality) AS procurement_modality,
                any_value(contract_type) AS contract_type,
                any_value(modality_justification) AS modality_justification,
                any_value(unspsc_class_id) AS unspsc_class_id,
                any_value(contractual_object) AS contractual_object,
                max(agreement_value) AS agreement_value,
                min(signing_date) AS signing_date,
                min(contract_start_date) AS contract_start_date,
                max(contract_end_date) AS contract_end_date,
                max(load_date) AS last_load_date,
                count(*) AS agreement_raw_row_count,
                regexp_matches(
                    lower(
                        coalesce(any_value(contractual_object), '') || ' ' ||
                        coalesce(any_value(contractor_name), '') || ' ' ||
                        coalesce(any_value(contractor_type), '')
                    ),
                    'fondo|patrimonio|fiduci|empresa social del estado|\\bese\\b|'
                    || 'fundaci[oó]n|corporaci[oó]n|asociaci[oó]n|universidad|'
                    || 'empresa de servicios p[uú]blicos|mixt[oa]'
                ) AS special_executor_flag
            FROM raw_agreements
            WHERE contractor_document_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
                AND NOT regexp_matches(contractor_document_key, '^0+$')
            GROUP BY agreement_id, agreement_number, contractor_document_key
        ),
        feature_rows AS (
            {feature_union}
        ),
        support_features AS (
            SELECT
                *,
                CASE
                    WHEN severity = 'critical' THEN 3
                    WHEN severity = 'high' THEN 2
                    WHEN severity = 'medium' THEN 1
                    ELSE 0
                END AS severity_rank
            FROM feature_rows
            WHERE signal_family IS NOT NULL
        ),
        support_rollup AS (
            SELECT
                entity_key AS contractor_document_key,
                count(DISTINCT signal_id) AS support_signal_count,
                count(DISTINCT signal_family) AS support_family_count,
                max(severity_rank) AS max_support_severity_rank,
                sum(coalesce(risk_signal, 0.0)) AS total_support_risk_signal,
                bool_or(signal_family = 'corporate_capacity')
                    AS has_corporate_capacity,
                bool_or(signal_family = 'execution_failure')
                    AS has_execution_failure,
                bool_or(signal_family = 'sanctions') AS has_sanctions,
                bool_or(signal_family = 'procurement_competition')
                    AS has_procurement_competition,
                bool_or(signal_family = 'conflict_interest')
                    AS has_conflict_interest,
                bool_or(signal_family = 'campaign_finance')
                    AS has_campaign_finance,
                bool_or(signal_family = 'project_royalties')
                    AS has_project_royalties,
                bool_or(signal_family = 'service_delivery')
                    AS has_service_delivery,
                list(DISTINCT signal_id ORDER BY signal_id) AS support_signal_ids,
                list(DISTINCT signal_family ORDER BY signal_family)
                    AS support_signal_families
            FROM support_features
            GROUP BY entity_key
        ),
        support_evidence_flat AS (
            SELECT DISTINCT
                entity_key AS contractor_document_key,
                signal_id,
                signal_family,
                scope_key,
                severity_rank,
                risk_signal,
                'signal_feature_' || signal_id || ':' || scope_key AS evidence_ref
            FROM support_features
            UNION ALL
            SELECT DISTINCT
                f.entity_key AS contractor_document_key,
                f.signal_id,
                f.signal_family,
                f.scope_key,
                f.severity_rank,
                f.risk_signal,
                e.evidence_ref
            FROM support_features f
            CROSS JOIN unnest(f.evidence_refs) AS e(evidence_ref)
            WHERE e.evidence_ref IS NOT NULL
                AND nullif(trim(e.evidence_ref), '') IS NOT NULL
        ),
        support_evidence_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY contractor_document_key
                    ORDER BY
                        CASE
                            WHEN evidence_ref LIKE 'signal_feature_%' THEN 0
                            ELSE 1
                        END,
                        severity_rank DESC,
                        coalesce(risk_signal, 0.0) DESC,
                        signal_family,
                        signal_id,
                        evidence_ref
                ) AS evidence_rank
            FROM support_evidence_flat
        ),
        support_evidence AS (
            SELECT
                contractor_document_key,
                list(evidence_ref ORDER BY evidence_rank) AS evidence_refs
            FROM support_evidence_ranked
            WHERE evidence_rank <= 14
            GROUP BY contractor_document_key
        ),
        candidates AS (
            SELECT
                a.*,
                s.support_signal_count,
                s.support_family_count,
                s.max_support_severity_rank,
                s.total_support_risk_signal,
                s.has_corporate_capacity,
                s.has_execution_failure,
                s.has_sanctions,
                s.has_procurement_competition,
                s.has_conflict_interest,
                s.has_campaign_finance,
                s.has_project_royalties,
                s.has_service_delivery,
                s.support_signal_ids,
                s.support_signal_families,
                coalesce(
                    a.agreement_url,
                    'secop_interadmin_agreements:' || a.agreement_id || ':'
                    || a.contractor_document_key
                ) AS agreement_evidence_ref
            FROM agreements a
            JOIN support_rollup s
                ON s.contractor_document_key = a.contractor_document_key
            WHERE a.agreement_value >= 1000000000
                AND s.support_signal_count >= 2
                AND (
                    s.support_family_count >= 3
                    OR (
                        s.has_procurement_competition
                        AND (s.has_corporate_capacity OR s.has_sanctions)
                    )
                    OR (
                        s.has_procurement_competition
                        AND s.has_execution_failure
                        AND a.special_executor_flag
                    )
                    OR (
                        s.has_service_delivery
                        AND (
                            s.has_procurement_competition
                            OR s.has_execution_failure
                            OR s.has_corporate_capacity
                            OR s.has_sanctions
                        )
                    )
                    OR (
                        (s.has_execution_failure OR s.has_sanctions)
                        AND a.agreement_value >= 10000000000
                    )
                )
        ),
        deduped_candidates AS (
            SELECT *
            FROM candidates
            QUALIFY row_number() OVER (
                PARTITION BY agreement_id, contractor_document_key
                ORDER BY agreement_value DESC NULLS LAST,
                    agreement_number,
                    process_id
            ) = 1
        )
        SELECT
            'secop_interadmin_executor_network_review_only' AS signal_id,
            'doc:' || c.contractor_document_key AS entity_id,
            c.contractor_document_key AS entity_key,
            'Company' AS entity_label,
            'interadmin_chain:' || c.agreement_id || ':' || c.contractor_document_key
                AS scope_key,
            'interadmin_executor_chain' AS scope_type,
            CASE
                WHEN c.support_family_count >= 4
                    OR c.has_sanctions
                    OR c.agreement_value >= 50000000000
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.56
                    + least(c.support_family_count * 0.05, 0.20)
                    + least(c.support_signal_count * 0.02, 0.14)
                    + least(log10(greatest(c.agreement_value, 1)) / 130.0, 0.10)
                    + CASE WHEN c.special_executor_flag THEN 0.04 ELSE 0.0 END
                    + CASE WHEN c.has_sanctions THEN 0.06 ELSE 0.0 END
                    + CASE WHEN c.has_corporate_capacity THEN 0.04 ELSE 0.0 END
                    + CASE WHEN c.has_execution_failure THEN 0.04 ELSE 0.0 END
                    + CASE WHEN c.has_service_delivery THEN 0.03 ELSE 0.0 END
            ) AS risk_signal,
            1.0 AS identity_confidence,
            'EXACT_COMPANY_NIT' AS identity_match_type,
            'exact' AS identity_quality,
            c.contractor_name,
            c.contractor_type,
            c.contractor_document_id,
            c.contractor_nit_canonical,
            c.agreement_id,
            c.agreement_number,
            c.process_id,
            c.agreement_url,
            c.origin_entity_id,
            c.origin_entity_name,
            c.department,
            c.municipality,
            c.government_order,
            c.source_system,
            c.resource_origins,
            c.procurement_modality,
            c.contract_type,
            c.modality_justification,
            c.unspsc_class_id,
            c.contractual_object,
            c.agreement_value,
            c.signing_date,
            c.contract_start_date,
            c.contract_end_date,
            c.last_load_date,
            c.agreement_raw_row_count,
            c.special_executor_flag,
            c.support_signal_count,
            c.support_family_count,
            c.support_signal_ids,
            c.support_signal_families,
            c.has_corporate_capacity,
            c.has_execution_failure,
            c.has_sanctions,
            c.has_procurement_competition,
            c.has_conflict_interest,
            c.has_campaign_finance,
            c.has_project_royalties,
            c.has_service_delivery,
            'interadministrative agreement recipient has exact-NIT overlap with independent procurement/capacity/execution/service-delivery risk queues; downstream subcontracting, legal incapacity, nonperformance, service failure, and corrupt intent are not proven by this feature'
                AS what_is_unproven,
            list_concat(
                [c.agreement_evidence_ref],
                coalesce(e.evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM deduped_candidates c
        LEFT JOIN support_evidence e
            ON e.contractor_document_key = c.contractor_document_key
        QUALIFY row_number() OVER (
            ORDER BY c.support_family_count DESC,
                c.support_signal_count DESC,
                c.agreement_value DESC NULLS LAST,
                c.agreement_id,
                c.contractor_document_key
        ) <= 1000
    """)


def _create_cross_signal_compound_risk_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = set(_CROSS_SIGNAL_REQUIRED_SOURCES)
    if not (required <= set(required_sources)):
        return

    # Family labels intentionally collapse many mechanical procurement signals so the
    # output highlights cross-domain convergence instead of repeated variants.
    inputs = (
        ("curated_single_bidder_high_value", "procurement_competition", "medium", True),
        ("curated_large_modifications", "execution_failure", "high", True),
        ("curated_contract_modification_ladder", "execution_failure", "high", True),
        ("curated_sanctioned_awards", "sanctions", "high", False),
        ("curated_secop_sanction_later_awards", "sanctions", "high", True),
        ("curated_fiscal_procurement_chronology", "sanctions", "high", True),
        ("curated_supplier_concentration", "procurement_competition", "medium", False),
        (
            "curated_contract_value_outlier_by_category",
            "procurement_competition",
            "medium",
            False,
        ),
        ("curated_repeat_awards_same_supplier", "procurement_competition", "medium", False),
        (
            "curated_buyer_supplier_network_density",
            "procurement_competition",
            "medium",
            False,
        ),
        ("curated_cartel_risk_cobidding", "procurement_competition", "high", True),
        (
            "curated_related_bidders_same_process",
            "procurement_competition",
            "high",
            True,
        ),
        ("curated_payment_plan_anomalies", "execution_failure", "medium", True),
        (
            "curated_payment_plan_reconciliation",
            "execution_failure",
            "high",
            True,
        ),
        ("curated_guarantee_advance_execution_chain", "execution_failure", "high", True),
        ("curated_guarantee_policy_reuse", "execution_failure", "high", True),
        (
            "curated_invoice_budget_reconciliation",
            "execution_failure",
            "high",
            True,
        ),
        ("curated_contract_suspensions", "execution_failure", "medium", True),
        ("curated_contract_execution_delay", "execution_failure", "low", True),
        ("curated_project_bpin_procurement_overlap", "project_royalties", "medium", True),
        (
            "curated_project_regalias_execution_procurement_overlap",
            "project_royalties",
            "medium",
            True,
        ),
        ("curated_sgr_ocad_executor_capacity_gap", "project_royalties", "high", True),
        (
            "curated_dnp_sgr_beneficiary_delivery_gap",
            "project_royalties",
            "high",
            True,
        ),
        (
            "curated_bpin_dnp_vs_pida27_obras_prioritarias",
            "project_royalties",
            "medium",
            True,
        ),
        ("curated_short_bidding_window", "procurement_competition", "low", True),
        ("curated_offers_competition_drop", "procurement_competition", "medium", False),
        (
            "curated_public_servant_conflict_disclosure_overlap",
            "conflict_interest",
            "medium",
            True,
        ),
        ("curated_role_supplier_same_buyer", "conflict_interest", "high", True),
        (
            "curated_public_declaration_supplier_chronology",
            "conflict_interest",
            "high",
            True,
        ),
        (
            "curated_cuentas_claras_donor_supplier_overlap",
            "campaign_finance",
            "medium",
            True,
        ),
        (
            "curated_cuentas_claras_donor_ineligibility_review",
            "campaign_finance",
            "medium",
            True,
        ),
        ("curated_pida_full30_meta", "project_royalties", "medium", True),
        ("curated_pida5_pida27_pida4_chain", "project_royalties", "medium", True),
        ("curated_tvec_multi_entity_capture", "procurement_competition", "medium", True),
        ("curated_tvec_item_price_dispersion", "procurement_competition", "high", True),
        (
            "curated_politically_exposed_supplier_overlap",
            "conflict_interest",
            "medium",
            False,
        ),
        ("curated_related_companies_shared_officer", "corporate_capacity", "medium", False),
        (
            "curated_cross_source_identity_inconsistency",
            "corporate_capacity",
            "low",
            True,
        ),
        ("curated_rues_supplier_capacity_status", "corporate_capacity", "medium", True),
        ("curated_health_pae_service_delivery_gap", "service_delivery", "high", True),
        (
            "curated_pae_beneficiary_territory_delivery_gap",
            "service_delivery",
            "high",
            True,
        ),
    )

    feature_root = curated_path("signal_feature_cross_signal_compound_risk_review_only").parent
    existing_feature_files = [
        path
        for path in feature_root.glob("table=signal_feature_*/*.parquet")
        if "table=signal_feature_cross_signal_compound_risk_review_only" not in str(path)
    ]
    if len(existing_feature_files) >= 20:
        feature_glob = feature_root / "table=signal_feature_*" / "*.parquet"
        feature_union = f"""
            SELECT
                signal_id,
                entity_id,
                entity_key,
                entity_label,
                scope_key,
                scope_type,
                coalesce(severity, 'medium') AS severity,
                try_cast(risk_signal AS DOUBLE) AS risk_signal,
                try_cast(identity_confidence AS DOUBLE) AS identity_confidence,
                identity_match_type,
                identity_quality,
                CASE
                    WHEN signal_id LIKE '%declaration%'
                        OR signal_id LIKE '%role_supplier%'
                        OR signal_id LIKE '%conflict%'
                        OR signal_id LIKE '%politically_exposed%'
                        THEN 'conflict_interest'
                    WHEN signal_id LIKE '%donor%'
                        OR signal_id LIKE '%cuentas%'
                        THEN 'campaign_finance'
                    WHEN signal_id LIKE '%sanction%'
                        OR signal_id LIKE '%fiscal%'
                        THEN 'sanctions'
                    WHEN signal_id LIKE '%guarantee%'
                        OR signal_id LIKE '%suspension%'
                        OR signal_id LIKE '%execution_delay%'
                        OR signal_id LIKE '%invoice%'
                        OR signal_id LIKE '%large_modification%'
                        OR signal_id LIKE '%modification_ladder%'
                        OR signal_id LIKE '%payment_plan%'
                        THEN 'execution_failure'
                    WHEN signal_id LIKE '%rues%'
                        OR signal_id LIKE '%related_companies%'
                        OR signal_id LIKE '%cross_source_identity%'
                        THEN 'corporate_capacity'
                    WHEN signal_id LIKE '%tvec%'
                        OR signal_id LIKE '%shared_representative_same_buyer%'
                        OR signal_id LIKE '%single_bidder%'
                        OR signal_id LIKE '%short_bidding%'
                        OR signal_id LIKE '%repeat_awards%'
                        OR signal_id LIKE '%supplier_concentration%'
                        OR signal_id LIKE '%cartel%'
                        OR signal_id LIKE '%related_bidders%'
                        OR signal_id LIKE '%offers_competition%'
                        OR signal_id LIKE '%contract_value_outlier%'
                        OR signal_id LIKE '%buyer_supplier_network%'
                        THEN 'procurement_competition'
                    WHEN signal_id LIKE '%bpin%'
                        OR signal_id LIKE '%regalias%'
                        OR signal_id LIKE '%sgr%'
                        OR signal_id LIKE '%pida%'
                        THEN 'project_royalties'
                    WHEN signal_id LIKE '%health_pae%'
                        OR signal_id LIKE '%pae_beneficiary%'
                        THEN 'service_delivery'
                    ELSE 'other'
                END AS signal_family,
                evidence_refs
            FROM read_parquet(
                {_sql_string(str(feature_glob))},
                union_by_name = true,
                hive_partitioning = true
            )
            WHERE signal_id != 'cross_signal_compound_risk_review_only'
                AND signal_id != 'secop_i_legacy_representative_current_risk_review_only'
                AND entity_key IS NOT NULL
                AND entity_label IN ('Company', 'Person', 'Project')
                AND identity_quality = 'exact'
        """
    else:
        selects: list[str] = []
        for view_name, family, default_severity, has_severity in inputs:
            severity_expr = (
                f"coalesce(severity, {_sql_string(default_severity)})"
                if has_severity
                else _sql_string(default_severity)
            )
            selects.append(
                f"""
                SELECT
                    signal_id,
                    entity_id,
                    entity_key,
                    entity_label,
                    scope_key,
                    scope_type,
                    {severity_expr} AS severity,
                    try_cast(risk_signal AS DOUBLE) AS risk_signal,
                    try_cast(identity_confidence AS DOUBLE) AS identity_confidence,
                    identity_match_type,
                    identity_quality,
                    {_sql_string(family)} AS signal_family,
                    evidence_refs
                FROM {view_name}
                WHERE entity_key IS NOT NULL
                    AND entity_label IN ('Company', 'Person', 'Project')
                    AND identity_quality = 'exact'
                """
            )
        feature_union = "\nUNION ALL\n".join(selects)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW curated_cross_signal_compound_risk AS
        WITH feature_rows AS (
            {feature_union}
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN severity = 'critical' THEN 3
                    WHEN severity = 'high' THEN 2
                    WHEN severity = 'medium' THEN 1
                    ELSE 0
                END AS severity_rank
            FROM feature_rows
            WHERE entity_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
                AND NOT regexp_matches(entity_key, '^0+$')
        ),
        entity_rollup AS (
            SELECT
                entity_label,
                entity_key,
                count(DISTINCT signal_id) AS signal_count,
                count(DISTINCT signal_family) AS family_count,
                count(*) AS feature_row_count,
                count(DISTINCT scope_key) AS scope_count,
                max(severity_rank) AS max_severity_rank,
                sum(coalesce(risk_signal, 0.0)) AS total_risk_signal,
                max(coalesce(identity_confidence, 0.0)) AS identity_confidence,
                bool_or(signal_family = 'campaign_finance') AS has_campaign_finance,
                bool_or(signal_family = 'conflict_interest') AS has_conflict_interest,
                bool_or(signal_family = 'sanctions') AS has_sanctions,
                bool_or(signal_family = 'corporate_capacity') AS has_corporate_capacity,
                bool_or(signal_family = 'execution_failure') AS has_execution_failure,
                bool_or(signal_family = 'project_royalties') AS has_project_royalties,
                bool_or(signal_family = 'procurement_competition')
                    AS has_procurement_competition,
                bool_or(signal_family = 'service_delivery')
                    AS has_service_delivery,
                list(DISTINCT signal_id ORDER BY signal_id) AS signal_ids,
                list(DISTINCT signal_family ORDER BY signal_family) AS signal_families
            FROM classified
            GROUP BY entity_label, entity_key
        ),
        candidates AS (
            SELECT
                *,
                (
                    family_count >= 3
                    OR (
                        has_campaign_finance
                        AND (
                            has_conflict_interest
                            OR has_sanctions
                            OR has_corporate_capacity
                            OR has_execution_failure
                        )
                    )
                    OR (
                        has_conflict_interest
                        AND (
                            has_sanctions
                            OR has_execution_failure
                            OR has_corporate_capacity
                        )
                    )
                    OR (
                        has_sanctions
                        AND (has_execution_failure OR has_corporate_capacity)
                    )
                    OR (
                        has_corporate_capacity
                        AND has_execution_failure
                        AND max_severity_rank >= 2
                    )
                    OR (
                        has_service_delivery
                        AND (
                            has_execution_failure
                            OR has_procurement_competition
                            OR has_sanctions
                            OR has_corporate_capacity
                        )
                    )
                ) AS compound_gate
            FROM entity_rollup
            WHERE signal_count >= 2
        ),
        evidence_flat AS (
            SELECT DISTINCT
                c.entity_label,
                c.entity_key,
                c.signal_id,
                c.signal_family,
                c.scope_key,
                c.severity_rank,
                c.risk_signal,
                'signal_feature_' || c.signal_id || ':' || c.scope_key AS evidence_ref
            FROM classified c
            JOIN candidates cand
                ON cand.entity_label = c.entity_label
                AND cand.entity_key = c.entity_key
            WHERE cand.compound_gate
            UNION ALL
            SELECT DISTINCT
                c.entity_label,
                c.entity_key,
                c.signal_id,
                c.signal_family,
                c.scope_key,
                c.severity_rank,
                c.risk_signal,
                e.evidence_ref
            FROM classified c
            JOIN candidates cand
                ON cand.entity_label = c.entity_label
                AND cand.entity_key = c.entity_key
            CROSS JOIN unnest(c.evidence_refs) AS e(evidence_ref)
            WHERE cand.compound_gate
                AND e.evidence_ref IS NOT NULL
                AND nullif(trim(e.evidence_ref), '') IS NOT NULL
        ),
        evidence_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY entity_label, entity_key
                    ORDER BY
                        CASE
                            WHEN evidence_ref LIKE 'signal_feature_%' THEN 0
                            ELSE 1
                        END,
                        severity_rank DESC,
                        coalesce(risk_signal, 0.0) DESC,
                        signal_family,
                        signal_id,
                        evidence_ref
                ) AS evidence_rank
            FROM evidence_flat
        ),
        evidence AS (
            SELECT
                entity_label,
                entity_key,
                list(evidence_ref ORDER BY evidence_rank) AS evidence_refs
            FROM evidence_ranked
            WHERE evidence_rank <= 20
            GROUP BY entity_label, entity_key
        )
        SELECT
            'cross_signal_compound_risk_review_only' AS signal_id,
            CASE
                WHEN c.entity_label = 'Company' THEN 'doc:' || c.entity_key
                WHEN c.entity_label = 'Person' THEN 'person:' || c.entity_key
                ELSE 'project:' || c.entity_key
            END AS entity_id,
            c.entity_key,
            c.entity_label,
            'cross_signal:' || lower(c.entity_label) || ':' || c.entity_key
                AS scope_key,
            'cross_signal_entity' AS scope_type,
            CASE
                WHEN c.family_count >= 4
                    OR c.signal_count >= 6
                    OR c.max_severity_rank = 3
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.98,
                0.55
                    + least(c.family_count * 0.06, 0.24)
                    + least(c.signal_count * 0.025, 0.15)
                    + least(c.feature_row_count / 5000.0, 0.08)
                    + CASE WHEN c.has_campaign_finance THEN 0.03 ELSE 0.0 END
                    + CASE WHEN c.has_conflict_interest THEN 0.03 ELSE 0.0 END
                    + CASE WHEN c.has_sanctions THEN 0.04 ELSE 0.0 END
                    + CASE WHEN c.has_corporate_capacity THEN 0.03 ELSE 0.0 END
                    + CASE WHEN c.has_execution_failure THEN 0.03 ELSE 0.0 END
                    + CASE WHEN c.has_service_delivery THEN 0.03 ELSE 0.0 END
            ) AS risk_signal,
            least(1.0, c.identity_confidence) AS identity_confidence,
            CASE
                WHEN c.entity_label = 'Company' THEN 'EXACT_COMPANY_NIT'
                WHEN c.entity_label = 'Person' THEN 'EXACT_PERSON_DOCUMENT'
                ELSE 'EXACT_BPIN'
            END AS identity_match_type,
            'exact' AS identity_quality,
            c.signal_count,
            c.family_count,
            c.feature_row_count,
            c.scope_count,
            c.signal_ids,
            c.signal_families,
            c.has_campaign_finance,
            c.has_conflict_interest,
            c.has_sanctions,
            c.has_corporate_capacity,
            c.has_execution_failure,
            c.has_project_royalties,
            c.has_procurement_competition,
            c.has_service_delivery,
            'multiple exact-identity risk queues converge; this does not prove corrupt intent, legal disability, conflict of interest, or nonperformance without source-document review'
                AS what_is_unproven,
            e.evidence_refs
        FROM candidates c
        JOIN evidence e
            ON e.entity_label = c.entity_label
            AND e.entity_key = c.entity_key
        WHERE c.compound_gate
        QUALIFY row_number() OVER (
            ORDER BY c.family_count DESC,
                c.signal_count DESC,
                c.max_severity_rank DESC,
                c.total_risk_signal DESC,
                c.entity_label,
                c.entity_key
        ) <= 1000
    """)


def _create_public_declaration_company_bridge_current_risk_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = set(_CROSS_SIGNAL_REQUIRED_SOURCES)
    if not (required <= set(required_sources)):
        return

    con.execute("""
        CREATE OR REPLACE TEMP VIEW
            curated_public_declaration_company_bridge_current_risk AS
        WITH asset_disclosures AS (
            SELECT
                coacc_cedula_key(ad.document_id, ad.document_type)
                    AS person_document_key,
                coacc_doc_digits(ad.document_id) AS person_document_id,
                nullif(trim(ad.document_type), '') AS person_doc_type,
                nullif(trim(ad.form_number), '') AS form_number,
                cast(try_cast(ad.publication_date AS TIMESTAMP) AS DATE)
                    AS observed_date,
                nullif(trim(ad.declaration_status), '') AS declaration_status,
                nullif(trim(ad.declaration_type), '') AS declaration_type,
                nullif(trim(ad.entity_name), '') AS disclosure_entity_name,
                nullif(trim(ad.declarant_role), '') AS declarant_role,
                nullif(trim(concat_ws(
                    ' ',
                    ad.declarant_first_name,
                    ad.declarant_second_name,
                    ad.declarant_first_lastname,
                    ad.declarant_second_lastname
                )), '') AS person_name,
                lower(trim(coalesce(ad.declarant_is_contractor, '')))
                    IN ('si', 'sí', 'true', '1', 'x')
                    AS asset_contractor_flag,
                lower(trim(coalesce(ad.private_economic_activities, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS asset_private_economic_activity_flag,
                lower(trim(coalesce(ad.corp_society_participations, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS asset_corporate_interest_flag,
                lower(trim(coalesce(ad.board_council_participations, '')))
                    NOT IN ('', 'no', 'n/a', 'na', 'ninguno', 'ninguna', 'null', '[]')
                    AS asset_board_interest_flag,
                'asset_disclosures:' || coalesce(
                    nullif(trim(ad.form_number), ''),
                    coacc_cedula_key(ad.document_id, ad.document_type)
                        || ':'
                        || coalesce(
                            strftime(
                                cast(try_cast(ad.publication_date AS TIMESTAMP) AS DATE),
                                '%Y%m%d'
                            ),
                            'undated'
                        )
                ) AS evidence_ref
            FROM src_asset_disclosures ad
            WHERE coacc_cedula_key(ad.document_id, ad.document_type) IS NOT NULL
        ),
        conflict_disclosures AS (
            SELECT
                coacc_cedula_key(cd.document_id, cd.document_type)
                    AS person_document_key,
                coacc_doc_digits(cd.document_id) AS person_document_id,
                nullif(trim(cd.document_type), '') AS person_doc_type,
                nullif(trim(cd.form_number), '') AS form_number,
                cast(try_cast(cd.publication_date AS TIMESTAMP) AS DATE)
                    AS observed_date,
                nullif(trim(cd.declaration_status), '') AS declaration_status,
                nullif(trim(cd.declaration_type), '') AS declaration_type,
                nullif(trim(cd.entity_name), '') AS disclosure_entity_name,
                nullif(trim(cd.declarant_role), '') AS declarant_role,
                nullif(trim(concat_ws(
                    ' ',
                    cd.declarant_first_name,
                    cd.declarant_second_name,
                    cd.declarant_first_lastname,
                    cd.declarant_second_lastname
                )), '') AS person_name,
                lower(trim(coalesce(cd.declarant_is_contractor, '')))
                    IN ('si', 'sí', 'true', '1', 'x')
                    AS conflict_contractor_flag,
                lower(trim(coalesce(cd.direct_interest_actions, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS direct_interest_flag,
                lower(trim(coalesce(cd.conflict_relatives, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS relative_conflict_flag,
                lower(trim(coalesce(cd.conflict_donations, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS donation_conflict_flag,
                lower(trim(coalesce(cd.other_potential_conflicts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS other_conflict_flag,
                lower(trim(coalesce(cd.conflict_trusts, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS trust_conflict_flag,
                lower(trim(coalesce(cd.other_conflict_investments, '')))
                    IN ('si', 'sí', 'true', '1', 'x') AS investment_conflict_flag,
                'conflict_disclosures:' || coalesce(
                    nullif(trim(cd.form_number), ''),
                    coacc_cedula_key(cd.document_id, cd.document_type)
                        || ':'
                        || coalesce(
                            strftime(
                                cast(try_cast(cd.publication_date AS TIMESTAMP) AS DATE),
                                '%Y%m%d'
                            ),
                            'undated'
                        )
                ) AS evidence_ref
            FROM src_conflict_disclosures cd
            WHERE coacc_cedula_key(cd.document_id, cd.document_type) IS NOT NULL
        ),
        conflict_scored AS (
            SELECT
                *,
                cast(direct_interest_flag AS INTEGER)
                    + cast(relative_conflict_flag AS INTEGER)
                    + cast(donation_conflict_flag AS INTEGER)
                    + cast(other_conflict_flag AS INTEGER)
                    + cast(trust_conflict_flag AS INTEGER)
                    + cast(investment_conflict_flag AS INTEGER)
                    AS conflict_flag_count
            FROM conflict_disclosures
        ),
        sensitive_positions AS (
            SELECT
                coacc_cedula_key(sp.funcionario_id, sp.document_type)
                    AS person_document_key,
                coacc_doc_digits(sp.funcionario_id) AS person_document_id,
                nullif(trim(sp.document_type), '') AS person_doc_type,
                nullif(trim(sp.full_name), '') AS person_name,
                nullif(trim(sp.institution_id), '') AS institution_id,
                nullif(trim(sp.institution_name), '') AS institution_name,
                nullif(trim(sp.job_hierarchy_level), '') AS job_hierarchy_level,
                nullif(trim(sp.appointment_type), '') AS appointment_type,
                nullif(trim(sp.current_job_title), '') AS current_job_title,
                try_cast(sp.start_date AS DATE) AS observed_date,
                'sigep_sensitive_positions:'
                    || coacc_cedula_key(sp.funcionario_id, sp.document_type)
                    || ':'
                    || coalesce(nullif(trim(sp.institution_id), ''), 'unknown')
                    || ':'
                    || coalesce(strftime(try_cast(sp.start_date AS DATE), '%Y%m%d'), 'undated')
                    AS evidence_ref
            FROM src_sigep_sensitive_positions sp
            WHERE coacc_cedula_key(sp.funcionario_id, sp.document_type) IS NOT NULL
        ),
        profile_keys AS (
            SELECT DISTINCT person_document_key FROM asset_disclosures
            UNION
            SELECT DISTINCT person_document_key FROM conflict_scored
            UNION
            SELECT DISTINCT person_document_key FROM sensitive_positions
        ),
        asset_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                min(person_name) FILTER (WHERE person_name IS NOT NULL)
                    AS person_name,
                any_value(disclosure_entity_name) AS asset_entity_name,
                any_value(declarant_role) AS asset_declarant_role,
                count(DISTINCT coalesce(form_number, evidence_ref))
                    AS asset_disclosure_count,
                min(observed_date) AS first_asset_observed_date,
                max(observed_date) AS last_asset_observed_date,
                bool_or(asset_contractor_flag) AS asset_contractor_flag,
                bool_or(asset_private_economic_activity_flag)
                    AS asset_private_economic_activity_flag,
                bool_or(asset_corporate_interest_flag)
                    AS asset_corporate_interest_flag,
                bool_or(asset_board_interest_flag) AS asset_board_interest_flag
            FROM asset_disclosures
            GROUP BY person_document_key
        ),
        conflict_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                min(person_name) FILTER (WHERE person_name IS NOT NULL)
                    AS person_name,
                any_value(disclosure_entity_name) AS conflict_entity_name,
                any_value(declarant_role) AS conflict_declarant_role,
                count(DISTINCT coalesce(form_number, evidence_ref))
                    AS conflict_disclosure_count,
                min(observed_date) AS first_conflict_observed_date,
                max(observed_date) AS last_conflict_observed_date,
                bool_or(conflict_contractor_flag) AS conflict_contractor_flag,
                max(conflict_flag_count) AS conflict_flag_count,
                bool_or(direct_interest_flag) AS conflict_direct_interest_flag,
                bool_or(relative_conflict_flag) AS conflict_relative_flag,
                bool_or(donation_conflict_flag) AS conflict_donation_flag,
                bool_or(other_conflict_flag) AS conflict_other_flag,
                bool_or(trust_conflict_flag) AS conflict_trust_flag,
                bool_or(investment_conflict_flag) AS conflict_investment_flag
            FROM conflict_scored
            GROUP BY person_document_key
        ),
        sensitive_profile AS (
            SELECT
                person_document_key,
                any_value(person_document_id) AS person_document_id,
                any_value(person_doc_type) AS person_doc_type,
                min(person_name) FILTER (WHERE person_name IS NOT NULL)
                    AS person_name,
                any_value(institution_id) AS sensitive_institution_id,
                any_value(institution_name) AS sensitive_institution_name,
                any_value(job_hierarchy_level) AS sensitive_job_hierarchy_level,
                any_value(appointment_type) AS sensitive_appointment_type,
                any_value(current_job_title) AS sensitive_current_job_title,
                count(*) AS sensitive_position_count,
                min(observed_date) AS first_sensitive_observed_date,
                max(observed_date) AS last_sensitive_observed_date
            FROM sensitive_positions
            GROUP BY person_document_key
        ),
        asset_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                observed_date,
                form_number,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY observed_date DESC NULLS LAST, form_number DESC NULLS LAST
                ) AS evidence_rank
            FROM asset_disclosures
        ),
        asset_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY observed_date DESC, form_number DESC)
                    AS asset_evidence_refs
            FROM asset_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        conflict_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                observed_date,
                form_number,
                conflict_flag_count,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY
                        conflict_flag_count DESC,
                        observed_date DESC NULLS LAST,
                        form_number DESC NULLS LAST
                ) AS evidence_rank
            FROM conflict_scored
        ),
        conflict_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY conflict_flag_count DESC, observed_date DESC)
                    AS conflict_evidence_refs
            FROM conflict_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        sensitive_evidence_ranked AS (
            SELECT
                person_document_key,
                evidence_ref,
                observed_date,
                institution_id,
                row_number() OVER (
                    PARTITION BY person_document_key
                    ORDER BY observed_date DESC NULLS LAST, institution_id
                ) AS evidence_rank
            FROM sensitive_positions
        ),
        sensitive_evidence AS (
            SELECT
                person_document_key,
                list(evidence_ref ORDER BY observed_date DESC, institution_id)
                    AS sensitive_evidence_refs
            FROM sensitive_evidence_ranked
            WHERE evidence_rank <= 2
            GROUP BY person_document_key
        ),
        declaration_profile AS (
            SELECT
                k.person_document_key,
                coalesce(a.person_document_id, c.person_document_id, s.person_document_id)
                    AS person_document_id,
                coalesce(a.person_doc_type, c.person_doc_type, s.person_doc_type)
                    AS person_doc_type,
                coalesce(a.person_name, c.person_name, s.person_name) AS person_name,
                a.asset_entity_name,
                c.conflict_entity_name,
                s.sensitive_institution_id,
                s.sensitive_institution_name,
                coalesce(a.asset_declarant_role, c.conflict_declarant_role)
                    AS declarant_role,
                s.sensitive_job_hierarchy_level,
                s.sensitive_appointment_type,
                s.sensitive_current_job_title,
                coalesce(a.asset_disclosure_count, 0) AS asset_disclosure_count,
                coalesce(c.conflict_disclosure_count, 0) AS conflict_disclosure_count,
                coalesce(s.sensitive_position_count, 0) AS sensitive_position_count,
                nullif(
                    least(
                        coalesce(a.first_asset_observed_date, DATE '9999-12-31'),
                        coalesce(c.first_conflict_observed_date, DATE '9999-12-31'),
                        coalesce(s.first_sensitive_observed_date, DATE '9999-12-31')
                    ),
                    DATE '9999-12-31'
                ) AS first_observed_date,
                nullif(
                    greatest(
                        coalesce(a.last_asset_observed_date, DATE '0001-01-01'),
                        coalesce(c.last_conflict_observed_date, DATE '0001-01-01'),
                        coalesce(s.last_sensitive_observed_date, DATE '0001-01-01')
                    ),
                    DATE '0001-01-01'
                ) AS last_observed_date,
                coalesce(a.asset_contractor_flag, false) AS asset_contractor_flag,
                coalesce(a.asset_private_economic_activity_flag, false)
                    AS asset_private_economic_activity_flag,
                coalesce(a.asset_corporate_interest_flag, false)
                    AS asset_corporate_interest_flag,
                coalesce(a.asset_board_interest_flag, false) AS asset_board_interest_flag,
                coalesce(c.conflict_contractor_flag, false) AS conflict_contractor_flag,
                coalesce(c.conflict_flag_count, 0) AS conflict_flag_count,
                coalesce(c.conflict_direct_interest_flag, false)
                    AS conflict_direct_interest_flag,
                coalesce(c.conflict_relative_flag, false) AS conflict_relative_flag,
                coalesce(c.conflict_donation_flag, false) AS conflict_donation_flag,
                coalesce(c.conflict_other_flag, false) AS conflict_other_flag,
                coalesce(c.conflict_trust_flag, false) AS conflict_trust_flag,
                coalesce(c.conflict_investment_flag, false)
                    AS conflict_investment_flag,
                list_concat(
                    list_concat(
                        coalesce(ae.asset_evidence_refs, []::VARCHAR[]),
                        coalesce(ce.conflict_evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(se.sensitive_evidence_refs, []::VARCHAR[])
                ) AS declaration_evidence_refs
            FROM profile_keys k
            LEFT JOIN asset_profile a
                ON a.person_document_key = k.person_document_key
            LEFT JOIN conflict_profile c
                ON c.person_document_key = k.person_document_key
            LEFT JOIN sensitive_profile s
                ON s.person_document_key = k.person_document_key
            LEFT JOIN asset_evidence ae
                ON ae.person_document_key = k.person_document_key
            LEFT JOIN conflict_evidence ce
                ON ce.person_document_key = k.person_document_key
            LEFT JOIN sensitive_evidence se
                ON se.person_document_key = k.person_document_key
        ),
        company_officers AS (
            SELECT *
            FROM (
                SELECT
                    coacc_document_key(document_id, identification_class)
                        AS company_document_key,
                    coacc_nit_canonical(document_id, identification_class)
                        AS company_nit_canonical,
                    nullif(trim(document_id), '') AS company_document_id,
                    nullif(trim(identification_class), '') AS company_document_type,
                    nullif(trim(business_name), '') AS company_name,
                    nullif(trim(matricula), '') AS matricula,
                    nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                    nullif(trim(matricula_status), '') AS matricula_status,
                    nullif(trim(matricula_category), '') AS matricula_category,
                    coacc_cedula_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    ) AS representative_document_key,
                    nullif(trim(num_identificacion_representante_legal), '')
                        AS representative_document_id,
                    nullif(trim(representante_legal), '') AS representative_name,
                    nullif(trim(clase_identificacion_rl), '') AS representative_doc_type,
                    'company_registry_c82u:' || coalesce(
                        nullif(trim(cast(":id" AS VARCHAR)), ''),
                        nullif(trim(matricula), ''),
                        nullif(trim(document_id), '')
                    ) AS company_evidence_ref,
                    row_number() OVER (
                        PARTITION BY
                            coacc_document_key(document_id, identification_class),
                            coacc_cedula_key(
                                num_identificacion_representante_legal,
                                clase_identificacion_rl
                            )
                        ORDER BY matricula DESC NULLS LAST
                    ) AS officer_rank
                FROM src_company_registry_c82u
                WHERE coacc_nit_canonical(document_id, identification_class) IS NOT NULL
                    AND coacc_document_key(document_id, identification_class) IS NOT NULL
                    AND coacc_cedula_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    ) IS NOT NULL
            )
            WHERE officer_rank = 1
                AND length(representative_document_key) >= 5
                AND NOT regexp_matches(representative_document_key, '^0+$')
                AND representative_document_key != company_document_key
        ),
        candidates AS (
            SELECT
                c.*,
                o.company_nit_canonical,
                o.company_document_id,
                o.company_document_type,
                o.company_name,
                o.matricula,
                o.chamber_of_commerce,
                o.matricula_status,
                o.matricula_category,
                o.representative_document_key,
                o.representative_document_id,
                o.representative_name,
                o.representative_doc_type,
                o.company_evidence_ref,
                d.person_document_id,
                d.person_doc_type,
                d.person_name,
                d.asset_entity_name,
                d.conflict_entity_name,
                d.sensitive_institution_id,
                d.sensitive_institution_name,
                d.declarant_role,
                d.sensitive_job_hierarchy_level,
                d.sensitive_appointment_type,
                d.sensitive_current_job_title,
                d.asset_disclosure_count,
                d.conflict_disclosure_count,
                d.sensitive_position_count,
                d.first_observed_date,
                d.last_observed_date,
                d.asset_contractor_flag,
                d.asset_private_economic_activity_flag,
                d.asset_corporate_interest_flag,
                d.asset_board_interest_flag,
                d.conflict_contractor_flag,
                d.conflict_flag_count,
                d.conflict_direct_interest_flag,
                d.conflict_relative_flag,
                d.conflict_donation_flag,
                d.conflict_other_flag,
                d.conflict_trust_flag,
                d.conflict_investment_flag,
                d.declaration_evidence_refs
            FROM curated_cross_signal_compound_risk c
            JOIN company_officers o
                ON o.company_document_key = c.entity_key
            JOIN declaration_profile d
                ON d.person_document_key = o.representative_document_key
            WHERE c.entity_label = 'Company'
                AND c.identity_quality = 'exact'
                AND c.family_count >= 3
                AND (
                    d.conflict_disclosure_count > 0
                    OR d.asset_contractor_flag
                    OR d.conflict_contractor_flag
                    OR d.asset_private_economic_activity_flag
                    OR d.asset_corporate_interest_flag
                    OR d.asset_board_interest_flag
                    OR (
                        d.sensitive_position_count > 0
                        AND c.family_count >= 4
                    )
                )
        )
        SELECT
            'public_declaration_company_bridge_current_risk_review_only' AS signal_id,
            entity_id,
            entity_key,
            entity_label,
            'declaration_company_bridge:' || representative_document_key || ':' || entity_key
                AS scope_key,
            'declaration_company_bridge_current_risk' AS scope_type,
            CASE
                WHEN severity = 'critical'
                    AND (
                        family_count >= 4
                        OR has_sanctions
                        OR has_service_delivery
                        OR conflict_flag_count > 0
                    )
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.99,
                coalesce(risk_signal, 0.70)
                    + CASE WHEN family_count >= 4 THEN 0.04 ELSE 0.0 END
                    + CASE WHEN conflict_disclosure_count > 0 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN conflict_flag_count > 0 THEN 0.03 ELSE 0.0 END
                    + CASE
                        WHEN asset_corporate_interest_flag OR asset_board_interest_flag
                            THEN 0.03
                        ELSE 0.0
                    END
                    + CASE WHEN sensitive_position_count > 0 THEN 0.02 ELSE 0.0 END
            ) AS risk_signal,
            identity_confidence,
            identity_match_type,
            identity_quality,
            severity AS base_cross_signal_severity,
            risk_signal AS base_cross_signal_risk_signal,
            signal_count AS current_signal_count,
            family_count AS current_family_count,
            feature_row_count AS current_feature_row_count,
            scope_count AS current_scope_count,
            signal_ids AS current_signal_ids,
            signal_families AS current_signal_families,
            has_campaign_finance,
            has_conflict_interest,
            has_sanctions,
            has_corporate_capacity,
            has_execution_failure,
            has_project_royalties,
            has_procurement_competition,
            has_service_delivery,
            company_nit_canonical,
            company_document_id,
            company_document_type,
            company_name,
            matricula,
            chamber_of_commerce,
            matricula_status,
            matricula_category,
            representative_document_key,
            representative_document_id,
            representative_name,
            representative_doc_type,
            person_document_id,
            person_doc_type,
            person_name,
            asset_entity_name,
            conflict_entity_name,
            sensitive_institution_id,
            sensitive_institution_name,
            declarant_role,
            sensitive_job_hierarchy_level,
            sensitive_appointment_type,
            sensitive_current_job_title,
            asset_disclosure_count,
            conflict_disclosure_count,
            sensitive_position_count,
            first_observed_date,
            last_observed_date,
            asset_contractor_flag,
            asset_private_economic_activity_flag,
            asset_corporate_interest_flag,
            asset_board_interest_flag,
            conflict_contractor_flag,
            conflict_flag_count,
            conflict_direct_interest_flag,
            conflict_relative_flag,
            conflict_donation_flag,
            conflict_other_flag,
            conflict_trust_flag,
            conflict_investment_flag,
            (
                'public declaration or sensitive-position identity plus RUES '
                || 'company-representative bridge and current compound-risk '
                || 'evidence does not prove ownership, beneficial control, '
                || 'undeclared interest, conflict of interest, legal disability, '
                || 'contract irregularity, nonperformance, or corrupt intent; '
                || 'it prioritizes source-document, role-date, and corporate-file review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    list_concat(
                        [
                            'signal_feature_cross_signal_compound_risk_review_only:'
                                || scope_key
                        ],
                        coalesce(evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(declaration_evidence_refs, []::VARCHAR[])
                ),
                [company_evidence_ref]
            ) AS evidence_refs
        FROM candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN severity = 'critical'
                        AND (
                            family_count >= 4
                            OR has_sanctions
                            OR has_service_delivery
                            OR conflict_flag_count > 0
                        )
                        THEN 1
                    ELSE 0
                END DESC,
                family_count DESC,
                signal_count DESC,
                conflict_flag_count DESC,
                sensitive_position_count DESC,
                entity_key,
                representative_document_key
        ) <= 1000
    """)


def _create_secop_i_legacy_supplier_current_risk_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {"secop_i_historical_processes", *_CROSS_SIGNAL_REQUIRED_SOURCES}
    if not (required <= set(required_sources)):
        return

    secop_i_columns = {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM src_secop_i_historical_processes"
        ).fetchall()
    }

    def secop_i_col(column: str, sql_type: str = "VARCHAR") -> str:
        if column in secop_i_columns:
            escaped = column.replace('"', '""')
            return f'"{escaped}"'
        return f"NULL::{sql_type}"

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW curated_secop_i_legacy_supplier_current_risk AS
        WITH legacy_raw AS (
            SELECT
                row_number() OVER () AS legacy_source_row_id,
                coacc_document_key(contractor_id, contractor_id_type)
                    AS contractor_document_key,
                coacc_nit_canonical(contractor_id, contractor_id_type)
                    AS contractor_nit_canonical,
                nullif(trim(contractor_id), '') AS contractor_document_id,
                nullif(trim(contractor_id_type), '') AS contractor_document_type,
                nullif(trim(contractor_business_name), '') AS contractor_name,
                nullif(trim(legal_rep_id), '') AS legal_rep_document_id,
                coacc_cedula_key(legal_rep_id, legal_rep_doc_type)
                    AS legal_rep_document_key,
                nullif(trim(legal_rep_name), '') AS legal_rep_name,
                nullif(trim(entity_nit), '') AS entity_nit,
                coacc_nit_canonical(entity_nit, 'NIT') AS entity_nit_canonical,
                nullif(trim(entity_name), '') AS entity_name,
                nullif(trim(entity_department), '') AS entity_department,
                nullif(trim(entity_municipality), '') AS entity_municipality,
                nullif(trim(procurement_modality), '') AS procurement_modality,
                nullif(trim(contracting_regime_name), '') AS contracting_regime_name,
                nullif(trim(process_status), '') AS process_status,
                nullif(trim(contract_type), '') AS contract_type,
                nullif(trim(contract_object), '') AS contract_object,
                nullif(trim(contract_object_detail), '') AS contract_object_detail,
                nullif(trim({secop_i_col("group_name")}), '') AS group_name,
                nullif(trim({secop_i_col("family_name")}), '') AS family_name,
                nullif(trim({secop_i_col("class_name")}), '') AS class_name,
                nullif(trim(certificate_number), '') AS certificate_number,
                nullif(trim(process_number), '') AS process_number,
                nullif(trim(contract_number), '') AS contract_number,
                nullif(trim(award_id), '') AS award_id,
                coacc_reference_url(process_url_secop_i) AS process_url,
                coalesce(
                    try_cast(contract_signing_date AS TIMESTAMP)::DATE,
                    try_cast(contract_signing_date AS DATE),
                    try_strptime(
                        nullif(trim(cast(contract_signing_date AS VARCHAR)), ''),
                        '%Y-%m-%dT%H:%M:%S.%n'
                    )::DATE
                ) AS contract_signing_date,
                try_cast(contract_signing_year AS INTEGER) AS contract_signing_year,
                coalesce(
                    coacc_money_decimal(contract_value_with_additions),
                    coacc_money_decimal(contract_amount),
                    coacc_money_decimal(process_amount)
                ) AS legacy_contract_value,
                coacc_money_decimal(contract_amount) AS legacy_base_contract_value,
                coacc_money_decimal(process_amount) AS legacy_process_value,
                coacc_money_decimal(total_additions_value) AS legacy_addition_value,
                try_cast(additions_days AS INTEGER) AS additions_days,
                try_cast(additions_months AS INTEGER) AS additions_months,
                nullif(trim(additions_marker), '') AS additions_marker,
                nullif(trim({secop_i_col("is_postconflict")}), '') AS is_postconflict,
                nullif(trim({secop_i_col("meets_t302_ruling")}), '') AS meets_t302_ruling,
                nullif(trim({secop_i_col("bpin_code")}), '') AS bpin_code,
                coalesce(
                    nullif(trim(uid), ''),
                    nullif(trim(award_id), ''),
                    nullif(trim(certificate_number), ''),
                    cast(row_number() OVER () AS VARCHAR)
                ) AS legacy_record_id
            FROM src_secop_i_historical_processes
            WHERE coacc_nit_canonical(contractor_id, contractor_id_type) IS NOT NULL
                AND coacc_document_key(contractor_id, contractor_id_type) IS NOT NULL
                AND upper(coalesce(contractor_id_type, '')) LIKE '%NIT%'
                AND upper(coalesce(process_status, '')) LIKE '%CELEBRADO%'
                AND coacc_document_key(contractor_id, contractor_id_type) NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
                AND NOT regexp_matches(
                    coacc_document_key(contractor_id, contractor_id_type),
                    '^0+$'
                )
        ),
        legacy_eligible AS (
            SELECT
                *,
                lower(
                    coalesce(procurement_modality, '') || ' ' ||
                    coalesce(contracting_regime_name, '')
                ) AS modality_text,
                (
                    legacy_addition_value IS NOT NULL
                    AND legacy_addition_value >= 100000000
                ) AS material_addition_flag,
                (
                    try_cast(additions_marker AS INTEGER) > 0
                    OR coalesce(additions_days, 0) > 0
                    OR coalesce(additions_months, 0) > 0
                    OR (
                        legacy_addition_value IS NOT NULL
                        AND legacy_addition_value > 0
                    )
                ) AS any_addition_flag,
                regexp_matches(
                    lower(
                        coalesce(procurement_modality, '') || ' ' ||
                        coalesce(contracting_regime_name, '')
                    ),
                    'directa|menor cuant|contrataci[oó]n directa|convenio'
                ) AS direct_or_exception_modality_flag,
                'secop_i_historical_processes:' || legacy_record_id
                    AS legacy_evidence_ref
            FROM legacy_raw
            WHERE legacy_contract_value IS NOT NULL
                AND legacy_contract_value >= 100000000
        ),
        legacy_rollup AS (
            SELECT
                contractor_document_key,
                any_value(contractor_nit_canonical) AS contractor_nit_canonical,
                any_value(contractor_document_id) AS contractor_document_id,
                any_value(contractor_document_type) AS contractor_document_type,
                min(contractor_name) FILTER (WHERE contractor_name IS NOT NULL)
                    AS contractor_name,
                count(DISTINCT legacy_record_id) AS legacy_contract_count,
                count(DISTINCT entity_nit) FILTER (WHERE entity_nit IS NOT NULL)
                    AS legacy_buyer_count,
                count(DISTINCT entity_department)
                    FILTER (WHERE entity_department IS NOT NULL)
                    AS legacy_department_count,
                sum(legacy_contract_value) AS legacy_total_contract_value,
                sum(coalesce(legacy_addition_value, 0)) AS legacy_total_addition_value,
                max(legacy_contract_value) AS legacy_max_contract_value,
                min(contract_signing_year) AS legacy_first_year,
                max(contract_signing_year) AS legacy_last_year,
                min(contract_signing_date) AS legacy_first_signing_date,
                max(contract_signing_date) AS legacy_last_signing_date,
                sum(CASE WHEN material_addition_flag THEN 1 ELSE 0 END)
                    AS legacy_material_addition_count,
                sum(CASE WHEN any_addition_flag THEN 1 ELSE 0 END)
                    AS legacy_any_addition_count,
                sum(CASE WHEN direct_or_exception_modality_flag THEN 1 ELSE 0 END)
                    AS legacy_direct_or_exception_count,
                sum(
                    CASE
                        WHEN nullif(trim(is_postconflict), '') IN ('1', 'true', 'TRUE', 'Si', 'SI')
                            THEN 1
                        ELSE 0
                    END
                ) AS legacy_postconflict_count,
                sum(
                    CASE
                        WHEN nullif(trim(meets_t302_ruling), '') IN ('1', 'true', 'TRUE', 'Si', 'SI')
                            THEN 1
                        ELSE 0
                    END
                ) AS legacy_t302_count,
                count(DISTINCT bpin_code)
                    FILTER (
                        WHERE bpin_code IS NOT NULL
                            AND coacc_text_key(bpin_code) <> coacc_text_key('No definido')
                    ) AS legacy_bpin_count,
                list(DISTINCT entity_department ORDER BY entity_department)
                    FILTER (WHERE entity_department IS NOT NULL)
                    AS legacy_departments,
                list(DISTINCT procurement_modality ORDER BY procurement_modality)
                    FILTER (WHERE procurement_modality IS NOT NULL)
                    AS legacy_procurement_modalities
            FROM legacy_eligible
            GROUP BY contractor_document_key
        ),
        legacy_evidence_ranked AS (
            SELECT
                contractor_document_key,
                legacy_evidence_ref,
                legacy_contract_value,
                legacy_addition_value,
                direct_or_exception_modality_flag,
                material_addition_flag,
                contract_signing_date,
                row_number() OVER (
                    PARTITION BY contractor_document_key
                    ORDER BY
                        material_addition_flag DESC,
                        direct_or_exception_modality_flag DESC,
                        legacy_contract_value DESC NULLS LAST,
                        contract_signing_date DESC NULLS LAST,
                        legacy_evidence_ref
                ) AS evidence_rank
            FROM legacy_eligible
        ),
        legacy_evidence AS (
            SELECT
                contractor_document_key,
                list(legacy_evidence_ref ORDER BY evidence_rank)
                    AS legacy_evidence_refs
            FROM legacy_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY contractor_document_key
        ),
        candidates AS (
            SELECT
                c.*,
                l.contractor_nit_canonical,
                l.contractor_document_id,
                l.contractor_document_type,
                l.contractor_name AS legacy_contractor_name,
                l.legacy_contract_count,
                l.legacy_buyer_count,
                l.legacy_department_count,
                l.legacy_total_contract_value,
                l.legacy_total_addition_value,
                l.legacy_max_contract_value,
                l.legacy_first_year,
                l.legacy_last_year,
                l.legacy_first_signing_date,
                l.legacy_last_signing_date,
                l.legacy_material_addition_count,
                l.legacy_any_addition_count,
                l.legacy_direct_or_exception_count,
                l.legacy_postconflict_count,
                l.legacy_t302_count,
                l.legacy_bpin_count,
                l.legacy_departments,
                l.legacy_procurement_modalities,
                e.legacy_evidence_refs
            FROM curated_cross_signal_compound_risk c
            JOIN legacy_rollup l
                ON l.contractor_document_key = c.entity_key
            LEFT JOIN legacy_evidence e
                ON e.contractor_document_key = l.contractor_document_key
            WHERE c.entity_label = 'Company'
                AND c.identity_quality = 'exact'
                AND l.legacy_total_contract_value >= 1000000000
                AND l.legacy_contract_count >= 2
        )
        SELECT
            'secop_i_legacy_supplier_current_risk_review_only' AS signal_id,
            entity_id,
            entity_key,
            entity_label,
            'secop_i_legacy_current_risk:' || entity_key AS scope_key,
            'secop_i_legacy_supplier_current_risk' AS scope_type,
            CASE
                WHEN severity = 'critical'
                    AND (
                        family_count >= 4
                        OR legacy_total_contract_value >= 50000000000
                        OR legacy_total_addition_value >= 5000000000
                    )
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.99,
                coalesce(risk_signal, 0.70)
                    + least(log10(greatest(legacy_total_contract_value, 1)) / 150.0, 0.06)
                    + CASE WHEN legacy_material_addition_count > 0 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN legacy_direct_or_exception_count >= 3 THEN 0.02 ELSE 0.0 END
                    + CASE WHEN legacy_buyer_count >= 10 THEN 0.02 ELSE 0.0 END
                    + CASE WHEN family_count >= 4 THEN 0.03 ELSE 0.0 END
            ) AS risk_signal,
            identity_confidence,
            identity_match_type,
            identity_quality,
            severity AS base_cross_signal_severity,
            risk_signal AS base_cross_signal_risk_signal,
            signal_count AS current_signal_count,
            family_count AS current_family_count,
            feature_row_count AS current_feature_row_count,
            scope_count AS current_scope_count,
            signal_ids AS current_signal_ids,
            signal_families AS current_signal_families,
            has_campaign_finance,
            has_conflict_interest,
            has_sanctions,
            has_corporate_capacity,
            has_execution_failure,
            has_project_royalties,
            has_procurement_competition,
            has_service_delivery,
            contractor_nit_canonical,
            contractor_document_id,
            contractor_document_type,
            legacy_contractor_name,
            legacy_contract_count,
            legacy_buyer_count,
            legacy_department_count,
            legacy_total_contract_value,
            legacy_total_addition_value,
            legacy_max_contract_value,
            legacy_first_year,
            legacy_last_year,
            legacy_first_signing_date,
            legacy_last_signing_date,
            legacy_material_addition_count,
            legacy_any_addition_count,
            legacy_direct_or_exception_count,
            legacy_postconflict_count,
            legacy_t302_count,
            legacy_bpin_count,
            legacy_departments,
            legacy_procurement_modalities,
            (
                'SECOP I historical supplier exposure plus current exact-NIT '
                || 'compound risk convergence does not prove continuity of '
                || 'management, legal disability, a legacy irregularity, a current '
                || 'irregularity, contract breach, nonperformance, or corrupt intent; '
                || 'it prioritizes source-document and corporate-history review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    [
                        'signal_feature_cross_signal_compound_risk_review_only:'
                            || scope_key
                    ],
                    coalesce(evidence_refs, []::VARCHAR[])
                ),
                coalesce(legacy_evidence_refs, []::VARCHAR[])
            ) AS evidence_refs
        FROM candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN severity = 'critical'
                        AND (
                            family_count >= 4
                            OR legacy_total_contract_value >= 50000000000
                            OR legacy_total_addition_value >= 5000000000
                        )
                        THEN 1
                    ELSE 0
                END DESC,
                family_count DESC,
                signal_count DESC,
                legacy_total_contract_value DESC NULLS LAST,
                entity_key
        ) <= 1000
    """)


def _create_secop_i_legacy_representative_current_risk_views(
    con: duckdb.DuckDBPyConnection,
    required_sources: Sequence[str],
) -> None:
    required = {"secop_i_historical_processes", *_CROSS_SIGNAL_REQUIRED_SOURCES}
    if not (required <= set(required_sources)):
        return

    secop_i_columns = {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM src_secop_i_historical_processes"
        ).fetchall()
    }

    def secop_i_col(column: str, sql_type: str = "VARCHAR") -> str:
        if column in secop_i_columns:
            escaped = column.replace('"', '""')
            return f'"{escaped}"'
        return f"NULL::{sql_type}"

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW curated_secop_i_legacy_representative_current_risk AS
        WITH legacy_raw AS (
            SELECT
                row_number() OVER () AS legacy_source_row_id,
                coacc_document_key(contractor_id, contractor_id_type)
                    AS contractor_document_key,
                coacc_nit_canonical(contractor_id, contractor_id_type)
                    AS contractor_nit_canonical,
                nullif(trim(contractor_id), '') AS contractor_document_id,
                nullif(trim(contractor_id_type), '') AS contractor_document_type,
                nullif(trim(contractor_business_name), '') AS contractor_name,
                nullif(trim(legal_rep_id), '') AS legal_rep_document_id,
                coacc_cedula_key(legal_rep_id, legal_rep_doc_type)
                    AS legal_rep_document_key,
                nullif(trim(legal_rep_name), '') AS legal_rep_name,
                nullif(trim(legal_rep_doc_type), '') AS legal_rep_doc_type,
                nullif(trim(entity_nit), '') AS entity_nit,
                coacc_nit_canonical(entity_nit, 'NIT') AS entity_nit_canonical,
                nullif(trim(entity_name), '') AS entity_name,
                nullif(trim(entity_department), '') AS entity_department,
                nullif(trim(entity_municipality), '') AS entity_municipality,
                nullif(trim(procurement_modality), '') AS procurement_modality,
                nullif(trim(contracting_regime_name), '') AS contracting_regime_name,
                nullif(trim(process_status), '') AS process_status,
                nullif(trim(contract_type), '') AS contract_type,
                nullif(trim(contract_object), '') AS contract_object,
                nullif(trim(contract_object_detail), '') AS contract_object_detail,
                nullif(trim({secop_i_col("group_name")}), '') AS group_name,
                nullif(trim({secop_i_col("family_name")}), '') AS family_name,
                nullif(trim({secop_i_col("class_name")}), '') AS class_name,
                nullif(trim(certificate_number), '') AS certificate_number,
                nullif(trim(process_number), '') AS process_number,
                nullif(trim(contract_number), '') AS contract_number,
                nullif(trim(award_id), '') AS award_id,
                coacc_reference_url(process_url_secop_i) AS process_url,
                coalesce(
                    try_cast(contract_signing_date AS TIMESTAMP)::DATE,
                    try_cast(contract_signing_date AS DATE),
                    try_strptime(
                        nullif(trim(cast(contract_signing_date AS VARCHAR)), ''),
                        '%Y-%m-%dT%H:%M:%S.%n'
                    )::DATE
                ) AS contract_signing_date,
                try_cast(contract_signing_year AS INTEGER) AS contract_signing_year,
                coalesce(
                    coacc_money_decimal(contract_value_with_additions),
                    coacc_money_decimal(contract_amount),
                    coacc_money_decimal(process_amount)
                ) AS legacy_contract_value,
                coacc_money_decimal(contract_amount) AS legacy_base_contract_value,
                coacc_money_decimal(process_amount) AS legacy_process_value,
                coacc_money_decimal(total_additions_value) AS legacy_addition_value,
                try_cast(additions_days AS INTEGER) AS additions_days,
                try_cast(additions_months AS INTEGER) AS additions_months,
                nullif(trim(additions_marker), '') AS additions_marker,
                nullif(trim({secop_i_col("is_postconflict")}), '') AS is_postconflict,
                nullif(trim({secop_i_col("meets_t302_ruling")}), '') AS meets_t302_ruling,
                nullif(trim({secop_i_col("bpin_code")}), '') AS bpin_code,
                coalesce(
                    nullif(trim(uid), ''),
                    nullif(trim(award_id), ''),
                    nullif(trim(certificate_number), ''),
                    cast(row_number() OVER () AS VARCHAR)
                ) AS legacy_record_id
            FROM src_secop_i_historical_processes
            WHERE coacc_document_key(contractor_id, contractor_id_type) IS NOT NULL
                AND coacc_nit_canonical(contractor_id, contractor_id_type) IS NOT NULL
                AND upper(coalesce(contractor_id_type, '')) LIKE '%NIT%'
                AND coacc_cedula_key(legal_rep_id, legal_rep_doc_type) IS NOT NULL
                AND upper(coalesce(process_status, '')) LIKE '%CELEBRADO%'
        ),
        legacy_eligible AS (
            SELECT
                *,
                lower(
                    coalesce(procurement_modality, '') || ' ' ||
                    coalesce(contracting_regime_name, '')
                ) AS modality_text,
                (
                    legacy_addition_value IS NOT NULL
                    AND legacy_addition_value >= 100000000
                ) AS material_addition_flag,
                (
                    try_cast(additions_marker AS INTEGER) > 0
                    OR coalesce(additions_days, 0) > 0
                    OR coalesce(additions_months, 0) > 0
                    OR (
                        legacy_addition_value IS NOT NULL
                        AND legacy_addition_value > 0
                    )
                ) AS any_addition_flag,
                regexp_matches(
                    lower(
                        coalesce(procurement_modality, '') || ' ' ||
                        coalesce(contracting_regime_name, '')
                    ),
                    'directa|menor cuant|contrataci[oó]n directa|convenio'
                ) AS direct_or_exception_modality_flag,
                'secop_i_historical_processes:' || legacy_record_id
                    AS legacy_evidence_ref
            FROM legacy_raw
            WHERE legacy_contract_value IS NOT NULL
                AND legacy_contract_value >= 100000000
                AND length(legal_rep_document_key) >= 5
                AND legal_rep_document_key NOT IN (
                    '999999999',
                    '9999999999',
                    '111111111',
                    '1111111111'
                )
                AND NOT regexp_matches(legal_rep_document_key, '^0+$')
                AND legal_rep_document_key != contractor_document_key
        ),
        legacy_rep_rollup AS (
            SELECT
                legal_rep_document_key AS representative_document_key,
                min(legal_rep_document_id)
                    FILTER (WHERE legal_rep_document_id IS NOT NULL)
                    AS legacy_representative_document_id,
                min(legal_rep_doc_type) FILTER (WHERE legal_rep_doc_type IS NOT NULL)
                    AS legacy_representative_doc_type,
                min(legal_rep_name) FILTER (WHERE legal_rep_name IS NOT NULL)
                    AS legacy_representative_name,
                count(DISTINCT contractor_document_key) AS legacy_contractor_count,
                count(DISTINCT legacy_record_id) AS legacy_contract_count,
                count(DISTINCT entity_nit) FILTER (WHERE entity_nit IS NOT NULL)
                    AS legacy_buyer_count,
                count(DISTINCT entity_department)
                    FILTER (WHERE entity_department IS NOT NULL)
                    AS legacy_department_count,
                sum(legacy_contract_value) AS legacy_total_contract_value,
                sum(coalesce(legacy_addition_value, 0)) AS legacy_total_addition_value,
                max(legacy_contract_value) AS legacy_max_contract_value,
                min(contract_signing_year) AS legacy_first_year,
                max(contract_signing_year) AS legacy_last_year,
                min(contract_signing_date) AS legacy_first_signing_date,
                max(contract_signing_date) AS legacy_last_signing_date,
                sum(CASE WHEN material_addition_flag THEN 1 ELSE 0 END)
                    AS legacy_material_addition_count,
                sum(CASE WHEN any_addition_flag THEN 1 ELSE 0 END)
                    AS legacy_any_addition_count,
                sum(CASE WHEN direct_or_exception_modality_flag THEN 1 ELSE 0 END)
                    AS legacy_direct_or_exception_count,
                sum(
                    CASE
                        WHEN nullif(trim(is_postconflict), '') IN ('1', 'true', 'TRUE', 'Si', 'SI')
                            THEN 1
                        ELSE 0
                    END
                ) AS legacy_postconflict_count,
                sum(
                    CASE
                        WHEN nullif(trim(meets_t302_ruling), '') IN ('1', 'true', 'TRUE', 'Si', 'SI')
                            THEN 1
                        ELSE 0
                    END
                ) AS legacy_t302_count,
                count(DISTINCT bpin_code)
                    FILTER (
                        WHERE bpin_code IS NOT NULL
                            AND coacc_text_key(bpin_code) <> coacc_text_key('No definido')
                    ) AS legacy_bpin_count,
                list(DISTINCT entity_department ORDER BY entity_department)
                    FILTER (WHERE entity_department IS NOT NULL)
                    AS legacy_departments,
                list(DISTINCT procurement_modality ORDER BY procurement_modality)
                    FILTER (WHERE procurement_modality IS NOT NULL)
                    AS legacy_procurement_modalities
            FROM legacy_eligible
            GROUP BY legal_rep_document_key
        ),
        legacy_contractor_samples_ranked AS (
            SELECT
                legal_rep_document_key AS representative_document_key,
                coalesce(contractor_name, contractor_document_key)
                    AS legacy_contractor_sample,
                sum(legacy_contract_value) AS contractor_legacy_total_value,
                row_number() OVER (
                    PARTITION BY legal_rep_document_key
                    ORDER BY sum(legacy_contract_value) DESC NULLS LAST,
                        coalesce(contractor_name, contractor_document_key)
                ) AS sample_rank
            FROM legacy_eligible
            GROUP BY legal_rep_document_key,
                coalesce(contractor_name, contractor_document_key)
        ),
        legacy_contractor_samples AS (
            SELECT
                representative_document_key,
                list(
                    legacy_contractor_sample
                    ORDER BY contractor_legacy_total_value DESC,
                        legacy_contractor_sample
                ) AS legacy_contractor_samples
            FROM legacy_contractor_samples_ranked
            WHERE sample_rank <= 8
            GROUP BY representative_document_key
        ),
        legacy_evidence_ranked AS (
            SELECT
                legal_rep_document_key AS representative_document_key,
                legacy_evidence_ref,
                legacy_contract_value,
                legacy_addition_value,
                direct_or_exception_modality_flag,
                material_addition_flag,
                contract_signing_date,
                row_number() OVER (
                    PARTITION BY legal_rep_document_key
                    ORDER BY
                        material_addition_flag DESC,
                        direct_or_exception_modality_flag DESC,
                        legacy_contract_value DESC NULLS LAST,
                        contract_signing_date DESC NULLS LAST,
                        legacy_evidence_ref
                ) AS evidence_rank
            FROM legacy_eligible
        ),
        legacy_evidence AS (
            SELECT
                representative_document_key,
                list(legacy_evidence_ref ORDER BY evidence_rank)
                    AS legacy_evidence_refs
            FROM legacy_evidence_ranked
            WHERE evidence_rank <= 5
            GROUP BY representative_document_key
        ),
        company_officers AS (
            SELECT *
            FROM (
                SELECT
                    coacc_document_key(document_id, identification_class)
                        AS company_document_key,
                    coacc_nit_canonical(document_id, identification_class)
                        AS company_nit_canonical,
                    nullif(trim(document_id), '') AS company_document_id,
                    nullif(trim(identification_class), '') AS company_document_type,
                    nullif(trim(business_name), '') AS company_name,
                    nullif(trim(matricula), '') AS matricula,
                    nullif(trim(chamber_of_commerce), '') AS chamber_of_commerce,
                    nullif(trim(matricula_status), '') AS matricula_status,
                    nullif(trim(matricula_category), '') AS matricula_category,
                    coacc_cedula_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    ) AS representative_document_key,
                    nullif(trim(num_identificacion_representante_legal), '')
                        AS representative_document_id,
                    nullif(trim(representante_legal), '') AS representative_name,
                    nullif(trim(clase_identificacion_rl), '') AS representative_doc_type,
                    'company_registry_c82u:' || coalesce(
                        nullif(trim(cast(":id" AS VARCHAR)), ''),
                        nullif(trim(matricula), ''),
                        nullif(trim(document_id), '')
                    ) AS company_evidence_ref,
                    row_number() OVER (
                        PARTITION BY
                            coacc_document_key(document_id, identification_class),
                            coacc_cedula_key(
                                num_identificacion_representante_legal,
                                clase_identificacion_rl
                            )
                        ORDER BY matricula DESC NULLS LAST
                    ) AS officer_rank
                FROM src_company_registry_c82u
                WHERE coacc_nit_canonical(document_id, identification_class) IS NOT NULL
                    AND coacc_document_key(document_id, identification_class) IS NOT NULL
                    AND coacc_cedula_key(
                        num_identificacion_representante_legal,
                        clase_identificacion_rl
                    ) IS NOT NULL
            )
            WHERE officer_rank = 1
                AND length(representative_document_key) >= 5
                AND NOT regexp_matches(representative_document_key, '^0+$')
                AND representative_document_key != company_document_key
        ),
        candidate_base AS (
            SELECT
                c.*,
                c.scope_key AS base_cross_signal_scope_key,
                o.company_document_key AS current_company_key,
                o.company_nit_canonical AS current_company_nit_canonical,
                o.company_document_id AS current_company_document_id,
                o.company_document_type AS current_company_document_type,
                o.company_name AS current_company_name,
                o.matricula AS current_matricula,
                o.chamber_of_commerce AS current_chamber_of_commerce,
                o.matricula_status AS current_matricula_status,
                o.matricula_category AS current_matricula_category,
                o.representative_document_key,
                o.representative_document_id,
                o.representative_name,
                o.representative_doc_type,
                o.company_evidence_ref,
                l.legacy_representative_document_id,
                l.legacy_representative_doc_type,
                l.legacy_representative_name,
                l.legacy_contractor_count,
                l.legacy_contract_count,
                l.legacy_buyer_count,
                l.legacy_department_count,
                l.legacy_total_contract_value,
                l.legacy_total_addition_value,
                l.legacy_max_contract_value,
                l.legacy_first_year,
                l.legacy_last_year,
                l.legacy_first_signing_date,
                l.legacy_last_signing_date,
                l.legacy_material_addition_count,
                l.legacy_any_addition_count,
                l.legacy_direct_or_exception_count,
                l.legacy_postconflict_count,
                l.legacy_t302_count,
                l.legacy_bpin_count,
                l.legacy_departments,
                l.legacy_procurement_modalities,
                s.legacy_contractor_samples,
                e.legacy_evidence_refs
            FROM curated_cross_signal_compound_risk c
            JOIN company_officers o
                ON o.company_document_key = c.entity_key
            JOIN legacy_rep_rollup l
                ON l.representative_document_key = o.representative_document_key
            LEFT JOIN legacy_contractor_samples s
                ON s.representative_document_key = l.representative_document_key
            LEFT JOIN legacy_evidence e
                ON e.representative_document_key = l.representative_document_key
            WHERE c.entity_label = 'Company'
                AND c.identity_quality = 'exact'
                AND c.family_count >= 3
                AND l.legacy_total_contract_value >= 5000000000
                AND (
                    l.legacy_total_addition_value >= 500000000
                    OR l.legacy_direct_or_exception_count >= 5
                    OR l.legacy_contract_count >= 10
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM legacy_eligible le
                    WHERE le.legal_rep_document_key = l.representative_document_key
                        AND le.contractor_document_key = c.entity_key
                )
        ),
        current_risk_company_counts AS (
            SELECT
                representative_document_key,
                count(DISTINCT entity_key) AS representative_current_risk_company_count
            FROM candidate_base
            GROUP BY representative_document_key
        ),
        candidates AS (
            SELECT
                b.*,
                rc.representative_current_risk_company_count
            FROM candidate_base b
            JOIN current_risk_company_counts rc
                ON rc.representative_document_key = b.representative_document_key
            WHERE rc.representative_current_risk_company_count <= 5
        )
        SELECT
            'secop_i_legacy_representative_current_risk_review_only' AS signal_id,
            entity_id,
            entity_key,
            entity_label,
            'secop_i_legacy_representative_current_risk:'
                || representative_document_key || ':' || entity_key AS scope_key,
            'secop_i_legacy_representative_current_risk' AS scope_type,
            CASE
                WHEN (
                    severity = 'critical'
                    AND (
                        family_count >= 4
                        OR legacy_total_contract_value >= 50000000000
                        OR legacy_total_addition_value >= 1000000000
                        OR legacy_direct_or_exception_count >= 10
                    )
                )
                    OR legacy_total_contract_value >= 100000000000
                    THEN 'critical'
                ELSE 'high'
            END AS severity,
            least(
                0.99,
                coalesce(risk_signal, 0.70)
                    + least(log10(greatest(legacy_total_contract_value, 1)) / 150.0, 0.06)
                    + CASE WHEN legacy_total_addition_value >= 500000000 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN legacy_direct_or_exception_count >= 5 THEN 0.03 ELSE 0.0 END
                    + CASE WHEN legacy_buyer_count >= 10 THEN 0.02 ELSE 0.0 END
                    + CASE WHEN family_count >= 4 THEN 0.03 ELSE 0.0 END
                    + CASE
                        WHEN representative_current_risk_company_count <= 2 THEN 0.02
                        ELSE 0.0
                    END
            ) AS risk_signal,
            identity_confidence,
            identity_match_type,
            identity_quality,
            severity AS base_cross_signal_severity,
            risk_signal AS base_cross_signal_risk_signal,
            signal_count AS current_signal_count,
            family_count AS current_family_count,
            feature_row_count AS current_feature_row_count,
            scope_count AS current_scope_count,
            signal_ids AS current_signal_ids,
            signal_families AS current_signal_families,
            has_campaign_finance,
            has_conflict_interest,
            has_sanctions,
            has_corporate_capacity,
            has_execution_failure,
            has_project_royalties,
            has_procurement_competition,
            has_service_delivery,
            current_company_key,
            current_company_nit_canonical,
            current_company_document_id,
            current_company_document_type,
            current_company_name,
            current_matricula,
            current_chamber_of_commerce,
            current_matricula_status,
            current_matricula_category,
            representative_document_key,
            representative_document_id,
            representative_name,
            representative_doc_type,
            legacy_representative_document_id,
            legacy_representative_doc_type,
            legacy_representative_name,
            representative_current_risk_company_count,
            legacy_contractor_count,
            legacy_contract_count,
            legacy_buyer_count,
            legacy_department_count,
            legacy_total_contract_value,
            legacy_total_addition_value,
            legacy_max_contract_value,
            legacy_first_year,
            legacy_last_year,
            legacy_first_signing_date,
            legacy_last_signing_date,
            legacy_material_addition_count,
            legacy_any_addition_count,
            legacy_direct_or_exception_count,
            legacy_postconflict_count,
            legacy_t302_count,
            legacy_bpin_count,
            legacy_departments,
            legacy_procurement_modalities,
            legacy_contractor_samples,
            true AS exact_representative_bridge_flag,
            true AS different_legacy_contractor_current_company_flag,
            (legacy_total_contract_value >= 5000000000) AS high_legacy_value_flag,
            (
                legacy_total_addition_value >= 500000000
                OR legacy_direct_or_exception_count >= 5
            ) AS legacy_additions_or_direct_flag,
            (family_count >= 3) AS current_compound_risk_flag,
            (representative_current_risk_company_count <= 5)
                AS bounded_current_company_bridge_flag,
            (
                'SECOP I historical legal-representative exposure plus a current '
                || 'RUES representative bridge into an exact-NIT compound-risk '
                || 'company does not prove ownership, beneficial control, legal '
                || 'disability, a legacy irregularity, a current irregularity, '
                || 'contract breach, nonperformance, or corrupt intent; it '
                || 'prioritizes source-document, corporate-file, and role-date review'
            ) AS what_is_unproven,
            list_concat(
                list_concat(
                    list_concat(
                        [
                            'signal_feature_cross_signal_compound_risk_review_only:'
                                || base_cross_signal_scope_key
                        ],
                        coalesce(evidence_refs, []::VARCHAR[])
                    ),
                    coalesce(legacy_evidence_refs, []::VARCHAR[])
                ),
                [company_evidence_ref]
            ) AS evidence_refs
        FROM candidates
        QUALIFY row_number() OVER (
            ORDER BY
                CASE
                    WHEN (
                        severity = 'critical'
                        AND (
                            family_count >= 4
                            OR legacy_total_contract_value >= 50000000000
                            OR legacy_total_addition_value >= 1000000000
                            OR legacy_direct_or_exception_count >= 10
                        )
                    )
                        OR legacy_total_contract_value >= 100000000000
                        THEN 1
                    ELSE 0
                END DESC,
                family_count DESC,
                signal_count DESC,
                legacy_total_contract_value DESC NULLS LAST,
                entity_key,
                representative_document_key
        ) <= 1000
    """)


def _create_views(con: duckdb.DuckDBPyConnection, required_sources: Sequence[str]) -> None:
    if not ({"secop_ii_contracts", "paco_sanctions"} & set(required_sources)):
        _create_person_views(con, required_sources)
        _create_process_views(con, required_sources)
        _create_cobidding_views(con, required_sources)
        _create_company_registry_overlap_views(con, required_sources)
        _create_related_bidders_same_process_views(con, required_sources)
        _create_contract_suspension_views(con, required_sources)
        _create_contract_execution_delay_views(con, required_sources)
        _create_project_bpin_views(con, required_sources)
        _create_bpin_priority_work_views(con, required_sources)
        _create_project_regalias_execution_views(con, required_sources)
        _create_supplier_identity_views(con, required_sources)
        _create_cuentas_claras_views(con, required_sources)
        _create_public_servant_conflict_disclosure_views(con, required_sources)
        _create_public_declaration_supplier_chronology_views(con, required_sources)
        _create_pida_full30_meta_views(con, required_sources)
        _create_pida_chain_views(con, required_sources)
        _create_secop_sanction_later_awards_views(con, required_sources)
        _create_siri_antecedent_procurement_chronology_views(con, required_sources)
        _create_guarantee_advance_execution_chain_views(con, required_sources)
        _create_guarantee_policy_reuse_views(con, required_sources)
        _create_budget_chain_reconciliation_views(con, required_sources)
        _create_tvec_multi_entity_capture_views(con, required_sources)
        _create_tvec_item_price_dispersion_views(con, required_sources)
        _create_large_modification_views(con, required_sources)
        _create_role_supplier_same_buyer_views(con, required_sources)
        _create_contract_modification_ladder_views(con, required_sources)
        _create_rues_supplier_capacity_status_views(con, required_sources)
        _create_sgr_ocad_executor_capacity_gap_views(con, required_sources)
        _create_dnp_sgr_beneficiary_delivery_gap_views(con, required_sources)
        _create_pae_beneficiary_territory_delivery_gap_views(con, required_sources)
        _create_interadmin_executor_network_views(con, required_sources)
        _create_cross_signal_compound_risk_views(con, required_sources)
        _create_public_declaration_company_bridge_current_risk_views(
            con,
            required_sources,
        )
        _create_secop_i_legacy_supplier_current_risk_views(con, required_sources)
        _create_secop_i_legacy_representative_current_risk_views(
            con,
            required_sources,
        )
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
    _create_project_bpin_views(con, required_sources)
    _create_bpin_priority_work_views(con, required_sources)
    _create_project_regalias_execution_views(con, required_sources)
    _create_cuentas_claras_views(con, required_sources)
    _create_public_servant_conflict_disclosure_views(con, required_sources)
    _create_public_declaration_supplier_chronology_views(con, required_sources)
    _create_pida_full30_meta_views(con, required_sources)
    _create_pida_chain_views(con, required_sources)
    _create_secop_sanction_later_awards_views(con, required_sources)
    _create_fiscal_procurement_chronology_views(con, required_sources)
    _create_siri_antecedent_procurement_chronology_views(con, required_sources)
    _create_guarantee_advance_execution_chain_views(con, required_sources)
    _create_guarantee_policy_reuse_views(con, required_sources)
    _create_budget_chain_reconciliation_views(con, required_sources)
    _create_invoice_budget_reconciliation_views(con, required_sources)
    _create_payment_plan_reconciliation_views(con, required_sources)
    _create_large_modification_views(con, required_sources)
    _create_health_pae_service_delivery_gap_views(con, required_sources)
    _create_pae_beneficiary_territory_delivery_gap_views(con, required_sources)
    _create_tvec_multi_entity_capture_views(con, required_sources)
    _create_tvec_item_price_dispersion_views(con, required_sources)
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
    _create_related_bidders_same_process_views(con, required_sources)
    _create_supplier_identity_views(con, required_sources)
    _create_role_supplier_same_buyer_views(con, required_sources)
    _create_contract_modification_ladder_views(con, required_sources)
    _create_rues_supplier_capacity_status_views(con, required_sources)
    _create_sgr_ocad_executor_capacity_gap_views(con, required_sources)
    _create_dnp_sgr_beneficiary_delivery_gap_views(con, required_sources)
    _create_interadmin_executor_network_views(con, required_sources)
    _create_cross_signal_compound_risk_views(con, required_sources)
    _create_public_declaration_company_bridge_current_risk_views(
        con,
        required_sources,
    )
    _create_secop_i_legacy_supplier_current_risk_views(con, required_sources)
    _create_secop_i_legacy_representative_current_risk_views(
        con,
        required_sources,
    )


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
    if table == "signal_feature_procurement_contract_modification_ladder_review_only":
        return "SELECT * FROM curated_contract_modification_ladder"
    if table == "signal_feature_procurement_sanctioned_supplier_awarded":
        return "SELECT * FROM curated_sanctioned_awards"
    if table == "signal_feature_procurement_secop_sanction_later_awards_review_only":
        return "SELECT * FROM curated_secop_sanction_later_awards"
    if table == "signal_feature_fiscal_procurement_chronology_review_only":
        return "SELECT * FROM curated_fiscal_procurement_chronology"
    if table == "signal_feature_siri_antecedent_procurement_chronology_review_only":
        return "SELECT * FROM curated_siri_antecedent_procurement_chronology"
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
    if table == "signal_feature_procurement_related_bidders_same_process_review_only":
        return "SELECT * FROM curated_related_bidders_same_process"
    if (
        table
        == "signal_feature_procurement_shared_representative_same_buyer_cluster_review_only"
    ):
        return "SELECT * FROM curated_shared_representative_same_buyer_cluster"
    if table == "signal_feature_procurement_payment_plan_anomalies":
        return "SELECT * FROM curated_payment_plan_anomalies"
    if table == "signal_feature_procurement_guarantee_advance_execution_chain":
        return "SELECT * FROM curated_guarantee_advance_execution_chain"
    if table == "signal_feature_procurement_guarantee_policy_reuse_review_only":
        return "SELECT * FROM curated_guarantee_policy_reuse"
    if table == "signal_feature_procurement_budget_chain_reconciliation_review_only":
        return "SELECT * FROM curated_budget_chain_reconciliation"
    if table == "signal_feature_procurement_invoice_budget_reconciliation_review_only":
        return "SELECT * FROM curated_invoice_budget_reconciliation"
    if table == "signal_feature_procurement_payment_plan_reconciliation_review_only":
        return "SELECT * FROM curated_payment_plan_reconciliation"
    if table == "signal_feature_health_pae_service_delivery_gap_review_only":
        return "SELECT * FROM curated_health_pae_service_delivery_gap"
    if table == "signal_feature_pae_beneficiary_territory_delivery_gap_review_only":
        return "SELECT * FROM curated_pae_beneficiary_territory_delivery_gap"
    if table == "signal_feature_procurement_contract_suspensions":
        return "SELECT * FROM curated_contract_suspensions"
    if table == "signal_feature_procurement_contract_execution_delay":
        return "SELECT * FROM curated_contract_execution_delay"
    if table == "signal_feature_project_bpin_procurement_overlap":
        return "SELECT * FROM curated_project_bpin_procurement_overlap"
    if table == "signal_feature_bpin_dnp_vs_pida27_obras_prioritarias":
        return "SELECT * FROM curated_bpin_dnp_vs_pida27_obras_prioritarias"
    if table == "signal_feature_project_regalias_execution_procurement_overlap":
        return "SELECT * FROM curated_project_regalias_execution_procurement_overlap"
    if table == "signal_feature_sgr_ocad_executor_capacity_gap":
        return "SELECT * FROM curated_sgr_ocad_executor_capacity_gap"
    if table == "signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only":
        return "SELECT * FROM curated_dnp_sgr_beneficiary_delivery_gap"
    if table == "signal_feature_procurement_short_bidding_window":
        return "SELECT * FROM curated_short_bidding_window"
    if table == "signal_feature_procurement_offers_competition_drop":
        return "SELECT * FROM curated_offers_competition_drop"
    if table == "signal_feature_procurement_public_servant_conflict_disclosure_overlap":
        return "SELECT * FROM curated_public_servant_conflict_disclosure_overlap"
    if table == "signal_feature_procurement_role_supplier_same_buyer_review_only":
        return "SELECT * FROM curated_role_supplier_same_buyer"
    if table == "signal_feature_public_declaration_supplier_chronology_review_only":
        return "SELECT * FROM curated_public_declaration_supplier_chronology"
    if table == "signal_feature_cuentas_claras_donor_supplier_overlap":
        return "SELECT * FROM curated_cuentas_claras_donor_supplier_overlap"
    if table == "signal_feature_cuentas_claras_donor_ineligibility_review":
        return "SELECT * FROM curated_cuentas_claras_donor_ineligibility_review"
    if table == "signal_feature_pida_full30_meta":
        return "SELECT * FROM curated_pida_full30_meta"
    if table == "signal_feature_pida5_pida27_pida4_chain":
        return "SELECT * FROM curated_pida5_pida27_pida4_chain"
    if table == "signal_feature_tvec_multi_entity_capture":
        return "SELECT * FROM curated_tvec_multi_entity_capture"
    if table == "signal_feature_tvec_item_price_dispersion_review_only":
        return "SELECT * FROM curated_tvec_item_price_dispersion"
    if table == "signal_feature_procurement_politically_exposed_position_supplier_overlap":
        return "SELECT * FROM curated_politically_exposed_supplier_overlap"
    if table == "signal_feature_procurement_related_companies_shared_officer":
        return "SELECT * FROM curated_related_companies_shared_officer"
    if table == "signal_feature_procurement_cross_source_identity_inconsistency":
        return "SELECT * FROM curated_cross_source_identity_inconsistency"
    if table == "signal_feature_rues_supplier_capacity_status_review_only":
        return "SELECT * FROM curated_rues_supplier_capacity_status"
    if table == "signal_feature_cross_signal_compound_risk_review_only":
        return "SELECT * FROM curated_cross_signal_compound_risk"
    if table == "signal_feature_public_declaration_company_bridge_current_risk_review_only":
        return "SELECT * FROM curated_public_declaration_company_bridge_current_risk"
    if table == "signal_feature_secop_i_legacy_supplier_current_risk_review_only":
        return "SELECT * FROM curated_secop_i_legacy_supplier_current_risk"
    if table == "signal_feature_secop_i_legacy_representative_current_risk_review_only":
        return "SELECT * FROM curated_secop_i_legacy_representative_current_risk"
    if table == "signal_feature_secop_interadmin_executor_network_review_only":
        return "SELECT * FROM curated_interadmin_executor_network"
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
