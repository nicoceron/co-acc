from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from coacc_etl.cli import cli
from coacc_etl.signals import SignalMaterializationError, materialize_signals
from coacc_etl.signals.contracts import EvidenceBundleRow, SignalHitRow

if TYPE_CHECKING:
    from pathlib import Path


def _write_feature_rows(
    root: Path,
    signal_id: str,
    rows: list[dict[str, object]],
) -> None:
    out = root / "curated" / f"table=signal_feature_{signal_id}"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), out / "part-00000.parquet")


def _write_demo_feature_tables(root: Path) -> None:
    _write_feature_rows(
        root,
        "procurement_single_bidder_high_value",
        [
            {
                "signal_id": "procurement_single_bidder_high_value",
                "entity_id": "doc:902000111",
                "entity_key": "902000111",
                "entity_label": "Company",
                "scope_key": "PV-1",
                "scope_type": "procurement_process",
                "severity": "medium",
                "risk_signal": 0.78,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": ["https://secop.example/CV-1"],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_sanctioned_supplier_awarded",
        [
            {
                "signal_id": "procurement_sanctioned_supplier_awarded",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1:paco-1",
                "scope_type": "sanction_record",
                "risk_signal": 1.0,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "https://paco.example/paco-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_large_modifications",
        [
            {
                "signal_id": "procurement_large_modifications",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1",
                "scope_type": "contract",
                "severity": "high",
                "risk_signal": 0.84,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "secop_contract_modifications:MOD-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_contract_modification_ladder_review_only",
        [
            {
                "signal_id": "procurement_contract_modification_ladder_review_only",
                "entity_id": "doc:908000113",
                "entity_key": "908000113",
                "entity_label": "Company",
                "scope_key": "CLADDER-1",
                "scope_type": "contract_modification_ladder",
                "severity": "high",
                "risk_signal": 0.82,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CLADDER-1",
                    "secop_contract_modifications:MOD-LADDER-2",
                    "secop_contract_modifications:MOD-LADDER-1",
                    "secop_contract_modifications:MOD-LADDER-3",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_secop_sanction_later_awards_review_only",
        [
            {
                "signal_id": "procurement_secop_sanction_later_awards_review_only",
                "entity_id": "doc:905123999",
                "entity_key": "905123999",
                "entity_label": "Company",
                "scope_key": "secop_sanction_later_awards:SECOP-SAN-1:905123999:1",
                "scope_type": "secop_sanction_later_awards",
                "severity": "high",
                "risk_signal": 0.86,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "secop_sanctions:ACT-SECOP-SAN-1",
                    "https://secop.example/SECOP-SAN-1",
                    "https://secop.example/SECOP-SAN-LATER",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "fiscal_procurement_chronology_review_only",
        [
            {
                "signal_id": "fiscal_procurement_chronology_review_only",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "fiscal_procurement:fiscal_responsibility:RF-2020-1:900123456",
                "scope_type": "fiscal_procurement_chronology",
                "severity": "critical",
                "risk_signal": 0.92,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "fiscal_responsibility:RF-2020-1",
                    "https://secop.example/C-FISCAL-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_supplier_concentration_across_entities",
        [
            {
                "signal_id": "procurement_supplier_concentration_across_entities",
                "entity_id": "doc:900765432",
                "entity_key": "900765432",
                "entity_label": "Company",
                "scope_key": "supplier:900765432",
                "scope_type": "supplier",
                "risk_signal": 0.8,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CX-1",
                    "https://secop.example/CX-2",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_contract_value_outlier_by_category",
        [
            {
                "signal_id": "procurement_contract_value_outlier_by_category",
                "entity_id": "contract:CO-OUTLIER-1",
                "entity_key": "CO-OUTLIER-1",
                "entity_label": "Contract",
                "scope_key": "CO-OUTLIER-1",
                "scope_type": "contract",
                "risk_signal": 0.88,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_CONTRACT_KEY",
                "identity_quality": "exact",
                "evidence_refs": ["https://secop.example/CO-OUTLIER-1"],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_repeat_awards_same_supplier",
        [
            {
                "signal_id": "procurement_repeat_awards_same_supplier",
                "entity_id": "doc:901000111",
                "entity_key": "901000111",
                "entity_label": "Company",
                "scope_key": "buyer:800999888",
                "scope_type": "buyer",
                "risk_signal": 0.75,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CR-1",
                    "https://secop.example/CR-2",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_buyer_supplier_network_density",
        [
            {
                "signal_id": "procurement_buyer_supplier_network_density",
                "entity_id": "doc:906000111",
                "entity_key": "906000111",
                "entity_label": "Company",
                "scope_key": "supplier_network:906000111",
                "scope_type": "supplier_network",
                "risk_signal": 0.86,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CN-0-0",
                    "https://secop.example/CN-0-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_cartel_risk_cobidding",
        [
            {
                "signal_id": "procurement_cartel_risk_cobidding",
                "entity_id": "doc:907000111",
                "entity_key": "907000111",
                "entity_label": "Company",
                "scope_key": "cobid_pair:907000111:907000112",
                "scope_type": "cobid_cluster",
                "severity": "high",
                "risk_signal": 0.95,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/PROC-COBID-19",
                    "https://secop.example/PROC-COBID-18",
                ],
            },
            {
                "signal_id": "procurement_cartel_risk_cobidding",
                "entity_id": "doc:907000112",
                "entity_key": "907000112",
                "entity_label": "Company",
                "scope_key": "cobid_pair:907000111:907000112",
                "scope_type": "cobid_cluster",
                "severity": "high",
                "risk_signal": 0.95,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/PROC-COBID-19",
                    "https://secop.example/PROC-COBID-18",
                ],
            },
        ],
    )
    _write_feature_rows(
        root,
        "procurement_related_bidders_same_process_review_only",
        [
            {
                "signal_id": "procurement_related_bidders_same_process_review_only",
                "entity_id": "doc:900765432",
                "entity_key": "900765432",
                "entity_label": "Company",
                "scope_key": (
                    "related_bidders_process:PREL-1:900765432:901000111:"
                    "222222222"
                ),
                "scope_type": "related_bidders_process",
                "severity": "high",
                "risk_signal": 0.88,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CREL-1",
                    "secop_offers:PREL-1:OFFER-ID-RELATED-LOSER",
                    (
                        "signal_feature_procurement_related_companies_shared_officer:"
                        "officer_cluster:222222222:900765432"
                    ),
                    "company_registry_c82u:row-c82u-shared-1",
                    "company_registry_c82u:row-c82u-shared-2",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_short_bidding_window",
        [
            {
                "signal_id": "procurement_short_bidding_window",
                "entity_id": "doc:903000111",
                "entity_key": "903000111",
                "entity_label": "Company",
                "scope_key": "PROC-SHORT-1",
                "scope_type": "procurement_process",
                "severity": "low",
                "risk_signal": 0.72,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": ["https://secop.example/PROC-SHORT-1"],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_shared_representative_same_buyer_cluster_review_only",
        [
            {
                "signal_id": (
                    "procurement_shared_representative_same_buyer_cluster"
                    "_review_only"
                ),
                "entity_id": "doc:910111222",
                "entity_key": "910111222",
                "entity_label": "Company",
                "scope_key": (
                    "shared_representative_same_buyer:"
                    "444444444:800999888:2026:910111222"
                ),
                "scope_type": "shared_representative_same_buyer_cluster",
                "severity": "high",
                "risk_signal": 0.91,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "company_registry_c82u:row-c82u-samebuyer-1",
                    "company_registry_c82u:row-c82u-samebuyer-2",
                    "https://secop.example/CSHARED-1",
                    "https://secop.example/CSHARED-3",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_payment_plan_anomalies",
        [
            {
                "signal_id": "procurement_payment_plan_anomalies",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "contract",
                "severity": "medium",
                "risk_signal": 0.71,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": ["https://secop.example/CPAY-1"],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_guarantee_advance_execution_chain",
        [
            {
                "signal_id": "procurement_guarantee_advance_execution_chain",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "contract_execution_chain",
                "severity": "high",
                "risk_signal": 0.88,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CPAY-1",
                    "secop_contract_suspensions:CPAY-1:2026-05-15",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_guarantee_policy_reuse_review_only",
        [
            {
                "signal_id": "procurement_guarantee_policy_reuse_review_only",
                "entity_id": "doc:909000111",
                "entity_key": "909000111",
                "entity_label": "Company",
                "scope_key": "guarantee_policy_reuse:segurosmundial:100777888:CREUSE-1",
                "scope_type": "guarantee_policy_cluster",
                "severity": "high",
                "risk_signal": 0.89,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CREUSE-1",
                    "secop_guarantees:CREUSE-1:100777888:1",
                    "https://secop.example/CREUSE-2",
                    "secop_guarantees:CREUSE-2:100777888:1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_budget_chain_reconciliation_review_only",
        [
            {
                "signal_id": "procurement_budget_chain_reconciliation_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "contract_budget_chain",
                "severity": "critical",
                "risk_signal": 0.91,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CPAY-1",
                    "secop_cdp_requests:CPAY-1:CDP-CPAY-1",
                    "secop_budget_commitments:CPAY-1:COMP-CPAY-1",
                    "secop_budget_items:CPAY-1:RUBRO-CPAY-1",
                    (
                        "signal_feature_procurement_guarantee_advance_execution_chain:"
                        "CPAY-1"
                    ),
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_invoice_budget_reconciliation_review_only",
        [
            {
                "signal_id": "procurement_invoice_budget_reconciliation_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "contract_invoice_budget_reconciliation",
                "severity": "critical",
                "risk_signal": 0.93,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CPAY-1",
                    "secop_invoices:CPAY-1:INV-CPAY-1",
                    (
                        "signal_feature_procurement_budget_chain_reconciliation"
                        "_review_only:CPAY-1"
                    ),
                    (
                        "signal_feature_procurement_guarantee_advance_execution_chain:"
                        "CPAY-1"
                    ),
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_payment_plan_reconciliation_review_only",
        [
            {
                "signal_id": "procurement_payment_plan_reconciliation_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "contract_payment_plan_reconciliation",
                "severity": "critical",
                "risk_signal": 0.94,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CPAY-1",
                    "secop_payment_plans:CPAY-1:PAY-CPAY-1",
                    (
                        "signal_feature_procurement_budget_chain_reconciliation"
                        "_review_only:CPAY-1"
                    ),
                    (
                        "signal_feature_procurement_guarantee_advance_execution_chain:"
                        "CPAY-1"
                    ),
                    (
                        "signal_feature_procurement_invoice_budget_reconciliation"
                        "_review_only:CPAY-1"
                    ),
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "health_pae_service_delivery_gap_review_only",
        [
            {
                "signal_id": "health_pae_service_delivery_gap_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "CPAY-1",
                "scope_type": "health_pae_service_delivery",
                "severity": "critical",
                "risk_signal": 0.96,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CPAY-1",
                    "health_providers:REPS-907000113-SEDE",
                    (
                        "signal_feature_procurement_payment_plan_reconciliation"
                        "_review_only:CPAY-1"
                    ),
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "pae_beneficiary_territory_delivery_gap_review_only",
        [
            {
                "signal_id": "pae_beneficiary_territory_delivery_gap_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "pae_beneficiary_territory:CPAY-1",
                "scope_type": "pae_beneficiary_territory_delivery_gap",
                "severity": "critical",
                "risk_signal": 0.97,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    (
                        "signal_feature_health_pae_service_delivery_gap_review_only:"
                        "CPAY-1"
                    ),
                    "pae_indicators:11001:2026:Indígenas:2",
                    "https://secop.example/CPAY-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_contract_suspensions",
        [
            {
                "signal_id": "procurement_contract_suspensions",
                "entity_id": "doc:908000113",
                "entity_key": "908000113",
                "entity_label": "Company",
                "scope_key": "CSUSP-1",
                "scope_type": "contract",
                "severity": "medium",
                "risk_signal": 0.74,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/CSUSP-1",
                    "secop_contract_suspensions:CSUSP-1:2026-06-11",
                    "secop_contract_suspensions:CSUSP-1:2026-05-11",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_contract_execution_delay",
        [
            {
                "signal_id": "procurement_contract_execution_delay",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "C-1",
                "scope_type": "contract",
                "severity": "low",
                "risk_signal": 0.68,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/C-1",
                    "secop_contract_execution:C-1:ITEM-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_offers_competition_drop",
        [
            {
                "signal_id": "procurement_offers_competition_drop",
                "entity_id": "doc:800555666",
                "entity_key": "800555666",
                "entity_label": "Company",
                "scope_key": "buyer:800555666",
                "scope_type": "buyer",
                "risk_signal": 0.92,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "https://secop.example/PROC-DROP-R-19",
                    "https://secop.example/PROC-DROP-R-18",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "cuentas_claras_donor_supplier_overlap",
        [
            {
                "signal_id": "cuentas_claras_donor_supplier_overlap",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "election:2019:900123456:700111222",
                "scope_type": "election",
                "severity": "medium",
                "risk_signal": 0.77,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "cuentas_claras_income_2019:VOUCHER-1",
                    "https://secop.example/C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "cuentas_claras_donor_ineligibility_review",
        [
            {
                "signal_id": "cuentas_claras_donor_ineligibility_review",
                "entity_id": "doc:900123456",
                "entity_key": "900123456",
                "entity_label": "Company",
                "scope_key": "donor_contract:2019:mayor:700111222:900123456:800111222",
                "scope_type": "donor_contract_ineligibility_review",
                "severity": "medium",
                "risk_signal": 0.82,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "cuentas_claras_income_2019:VOUCHER-1",
                    "https://secop.example/C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "pida5_pida27_pida4_chain",
        [
            {
                "signal_id": "pida5_pida27_pida4_chain",
                "entity_id": "territory:BOGOTA:BOGOTA",
                "entity_key": "BOGOTA:BOGOTA",
                "entity_label": "Territory",
                "scope_key": "pida5_pida27_pida4_chain:BOGOTA:BOGOTA",
                "scope_type": "territory",
                "severity": "medium",
                "risk_signal": 0.81,
                "identity_confidence": 0.9,
                "identity_match_type": "EXACT_CONTRACT_KEY_AGGREGATE",
                "identity_quality": "aggregate",
                "evidence_refs": [
                    "secop_sanctions:ACT-SAN-1",
                    "https://secop-integrado.example/INT-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "pida_full30_meta",
        [
            {
                "signal_id": "pida_full30_meta",
                "entity_id": "territory:BOGOTA:BOGOTA",
                "entity_key": "BOGOTA:BOGOTA",
                "entity_label": "Territory",
                "scope_key": "pida_full30_meta:BOGOTA:BOGOTA",
                "scope_type": "territory",
                "severity": "medium",
                "risk_signal": 0.82,
                "identity_confidence": 0.9,
                "identity_match_type": "TERRITORY_AGGREGATE",
                "identity_quality": "aggregate",
                "evidence_refs": [
                    "https://secop-integrado.example/PIDA-FULL-school_feeding-000",
                    "secop_integrado:PIDA-FULL-school_feeding-001",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "project_bpin_procurement_overlap",
        [
            {
                "signal_id": "project_bpin_procurement_overlap",
                "entity_id": "project:202612345678901",
                "entity_key": "202612345678901",
                "entity_label": "Project",
                "scope_key": "bpin:202612345678901",
                "scope_type": "project",
                "severity": "medium",
                "risk_signal": 0.72,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_BPIN",
                "identity_quality": "exact",
                "evidence_refs": [
                    "secop_process_bpin:202612345678901:BPIN-C-1",
                    "https://secop.example/BPIN-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "bpin_dnp_vs_pida27_obras_prioritarias",
        [
            {
                "signal_id": "bpin_dnp_vs_pida27_obras_prioritarias",
                "entity_id": "project:202699990000001",
                "entity_key": "202699990000001",
                "entity_label": "Project",
                "scope_key": "bpin_priority_work:202699990000001",
                "scope_type": "project",
                "severity": "medium",
                "risk_signal": 0.76,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_BPIN_CONTRACT_LINK",
                "identity_quality": "exact",
                "evidence_refs": [
                    "secop_process_bpin:202699990000001:BPIN-PRIO-1",
                    "https://secop-integrado.example/BPIN-PRIO-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "project_regalias_execution_procurement_overlap",
        [
            {
                "signal_id": "project_regalias_execution_procurement_overlap",
                "entity_id": "project:202612345678901",
                "entity_key": "202612345678901",
                "entity_label": "Project",
                "scope_key": "sgr_bpin:202612345678901",
                "scope_type": "project",
                "severity": "medium",
                "risk_signal": 0.78,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_BPIN",
                "identity_quality": "exact",
                "evidence_refs": [
                    "sgr_projects:202612345678901",
                    "sgr_expense_execution:202612345678901:20260901:700001:2.3.2.02",
                    "secop_process_bpin:202612345678901:BPIN-C-1",
                    "https://secop.example/BPIN-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "sgr_ocad_executor_capacity_gap",
        [
            {
                "signal_id": "sgr_ocad_executor_capacity_gap",
                "entity_id": "project:202612345678901",
                "entity_key": "202612345678901",
                "entity_label": "Project",
                "scope_key": "sgr_ocad_capacity:202612345678901",
                "scope_type": "sgr_ocad_project",
                "severity": "critical",
                "risk_signal": 0.9,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_BPIN",
                "identity_quality": "exact",
                "evidence_refs": [
                    "sgr_projects:202612345678901",
                    "sgr_expense_execution:202612345678901:20260901:700001:2.3.2.02",
                    "secop_process_bpin:202612345678901:BPIN-C-1",
                    "https://secop.example/BPIN-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "dnp_sgr_beneficiary_delivery_gap_review_only",
        [
            {
                "signal_id": "dnp_sgr_beneficiary_delivery_gap_review_only",
                "entity_id": "project:202612345678901",
                "entity_key": "202612345678901",
                "entity_label": "Project",
                "scope_key": "dnp_sgr_beneficiary_delivery:202612345678901",
                "scope_type": "dnp_sgr_beneficiary_delivery_gap",
                "severity": "critical",
                "risk_signal": 0.94,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_BPIN",
                "identity_quality": "exact",
                "evidence_refs": [
                    "sgr_projects:202612345678901",
                    (
                        "dnp_project_beneficiary_characterization:"
                        "202612345678901:Personas con discapacidad"
                    ),
                    "https://secop.example/BPIN-C-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "tvec_multi_entity_capture",
        [
            {
                "signal_id": "tvec_multi_entity_capture",
                "entity_id": "doc:906000111",
                "entity_key": "906000111",
                "entity_label": "Company",
                "scope_key": "tvec_supplier:906000111",
                "scope_type": "tvec_order",
                "severity": "medium",
                "risk_signal": 0.76,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "tvec_orders_consolidated:TVEC-0",
                    "https://secop.example/CN-0-0",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "tvec_item_price_dispersion_review_only",
        [
            {
                "signal_id": "tvec_item_price_dispersion_review_only",
                "entity_id": "doc:910000002",
                "entity_key": "910000002",
                "entity_label": "Company",
                "scope_key": (
                    "tvec_price:termometrodigitalqa:unidad:q_001:2026:"
                    "TVEC-PRICE-19:910000002"
                ),
                "scope_type": "tvec_price",
                "severity": "high",
                "risk_signal": 0.86,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "tvec_orders_consolidated:TVEC-PRICE-19:termometrodigitalqa",
                    "tvec_orders_consolidated:TVEC-PRICE-0:termometrodigitalqa",
                    "tvec_orders_consolidated:TVEC-PRICE-1:termometrodigitalqa",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_politically_exposed_position_supplier_overlap",
        [
            {
                "signal_id": "procurement_politically_exposed_position_supplier_overlap",
                "entity_id": "doc:902000111",
                "entity_key": "902000111",
                "entity_label": "Company",
                "scope_key": "sensitive_position:123456789:INST-1:902000111",
                "scope_type": "sensitive_position",
                "risk_signal": 0.9,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "company_registry_c82u:row-c82u-1",
                    "sigep_sensitive_positions:123456789",
                    "https://secop.example/CV-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_public_servant_conflict_disclosure_overlap",
        [
            {
                "signal_id": "procurement_public_servant_conflict_disclosure_overlap",
                "entity_id": "person:1001234567",
                "entity_key": "1001234567",
                "entity_label": "Person",
                "scope_key": "disclosure:FORM-1:1001234567",
                "scope_type": "disclosure",
                "severity": "medium",
                "risk_signal": 0.82,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_PERSON_DOCUMENT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "conflict_disclosures:FORM-1",
                    "https://secop.example/CPER-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_role_supplier_same_buyer_review_only",
        [
            {
                "signal_id": "procurement_role_supplier_same_buyer_review_only",
                "entity_id": "person:1001234567",
                "entity_key": "1001234567",
                "entity_label": "Person",
                "scope_key": "role_supplier:800111222:1001234567:supervisor_doc_number",
                "scope_type": "buyer_role_supplier",
                "severity": "high",
                "risk_signal": 0.82,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_PERSON_DOCUMENT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "secop_ii_contracts:role:C-1:supervisor_doc_number",
                    "https://secop.example/CPER-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "public_declaration_supplier_chronology_review_only",
        [
            {
                "signal_id": "public_declaration_supplier_chronology_review_only",
                "entity_id": "person:1001234567",
                "entity_key": "1001234567",
                "entity_label": "Person",
                "scope_key": "declaration_supplier:1001234567:800111222",
                "scope_type": "declaration_supplier",
                "severity": "high",
                "risk_signal": 0.84,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_PERSON_DOCUMENT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "conflict_disclosures:FORM-1",
                    "https://secop.example/CPER-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "public_declaration_company_bridge_current_risk_review_only",
        [
            {
                "signal_id": "public_declaration_company_bridge_current_risk_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "declaration_company_bridge:1001234567:907000113",
                "scope_type": "declaration_company_bridge_current_risk",
                "severity": "critical",
                "risk_signal": 0.93,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    (
                        "signal_feature_cross_signal_compound_risk_review_only:"
                        "cross_signal:company:907000113"
                    ),
                    "conflict_disclosures:FORM-1",
                    "company_registry_c82u:row-c82u-capacity-1",
                    "https://secop.example/CPAY-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_related_companies_shared_officer",
        [
            {
                "signal_id": "procurement_related_companies_shared_officer",
                "entity_id": "doc:900765432",
                "entity_key": "900765432",
                "entity_label": "Company",
                "scope_key": "officer_cluster:222222222",
                "scope_type": "officer_cluster",
                "risk_signal": 0.9,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "company_registry_c82u:row-c82u-shared-1",
                    "company_registry_c82u:row-c82u-shared-2",
                    "https://secop.example/CX-1",
                    "https://secop.example/CX-2",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "procurement_cross_source_identity_inconsistency",
        [
            {
                "signal_id": "procurement_cross_source_identity_inconsistency",
                "entity_id": "doc:902000111",
                "entity_key": "902000111",
                "entity_label": "Company",
                "scope_key": "alias_cluster:902000111",
                "scope_type": "alias_cluster",
                "severity": "low",
                "risk_signal": 0.74,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "company_registry_c82u:row-c82u-1",
                    "secop_suppliers:SUP-902",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "rues_supplier_capacity_status_review_only",
        [
            {
                "signal_id": "rues_supplier_capacity_status_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "rues_capacity:907000113",
                "scope_type": "rues_supplier_capacity_status",
                "severity": "high",
                "risk_signal": 0.86,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "company_registry_c82u:row-c82u-capacity-1",
                    "https://secop.example/CPAY-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "cross_signal_compound_risk_review_only",
        [
            {
                "signal_id": "cross_signal_compound_risk_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "cross_signal:company:907000113",
                "scope_type": "cross_signal_entity",
                "severity": "high",
                "risk_signal": 0.82,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    (
                        "signal_feature_procurement_guarantee_advance_execution_chain:"
                        "CPAY-1"
                    ),
                    (
                        "signal_feature_rues_supplier_capacity_status_review_only:"
                        "rues_capacity:907000113"
                    ),
                    "https://secop.example/CPAY-1",
                    "company_registry_c82u:row-c82u-capacity-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "siri_antecedent_procurement_chronology_review_only",
        [
            {
                "signal_id": "siri_antecedent_procurement_chronology_review_only",
                "entity_id": "person:1001234567",
                "entity_key": "1001234567",
                "entity_label": "Person",
                "scope_key": "siri_procurement:1001234567:role_supervisor:C-1:SIRI-1",
                "scope_type": "siri_procurement_chronology",
                "severity": "critical",
                "risk_signal": 0.91,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_PERSON_DOCUMENT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "siri_antecedents:SIRI-1",
                    "secop_ii_contracts:role:C-1:supervisor_doc_number",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "secop_i_legacy_supplier_current_risk_review_only",
        [
            {
                "signal_id": "secop_i_legacy_supplier_current_risk_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "secop_i_legacy_current_risk:907000113",
                "scope_type": "secop_i_legacy_supplier_current_risk",
                "severity": "critical",
                "risk_signal": 0.98,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    (
                        "signal_feature_cross_signal_compound_risk_review_only:"
                        "cross_signal:company:907000113"
                    ),
                    "secop_i_historical_processes:LEGACY-CPAY-1",
                    "https://secop.example/CPAY-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "secop_i_legacy_representative_current_risk_review_only",
        [
            {
                "signal_id": "secop_i_legacy_representative_current_risk_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": (
                    "secop_i_legacy_representative_current_risk:"
                    "333333333:907000113"
                ),
                "scope_type": "secop_i_legacy_representative_current_risk",
                "severity": "high",
                "risk_signal": 0.94,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    (
                        "signal_feature_cross_signal_compound_risk_review_only:"
                        "cross_signal:company:907000113"
                    ),
                    "secop_i_historical_processes:LEGACY-REP-BRIDGE-1",
                    "company_registry_c82u:row-c82u-capacity-1",
                    "https://secop.example/CPAY-1",
                ],
            }
        ],
    )
    _write_feature_rows(
        root,
        "secop_interadmin_executor_network_review_only",
        [
            {
                "signal_id": "secop_interadmin_executor_network_review_only",
                "entity_id": "doc:907000113",
                "entity_key": "907000113",
                "entity_label": "Company",
                "scope_key": "interadmin_chain:IA-1:907000113",
                "scope_type": "interadmin_executor_chain",
                "severity": "high",
                "risk_signal": 0.88,
                "identity_confidence": 1.0,
                "identity_match_type": "EXACT_COMPANY_NIT",
                "identity_quality": "exact",
                "evidence_refs": [
                    "secop_interadmin_agreements:IA-1:907000113",
                    (
                        "signal_feature_procurement_guarantee_advance_execution_chain:"
                        "CPAY-1"
                    ),
                ],
            }
        ],
    )


def test_materialize_signals_writes_hits_evidence_and_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = materialize_signals(run_id="test-run")

    assert result.run_id == "test-run"
    assert result.hit_count == 50
    assert result.evidence_count == 133
    assert {row.signal_id: row.hit_count for row in result.signal_results} == {
        "procurement_single_bidder_high_value": 1,
        "procurement_large_modifications": 1,
        "procurement_contract_modification_ladder_review_only": 1,
        "procurement_sanctioned_supplier_awarded": 1,
        "procurement_secop_sanction_later_awards_review_only": 1,
        "fiscal_procurement_chronology_review_only": 1,
        "procurement_supplier_concentration_across_entities": 1,
        "procurement_contract_value_outlier_by_category": 1,
        "procurement_repeat_awards_same_supplier": 1,
        "procurement_buyer_supplier_network_density": 1,
        "procurement_cartel_risk_cobidding": 2,
        "procurement_related_bidders_same_process_review_only": 1,
        "procurement_shared_representative_same_buyer_cluster_review_only": 1,
        "procurement_payment_plan_anomalies": 1,
        "procurement_payment_plan_reconciliation_review_only": 1,
        "health_pae_service_delivery_gap_review_only": 1,
        "pae_beneficiary_territory_delivery_gap_review_only": 1,
        "procurement_guarantee_advance_execution_chain": 1,
        "procurement_guarantee_policy_reuse_review_only": 1,
        "procurement_budget_chain_reconciliation_review_only": 1,
        "procurement_invoice_budget_reconciliation_review_only": 1,
        "procurement_contract_suspensions": 1,
        "procurement_contract_execution_delay": 1,
        "procurement_short_bidding_window": 1,
        "procurement_offers_competition_drop": 1,
        "cuentas_claras_donor_ineligibility_review": 1,
        "cuentas_claras_donor_supplier_overlap": 1,
        "cross_signal_compound_risk_review_only": 1,
        "pida_full30_meta": 1,
        "pida5_pida27_pida4_chain": 1,
        "project_bpin_procurement_overlap": 1,
        "project_regalias_execution_procurement_overlap": 1,
        "sgr_ocad_executor_capacity_gap": 1,
        "dnp_sgr_beneficiary_delivery_gap_review_only": 1,
        "bpin_dnp_vs_pida27_obras_prioritarias": 1,
        "tvec_multi_entity_capture": 1,
        "tvec_item_price_dispersion_review_only": 1,
        "procurement_politically_exposed_position_supplier_overlap": 1,
        "procurement_public_servant_conflict_disclosure_overlap": 1,
        "procurement_role_supplier_same_buyer_review_only": 1,
        "public_declaration_supplier_chronology_review_only": 1,
        "public_declaration_company_bridge_current_risk_review_only": 1,
        "procurement_related_companies_shared_officer": 1,
        "procurement_cross_source_identity_inconsistency": 1,
        "rues_supplier_capacity_status_review_only": 1,
        "siri_antecedent_procurement_chronology_review_only": 1,
        "secop_i_legacy_representative_current_risk_review_only": 1,
        "secop_i_legacy_supplier_current_risk_review_only": 1,
        "secop_interadmin_executor_network_review_only": 1,
    }

    con = duckdb.connect()
    try:
        hit_rows = con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY signal_id",
            [str(tmp_path / "curated" / "signal_hits" / "run_id=test-run" / "*.parquet")],
        ).fetchall()
        hit_columns = [item[0] for item in con.description or []]
        evidence_rows = con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY signal_id, item_index",
            [
                str(
                    tmp_path
                    / "curated"
                    / "evidence_bundles"
                    / "run_id=test-run"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        evidence_columns = [item[0] for item in con.description or []]
    finally:
        con.close()

    hit_payloads = [dict(zip(hit_columns, row, strict=False)) for row in hit_rows]
    evidence_payloads = [
        dict(zip(evidence_columns, row, strict=False)) for row in evidence_rows
    ]
    SignalHitRow.model_validate(hit_payloads[0])
    EvidenceBundleRow.model_validate(evidence_payloads[0])
    assert [row["signal_id"] for row in hit_payloads] == [
        "bpin_dnp_vs_pida27_obras_prioritarias",
        "cross_signal_compound_risk_review_only",
        "cuentas_claras_donor_ineligibility_review",
        "cuentas_claras_donor_supplier_overlap",
        "dnp_sgr_beneficiary_delivery_gap_review_only",
        "fiscal_procurement_chronology_review_only",
        "health_pae_service_delivery_gap_review_only",
        "pae_beneficiary_territory_delivery_gap_review_only",
        "pida5_pida27_pida4_chain",
        "pida_full30_meta",
        "procurement_budget_chain_reconciliation_review_only",
        "procurement_buyer_supplier_network_density",
        "procurement_cartel_risk_cobidding",
        "procurement_cartel_risk_cobidding",
        "procurement_contract_execution_delay",
        "procurement_contract_modification_ladder_review_only",
        "procurement_contract_suspensions",
        "procurement_contract_value_outlier_by_category",
        "procurement_cross_source_identity_inconsistency",
        "procurement_guarantee_advance_execution_chain",
        "procurement_guarantee_policy_reuse_review_only",
        "procurement_invoice_budget_reconciliation_review_only",
        "procurement_large_modifications",
        "procurement_offers_competition_drop",
        "procurement_payment_plan_anomalies",
        "procurement_payment_plan_reconciliation_review_only",
        "procurement_politically_exposed_position_supplier_overlap",
        "procurement_public_servant_conflict_disclosure_overlap",
        "procurement_related_bidders_same_process_review_only",
        "procurement_related_companies_shared_officer",
        "procurement_repeat_awards_same_supplier",
        "procurement_role_supplier_same_buyer_review_only",
        "procurement_sanctioned_supplier_awarded",
        "procurement_secop_sanction_later_awards_review_only",
        "procurement_shared_representative_same_buyer_cluster_review_only",
        "procurement_short_bidding_window",
        "procurement_single_bidder_high_value",
        "procurement_supplier_concentration_across_entities",
        "project_bpin_procurement_overlap",
        "project_regalias_execution_procurement_overlap",
        "public_declaration_company_bridge_current_risk_review_only",
        "public_declaration_supplier_chronology_review_only",
        "rues_supplier_capacity_status_review_only",
        "secop_i_legacy_representative_current_risk_review_only",
        "secop_i_legacy_supplier_current_risk_review_only",
        "secop_interadmin_executor_network_review_only",
        "sgr_ocad_executor_capacity_gap",
        "siri_antecedent_procurement_chronology_review_only",
        "tvec_item_price_dispersion_review_only",
        "tvec_multi_entity_capture",
    ]
    evidence_counts = {row["signal_id"]: row["evidence_count"] for row in hit_payloads}
    assert evidence_counts == {
        "procurement_buyer_supplier_network_density": 2,
        "procurement_budget_chain_reconciliation_review_only": 5,
        "cross_signal_compound_risk_review_only": 4,
        "health_pae_service_delivery_gap_review_only": 3,
        "pae_beneficiary_territory_delivery_gap_review_only": 3,
        "procurement_cartel_risk_cobidding": 2,
        "procurement_contract_execution_delay": 2,
        "procurement_contract_suspensions": 3,
        "procurement_cross_source_identity_inconsistency": 2,
        "procurement_guarantee_advance_execution_chain": 2,
        "procurement_guarantee_policy_reuse_review_only": 4,
        "procurement_invoice_budget_reconciliation_review_only": 4,
        "cuentas_claras_donor_ineligibility_review": 2,
        "cuentas_claras_donor_supplier_overlap": 2,
        "pida_full30_meta": 2,
        "pida5_pida27_pida4_chain": 2,
        "procurement_payment_plan_anomalies": 1,
        "procurement_payment_plan_reconciliation_review_only": 5,
        "procurement_large_modifications": 2,
        "procurement_contract_modification_ladder_review_only": 4,
        "procurement_offers_competition_drop": 2,
        "procurement_contract_value_outlier_by_category": 1,
        "procurement_politically_exposed_position_supplier_overlap": 3,
        "procurement_public_servant_conflict_disclosure_overlap": 2,
        "procurement_related_bidders_same_process_review_only": 5,
        "procurement_related_companies_shared_officer": 4,
        "procurement_repeat_awards_same_supplier": 2,
        "procurement_role_supplier_same_buyer_review_only": 2,
        "procurement_sanctioned_supplier_awarded": 2,
        "procurement_secop_sanction_later_awards_review_only": 3,
        "procurement_shared_representative_same_buyer_cluster_review_only": 4,
        "fiscal_procurement_chronology_review_only": 2,
        "procurement_short_bidding_window": 1,
        "procurement_single_bidder_high_value": 1,
        "procurement_supplier_concentration_across_entities": 2,
        "project_bpin_procurement_overlap": 2,
        "project_regalias_execution_procurement_overlap": 4,
        "sgr_ocad_executor_capacity_gap": 4,
        "dnp_sgr_beneficiary_delivery_gap_review_only": 3,
        "public_declaration_company_bridge_current_risk_review_only": 4,
        "public_declaration_supplier_chronology_review_only": 2,
        "bpin_dnp_vs_pida27_obras_prioritarias": 2,
        "tvec_item_price_dispersion_review_only": 3,
        "tvec_multi_entity_capture": 2,
        "rues_supplier_capacity_status_review_only": 2,
        "siri_antecedent_procurement_chronology_review_only": 2,
        "secop_i_legacy_representative_current_risk_review_only": 4,
        "secop_i_legacy_supplier_current_risk_review_only": 3,
        "secop_interadmin_executor_network_review_only": 2,
    }
    single_bidder = next(
        row for row in hit_payloads if row["signal_id"] == "procurement_single_bidder_high_value"
    )
    assert single_bidder["severity"] == "medium"
    paco_evidence = next(
        row for row in evidence_payloads if row["url"] == "https://paco.example/paco-1"
    )
    assert paco_evidence["source_id"] == "paco_sanctions"
    assert "scope_key=C-1:paco-1" in paco_evidence["row_selector"]
    fiscal_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "fiscal_responsibility:RF-2020-1"
    )
    assert fiscal_evidence["source_id"] == "fiscal_responsibility"
    dnp_beneficiary_evidence = next(
        row
        for row in evidence_payloads
        if row["label"]
        == (
            "dnp_project_beneficiary_characterization:"
            "202612345678901:Personas con discapacidad"
        )
    )
    assert dnp_beneficiary_evidence["source_id"] == (
        "dnp_project_beneficiary_characterization"
    )
    process_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop.example/PROC-SHORT-1"
    )
    assert process_evidence["source_id"] == "secop_ii_processes"
    competition_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop.example/PROC-DROP-R-19"
    )
    assert competition_evidence["source_id"] == "secop_ii_processes"
    payment_evidence = next(
        row for row in evidence_payloads if row["url"] == "https://secop.example/CPAY-1"
    )
    assert payment_evidence["source_id"] == "secop_ii_contracts"
    budget_commitment_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_budget_commitments:CPAY-1:COMP-CPAY-1"
    )
    assert budget_commitment_evidence["source_id"] == "secop_budget_commitments"
    budget_item_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_budget_items:CPAY-1:RUBRO-CPAY-1"
    )
    assert budget_item_evidence["source_id"] == "secop_budget_items"
    cdp_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_cdp_requests:CPAY-1:CDP-CPAY-1"
    )
    assert cdp_evidence["source_id"] == "secop_cdp_requests"
    invoice_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_invoices:CPAY-1:INV-CPAY-1"
    )
    assert invoice_evidence["source_id"] == "secop_invoices"
    payment_plan_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_payment_plans:CPAY-1:PAY-CPAY-1"
    )
    assert payment_plan_evidence["source_id"] == "secop_payment_plans"
    health_provider_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "health_providers:REPS-907000113-SEDE"
    )
    assert health_provider_evidence["source_id"] == "health_providers"
    pae_indicator_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "pae_indicators:11001:2026:Indígenas:2"
    )
    assert pae_indicator_evidence["source_id"] == "pae_indicators"
    suspension_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_suspensions:CSUSP-1:2026-06-11"
    )
    assert suspension_evidence["source_id"] == "secop_contract_suspensions"
    guarantee_suspension_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_suspensions:CPAY-1:2026-05-15"
    )
    assert guarantee_suspension_evidence["source_id"] == "secop_contract_suspensions"
    execution_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_execution:C-1:ITEM-C-1"
    )
    assert execution_evidence["source_id"] == "secop_contract_execution"
    modification_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_modifications:MOD-C-1"
    )
    assert modification_evidence["source_id"] == "secop_contract_modifications"
    ladder_modification_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_modifications:MOD-LADDER-2"
    )
    assert ladder_modification_evidence["source_id"] == "secop_contract_modifications"
    cobidding_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop.example/PROC-COBID-19"
    )
    assert cobidding_evidence["source_id"] == "secop_ii_processes"
    company_registry_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "company_registry_c82u:row-c82u-1"
    )
    assert company_registry_evidence["source_id"] == "company_registry_c82u"
    rues_capacity_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "company_registry_c82u:row-c82u-capacity-1"
    )
    assert rues_capacity_evidence["source_id"] == "company_registry_c82u"
    sigep_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "sigep_sensitive_positions:123456789"
    )
    assert sigep_evidence["source_id"] == "sigep_sensitive_positions"
    supplier_registry_evidence = next(
        row for row in evidence_payloads if row["label"] == "secop_suppliers:SUP-902"
    )
    assert supplier_registry_evidence["source_id"] == "secop_suppliers"
    cuentas_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "cuentas_claras_income_2019:VOUCHER-1"
    )
    assert cuentas_evidence["source_id"] == "cuentas_claras_income_2019"
    disclosure_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "conflict_disclosures:FORM-1"
    )
    assert disclosure_evidence["source_id"] == "conflict_disclosures"
    role_supplier_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_ii_contracts:role:C-1:supervisor_doc_number"
    )
    assert role_supplier_evidence["source_id"] == "secop_ii_contracts"
    interadmin_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_interadmin_agreements:IA-1:907000113"
    )
    assert interadmin_evidence["source_id"] == "secop_interadmin_agreements"
    secop_i_legacy_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_i_historical_processes:LEGACY-CPAY-1"
    )
    assert secop_i_legacy_evidence["source_id"] == "secop_i_historical_processes"
    siri_evidence = next(
        row for row in evidence_payloads if row["label"] == "siri_antecedents:SIRI-1"
    )
    assert siri_evidence["source_id"] == "siri_antecedents"
    secop_sanction_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_sanctions:ACT-SAN-1"
    )
    assert secop_sanction_evidence["source_id"] == "secop_sanctions"
    secop_later_sanction_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_sanctions:ACT-SECOP-SAN-1"
    )
    assert secop_later_sanction_evidence["source_id"] == "secop_sanctions"
    secop_integrado_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop-integrado.example/INT-1"
    )
    assert secop_integrado_evidence["source_id"] == "secop_integrado"
    bpin_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_process_bpin:202612345678901:BPIN-C-1"
    )
    assert bpin_evidence["source_id"] == "secop_process_bpin"
    bpin_contract_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop.example/BPIN-C-1"
    )
    assert bpin_contract_evidence["source_id"] == "secop_ii_contracts"
    bpin_priority_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop-integrado.example/BPIN-PRIO-1"
    )
    assert bpin_priority_evidence["source_id"] == "secop_integrado"
    pida_full_evidence = next(
        row
        for row in evidence_payloads
        if row["url"] == "https://secop-integrado.example/PIDA-FULL-school_feeding-000"
    )
    assert pida_full_evidence["source_id"] == "secop_integrado"
    sgr_evidence = next(
        row
        for row in evidence_payloads
        if row["label"]
        == "sgr_expense_execution:202612345678901:20260901:700001:2.3.2.02"
    )
    assert sgr_evidence["source_id"] == "sgr_expense_execution"
    tvec_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "tvec_orders_consolidated:TVEC-0"
    )
    assert tvec_evidence["source_id"] == "tvec_orders_consolidated"

    manifest = json.loads((tmp_path / "meta" / "signal_runs" / "test-run.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["hit_count"] == 50
    assert manifest["evidence_count"] == 133


def test_materialize_signals_deduplicates_repeated_feature_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    feature_row = {
        "signal_id": "procurement_sanctioned_supplier_awarded",
        "entity_id": "doc:900123456",
        "entity_key": "900123456",
        "entity_label": "Company",
        "scope_key": "C-1:paco-1",
        "scope_type": "sanction_record",
        "risk_signal": 1.0,
        "identity_confidence": 1.0,
        "identity_match_type": "EXACT_COMPANY_NIT",
        "identity_quality": "exact",
        "evidence_refs": [
            "https://secop.example/C-1",
            "https://paco.example/paco-1",
        ],
    }
    _write_feature_rows(
        tmp_path,
        "procurement_sanctioned_supplier_awarded",
        [feature_row, dict(feature_row)],
    )

    result = materialize_signals(
        ["procurement_sanctioned_supplier_awarded"],
        run_id="dedup-run",
    )

    assert result.hit_count == 1
    assert result.evidence_count == 2


def test_materialize_signal_accepts_registry_alias(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = materialize_signals(["split_contracts_below_threshold"], run_id="alias-run")

    assert [(row.signal_id, row.hit_count) for row in result.signal_results] == [
        ("procurement_repeat_awards_same_supplier", 1)
    ]


def test_materialize_signals_errors_when_feature_table_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    with pytest.raises(SignalMaterializationError, match="missing curated signal feature"):
        materialize_signals(["procurement_repeat_awards_same_supplier"], run_id="missing")


def test_signals_materialize_cli(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = CliRunner().invoke(cli, ["signals", "materialize", "--all", "--run-id", "cli-run"])

    assert result.exit_code == 0, result.output
    assert "signal run cli-run: wrote 50 hits and 133 evidence rows" in result.output
    assert (tmp_path / "meta" / "signal_runs" / "cli-run.json").exists()
