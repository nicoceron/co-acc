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


def test_materialize_signals_writes_hits_evidence_and_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_demo_feature_tables(tmp_path)

    result = materialize_signals(run_id="test-run")

    assert result.run_id == "test-run"
    assert result.hit_count == 21
    assert result.evidence_count == 42
    assert {row.signal_id: row.hit_count for row in result.signal_results} == {
        "procurement_single_bidder_high_value": 1,
        "procurement_large_modifications": 1,
        "procurement_sanctioned_supplier_awarded": 1,
        "procurement_supplier_concentration_across_entities": 1,
        "procurement_contract_value_outlier_by_category": 1,
        "procurement_repeat_awards_same_supplier": 1,
        "procurement_buyer_supplier_network_density": 1,
        "procurement_cartel_risk_cobidding": 2,
        "procurement_payment_plan_anomalies": 1,
        "procurement_contract_suspensions": 1,
        "procurement_contract_execution_delay": 1,
        "procurement_short_bidding_window": 1,
        "procurement_offers_competition_drop": 1,
        "cuentas_claras_donor_supplier_overlap": 1,
        "pida5_pida27_pida4_chain": 1,
        "project_bpin_procurement_overlap": 1,
        "procurement_politically_exposed_position_supplier_overlap": 1,
        "procurement_public_servant_conflict_disclosure_overlap": 1,
        "procurement_related_companies_shared_officer": 1,
        "procurement_cross_source_identity_inconsistency": 1,
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
        "cuentas_claras_donor_supplier_overlap",
        "pida5_pida27_pida4_chain",
        "procurement_buyer_supplier_network_density",
        "procurement_cartel_risk_cobidding",
        "procurement_cartel_risk_cobidding",
        "procurement_contract_execution_delay",
        "procurement_contract_suspensions",
        "procurement_contract_value_outlier_by_category",
        "procurement_cross_source_identity_inconsistency",
        "procurement_large_modifications",
        "procurement_offers_competition_drop",
        "procurement_payment_plan_anomalies",
        "procurement_politically_exposed_position_supplier_overlap",
        "procurement_public_servant_conflict_disclosure_overlap",
        "procurement_related_companies_shared_officer",
        "procurement_repeat_awards_same_supplier",
        "procurement_sanctioned_supplier_awarded",
        "procurement_short_bidding_window",
        "procurement_single_bidder_high_value",
        "procurement_supplier_concentration_across_entities",
        "project_bpin_procurement_overlap",
    ]
    evidence_counts = {row["signal_id"]: row["evidence_count"] for row in hit_payloads}
    assert evidence_counts == {
        "procurement_buyer_supplier_network_density": 2,
        "procurement_cartel_risk_cobidding": 2,
        "procurement_contract_execution_delay": 2,
        "procurement_contract_suspensions": 3,
        "procurement_cross_source_identity_inconsistency": 2,
        "cuentas_claras_donor_supplier_overlap": 2,
        "pida5_pida27_pida4_chain": 2,
        "procurement_payment_plan_anomalies": 1,
        "procurement_large_modifications": 2,
        "procurement_offers_competition_drop": 2,
        "procurement_contract_value_outlier_by_category": 1,
        "procurement_politically_exposed_position_supplier_overlap": 3,
        "procurement_public_servant_conflict_disclosure_overlap": 2,
        "procurement_related_companies_shared_officer": 4,
        "procurement_repeat_awards_same_supplier": 2,
        "procurement_sanctioned_supplier_awarded": 2,
        "procurement_short_bidding_window": 1,
        "procurement_single_bidder_high_value": 1,
        "procurement_supplier_concentration_across_entities": 2,
        "project_bpin_procurement_overlap": 2,
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
    suspension_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_contract_suspensions:CSUSP-1:2026-06-11"
    )
    assert suspension_evidence["source_id"] == "secop_contract_suspensions"
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
    secop_sanction_evidence = next(
        row
        for row in evidence_payloads
        if row["label"] == "secop_sanctions:ACT-SAN-1"
    )
    assert secop_sanction_evidence["source_id"] == "secop_sanctions"
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

    manifest = json.loads((tmp_path / "meta" / "signal_runs" / "test-run.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["hit_count"] == 21
    assert manifest["evidence_count"] == 42


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
    assert "signal run cli-run: wrote 21 hits and 42 evidence rows" in result.output
    assert (tmp_path / "meta" / "signal_runs" / "cli-run.json").exists()
