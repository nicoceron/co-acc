from coacc.routers.meta import (
    _build_buyer_alerts,
    _build_company_alerts,
    _build_person_alerts,
    _build_territory_alerts,
)


def test_company_discrepancy_alert_uses_execution_sources_when_no_commitments() -> None:
    alerts = _build_company_alerts({
        "document_id": "900123456",
        "execution_gap_contract_count": 2,
        "commitment_gap_contract_count": 0,
        "contract_value": 150_000_000.0,
    })

    discrepancy_alert = next(
        alert for alert in alerts if alert.alert_type == "budget_execution_discrepancy"
    )

    assert discrepancy_alert.reason_text == (
        "Los contratos asociados muestran brechas entre facturación"
        " y avance de ejecución."
    )
    assert discrepancy_alert.source_list == [
        "SECOP II facturas",
        "SECOP II ejecución contratos",
        "SECOP / SECOP II contratos",
    ]


def test_company_suspension_alert_uses_suspension_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "900123456",
        "suspension_contract_count": 2,
        "suspension_event_count": 3,
    })

    suspension_alert = next(
        alert for alert in alerts if alert.alert_type == "contract_suspension_stacking"
    )

    assert "suspensiones reiteradas" in suspension_alert.reason_text
    assert suspension_alert.source_list == [
        "SECOP II - Suspensiones de Contratos",
        "SECOP / SECOP II contratos",
    ]


def test_company_interadmin_alert_uses_interadmin_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "900258772",
        "interadmin_agreement_count": 14,
        "interadmin_total": 66_801_179_912.0,
        "interadmin_risk_contract_count": 1,
        "official_officer_count": 1,
        "execution_gap_contract_count": 1,
        "commitment_gap_contract_count": 0,
    })

    interadmin_alert = next(
        alert for alert in alerts if alert.alert_type == "interadministrative_channel_stacking"
    )

    assert "convenios interadministrativos" in interadmin_alert.reason_text
    assert interadmin_alert.source_list == [
        "SECOP II - Convenios Interadministrativos",
        "SECOP / SECOP II contratos",
        "SIGEP II",
        "SECOP II facturas",
        "SECOP II ejecución contratos",
    ]


def test_company_sensitive_official_overlap_uses_sensitive_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "900258772",
        "official_officer_count": 1,
        "official_role_count": 1,
        "sensitive_officer_count": 1,
        "sensitive_role_count": 1,
    })

    sensitive_alert = next(
        alert
        for alert in alerts
        if alert.alert_type == "sensitive_public_official_supplier_overlap"
    )

    assert "cargos públicos sensibles" in sensitive_alert.reason_text
    assert sensitive_alert.source_list == [
        "SIGEP II",
        "Puestos Sensibles SIGEP",
        "RUES / registros societarios",
        "SECOP / SECOP II",
    ]


def test_company_sanctioned_still_receiving_alert_uses_sanction_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "800154801",
        "sanction_count": 1,
        "sanctioned_still_receiving_contract_count": 1,
        "sanctioned_still_receiving_total": 372_549_587.0,
    })

    still_receiving_alert = next(
        alert for alert in alerts if alert.alert_type == "sanctioned_still_receiving"
    )

    assert "ventana pública de sanción" in still_receiving_alert.reason_text
    assert still_receiving_alert.source_list == [
        "SIRI / Responsabilidad Fiscal / PACO",
        "SECOP / SECOP II",
    ]


def test_company_sanctioned_still_receiving_alert_uses_archive_sources_when_present() -> None:
    alerts = _build_company_alerts({
        "document_id": "800154801",
        "sanction_count": 1,
        "sanctioned_still_receiving_contract_count": 1,
        "sanctioned_still_receiving_total": 372_549_587.0,
        "archive_contract_count": 2,
        "archive_document_total": 11,
        "archive_supervision_document_total": 3,
        "archive_payment_document_total": 2,
    })

    still_receiving_alert = next(
        alert for alert in alerts if alert.alert_type == "sanctioned_still_receiving"
    )

    assert "SECOP II - Archivos Descarga Desde 2025" in still_receiving_alert.source_list
    assert "soporte(s) documentales visibles" in still_receiving_alert.reason_text


def test_company_split_contract_alert_uses_secop_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "1012441381",
        "split_contract_group_count": 1,
        "split_contract_total": 131_000_000.0,
    })

    split_alert = next(
        alert for alert in alerts if alert.alert_type == "split_contracts_below_threshold"
    )

    assert "posible fraccionamiento" in split_alert.reason_text
    assert split_alert.source_list == [
        "SECOP / SECOP II",
    ]


def test_company_education_control_alert_uses_men_and_secop_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "8605242195",
        "company_type": "INSTITUCION_EDUCACION_SUPERIOR",
        "education_director_count": 2,
        "education_alias_count": 1,
        "education_procurement_link_count": 2,
        "education_family_tie_count": 0,
    })

    education_alert = next(
        alert for alert in alerts if alert.alert_type == "education_control_capture"
    )

    assert "alias institucional" in education_alert.reason_text
    assert education_alert.source_list == [
        "MEN instituciones de educación superior",
        "MEN directivos de IES",
        "SECOP II - Convenios Interadministrativos",
    ]


def test_person_candidate_supplier_alert_uses_election_sources() -> None:
    alerts = _build_person_alerts({
        "document_id": "52184154",
        "candidacy_count": 1,
        "supplier_contract_count": 2,
        "donation_count": 2,
    })

    candidate_alert = next(
        alert for alert in alerts if alert.alert_type == "candidate_supplier_overlap"
    )

    assert "candidaturas electorales" in candidate_alert.reason_text
    assert candidate_alert.source_list == [
        "Cuentas Claras / elecciones",
        "SECOP / SECOP II",
    ]


def test_person_payment_supervision_alert_uses_payment_sources() -> None:
    alerts = _build_person_alerts({
        "document_id": "80798446",
        "office_count": 1,
        "donation_count": 0,
        "candidacy_count": 0,
        "payment_supervision_count": 6,
        "payment_supervision_company_count": 4,
        "payment_supervision_risk_contract_count": 3,
        "payment_supervision_discrepancy_contract_count": 2,
        "payment_supervision_suspension_contract_count": 1,
        "payment_supervision_pending_contract_count": 1,
    })

    supervision_alert = next(
        alert for alert in alerts if alert.alert_type == "payment_supervision_risk_stack"
    )

    assert "supervisor(a) de pago" in supervision_alert.reason_text
    assert supervision_alert.source_list == [
        "SECOP II - Plan de pagos",
        "SECOP / SECOP II contratos",
        "SECOP II facturas",
        "SECOP II ejecución contratos",
        "SECOP II - Suspensiones de Contratos",
        "SIGEP II",
    ]


def test_person_payment_supervision_alert_uses_archive_sources_when_present() -> None:
    alerts = _build_person_alerts({
        "document_id": "80798446",
        "office_count": 1,
        "donation_count": 0,
        "candidacy_count": 0,
        "payment_supervision_count": 6,
        "payment_supervision_company_count": 4,
        "payment_supervision_risk_contract_count": 3,
        "payment_supervision_discrepancy_contract_count": 2,
        "payment_supervision_suspension_contract_count": 1,
        "payment_supervision_pending_contract_count": 1,
        "payment_supervision_archive_contract_count": 2,
        "archive_document_total": 14,
        "archive_supervision_document_total": 4,
        "archive_payment_document_total": 3,
        "archive_assignment_document_total": 2,
    })

    supervision_alert = next(
        alert for alert in alerts if alert.alert_type == "payment_supervision_risk_stack"
    )

    assert "SECOP II - Archivos Descarga Desde 2025" in supervision_alert.source_list
    assert "expediente público ya aporta" in supervision_alert.reason_text


def test_person_sanctioned_exposure_alert_uses_control_sources() -> None:
    alerts = _build_person_alerts({
        "document_id": "23623740",
        "office_count": 1,
        "supplier_contract_count": 1,
        "donation_count": 2,
        "candidacy_count": 0,
        "payment_supervision_count": 0,
        "payment_supervision_risk_contract_count": 0,
        "person_sanction_count": 6,
        "disciplinary_sanction_count": 6,
        "fiscal_responsibility_count": 0,
    })

    sanction_alert = next(
        alert for alert in alerts if alert.alert_type == "sanctioned_person_exposure_stack"
    )

    assert "sanción" in sanction_alert.reason_text
    assert sanction_alert.source_list == [
        "SIRI / Responsabilidad Fiscal / PACO",
        "SIGEP II",
        "SECOP / SECOP II",
        "Cuentas Claras / elecciones",
    ]


def test_person_official_case_bulletin_alert_uses_official_sources() -> None:
    alerts = _build_person_alerts({
        "document_id": "10136043",
        "case_person_id": None,
        "office_count": 0,
        "supplier_contract_count": 0,
        "donation_count": 0,
        "candidacy_count": 0,
        "payment_supervision_count": 0,
        "payment_supervision_risk_contract_count": 0,
        "person_sanction_count": 18,
        "official_case_bulletin_count": 1,
        "official_case_bulletin_titles": [
            "Procuraduria confirmo sancion a supervisor por omitir vigilancia de contrato en Pereira"
        ],
    })

    bulletin_alert = next(
        alert for alert in alerts if alert.alert_type == "official_case_bulletin_exposure"
    )

    assert "boletin(es) oficial(es)" in bulletin_alert.reason_text
    assert bulletin_alert.source_list == [
        "Procuraduria / Fiscalia / Contraloria / MEN - boletines oficiales",
    ]


def test_company_fiscal_finding_alert_uses_official_control_sources() -> None:
    alerts = _build_company_alerts({
        "document_id": "891580016",
        "fiscal_finding_count": 2,
        "fiscal_finding_total": 155_000_000.0,
        "contract_count": 4,
    })

    finding_alert = next(
        alert for alert in alerts if alert.alert_type == "fiscal_finding_record"
    )

    assert "hallazgo(s) fiscal(es)" in finding_alert.reason_text
    assert finding_alert.source_list == [
        "Hallazgos Fiscales",
        "SECOP / SECOP II",
    ]


def test_buyer_fiscal_findings_alert_uses_official_control_sources() -> None:
    alerts = _build_buyer_alerts({
        "buyer_document_id": "891580016",
        "buyer_name": "GOBERNACION DEL CAUCA",
        "fiscal_finding_count": 3,
        "fiscal_finding_total": 300_000_000.0,
        "contract_count": 10,
    })

    finding_alert = next(
        alert for alert in alerts if alert.alert_type == "buyer_fiscal_findings_exposure"
    )

    assert "hallazgos fiscales oficiales" in finding_alert.reason_text
    assert finding_alert.source_list == [
        "Hallazgos Fiscales",
        "SECOP / SECOP II",
    ]


def test_buyer_discrepancy_alert_includes_commitment_sources_when_present() -> None:
    alerts = _build_buyer_alerts({
        "buyer_document_id": "830063506",
        "buyer_name": "TRANSMILENIO S.A.",
        "discrepancy_contract_count": 3,
        "execution_gap_contract_count": 1,
        "commitment_gap_contract_count": 2,
        "discrepancy_value": 320_000_000.0,
    })

    discrepancy_alert = next(
        alert for alert in alerts if alert.alert_type == "buyer_budget_execution_discrepancy"
    )

    assert discrepancy_alert.reason_text == (
        "Los contratos del comprador muestran brechas entre compromiso,"
        " facturación y avance de ejecución."
    )
    assert discrepancy_alert.source_list == [
        "SECOP II facturas",
        "SECOP II compromisos",
        "SECOP II ejecución contratos",
        "SECOP / SECOP II contratos",
    ]


def test_territory_discrepancy_alert_keeps_project_snapshot_sources() -> None:
    alerts = _build_territory_alerts({
        "territory_name": "Bogotá, Distrito Capital de Bogotá",
        "supplier_count": 0,
        "buyer_count": 1,
        "discrepancy_contract_count": 2,
        "discrepancy_value": 540_000_000.0,
    })

    discrepancy_alert = next(
        alert
        for alert in alerts
        if alert.alert_type == "territory_budget_execution_discrepancy"
    )

    assert discrepancy_alert.reason_text == (
        "En Bogotá, Distrito Capital de Bogotá hay proyectos públicos con brechas"
        " entre avance financiero y avance físico."
    )
    assert discrepancy_alert.source_list == ["MapaInversiones", "PTE / SGR"]
