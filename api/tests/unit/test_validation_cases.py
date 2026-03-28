from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_known_case_validation_endpoint_returns_live_case_matches(
    client: AsyncClient,
) -> None:
    def _fake_execute_query_single(
        _session,
        query_name: str,
        params: dict[str, str],
        timeout: int | None = None,
    ) -> dict[str, object] | None:
        del timeout
        if query_name == "meta_validation_company_case" and params["document_id"] == "900258772":
            return {
                "entity_id": "4:fondecun",
                "name": "FONDO DE DESARROLLO DE PROYECTOS DE CUNDINAMARCA",
                "document_id": "900258772",
                "contract_count": 4,
                "contract_value": 32_854_689_276.0,
                "sanction_count": 0,
                "official_officer_count": 1,
                "official_role_count": 1,
                "sensitive_officer_count": 1,
                "sensitive_role_count": 1,
                "official_names": ["GERMAN FUERTES CHAPARRO"],
                "funding_overlap_event_count": 0,
                "funding_overlap_total": 0.0,
                "capacity_mismatch_contract_count": 0,
                "capacity_mismatch_contract_value": 0.0,
                "capacity_mismatch_revenue_ratio": 0.0,
                "capacity_mismatch_asset_ratio": 0.0,
                "execution_gap_contract_count": 1,
                "execution_gap_invoice_total": 2_142_616_823.0,
                "commitment_gap_contract_count": 0,
                "commitment_gap_total": 0.0,
                "suspension_contract_count": 0,
                "suspension_event_count": 0,
                "sanctioned_still_receiving_contract_count": 0,
                "sanctioned_still_receiving_total": 0.0,
                "split_contract_group_count": 0,
                "split_contract_total": 0.0,
            }
        if query_name == "meta_validation_company_case" and params["document_id"] == "900398793":
            return {
                "entity_id": "4:egobus",
                "name": "EGOBUS SAS",
                "document_id": "900398793",
                "contract_count": 0,
                "contract_value": 0.0,
                "sanction_count": 12,
                "official_officer_count": 0,
                "official_role_count": 0,
                "sensitive_officer_count": 0,
                "sensitive_role_count": 0,
                "official_names": [],
                "funding_overlap_event_count": 0,
                "funding_overlap_total": 0.0,
                "capacity_mismatch_contract_count": 0,
                "capacity_mismatch_contract_value": 0.0,
                "capacity_mismatch_revenue_ratio": 0.0,
                "capacity_mismatch_asset_ratio": 0.0,
                "execution_gap_contract_count": 0,
                "execution_gap_invoice_total": 0.0,
                "commitment_gap_contract_count": 0,
                "commitment_gap_total": 0.0,
                "suspension_contract_count": 0,
                "suspension_event_count": 0,
                "sanctioned_still_receiving_contract_count": 0,
                "sanctioned_still_receiving_total": 0.0,
                "split_contract_group_count": 0,
                "split_contract_total": 0.0,
            }
        if query_name == "meta_validation_company_case" and params["document_id"] == "900396145":
            return {
                "entity_id": "4:coobus",
                "name": "COOBUS SAS",
                "document_id": "900396145",
                "contract_count": 0,
                "contract_value": 0.0,
                "sanction_count": 4,
                "official_officer_count": 0,
                "official_role_count": 0,
                "sensitive_officer_count": 0,
                "sensitive_role_count": 0,
                "official_names": [],
                "funding_overlap_event_count": 0,
                "funding_overlap_total": 0.0,
                "capacity_mismatch_contract_count": 0,
                "capacity_mismatch_contract_value": 0.0,
                "capacity_mismatch_revenue_ratio": 0.0,
                "capacity_mismatch_asset_ratio": 0.0,
                "execution_gap_contract_count": 0,
                "execution_gap_invoice_total": 0.0,
                "commitment_gap_contract_count": 0,
                "commitment_gap_total": 0.0,
                "suspension_contract_count": 0,
                "suspension_event_count": 0,
                "sanctioned_still_receiving_contract_count": 0,
                "sanctioned_still_receiving_total": 0.0,
                "split_contract_group_count": 0,
                "split_contract_total": 0.0,
            }
        if query_name == "meta_validation_company_case" and params["document_id"] == "800154801":
            return {
                "entity_id": "4:suministros-maybe",
                "name": "SUMINISTROS MAYBE S.A.S.",
                "document_id": "800154801",
                "contract_count": 1,
                "contract_value": 372_549_587.0,
                "sanction_count": 1,
                "official_officer_count": 0,
                "official_role_count": 0,
                "sensitive_officer_count": 0,
                "sensitive_role_count": 0,
                "official_names": [],
                "funding_overlap_event_count": 0,
                "funding_overlap_total": 0.0,
                "capacity_mismatch_contract_count": 0,
                "capacity_mismatch_contract_value": 0.0,
                "capacity_mismatch_revenue_ratio": 0.0,
                "capacity_mismatch_asset_ratio": 0.0,
                "execution_gap_contract_count": 0,
                "execution_gap_invoice_total": 0.0,
                "commitment_gap_contract_count": 0,
                "commitment_gap_total": 0.0,
                "suspension_contract_count": 0,
                "suspension_event_count": 0,
                "sanctioned_still_receiving_contract_count": 1,
                "sanctioned_still_receiving_total": 372_549_587.0,
                "split_contract_group_count": 0,
                "split_contract_total": 0.0,
            }
        if query_name == "meta_validation_company_case" and params["document_id"] == "8605242195":
            return {
                "entity_id": "4:san-jose",
                "name": "FUNDACION DE EDUCACION SUPERIOR SAN JOSE -FESSANJOSE-",
                "document_id": "8605242195",
                "company_type": "INSTITUCION_EDUCACION_SUPERIOR",
                "contract_count": 0,
                "contract_value": 0.0,
                "sanction_count": 0,
                "official_officer_count": 0,
                "official_role_count": 0,
                "sensitive_officer_count": 0,
                "sensitive_role_count": 0,
                "official_names": [],
                "funding_overlap_event_count": 0,
                "funding_overlap_total": 0.0,
                "capacity_mismatch_contract_count": 0,
                "capacity_mismatch_contract_value": 0.0,
                "capacity_mismatch_revenue_ratio": 0.0,
                "capacity_mismatch_asset_ratio": 0.0,
                "execution_gap_contract_count": 0,
                "execution_gap_invoice_total": 0.0,
                "commitment_gap_contract_count": 0,
                "commitment_gap_total": 0.0,
                "interadmin_agreement_count": 0,
                "interadmin_total": 0.0,
                "education_director_count": 2,
                "education_family_tie_count": 0,
                "education_alias_count": 1,
                "education_procurement_link_count": 2,
                "education_procurement_total": 0.0,
                "suspension_contract_count": 0,
                "suspension_event_count": 0,
                "sanctioned_still_receiving_contract_count": 0,
                "sanctioned_still_receiving_total": 0.0,
                "split_contract_group_count": 0,
                "split_contract_total": 0.0,
                "interadmin_risk_contract_count": 0,
            }
        if query_name == "meta_validation_person_case" and params["person_ref"] == "52184154":
            return {
                "entity_id": "4:vivian",
                "name": "VIVIAN DEL ROSARIO MORENO PEREZ",
                "document_id": "52184154",
                "case_person_id": None,
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 1,
                "supplier_contract_count": 2,
                "supplier_contract_value": 111_240_000.0,
                "donation_count": 2,
                "donation_value": 2_400_000.0,
                "candidacy_count": 1,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "official_case_bulletin_count": 0,
                "official_case_bulletin_titles": [],
                "donor_vendor_loop_count": 0,
            }
        if query_name == "meta_validation_person_case" and params["person_ref"] == "10136043":
            return {
                "entity_id": "4:alejandro",
                "name": "ALEJANDRO OSPINA COLL",
                "document_id": "10136043",
                "case_person_id": None,
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 18,
                "disciplinary_sanction_count": 18,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Procuraduria confirmo sancion a supervisor por omitir vigilancia de contrato en Pereira"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_federico_garcia_arbelaez"
        ):
            return {
                "entity_id": "4:federico-case",
                "name": "FEDERICO GARCIA ARBELAEZ",
                "document_id": None,
                "case_person_id": "case_person_federico_garcia_arbelaez",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 1,
                "supplier_contract_count": 81,
                "supplier_contract_value": 5_533_464_368.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Procuraduria formulo cargos a contratista del SENA por extralimitacion en funciones de interventoria"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_jaime_jose_garces_garcia"
        ):
            return {
                "entity_id": "4:jaime-case",
                "name": "JAIME JOSE GARCES GARCIA",
                "document_id": None,
                "case_person_id": "case_person_jaime_jose_garces_garcia",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Procuraduria formulo cargos a funcionario de Aguas del Cesar por presuntas irregularidades en supervision"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_olmedo_de_jesus_lopez_martinez"
        ):
            return {
                "entity_id": "4:olmedo-case",
                "name": "OLMEDO DE JESUS LOPEZ MARTINEZ",
                "document_id": None,
                "case_person_id": "case_person_olmedo_de_jesus_lopez_martinez",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Nueva imputacion de cargos contra exdirectivos de la UNGRD, Olmedo Lopez y Sneyder Pinilla, por direccionamiento irregular de la contratacion en la entidad"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_sneyder_augusto_pinilla_alvarez"
        ):
            return {
                "entity_id": "4:sneyder-case",
                "name": "SNEYDER AUGUSTO PINILLA ALVAREZ",
                "document_id": None,
                "case_person_id": "case_person_sneyder_augusto_pinilla_alvarez",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Nueva imputacion de cargos contra exdirectivos de la UNGRD, Olmedo Lopez y Sneyder Pinilla, por direccionamiento irregular de la contratacion en la entidad"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_carlos_ramon_gonzalez_merchan"
        ):
            return {
                "entity_id": "4:carlos-case",
                "name": "CARLOS RAMON GONZALEZ MERCHAN",
                "document_id": None,
                "case_person_id": "case_person_carlos_ramon_gonzalez_merchan",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Acusado exdirector del Departamento Administrativo de Presidencia, Carlos Ramon Gonzalez Merchan, por presuntamente direccionar dadivas en favor de congresistas con recursos de la UNGRD"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_luis_carlos_barreto_gantiva"
        ):
            return {
                "entity_id": "4:barreto-case",
                "name": "LUIS CARLOS BARRETO GANTIVA",
                "document_id": None,
                "case_person_id": "case_person_luis_carlos_barreto_gantiva",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Exdirector de conocimiento de la UNGRD, Luis Carlos Barreto Gantiva, sera condenado mediante preacuerdo por direccionamiento de contratos en la entidad"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_sandra_liliana_ortiz_nova"
        ):
            return {
                "entity_id": "4:sandra-case",
                "name": "SANDRA LILIANA ORTIZ NOVA",
                "document_id": None,
                "case_person_id": "case_person_sandra_liliana_ortiz_nova",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Acusada exconsejera presidencial para las regiones, Sandra Ortiz, por presuntamente trasladar dadivas relacionadas con actos de corrupcion en la UNGRD"
                ],
                "donor_vendor_loop_count": 0,
            }
        if (
            query_name == "meta_validation_person_case"
            and params["person_ref"] == "case_person_maria_alejandra_benavides_soto"
        ):
            return {
                "entity_id": "4:benavides-case",
                "name": "MARIA ALEJANDRA BENAVIDES SOTO",
                "document_id": None,
                "case_person_id": "case_person_maria_alejandra_benavides_soto",
                "office_count": 0,
                "sensitive_office_count": 0,
                "offices": [],
                "linked_supplier_company_count": 0,
                "supplier_contract_count": 0,
                "supplier_contract_value": 0.0,
                "donation_count": 0,
                "donation_value": 0.0,
                "candidacy_count": 0,
                "asset_count": 0,
                "conflict_disclosure_count": 0,
                "disclosure_reference_count": 0,
                "corporate_activity_disclosure_count": 0,
                "person_sanction_count": 0,
                "disciplinary_sanction_count": 0,
                "fiscal_responsibility_count": 0,
                "official_case_bulletin_count": 1,
                "official_case_bulletin_titles": [
                    "Imputada exasesora del Ministerio de Hacienda por su presunta intervencion en el direccionamiento de contratos en la UNGRD en favor de congresistas"
                ],
                "donor_vendor_loop_count": 0,
            }
        return None

    with patch(
        "coacc.routers.meta.execute_query_single",
        new=AsyncMock(side_effect=_fake_execute_query_single),
    ):
        response = await client.get("/api/v1/meta/validation/known-cases")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 16
    assert data["matched"] == 16

    egobus = next(case for case in data["cases"] if case["case_id"] == "egobus_sanctioned_supplier")
    assert egobus["status"] == "matched"
    assert "sanctioned_supplier_record" in egobus["observed_signals"]

    coobus = next(case for case in data["cases"] if case["case_id"] == "coobus_sanctioned_supplier")
    assert coobus["status"] == "matched"
    assert "sanctioned_supplier_record" in coobus["observed_signals"]

    suministros = next(
        case
        for case in data["cases"]
        if case["case_id"] == "suministros_maybe_sanctioned_still_receiving"
    )
    assert suministros["status"] == "matched"
    assert "sanctioned_supplier_record" in suministros["observed_signals"]
    assert "sanctioned_still_receiving" in suministros["observed_signals"]

    vivian = next(
        case for case in data["cases"] if case["case_id"] == "vivian_moreno_candidate_supplier"
    )
    assert vivian["status"] == "matched"
    assert vivian["matched_signals"] == ["candidate_supplier_overlap"]

    san_jose = next(
        case for case in data["cases"] if case["case_id"] == "san_jose_education_control_capture"
    )
    assert san_jose["status"] == "matched"
    assert san_jose["matched_signals"] == ["education_control_capture"]

    alejandro = next(
        case for case in data["cases"] if case["case_id"] == "alejandro_ospina_coll_bulletin_exposure"
    )
    assert alejandro["status"] == "matched"
    assert alejandro["matched_signals"] == ["official_case_bulletin_exposure"]

    federico = next(
        case
        for case in data["cases"]
        if case["case_id"] == "federico_garcia_arbelaez_bulletin_exposure"
    )
    assert federico["status"] == "matched"
    assert federico["matched_signals"] == ["official_case_bulletin_exposure"]

    jaime = next(
        case
        for case in data["cases"]
        if case["case_id"] == "jaime_jose_garces_garcia_bulletin_record"
    )
    assert jaime["status"] == "matched"
    assert jaime["matched_signals"] == ["official_case_bulletin_record"]

    for case_id in (
        "olmedo_lopez_ungrd_bulletin_record",
        "sneyder_pinilla_ungrd_bulletin_record",
        "carlos_ramon_gonzalez_ungrd_bulletin_record",
        "luis_carlos_barreto_ungrd_bulletin_record",
        "sandra_ortiz_ungrd_bulletin_record",
        "maria_alejandra_benavides_ungrd_bulletin_record",
    ):
        case = next(item for item in data["cases"] if item["case_id"] == case_id)
        assert case["status"] == "matched"
        assert case["matched_signals"] == ["official_case_bulletin_record"]
