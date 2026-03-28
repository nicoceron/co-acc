from pydantic import BaseModel


class RiskAlertResponse(BaseModel):
    alert_type: str
    finding_class: str
    severity_score: int
    confidence_tier: str
    reason_text: str
    evidence_refs: list[str]
    source_list: list[str]
    human_review_needed: bool = True
    what_is_unproven: str | None = None
    next_step: str | None = None


class PrioritizedPersonResponse(BaseModel):
    entity_id: str
    name: str
    document_id: str | None = None
    case_person_id: str | None = None
    suspicion_score: int
    signal_types: int
    office_count: int
    donation_count: int
    donation_value: float
    candidacy_count: int
    asset_count: int
    asset_value: float
    finance_count: int
    finance_value: float
    supplier_contract_count: int
    supplier_contract_value: float
    person_sanction_count: int
    disciplinary_sanction_count: int
    fiscal_responsibility_count: int
    conflict_disclosure_count: int
    disclosure_reference_count: int
    corporate_activity_disclosure_count: int
    donor_vendor_loop_count: int
    payment_supervision_count: int
    payment_supervision_company_count: int
    payment_supervision_risk_contract_count: int
    payment_supervision_discrepancy_contract_count: int
    payment_supervision_suspension_contract_count: int
    payment_supervision_pending_contract_count: int
    payment_supervision_contract_value: float
    payment_supervision_archive_contract_count: int
    archive_document_total: int
    archive_supervision_document_total: int
    archive_payment_document_total: int
    archive_assignment_document_total: int
    official_case_bulletin_count: int
    official_case_bulletin_titles: list[str]
    offices: list[str]
    alerts: list[RiskAlertResponse]


class PrioritizedPeopleResponse(BaseModel):
    people: list[PrioritizedPersonResponse]
    total: int


class PrioritizedCompanyResponse(BaseModel):
    entity_id: str
    name: str
    document_id: str | None = None
    suspicion_score: int
    signal_types: int
    contract_count: int
    contract_value: float
    buyer_count: int
    sanction_count: int
    official_officer_count: int
    official_role_count: int
    low_competition_bid_count: int
    low_competition_bid_value: float
    direct_invitation_bid_count: int
    funding_overlap_event_count: int
    funding_overlap_total: float
    capacity_mismatch_contract_count: int
    capacity_mismatch_contract_value: float
    capacity_mismatch_revenue_ratio: float
    capacity_mismatch_asset_ratio: float
    execution_gap_contract_count: int
    execution_gap_invoice_total: float
    commitment_gap_contract_count: int
    commitment_gap_total: float
    suspension_contract_count: int
    suspension_event_count: int
    sanctioned_still_receiving_contract_count: int
    sanctioned_still_receiving_total: float
    split_contract_group_count: int
    split_contract_total: float
    archive_contract_count: int
    archive_document_total: int
    archive_supervision_contract_count: int
    archive_supervision_document_total: int
    archive_payment_contract_count: int
    archive_payment_document_total: int
    archive_assignment_contract_count: int
    archive_assignment_document_total: int
    official_names: list[str]
    alerts: list[RiskAlertResponse]


class PrioritizedCompaniesResponse(BaseModel):
    companies: list[PrioritizedCompanyResponse]
    total: int


class PrioritizedBuyerResponse(BaseModel):
    buyer_id: str
    buyer_name: str
    buyer_document_id: str | None = None
    suspicion_score: int
    signal_types: int
    contract_count: int
    contract_value: float
    supplier_count: int
    top_supplier_name: str | None = None
    top_supplier_document_id: str | None = None
    top_supplier_share: float
    low_competition_contract_count: int
    direct_invitation_contract_count: int
    sanctioned_supplier_contract_count: int
    sanctioned_supplier_value: float
    official_overlap_contract_count: int
    official_overlap_supplier_count: int
    capacity_mismatch_supplier_count: int
    discrepancy_contract_count: int
    discrepancy_value: float
    alerts: list[RiskAlertResponse]


class PrioritizedBuyersResponse(BaseModel):
    buyers: list[PrioritizedBuyerResponse]
    total: int


class PrioritizedTerritoryResponse(BaseModel):
    territory_id: str
    territory_name: str
    department: str
    municipality: str | None = None
    suspicion_score: int
    signal_types: int
    contract_count: int
    contract_value: float
    buyer_count: int
    supplier_count: int
    top_supplier_name: str | None = None
    top_supplier_share: float
    low_competition_contract_count: int
    direct_invitation_contract_count: int
    sanctioned_supplier_contract_count: int
    sanctioned_supplier_value: float
    official_overlap_contract_count: int
    capacity_mismatch_supplier_count: int
    discrepancy_contract_count: int
    discrepancy_value: float
    alerts: list[RiskAlertResponse]


class PrioritizedTerritoriesResponse(BaseModel):
    territories: list[PrioritizedTerritoryResponse]
    total: int


class ValidationCaseResult(BaseModel):
    case_id: str
    title: str
    category: str
    entity_type: str
    entity_ref: str
    entity_id: str | None = None
    entity_name: str | None = None
    status: str
    matched: bool
    expected_signals: list[str]
    observed_signals: list[str]
    matched_signals: list[str]
    summary: str
    metrics: dict[str, str | float | int | bool | list[str] | None]
    public_sources: list[str]


class ValidationCasesResponse(BaseModel):
    cases: list[ValidationCaseResult]
    total: int
    matched: int
