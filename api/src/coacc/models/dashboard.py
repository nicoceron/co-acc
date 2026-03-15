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


class SuspiciousPersonResponse(BaseModel):
    entity_id: str
    name: str
    document_id: str | None = None
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
    conflict_disclosure_count: int
    disclosure_reference_count: int
    corporate_activity_disclosure_count: int
    donor_vendor_loop_count: int
    offices: list[str]
    alerts: list[RiskAlertResponse]


class SuspiciousPeopleResponse(BaseModel):
    people: list[SuspiciousPersonResponse]
    total: int


class SuspiciousCompanyResponse(BaseModel):
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
    official_names: list[str]
    alerts: list[RiskAlertResponse]


class SuspiciousCompaniesResponse(BaseModel):
    companies: list[SuspiciousCompanyResponse]
    total: int


class SuspiciousBuyerResponse(BaseModel):
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


class SuspiciousBuyersResponse(BaseModel):
    buyers: list[SuspiciousBuyerResponse]
    total: int


class SuspiciousTerritoryResponse(BaseModel):
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


class SuspiciousTerritoriesResponse(BaseModel):
    territories: list[SuspiciousTerritoryResponse]
    total: int
