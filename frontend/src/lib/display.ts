const SOURCE_LABELS: Record<string, string> = {
  asset_disclosures: "Asset disclosures",
  conflict_disclosures: "Conflict-of-interest disclosures",
  cuentas_claras_income_2019: "Cuentas Claras 2019 campaign income",
  health_providers: "REPS health providers",
  higher_ed_enrollment: "Higher education enrollment",
  igac_property_transactions: "IGAC property transactions",
  mapa_inversiones_projects: "MapaInversiones projects",
  paco_sanctions: "PACO sanctions and red flags",
  pte_sector_commitments: "PTE sector commitments",
  pte_top_contracts: "PTE top contracts",
  registraduria_death_status_checks: "Registraduria death-status checks",
  rues_chambers: "RUES chambers of commerce",
  secop_budget_commitments: "SECOP II budget commitments",
  secop_cdp_requests: "SECOP II CDP requests",
  secop_contract_additions: "SECOP II contract additions",
  secop_contract_execution: "SECOP II contract execution",
  secop_contract_modifications: "SECOP II contract modifications",
  secop_execution_locations: "SECOP II execution locations",
  secop_ii_contracts: "SECOP II contracts",
  secop_ii_processes: "SECOP II procurement processes",
  secop_integrado: "Integrated SECOP contracts",
  secop_invoices: "SECOP II invoices",
  secop_offers: "SECOP II offers",
  secop_sanctions: "SECOP sanctions",
  secop_suppliers: "SECOP II suppliers",
  sgr_expense_execution: "SGR expense execution",
  sgr_projects: "SGR projects",
  sigep_public_servants: "SIGEP public servants",
  sigep_sensitive_positions: "SIGEP sensitive positions",
  supersoc_top_companies: "Supersociedades top companies",
};

const PROPERTY_LABELS: Record<string, string> = {
  amount: "Amount",
  average_value: "Average value",
  birth_department: "Birth department",
  birth_municipality: "Birth municipality",
  buyer_document_id: "Buyer document ID",
  buyer_name: "Buyer name",
  case_name: "Case",
  cedula: "National ID",
  city: "City",
  commitment_number: "Commitment number",
  comparison_year: "Comparison year",
  confidence: "Confidence",
  contract_count: "Contract count",
  contract_value: "Contract value",
  country: "Country",
  department: "Department",
  document_id: "Document ID",
  document_type: "Document type",
  education_level: "Education level",
  education_track: "Education track",
  entity_chain: "Entity chain",
  entity_type: "Entity type",
  evidence_refs: "Evidence references",
  execution_ratio: "Execution ratio",
  executed_value: "Executed value",
  death_status_checked_at: "Death-status checked at",
  financial_year: "Financial year",
  finance_value: "Finance value",
  from_party_count: "Originating party count",
  horizon: "Project horizon",
  identity_status: "Identity status",
  identity_status_detail: "Identity status detail",
  identity_quality: "Identity quality",
  independent_experience_months: "Independent experience (months)",
  is_group: "Group supplier",
  is_pep: "Politically exposed person",
  is_pyme: "SME",
  ifrs_group: "IFRS group",
  legal_nature_name: "Legal nature",
  low_competition_bid_count: "Low-competition bid count",
  low_competition_bid_value: "Low-competition bid value",
  macrosector: "Macro sector",
  name: "Name",
  nationality: "Nationality",
  nit: "NIT",
  net_profit_current: "Net profit (current year)",
  net_profit_previous: "Net profit (comparison year)",
  new_property_count: "New property count",
  numero_documento: "Document number",
  ocad_name: "OCAD",
  official_names: "Public officials",
  official_officer_count: "Official-officer overlaps",
  official_role_count: "Public-role overlaps",
  operating_revenue_current: "Operating revenue (current year)",
  operating_revenue_previous: "Operating revenue (comparison year)",
  origin: "Origin",
  private_experience_months: "Private-sector experience (months)",
  project_type: "Project type",
  public_experience_months: "Public-sector experience (months)",
  privacy_policy_url: "Privacy policy URL",
  razao_social: "Legal name",
  requested_value: "Requested value",
  rurality_category: "Rurality category",
  sanction_count: "Sanction count",
  score: "Score",
  sector_name: "Sector",
  sex: "Sex",
  signal_types: "Signal types",
  source_count: "Source count",
  status: "Status",
  status_source_url: "Status source URL",
  subunit: "Sub-unit",
  summary_id: "Summary ID",
  supersoc_company_rank: "Supersoc rank",
  supersoc_financial_year: "Supersoc financial year",
  supersoc_net_profit: "Supersoc net profit",
  supersoc_operating_revenue: "Supersoc operating revenue",
  supersoc_total_assets: "Supersoc total assets",
  supersoc_total_equity: "Supersoc total equity",
  supersoc_total_liabilities: "Supersoc total liabilities",
  supplier_code: "Supplier code",
  supplier_document_id: "Supplier document ID",
  supplier_document_type: "Supplier document type",
  supplier_name: "Supplier name",
  territory_code: "Territory code",
  total_value: "Total value",
  total_assets_current: "Total assets (current year)",
  total_assets_previous: "Total assets (comparison year)",
  total_equity_current: "Total equity (current year)",
  total_equity_previous: "Total equity (comparison year)",
  total_liabilities_current: "Total liabilities (current year)",
  total_liabilities_previous: "Total liabilities (comparison year)",
  transaction_count: "Transaction count",
  value: "Value",
  value_paid: "Paid value",
  zone_type: "Zone type",
};

const TOKEN_LABELS: Record<string, string> = {
  cdp: "CDP",
  id: "ID",
  nit: "NIT",
  reps: "REPS",
  secop: "SECOP",
  sgr: "SGR",
  sigep: "SIGEP",
  siif: "SIIF",
  sme: "SME",
  url: "URL",
};

function titleizeToken(token: string): string {
  const normalized = token.trim();
  if (!normalized) return "";

  const lower = normalized.toLowerCase();
  if (TOKEN_LABELS[lower]) {
    return TOKEN_LABELS[lower];
  }
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

export function humanizeIdentifier(value: string): string {
  return value
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .split(/[_\-\s]+/)
    .filter(Boolean)
    .map(titleizeToken)
    .join(" ");
}

export function formatSourceName(source: string): string {
  return SOURCE_LABELS[source] ?? humanizeIdentifier(source);
}

export function formatPropertyLabel(key: string): string {
  return PROPERTY_LABELS[key] ?? humanizeIdentifier(key);
}
