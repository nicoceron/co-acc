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
  amount: "Valor",
  average_value: "Valor promedio",
  birth_department: "Departamento de nacimiento",
  birth_municipality: "Municipio de nacimiento",
  buyer_document_id: "Documento del comprador",
  buyer_name: "Comprador público",
  case_name: "Caso",
  cedula: "Cédula",
  city: "Ciudad",
  commitment_number: "Número de compromiso",
  comparison_year: "Año de comparación",
  confidence: "Confianza",
  contract_count: "Contratos",
  contract_value: "Valor contratado",
  country: "País",
  department: "Departamento",
  document_id: "Documento",
  document_type: "Tipo de documento",
  education_level: "Nivel educativo",
  education_track: "Trayectoria educativa",
  entity_chain: "Cadena de entidades",
  entity_type: "Tipo de actor",
  evidence_refs: "Referencias de evidencia",
  execution_ratio: "Avance de ejecución",
  executed_value: "Valor ejecutado",
  death_status_checked_at: "Fecha de verificación de vigencia",
  financial_year: "Año financiero",
  finance_value: "Valor financiero",
  from_party_count: "Cantidad de actores de origen",
  horizon: "Horizonte del proyecto",
  identity_status: "Estado de identidad",
  identity_status_detail: "Detalle del estado de identidad",
  identity_quality: "Calidad de identidad",
  independent_experience_months: "Experiencia independiente (meses)",
  is_group: "Proveedor agrupado",
  is_pep: "Persona expuesta políticamente",
  is_pyme: "SME",
  ifrs_group: "Grupo IFRS",
  legal_nature_name: "Naturaleza jurídica",
  low_competition_bid_count: "Contratos con baja competencia",
  low_competition_bid_value: "Valor de contratos con baja competencia",
  macrosector: "Macrosector",
  name: "Nombre",
  nationality: "Nacionalidad",
  nit: "NIT",
  net_profit_current: "Utilidad neta (año actual)",
  net_profit_previous: "Utilidad neta (año de comparación)",
  new_property_count: "Nuevos predios",
  numero_documento: "Número de documento",
  ocad_name: "OCAD",
  official_names: "Funcionarios relacionados",
  official_officer_count: "Cruces con cargo público",
  official_role_count: "Cruces con rol público",
  operating_revenue_current: "Ingresos operacionales (año actual)",
  operating_revenue_previous: "Ingresos operacionales (año de comparación)",
  origin: "Origen",
  private_experience_months: "Experiencia privada (meses)",
  project_type: "Tipo de proyecto",
  public_experience_months: "Experiencia pública (meses)",
  privacy_policy_url: "URL de política de privacidad",
  razon_social: "Razón social",
  requested_value: "Valor solicitado",
  rurality_category: "Categoría de ruralidad",
  sanction_count: "Sanciones",
  score: "Puntaje",
  sector_name: "Sector",
  sex: "Sexo",
  signal_types: "Tipos de alerta",
  source_count: "Cantidad de fuentes",
  status: "Estado",
  status_source_url: "Fuente del estado",
  subunit: "Subunidad",
  summary_id: "Summary ID",
  supersoc_company_rank: "Puesto en Supersociedades",
  supersoc_financial_year: "Año financiero Supersociedades",
  supersoc_net_profit: "Utilidad neta Supersociedades",
  supersoc_operating_revenue: "Ingresos operacionales Supersociedades",
  supersoc_total_assets: "Activos totales Supersociedades",
  supersoc_total_equity: "Patrimonio total Supersociedades",
  supersoc_total_liabilities: "Pasivos totales Supersociedades",
  supplier_code: "Código del proveedor",
  supplier_document_id: "Documento del proveedor",
  supplier_document_type: "Tipo de documento del proveedor",
  supplier_name: "Proveedor",
  territory_code: "Código territorial",
  total_value: "Valor total",
  total_assets_current: "Activos totales (año actual)",
  total_assets_previous: "Activos totales (año de comparación)",
  total_equity_current: "Patrimonio total (año actual)",
  total_equity_previous: "Patrimonio total (año de comparación)",
  total_liabilities_current: "Pasivos totales (año actual)",
  total_liabilities_previous: "Pasivos totales (año de comparación)",
  transaction_count: "Cantidad de transacciones",
  value: "Valor",
  value_paid: "Valor pagado",
  zone_type: "Tipo de zona",
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
