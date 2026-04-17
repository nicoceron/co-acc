import type {
  GraphData,
  GraphEdge,
  GraphNode,
  PrioritizedBuyer,
  PrioritizedCompany,
  PrioritizedPerson,
  PrioritizedTerritory,
  StatsResponse,
} from "@/api/client";

export interface MaterializedAlertSummary {
  alert_type: string;
  label: string;
  reason_text: string;
  confidence_tier: string;
  severity_score: number;
  source_list: string[];
}

export interface MaterializedPatternSummary {
  pattern_id: string;
  pattern_name: string;
  description: string;
  metric_chips: string[];
}

export interface MaterializedGraphSummary {
  node_count: number;
  edge_count: number;
  node_types: Array<{ type: string; count: number }>;
  edge_types: Array<{ type: string; count: number }>;
  connected_names: string[];
}

export interface MaterializedGraphPayload extends GraphData {
  center_id: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface MaterializedCaseDetail {
  id: string;
  title: string;
  subtitle?: string;
  summary?: string | null;
  graph?: MaterializedGraphPayload | null;
  public_sources: string[];
  tags: string[];
}

export interface MaterializedLead {
  entity_type: "company" | "person";
  entity_id: string;
  document_id?: string | null;
  name: string;
  risk_score: number;
  signal_types: number;
  primary_reason?: string | null;
  practice_labels: string[];
  highlights: string[];
  alerts: MaterializedAlertSummary[];
  matched_validation_titles: string[];
  public_sources: string[];
  patterns?: MaterializedPatternSummary[];
  graph_summary?: MaterializedGraphSummary | null;
  graph?: MaterializedGraphPayload | null;
}

export interface MaterializedValidationCase {
  case_id: string;
  title: string;
  category: string;
  entity_id: string;
  entity_type: "company" | "person";
  entity_ref: string;
  entity_name: string;
  status: string;
  matched: boolean;
  expected_signals: string[];
  observed_signals: string[];
  matched_signals: string[];
  summary: string;
  metrics: Record<string, number | string | string[]>;
  public_sources: string[];
  graph?: MaterializedGraphPayload | null;
}

export interface MaterializedInvestigationEvidence {
  label: string;
  value: string;
  detail?: string | null;
}

export interface MaterializedInvestigation {
  slug: string;
  title: string;
  category: string;
  status: "public_case" | "generated_lead";
  entity_id: string;
  entity_type: "company" | "person";
  subject_name: string;
  subject_ref?: string | null;
  summary: string;
  why_it_matters?: string | null;
  findings: string[];
  evidence: MaterializedInvestigationEvidence[];
  reported_claims?: string[];
  reported_sources?: string[];
  verified_open_data?: string[];
  open_questions?: string[];
  tags: string[];
  public_sources: string[];
  graph?: MaterializedGraphPayload | null;
}

export interface MaterializedPracticeGroupItem {
  entity_type: "company" | "person";
  entity_id: string;
  document_id?: string | null;
  name: string;
  risk_score: number;
  reason_text?: string | null;
  matched_validation_titles: string[];
}

export interface MaterializedPracticeGroup {
  label: string;
  company_count: number;
  person_count: number;
  total_hits: number;
  validation_hits: number;
  companies: MaterializedPracticeGroupItem[];
  people: MaterializedPracticeGroupItem[];
}

export interface MaterializedTerritorialHit {
  divipola?: string | null;
  municipality: string;
  department?: string | null;
  sector?: string | null;
  hits: number;
  geometry?: GeoJSON.Geometry | null;
}

export interface MaterializedWatchlistCompany extends PrioritizedCompany {
  case_file?: string | null;
}

export interface MaterializedWatchlistPerson extends PrioritizedPerson {
  case_file?: string | null;
}

export interface MaterializedResultsPack {
  generated_at_utc: string;
  pack_type: string;
  scope_note: string;
  stats: StatsResponse & {
    promoted_sources: number;
    enrichment_only_sources: number;
    quarantined_sources: number;
  };
  validation: {
    cases: MaterializedValidationCase[];
    total: number;
    matched: number;
  };
  summary: {
    validation_match_rate: number;
    featured_company_count: number;
    featured_person_count: number;
    company_watchlist_count: number;
    people_watchlist_count: number;
    buyer_watchlist_count: number;
    territory_watchlist_count: number;
  };
  practice_summary: Array<{ label: string; count: number }>;
  active_sectors?: string[];
  territorial_hits?: MaterializedTerritorialHit[];
  practice_groups?: MaterializedPracticeGroup[];
  investigations?: MaterializedInvestigation[];
  featured_companies: MaterializedLead[];
  featured_people: MaterializedLead[];
  watchlists: {
    companies: MaterializedWatchlistCompany[];
    people: MaterializedWatchlistPerson[];
    buyers: PrioritizedBuyer[];
    territories: PrioritizedTerritory[];
  };
}

export async function loadMaterializedResultsPack(
  signal?: AbortSignal,
): Promise<MaterializedResultsPack> {
  const response = await fetch("/data/materialized-results.json", {
    cache: "no-store",
    signal,
  });

  if (!response.ok) {
    throw new Error(`Failed to load materialized results pack: ${response.status}`);
  }

  return response.json() as Promise<MaterializedResultsPack>;
}

export async function loadMaterializedCase(
  path: string,
  signal?: AbortSignal,
): Promise<MaterializedCaseDetail> {
  const response = await fetch(path, {
    cache: "no-store",
    signal,
  });

  if (!response.ok) {
    throw new Error(`Failed to load materialized case: ${response.status}`);
  }

  return response.json() as Promise<MaterializedCaseDetail>;
}
