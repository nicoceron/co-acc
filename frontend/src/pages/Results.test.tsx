import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Results } from "./Results";

vi.mock("@/components/graph/GraphCanvas", () => ({
  GraphCanvas: () => <div data-testid="graph-canvas">graph-canvas</div>,
}));

const SAMPLE_GRAPH = {
  center_id: "company-1",
  nodes: [
    {
      id: "company-1",
      label: "FONDECUN",
      type: "company",
      document_id: "900258772",
      properties: { name: "FONDECUN" },
      sources: [{ database: "secop" }],
    },
  ],
  edges: [],
};

const SAMPLE_PACK = {
  generated_at_utc: "2026-03-20T10:00:00Z",
  pack_type: "materialized_real_results",
  scope_note: "Lote real guardado.",
  stats: {
    total_nodes: 1,
    total_relationships: 2,
    person_count: 3,
    company_count: 4,
    health_count: 5,
    finance_count: 6,
    contract_count: 7,
    sanction_count: 8,
    election_count: 9,
    amendment_count: 10,
    education_count: 11,
    bid_count: 12,
    source_document_count: 13,
    ingestion_run_count: 14,
    data_sources: 15,
    promoted_sources: 20,
    enrichment_only_sources: 14,
    quarantined_sources: 1,
  },
  validation: {
    total: 1,
    matched: 1,
    cases: [
      {
        case_id: "fondecun",
        title: "FONDECUN validado",
        category: "elefante_blanco",
        entity_type: "company",
        entity_ref: "900258772",
        entity_name: "FONDECUN",
        status: "matched",
        matched: true,
        expected_signals: ["budget_execution_discrepancy"],
        observed_signals: ["budget_execution_discrepancy"],
        matched_signals: ["budget_execution_discrepancy"],
        summary: "Caso público reproducido.",
        metrics: {},
        public_sources: ["https://example.com/case"],
        graph: SAMPLE_GRAPH,
      },
    ],
  },
  summary: {
    validation_match_rate: 1,
    featured_company_count: 1,
    featured_person_count: 1,
    company_watchlist_count: 28,
    people_watchlist_count: 19,
    buyer_watchlist_count: 1,
    territory_watchlist_count: 1,
  },
  practice_summary: [{ label: "Convenios interadministrativos apilados con contratación regular", count: 2 }],
  practice_groups: [
    {
      label: "Convenios interadministrativos apilados con contratación regular",
      company_count: 3,
      person_count: 1,
      total_hits: 4,
      validation_hits: 1,
      companies: [
        {
          entity_type: "company",
          entity_id: "company-1",
          document_id: "900258772",
          name: "FONDECUN",
          risk_score: 21,
          reason_text: "Cruce real con contratación pública.",
          matched_validation_titles: ["FONDECUN validado"],
        },
      ],
      people: [
        {
          entity_type: "person",
          entity_id: "person-1",
          document_id: "52184154",
          name: "Vivian Moreno",
          risk_score: 14,
          reason_text: "Candidata y proveedora.",
          matched_validation_titles: [],
        },
      ],
    },
  ],
  investigations: [
    {
      slug: "san-jose-icaft-network",
      title: "Fundación San José / ICAFT: control educativo, alias SECOP y contratación pública",
      category: "captura_educativa",
      status: "public_case",
      entity_id: "company-1",
      entity_type: "company",
      subject_name: "FESSANJOSE",
      subject_ref: "8605242195",
      summary: "Lead generado desde capas oficiales: sanciones y official_case_bulletin_count.",
      why_it_matters: "Permite enseñar la investigación sin recorrer todo el grafo.",
      findings: [
        "Hay un alias contractual.",
        "Hay dos convenios detectados.",
      ],
      evidence: [
        { label: "official_case_bulletin_count", value: "2" },
        { label: "convenios interadministrativos enlazados", value: "2" },
      ],
      tags: ["Control institucional concentrado con alias contractuales"],
      public_sources: ["https://example.com/san-jose"],
      graph: SAMPLE_GRAPH,
    },
  ],
  featured_companies: [
    {
      entity_type: "company",
      entity_id: "company-1",
      document_id: "900258772",
      name: "FONDECUN",
      risk_score: 21,
      signal_types: 4,
      primary_reason: "Cruce real con contratación pública.",
      practice_labels: ["Convenios interadministrativos apilados con contratación regular"],
      highlights: ["4 contratos por COP 32.8B"],
      alerts: [],
      matched_validation_titles: ["FONDECUN validado"],
      public_sources: ["https://example.com/case"],
      patterns: [],
      graph_summary: {
        node_count: 9,
        edge_count: 12,
        node_types: [],
        edge_types: [{ type: "CONTRATOU", count: 4 }],
        connected_names: [],
      },
      graph: SAMPLE_GRAPH,
    },
  ],
  featured_people: [
    {
      entity_type: "person",
      entity_id: "person-1",
      document_id: "52184154",
      name: "Vivian Moreno",
      risk_score: 14,
      signal_types: 3,
      primary_reason: "Candidata y proveedora.",
      practice_labels: ["Candidatura y contratación en la misma persona"],
      highlights: ["1 candidatura registrada"],
      alerts: [],
      matched_validation_titles: [],
      public_sources: [],
      graph: {
        ...SAMPLE_GRAPH,
        center_id: "person-1",
        nodes: [
          {
            id: "person-1",
            label: "Vivian Moreno",
            type: "person",
            document_id: "52184154",
            properties: { name: "Vivian Moreno" },
            sources: [{ database: "sigep" }],
          },
        ],
      },
    },
  ],
  watchlists: {
    companies: [
      {
        entity_id: "company-1",
        name: "FONDECUN",
        document_id: "900258772",
        suspicion_score: 21,
        signal_types: 4,
        contract_count: 4,
        contract_value: 32800000000,
        buyer_count: 2,
        sanction_count: 0,
        official_officer_count: 1,
        official_role_count: 1,
        low_competition_bid_count: 0,
        low_competition_bid_value: 0,
        direct_invitation_bid_count: 0,
        funding_overlap_event_count: 0,
        funding_overlap_total: 0,
        capacity_mismatch_contract_count: 0,
        capacity_mismatch_contract_value: 0,
        capacity_mismatch_revenue_ratio: 0,
        capacity_mismatch_asset_ratio: 0,
        execution_gap_contract_count: 1,
        execution_gap_invoice_total: 2140000000,
        commitment_gap_contract_count: 0,
        commitment_gap_total: 0,
        official_names: ["Directivo de prueba"],
        alerts: [{ reason_text: "Cruce real con contratación pública.", alert_type: "public_official_supplier_overlap" }],
        case_file: "/data/cases/company-company-1.json",
      },
      {
        entity_id: "company-2",
        name: "Fondo Metropolitano de Obras",
        document_id: "901000001",
        suspicion_score: 19,
        signal_types: 3,
        contract_count: 2,
        contract_value: 9200000000,
        buyer_count: 1,
        sanction_count: 0,
        official_officer_count: 1,
        official_role_count: 1,
        low_competition_bid_count: 0,
        low_competition_bid_value: 0,
        direct_invitation_bid_count: 0,
        funding_overlap_event_count: 0,
        funding_overlap_total: 0,
        capacity_mismatch_contract_count: 0,
        capacity_mismatch_contract_value: 0,
        capacity_mismatch_revenue_ratio: 0,
        capacity_mismatch_asset_ratio: 0,
        execution_gap_contract_count: 0,
        execution_gap_invoice_total: 0,
        commitment_gap_contract_count: 0,
        commitment_gap_total: 0,
        official_names: ["Directivo de prueba"],
        alerts: [
          { reason_text: "Coincide con directivo en cargo público.", alert_type: "public_official_supplier_overlap" },
          { reason_text: "Pagos y ejecución no están cerrando bien.", alert_type: "budget_execution_discrepancy" },
        ],
      },
      {
        entity_id: "company-3",
        name: "Servicios Urbanos Andinos",
        document_id: "901000002",
        suspicion_score: 17,
        signal_types: 3,
        contract_count: 3,
        contract_value: 7100000000,
        buyer_count: 2,
        sanction_count: 0,
        official_officer_count: 2,
        official_role_count: 2,
        low_competition_bid_count: 0,
        low_competition_bid_value: 0,
        direct_invitation_bid_count: 0,
        funding_overlap_event_count: 0,
        funding_overlap_total: 0,
        capacity_mismatch_contract_count: 0,
        capacity_mismatch_contract_value: 0,
        capacity_mismatch_revenue_ratio: 0,
        capacity_mismatch_asset_ratio: 0,
        execution_gap_contract_count: 0,
        execution_gap_invoice_total: 0,
        commitment_gap_contract_count: 0,
        commitment_gap_total: 0,
        official_names: ["Directivo de prueba"],
        alerts: [{ reason_text: "Aparece conectada con cargos públicos.", alert_type: "public_official_supplier_overlap" }],
      },
      {
        entity_id: "company-4",
        name: "Constructora del Caribe",
        document_id: "901000003",
        suspicion_score: 16,
        signal_types: 2,
        contract_count: 1,
        contract_value: 5100000000,
        buyer_count: 1,
        sanction_count: 0,
        official_officer_count: 1,
        official_role_count: 1,
        low_competition_bid_count: 0,
        low_competition_bid_value: 0,
        direct_invitation_bid_count: 0,
        funding_overlap_event_count: 0,
        funding_overlap_total: 0,
        capacity_mismatch_contract_count: 0,
        capacity_mismatch_contract_value: 0,
        capacity_mismatch_revenue_ratio: 0,
        capacity_mismatch_asset_ratio: 0,
        execution_gap_contract_count: 0,
        execution_gap_invoice_total: 0,
        commitment_gap_contract_count: 0,
        commitment_gap_total: 0,
        official_names: ["Directivo de prueba"],
        alerts: [{ reason_text: "Proveedor con puente oficial detectado.", alert_type: "public_official_supplier_overlap" }],
      },
      {
        entity_id: "company-5",
        name: "Consorcio Río Verde",
        document_id: "901000004",
        suspicion_score: 18,
        signal_types: 3,
        contract_count: 2,
        contract_value: 8400000000,
        buyer_count: 1,
        sanction_count: 0,
        official_officer_count: 0,
        official_role_count: 0,
        low_competition_bid_count: 0,
        low_competition_bid_value: 0,
        direct_invitation_bid_count: 0,
        funding_overlap_event_count: 0,
        funding_overlap_total: 0,
        capacity_mismatch_contract_count: 0,
        capacity_mismatch_contract_value: 0,
        capacity_mismatch_revenue_ratio: 0,
        capacity_mismatch_asset_ratio: 0,
        execution_gap_contract_count: 2,
        execution_gap_invoice_total: 1280000000,
        commitment_gap_contract_count: 0,
        commitment_gap_total: 0,
        official_names: [],
        alerts: [{ reason_text: "Pagos y ejecución no están cerrando bien.", alert_type: "budget_execution_discrepancy" }],
      },
    ],
    people: [
      {
        entity_id: "person-1",
        name: "Vivian Moreno",
        document_id: "52184154",
        suspicion_score: 14,
        signal_types: 3,
        office_count: 0,
        donation_count: 1,
        donation_value: 1000000,
        candidacy_count: 1,
        asset_count: 0,
        asset_value: 0,
        finance_count: 0,
        finance_value: 0,
        supplier_contract_count: 1,
        supplier_contract_value: 111000000,
        conflict_disclosure_count: 0,
        disclosure_reference_count: 0,
        corporate_activity_disclosure_count: 0,
        donor_vendor_loop_count: 0,
        offices: [],
        alerts: [{ reason_text: "Candidata y proveedora.", alert_type: "candidate_supplier_overlap" }],
        case_file: "/data/cases/person-person-1.json",
      },
    ],
    buyers: [
      {
        buyer_id: "buyer-1",
        buyer_name: "Transmilenio",
        suspicion_score: 12,
        contract_count: 10,
        contract_value: 1000000,
        supplier_count: 1,
        top_supplier_share: 0.5,
        low_competition_contract_count: 1,
        direct_invitation_contract_count: 0,
        sanctioned_supplier_contract_count: 0,
        sanctioned_supplier_value: 0,
        official_overlap_contract_count: 0,
        official_overlap_supplier_count: 0,
        capacity_mismatch_supplier_count: 0,
        discrepancy_contract_count: 0,
        discrepancy_value: 0,
        signal_types: 2,
        alerts: [{ reason_text: "Concentración alta.", alert_type: "buyer_supplier_concentration" }],
      },
    ],
    territories: [
      {
        territory_id: "bogota",
        territory_name: "Bogotá",
        department: "Bogotá D.C.",
        suspicion_score: 11,
        contract_count: 8,
        contract_value: 2000000,
        buyer_count: 2,
        supplier_count: 3,
        top_supplier_share: 0.4,
        low_competition_contract_count: 1,
        direct_invitation_contract_count: 0,
        sanctioned_supplier_contract_count: 0,
        sanctioned_supplier_value: 0,
        official_overlap_contract_count: 0,
        capacity_mismatch_supplier_count: 0,
        discrepancy_contract_count: 0,
        discrepancy_value: 0,
        signal_types: 2,
        alerts: [{ reason_text: "Concentración territorial.", alert_type: "territory_supplier_concentration" }],
      },
    ],
  },
};

describe("Results", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function renderResults(initialEntry: string) {
    return render(
      <MemoryRouter initialEntries={[initialEntry]}>
        <Routes>
          <Route path="/casos" element={<Results />} />
          <Route path="/casos/modalidad/:categoryId" element={<Results />} />
          <Route path="/biblioteca" element={<Results />} />
          <Route path="/biblioteca/modalidad/:categoryId" element={<Results />} />
        </Routes>
      </MemoryRouter>,
    );
  }

  it("renders the category index for public cases", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    renderResults("/casos");

    await waitFor(() => {
      expect(screen.getByText(/Explora modalidades/i)).toBeInTheDocument();
    });

    expect(screen.getByText(/Directorio público/i)).toBeInTheDocument();
    expect(screen.getByRole("searchbox", { name: /Buscar modalidad o actor/i })).toBeInTheDocument();
    expect(screen.getAllByText(/Elefante blanco/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Vendedor de objetos robados/i).length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: /Abrir modalidad Elefante blanco \/ obra trabada/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Ir a biblioteca/i })).toBeInTheDocument();
  });

  it("renders a dedicated category page for a modality", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    renderResults("/casos/modalidad/elefante_blanco");

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /Elefante blanco \/ obra trabada/i })).toBeInTheDocument();
    });

    expect(screen.getByRole("link", { name: /Volver a modalidades/i })).toBeInTheDocument();
    expect(screen.getByRole("searchbox", { name: /Buscar caso dentro de la modalidad/i })).toBeInTheDocument();
    expect(screen.getAllByText("Consorcio Río Verde").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Fondo Metropolitano de Obras").length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: /Ver biblioteca de esta modalidad/i })).toBeInTheDocument();
  });

  it("opens a proof-case graph exhibit from the category page", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    renderResults("/casos/modalidad/elefante_blanco");

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /Elefante blanco \/ obra trabada/i })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: /Ver evidencia/i }));

    expect(screen.getByText(/Evidencia conectada/i)).toBeInTheDocument();
    expect(screen.getByTestId("graph-canvas")).toBeInTheDocument();
    expect(screen.getAllByText(/Fuentes públicas/i).length).toBeGreaterThan(0);
  });
});
