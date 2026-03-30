import { act, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

vi.mock("@/api/client", () => ({
  getStats: vi.fn(),
}));

// Stub heavy visual components that depend on canvas/animation
vi.mock("@/components/landing/NetworkAnimation", () => ({
  NetworkAnimation: () => <div data-testid="network-animation" />,
}));

vi.mock("@/components/landing/HeroGraph", () => ({
  HeroGraph: () => <div data-testid="hero-graph" />,
}));

vi.mock("@/components/landing/StatsBar", () => ({
  StatsBar: () => <div data-testid="stats-bar" />,
}));

import { getStats } from "@/api/client";
import { Landing } from "./Landing";

const mockGetStats = vi.mocked(getStats);
const SAMPLE_PACK = {
  generated_at_utc: "2026-03-27T12:00:00Z",
  pack_type: "materialized_real_results",
  scope_note: "Lote público",
  stats: {
    total_nodes: 10_000_000,
    total_relationships: 15_000_000,
    person_count: 1_200_000,
    company_count: 3_500_000,
    health_count: 150_000,
    finance_count: 2_000_000,
    contract_count: 3_000_000,
    sanction_count: 130_000,
    election_count: 50_000,
    amendment_count: 200_000,
    education_count: 10_000,
    bid_count: 500_000,
    source_document_count: 100_000,
    ingestion_run_count: 150,
    data_sources: 30,
    promoted_sources: 20,
    enrichment_only_sources: 14,
    quarantined_sources: 1,
  },
  validation: {
    total: 2,
    matched: 2,
    cases: [
      {
        case_id: "san-jose",
        title: "Fundación San José",
        category: "captura_educativa",
        entity_id: "company-1",
        entity_type: "company",
        entity_ref: "8605242195",
        entity_name: "FESSANJOSE",
        status: "matched",
        matched: true,
        expected_signals: ["captura_educativa"],
        observed_signals: ["captura_educativa"],
        matched_signals: ["captura_educativa"],
        summary: "Caso conocido reproducido.",
        metrics: {},
        public_sources: ["https://example.com/san-jose"],
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
  practice_summary: [],
  investigations: [
    {
      slug: "nuevo-elefante",
      title: "Consorcio Río Verde: obra con pagos y ejecución en tensión",
      category: "elefante_blanco",
      status: "generated_lead",
      entity_id: "company-99",
      entity_type: "company",
      subject_name: "Consorcio Río Verde",
      subject_ref: "901999999",
      summary: "Pista nueva con señales de ejecución y pagos.",
      findings: ["Hallazgo nuevo"],
      evidence: [{ label: "brecha de ejecución", value: "1" }],
      tags: ["budget_execution_discrepancy"],
      public_sources: ["https://example.com/fuente-oficial"],
      verified_open_data: ["Hallazgo estructurado en documento oficial."],
    },
    {
      slug: "san-jose-icaft-network",
      title: "Fundación San José / ICAFT: control educativo, alias SECOP y contratación pública",
      category: "captura_educativa",
      status: "public_case",
      entity_id: "company-1",
      entity_type: "company",
      subject_name: "FESSANJOSE",
      subject_ref: "8605242195",
      summary: "Caso corroborado con soporte externo.",
      findings: ["Caso corroborado"],
      evidence: [{ label: "boletines oficiales", value: "2" }],
      tags: ["captura_educativa"],
      public_sources: ["https://example.com/san-jose"],
      verified_open_data: ["Rastro documental confirmado."],
    },
  ],
  featured_companies: [],
  featured_people: [],
  watchlists: {
    companies: [],
    people: [],
    buyers: [],
    territories: [],
  },
};

function renderLanding() {
  return render(
    <MemoryRouter>
      <Landing />
    </MemoryRouter>,
  );
}

describe("Landing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetStats.mockResolvedValue({
      total_nodes: 10_000_000,
      total_relationships: 15_000_000,
      person_count: 1_200_000,
      company_count: 3_500_000,
      health_count: 150_000,
      finance_count: 2_000_000,
      contract_count: 3_000_000,
      sanction_count: 130_000,
      election_count: 50_000,
      amendment_count: 200_000,
      education_count: 10_000,
      bid_count: 500_000,
      source_document_count: 100_000,
      ingestion_run_count: 150,
      data_sources: 30,
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders without crashing", async () => {
    await act(async () => {
      renderLanding();
    });
    expect(screen.getByText("Descubre primero. Corrobora después.")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getAllByText("3.0M").length).toBeGreaterThan(0);
    });
  });

  it("shows key heading text and CTA", async () => {
    await act(async () => {
      renderLanding();
    });
    expect(screen.getByText("Descubre primero. Corrobora después.")).toBeInTheDocument();
    expect(screen.getByText("Descubrir pistas")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Abrir biblioteca" })).toBeInTheDocument();
    expect(screen.getByText("Pista nueva")).toBeInTheDocument();
    expect(screen.getByText("Caso corroborado")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getAllByText("3.0M").length).toBeGreaterThan(0);
    });
  });

  it("shows the curated Colombia data sources", async () => {
    await act(async () => {
      renderLanding();
    });

    await waitFor(() => {
      expect(screen.getByText("SECOP Integrado")).toBeInTheDocument();
    });

    const sourceNames = [
      "SECOP Integrado", "SECOP II Procesos", "SECOP II Contratos", "SECOP Proveedores",
      "SECOP Sanciones", "SIGEP", "Puestos Sensibles", "Activos Ley 2013",
      "Conflictos", "SGR Gastos", "REPS Salud",
      "MEN Matrícula", "Cuentas Claras",
    ];

    for (const name of sourceNames) {
      expect(screen.getByText(name)).toBeInTheDocument();
    }
  });
});
