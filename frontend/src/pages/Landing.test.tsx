import { act, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

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
  });

  it("renders without crashing", async () => {
    await act(async () => {
      renderLanding();
    });
    // Hero heading should be rendered
    expect(screen.getByText("Trace Colombia's public contracts")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("3.5M")).toBeInTheDocument();
    });
  });

  it("shows key heading text and CTA", async () => {
    await act(async () => {
      renderLanding();
    });
    expect(screen.getByText("Trace Colombia's public contracts")).toBeInTheDocument();
    expect(screen.getByText("Explore the graph")).toBeInTheDocument();
    expect(screen.getByText("CO-ACC \u00B7 Colombia")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("3.5M")).toBeInTheDocument();
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
      "Conflictos", "SGR Gastos", "Proyectos SGR", "REPS Salud",
      "MEN Matrícula", "Cuentas Claras",
    ];

    for (const name of sourceNames) {
      expect(screen.getByText(name)).toBeInTheDocument();
    }
  });
});
