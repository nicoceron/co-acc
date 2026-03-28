import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import { InvestigationDossier } from "./InvestigationDossier";

vi.mock("@/components/graph/GraphCanvas", () => ({
  GraphCanvas: () => <div data-testid="graph-canvas">graph-canvas</div>,
}));

const SAMPLE_GRAPH = {
  center_id: "company-1",
  nodes: [
    {
      id: "company-1",
      label: "FESSANJOSE",
      type: "company",
      document_id: "8605242195",
      properties: { name: "FESSANJOSE" },
      sources: [{ database: "men" }],
    },
    {
      id: "person-1",
      label: "Romelia Nuste Castro",
      type: "person",
      document_id: "123",
      properties: { role: "Rector" },
      sources: [{ database: "men" }],
    },
  ],
  edges: [
    {
      source: "company-1",
      target: "person-1",
      type: "ADMINISTRA",
      confidence: 1,
      properties: { role: "Rector" },
    },
  ],
};

const SAMPLE_PACK = {
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
      summary: "Lead generado desde capas oficiales: alias SAME_AS y official_case_bulletin_count.",
      why_it_matters: "Permite abrir una revisión documental.",
      findings: ["Hay un alias SAME_AS contractual.", "Hay dos convenios detectados."],
      evidence: [
        { label: "official_case_bulletin_count", value: "2" },
        { label: "convenios interadministrativos enlazados", value: "2" },
      ],
      reported_claims: ["El reportaje dice que existe un operador privado."],
      reported_sources: ["https://example.com/observatorio"],
      verified_open_data: ["c82u-588k confirma una sociedad activa."],
      open_questions: ["Falta un puente oficial abierto."],
      tags: ["education_control_capture"],
      public_sources: ["https://example.com/san-jose"],
      graph: SAMPLE_GRAPH,
    },
    {
      slug: "nuevo-elefante",
      title: "Consorcio Río Verde: obra con pagos y ejecución en tensión",
      category: "elefante_blanco",
      status: "generated_lead",
      entity_id: "company-2",
      entity_type: "company",
      subject_name: "Consorcio Río Verde",
      subject_ref: "901999999",
      summary: "Lead generado desde capas oficiales: budget_execution_discrepancy.",
      why_it_matters: "Sirve para abrir una revisión nueva.",
      findings: ["Hay una brecha de ejecución."],
      evidence: [
        { label: "budget_execution_discrepancy", value: "1" },
      ],
      reported_claims: [],
      reported_sources: [],
      verified_open_data: [],
      open_questions: ["Falta un acta de cierre."],
      tags: ["budget_execution_discrepancy"],
      public_sources: [],
      graph: SAMPLE_GRAPH,
    },
  ],
};

describe("InvestigationDossier", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders a materialized investigation dossier with graph exhibit", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    render(
      <MemoryRouter initialEntries={["/investigations/san-jose-icaft-network"]}>
        <Routes>
          <Route path="/investigations/:slug" element={<InvestigationDossier />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Fundación San José \/ ICAFT/i)).toBeInTheDocument();
    });

    expect(screen.getByText(/Red de relaciones/i)).toBeInTheDocument();
    expect(screen.getByText(/Por qué importa/i)).toBeInTheDocument();
    expect(screen.getByText(/Estado del caso/i)).toBeInTheDocument();
    expect(screen.getByText(/Glosario rápido/i)).toBeInTheDocument();
    expect(screen.getByText(/^Caso corroborado$/i)).toBeInTheDocument();
    expect(screen.getByText(/Volver a biblioteca corroborada/i)).toBeInTheDocument();
    expect(screen.getByText(/Señal automática basada en alias coincidencia de identidad y boletines oficiales/i)).toBeInTheDocument();
    expect(screen.getByText(/Hay un alias coincidencia de identidad contractual/i)).toBeInTheDocument();
    expect(screen.getByText(/^boletines oficiales$/i)).toBeInTheDocument();
    expect(screen.getByText(/Pistas tomadas de reportajes y denuncias/i)).toBeInTheDocument();
    expect(screen.getByText(/Confirmado por registros abiertos y documentos públicos/i)).toBeInTheDocument();
    expect(screen.getByText(/Vacíos documentales que siguen abiertos/i)).toBeInTheDocument();
    expect(screen.getByText(/Fuentes periodísticas usadas para estas hipótesis/i)).toBeInTheDocument();
    expect(screen.getByTestId("graph-canvas")).toBeInTheDocument();
  });

  it("routes generated leads back to new leads instead of the corroborated library", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    render(
      <MemoryRouter initialEntries={["/investigations/nuevo-elefante"]}>
        <Routes>
          <Route path="/investigations/:slug" element={<InvestigationDossier />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { name: /Consorcio Río Verde: obra con pagos y ejecución en tensión/i }),
      ).toBeInTheDocument();
    });

    expect(screen.getByText(/^Pista nueva$/i)).toBeInTheDocument();
    expect(screen.getByText(/Volver a pistas nuevas/i)).toBeInTheDocument();
  });
});
