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
      verified_open_data: ["Acta oficial con alerta estructurada."],
      open_questions: ["Falta un acta de cierre."],
      tags: ["budget_execution_discrepancy"],
      public_sources: ["https://example.com/fuente-oficial"],
      graph: SAMPLE_GRAPH,
    },
  ],
};

const SAMPLE_EVIDENCE_TRAIL = {
  entity_id: "company-1",
  total_bundles: 1,
  total_documents: 2,
  bundles: [
    {
      id: "bundle-1",
      bundle_type: "proceso_secop",
      title: "Proceso SECOP de ejemplo",
      reference: "SECOP-123",
      description: "Proceso con documentos indexados",
      relation_summary: "Aparece como proveedor adjudicado",
      via_entity_name: null,
      via_entity_ref: null,
      document_count: 2,
      document_kinds: ["payment", "report"],
      source: "secop_document_archives",
      parties: [
        { role: "Comprador", name: "Gobernación ejemplo", document_id: "800100200", entity_id: "buyer-1" },
        { role: "Proveedor", name: "FESSANJOSE", document_id: "8605242195", entity_id: "company-1" },
      ],
      documents: [
        {
          id: "doc-1",
          title: "Cuenta de cobro 01",
          url: "https://example.com/doc-1.pdf",
          kind: "payment",
          extension: "pdf",
          uploaded_at: "2025-01-01",
          source: "secop_document_archives",
        },
        {
          id: "doc-2",
          title: "Informe de supervisión",
          url: "https://example.com/doc-2.pdf",
          kind: "report",
          extension: "pdf",
          uploaded_at: "2025-01-02",
          source: "secop_document_archives",
        },
      ],
    },
  ],
};

function mockDossierFetches() {
  vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : String(input.url);

    if (url.includes("/evidence-trail")) {
      return Promise.resolve(
        new Response(JSON.stringify(SAMPLE_EVIDENCE_TRAIL), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    }

    return Promise.resolve(
      new Response(JSON.stringify(SAMPLE_PACK), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
  });
}

describe("InvestigationDossier", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders a materialized investigation dossier with graph exhibit", async () => {
    mockDossierFetches();

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
    await waitFor(() => {
      expect(screen.getByText(/Proceso SECOP de ejemplo/i)).toBeInTheDocument();
    });

    expect(screen.getByText(/Red y actores/i)).toBeInTheDocument();
    expect(screen.getByText(/Por qué importa/i)).toBeInTheDocument();
    expect(screen.getByText(/^Glosario$/i)).toBeInTheDocument();
    expect(screen.getByText(/^Caso corroborado$/i)).toBeInTheDocument();
    expect(screen.getByText(/Volver a Captura educativa/i)).toBeInTheDocument();
    expect(screen.getByText(/Señal automática basada en alias coincidencia de identidad y boletines oficiales/i)).toBeInTheDocument();
    expect(screen.getByText(/Hay un alias coincidencia de identidad contractual/i)).toBeInTheDocument();
    expect(screen.getByText(/^Indicadores clave$/i)).toBeInTheDocument();
    expect(screen.getByText(/^Ya reportado$/i)).toBeInTheDocument();
    expect(screen.getByText(/Lo que sí cierran los datos públicos/i)).toBeInTheDocument();
    expect(screen.getByText(/Qué todavía no está cerrado/i)).toBeInTheDocument();
    expect(screen.getByText(/^Expedientes y documentos$/i)).toBeInTheDocument();
    expect(screen.getByText(/Cuenta de cobro 01/i)).toBeInTheDocument();
    expect(screen.getByText(/Fuentes externas usadas aquí/i)).toBeInTheDocument();
    expect(screen.getByTestId("graph-canvas")).toBeInTheDocument();
  });

  it("routes generated leads back to new leads instead of the corroborated library", async () => {
    mockDossierFetches();

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
    expect(screen.getByText(/Volver a Elefante blanco \/ obra trabada/i)).toBeInTheDocument();
  });
});
