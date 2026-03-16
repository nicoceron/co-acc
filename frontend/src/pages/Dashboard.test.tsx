import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "@/i18n";

vi.mock("@/api/client", () => ({
  listInvestigations: vi.fn(),
  searchEntities: vi.fn(),
  getPrioritizedPeople: vi.fn(),
  getPrioritizedCompanies: vi.fn(),
  getPrioritizedBuyers: vi.fn(),
  getPrioritizedTerritories: vi.fn(),
}));

import {
  getPrioritizedBuyers,
  getPrioritizedCompanies,
  getPrioritizedPeople,
  getPrioritizedTerritories,
  listInvestigations,
  searchEntities,
} from "@/api/client";
import { Dashboard } from "./Dashboard";

const mockListInvestigations = vi.mocked(listInvestigations);
const mockSearchEntities = vi.mocked(searchEntities);
const mockGetPrioritizedPeople = vi.mocked(getPrioritizedPeople);
const mockGetPrioritizedCompanies = vi.mocked(getPrioritizedCompanies);
const mockGetPrioritizedBuyers = vi.mocked(getPrioritizedBuyers);
const mockGetPrioritizedTerritories = vi.mocked(getPrioritizedTerritories);

async function renderDashboard() {
  await act(async () => {
    render(
      <MemoryRouter>
        <Dashboard />
      </MemoryRouter>,
    );
  });
}

async function setLanguage(language: string) {
  await act(async () => {
    await i18n.changeLanguage(language);
  });
}

describe("Dashboard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListInvestigations.mockResolvedValue({
      investigations: [],
      total: 0,
    });
    mockSearchEntities.mockResolvedValue({
      results: [],
      total: 0,
      page: 1,
      size: 5,
    });
    mockGetPrioritizedPeople.mockResolvedValue({
      total: 2,
      people: [
        {
          entity_id: "4:abc",
          name: "Adriana Maria Mejia Aguado",
          document_id: "31862756",
          suspicion_score: 13,
          signal_types: 5,
          office_count: 1,
          donation_count: 22,
          donation_value: 329_997_800,
          candidacy_count: 3,
          asset_count: 1,
          asset_value: 0,
          finance_count: 1,
          finance_value: 0,
          supplier_contract_count: 4,
          supplier_contract_value: 1_276_000_000,
          conflict_disclosure_count: 1,
          disclosure_reference_count: 3,
          corporate_activity_disclosure_count: 1,
          donor_vendor_loop_count: 4,
          offices: ["Gerente"],
          alerts: [
            {
              alert_type: "donor_official_vendor_loop",
              finding_class: "incompatibility",
              severity_score: 88,
              confidence_tier: "A",
              reason_text:
                "La misma persona aparece en donaciones electorales y contratación pública, formando un circuito donante-funcionario-proveedor.",
              evidence_refs: ["documento:31862756"],
              source_list: ["Cuentas Claras", "SIGEP II", "SECOP / SECOP II"],
              human_review_needed: true,
              what_is_unproven: "No demuestra intercambio indebido.",
              next_step: "Compare fechas de donación y adjudicación.",
            },
          ],
        },
        {
          entity_id: "4:ghi",
          name: "Carlos Disclosure",
          document_id: "99887766",
          suspicion_score: 11,
          signal_types: 4,
          office_count: 0,
          donation_count: 0,
          donation_value: 0,
          candidacy_count: 0,
          asset_count: 2,
          asset_value: 0,
          finance_count: 2,
          finance_value: 0,
          supplier_contract_count: 3,
          supplier_contract_value: 880_000_000,
          conflict_disclosure_count: 2,
          disclosure_reference_count: 6,
          corporate_activity_disclosure_count: 1,
          donor_vendor_loop_count: 0,
          offices: [],
          alerts: [
            {
              alert_type: "disclosure_risk_stack",
              finding_class: "textual_mention",
              severity_score: 74,
              confidence_tier: "C",
              reason_text:
                "Las declaraciones oficiales muestran intereses privados o referencias textuales relevantes.",
              evidence_refs: ["documento:99887766"],
              source_list: ["Ley 2013 / Integridad Pública", "SECOP / SECOP II"],
              human_review_needed: true,
              what_is_unproven: "Las menciones textuales no identifican por sí solas un beneficiario final.",
              next_step: "Revise la declaración original y contraste empresas o procesos mencionados.",
            },
          ],
        },
      ],
    });
    mockGetPrioritizedCompanies.mockResolvedValue({
      total: 1,
      companies: [
        {
          entity_id: "4:def",
          name: "Consorcio Andino",
          document_id: "900123456",
          suspicion_score: 11,
          signal_types: 4,
          contract_count: 9,
          contract_value: 245_000_000,
          buyer_count: 3,
          sanction_count: 1,
          official_officer_count: 2,
          official_role_count: 2,
          low_competition_bid_count: 5,
          low_competition_bid_value: 110_000_000,
          direct_invitation_bid_count: 2,
          funding_overlap_event_count: 12,
          funding_overlap_total: 930_000_000,
          capacity_mismatch_contract_count: 9,
          capacity_mismatch_contract_value: 245_000_000,
          capacity_mismatch_revenue_ratio: 6.2,
          capacity_mismatch_asset_ratio: 3.1,
          execution_gap_contract_count: 3,
          execution_gap_invoice_total: 47_000_000,
          commitment_gap_contract_count: 1,
          commitment_gap_total: 8_000_000,
          official_names: ["Ana Perez"],
          alerts: [
            {
              alert_type: "company_capacity_mismatch",
              finding_class: "discrepancy",
              severity_score: 79,
              confidence_tier: "A",
              reason_text:
                "La exposición contractual supera de forma material la escala financiera reportada por la empresa (6.2x).",
              evidence_refs: ["nit:900123456"],
              source_list: ["Supersociedades / SIIS", "SECOP / SECOP II"],
              human_review_needed: true,
              what_is_unproven: "Puede haber consorcios o subcontratación.",
              next_step: "Revise capacidad financiera habilitante.",
            },
          ],
        },
      ],
    });
    mockGetPrioritizedBuyers.mockResolvedValue({
      total: 1,
      buyers: [
        {
          buyer_id: "830000001",
          buyer_name: "Alcaldía de Prueba",
          buyer_document_id: "830000001",
          suspicion_score: 12,
          signal_types: 3,
          contract_count: 14,
          contract_value: 900_000_000,
          supplier_count: 4,
          top_supplier_name: "Consorcio Andino",
          top_supplier_document_id: "900123456",
          top_supplier_share: 0.62,
          low_competition_contract_count: 0,
          direct_invitation_contract_count: 0,
          sanctioned_supplier_contract_count: 2,
          sanctioned_supplier_value: 120_000_000,
          official_overlap_contract_count: 1,
          official_overlap_supplier_count: 1,
          capacity_mismatch_supplier_count: 1,
          discrepancy_contract_count: 2,
          discrepancy_value: 35_000_000,
          alerts: [
            {
              alert_type: "buyer_supplier_concentration",
              finding_class: "concentration",
              severity_score: 90,
              confidence_tier: "A",
              reason_text:
                "Una sola empresa concentra una porción relevante del gasto contractual del comprador (62.0%).",
              evidence_refs: ["comprador:830000001"],
              source_list: ["SECOP / SECOP II"],
              human_review_needed: true,
              what_is_unproven: "La concentración no prueba direccionamiento.",
              next_step: "Revise procesos repetidos y oferentes excluidos.",
            },
          ],
        },
      ],
    });
    mockGetPrioritizedTerritories.mockResolvedValue({
      total: 1,
      territories: [
        {
          territory_id: "Bogota|Cundinamarca",
          territory_name: "Bogota, Cundinamarca",
          department: "Cundinamarca",
          municipality: "Bogota",
          suspicion_score: 10,
          signal_types: 3,
          contract_count: 18,
          contract_value: 1_400_000_000,
          buyer_count: 5,
          supplier_count: 7,
          top_supplier_name: "Consorcio Andino",
          top_supplier_share: 0.48,
          low_competition_contract_count: 0,
          direct_invitation_contract_count: 0,
          sanctioned_supplier_contract_count: 3,
          sanctioned_supplier_value: 200_000_000,
          official_overlap_contract_count: 2,
          capacity_mismatch_supplier_count: 1,
          discrepancy_contract_count: 2,
          discrepancy_value: 80_000_000,
          alerts: [
            {
              alert_type: "territory_supplier_concentration",
              finding_class: "concentration",
              severity_score: 83,
              confidence_tier: "A",
              reason_text:
                "Una sola empresa concentra una porción alta de la contratación observada en Bogota, Cundinamarca (48.0%).",
              evidence_refs: ["territorio:Bogota, Cundinamarca"],
              source_list: ["SECOP / SECOP II"],
              human_review_needed: true,
              what_is_unproven: "La concentración territorial no prueba favoritismo.",
              next_step: "Compare objetos, compradores y calendarios.",
            },
          ],
        },
      ],
    });
  });

  it("renders the expanded Spanish risk radar", async () => {
    await setLanguage("es-CO");
    try {
      await renderDashboard();

      await waitFor(() => {
        expect(screen.getAllByText(/Personas priorizadas/i).length).toBeGreaterThan(0);
        expect(screen.getByText("Adriana Maria Mejia Aguado")).toBeInTheDocument();
        expect(screen.getByText("31862756")).toBeInTheDocument();
        expect(
          screen.getAllByText(/circuito donante-funcionario-proveedor/i).length,
        ).toBeGreaterThan(0);

        expect(screen.getAllByText(/Empresas priorizadas/i).length).toBeGreaterThan(0);
        expect(screen.getByText("Consorcio Andino")).toBeInTheDocument();
        expect(
          screen.getAllByText(/escala financiera reportada/i).length,
        ).toBeGreaterThan(0);

        expect(
          screen.getAllByText(/Compradores públicos priorizados/i).length,
        ).toBeGreaterThan(0);
        expect(screen.getByText("Alcaldía de Prueba")).toBeInTheDocument();
        expect(screen.getAllByText(/62.0%/i).length).toBeGreaterThan(0);

        expect(screen.getAllByText(/Territorios priorizados/i).length).toBeGreaterThan(0);
        expect(screen.getByText("Bogota, Cundinamarca")).toBeInTheDocument();
        expect(screen.getAllByText(/Fuentes: SECOP \/ SECOP II/i).length).toBeGreaterThan(0);
        expect(screen.getAllByText(/Aún no probado/i).length).toBeGreaterThan(0);
      });
    } finally {
      await setLanguage("en");
    }
  });

  it("filters prioritized people by corruption-style overlap groups", async () => {
    await setLanguage("en");
    try {
      const user = userEvent.setup();
      await renderDashboard();

      await waitFor(() => {
        expect(screen.getByText("Adriana Maria Mejia Aguado")).toBeInTheDocument();
        expect(screen.getByText("Carlos Disclosure")).toBeInTheDocument();
        expect(screen.getByText("2 of 2 visible")).toBeInTheDocument();
      });

      await user.click(screen.getByRole("button", { name: "Donations + contracts" }));

      expect(screen.getByText("Adriana Maria Mejia Aguado")).toBeInTheDocument();
      expect(screen.queryByText("Carlos Disclosure")).not.toBeInTheDocument();
      expect(screen.getByText("1 of 2 visible")).toBeInTheDocument();
    } finally {
      await setLanguage("en");
    }
  });
});
