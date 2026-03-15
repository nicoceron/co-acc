import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

vi.mock("@/api/client", () => ({
  listInvestigations: vi.fn(),
  searchEntities: vi.fn(),
  getSuspiciousPeople: vi.fn(),
  getSuspiciousCompanies: vi.fn(),
  getSuspiciousBuyers: vi.fn(),
  getSuspiciousTerritories: vi.fn(),
}));

import {
  getSuspiciousBuyers,
  getSuspiciousCompanies,
  getSuspiciousPeople,
  getSuspiciousTerritories,
  listInvestigations,
  searchEntities,
} from "@/api/client";
import { Dashboard } from "./Dashboard";

const mockListInvestigations = vi.mocked(listInvestigations);
const mockSearchEntities = vi.mocked(searchEntities);
const mockGetSuspiciousPeople = vi.mocked(getSuspiciousPeople);
const mockGetSuspiciousCompanies = vi.mocked(getSuspiciousCompanies);
const mockGetSuspiciousBuyers = vi.mocked(getSuspiciousBuyers);
const mockGetSuspiciousTerritories = vi.mocked(getSuspiciousTerritories);

function renderDashboard() {
  return render(
    <MemoryRouter>
      <Dashboard />
    </MemoryRouter>,
  );
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
    mockGetSuspiciousPeople.mockResolvedValue({
      total: 1,
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
      ],
    });
    mockGetSuspiciousCompanies.mockResolvedValue({
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
    mockGetSuspiciousBuyers.mockResolvedValue({
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
    mockGetSuspiciousTerritories.mockResolvedValue({
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
    renderDashboard();

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
  });
});
