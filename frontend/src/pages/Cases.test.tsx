import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

import { Cases } from "./Cases";

const CASES_RESPONSE = {
  total: 1,
  cases: [
    {
      id: "case-1",
      title: "Caso de prueba",
      description: "Descripción del caso",
      status: "new",
      created_at: "2026-03-31T00:00:00Z",
      updated_at: "2026-03-31T00:00:00Z",
      entity_ids: ["8601"],
      signal_count: 2,
      public_signal_count: 2,
      last_refreshed_at: "2026-03-31T00:00:00Z",
      last_run_id: "run-1",
      stale: false,
    },
  ],
};

const CASE_DETAIL_RESPONSE = {
  ...CASES_RESPONSE.cases[0],
  signals: [
    {
      hit_id: "hit-1",
      run_id: "run-1",
      signal_id: "contract_concentration",
      signal_version: 1,
      title: "Concentración contractual por comprador",
      description: "Descripción",
      category: "procurement",
      severity: "high",
      public_safe: true,
      reviewer_only: false,
      entity_id: "8601",
      entity_key: "8601",
      entity_label: "Company",
      scope_key: "scope-1",
      scope_type: "buyer",
      dedup_key: "signal:1",
      score: 8,
      identity_confidence: 1,
      identity_match_type: "EXACT_COMPANY_NIT",
      identity_quality: "exact",
      evidence_count: 3,
      evidence_bundle_id: "bundle-1",
      evidence_refs: [],
      data: {},
      sources: [{ database: "neo4j_public" }],
      evidence_items: [],
      created_at: "2026-03-31T00:00:00Z",
      first_seen_at: "2026-03-31T00:00:00Z",
      last_seen_at: "2026-03-31T00:00:00Z",
    },
  ],
  evidence_bundles: [
    {
      bundle_id: "bundle-1",
      headline: "Concentración contractual por comprador",
      source_list: ["neo4j_public"],
      evidence_items: [
        {
          item_id: "item-1",
          source_id: "neo4j_public",
          record_id: "ref-1",
          url: null,
          label: "ref-1",
          item_type: "reference",
          node_ref: "Contract:ref-1",
          observed_at: "2026-03-31T00:00:00Z",
          public_safe: true,
          identity_match_type: "EXACT_COMPANY_NIT",
          identity_quality: "exact",
        },
      ],
    },
  ],
  events: [
    {
      id: "event-1",
      type: "signal_hit",
      label: "Concentración contractual por comprador",
      date: "2026-03-31T00:00:00Z",
      entity_id: "8601",
      signal_hit_id: "hit-1",
      evidence_bundle_id: "bundle-1",
      bundle_document_count: 1,
    },
  ],
};

describe("Cases", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders case board and dossier detail", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : String(input.url);
      if (url.includes("/api/v1/cases/case-1")) {
        return Promise.resolve(new Response(JSON.stringify(CASE_DETAIL_RESPONSE), { status: 200, headers: { "Content-Type": "application/json" } }));
      }
      return Promise.resolve(new Response(JSON.stringify(CASES_RESPONSE), { status: 200, headers: { "Content-Type": "application/json" } }));
    });

    render(
      <MemoryRouter initialEntries={["/app/cases/case-1"]}>
        <Routes>
          <Route path="/app/cases/:caseId" element={<Cases />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(screen.getByText(/Dossiers de revisión/i)).toBeInTheDocument());
    await waitFor(() => expect(screen.getAllByText(/Caso de prueba/i).length).toBeGreaterThan(0));
    expect(screen.getByText(/Señales activas/i)).toBeInTheDocument();
    expect(screen.getAllByText(/Concentración contractual por comprador/i).length).toBeGreaterThan(0);
    expect(screen.getByRole("heading", { name: /Eventos y soporte/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /refrescar dossier/i })).toBeInTheDocument();
  });
});
