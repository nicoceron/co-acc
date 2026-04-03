import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

import { Signals } from "./Signals";

const LIST_RESPONSE = {
  registry_version: 1,
  signals: [
    {
      id: "split_contracts_below_threshold",
      version: 1,
      title: "Fraccionamiento contractual bajo tope",
      description: "Recurrencia de contratos sensibles.",
      category: "procurement",
      severity: "high",
      entity_types: ["Company"],
      public_safe: true,
      reviewer_only: false,
      requires_identity: ["EXACT_COMPANY_NIT"],
      sources_required: ["secop_ii_contracts"],
      scope_type: "contract",
      dedup_fields: ["scope_key"],
      pattern_id: "split_contracts_below_threshold",
      dedup_key_template: "signal:{id}",
      runner: { kind: "pattern", ref: "split_contracts_below_threshold" },
      hit_count: 8,
      last_seen_at: "2026-03-31T00:00:00Z",
    },
  ],
};

const DETAIL_RESPONSE = {
  definition: LIST_RESPONSE.signals[0],
  sample_hits: [
    {
      hit_id: "hit-1",
      run_id: "run-1",
      signal_id: "split_contracts_below_threshold",
      signal_version: 1,
      title: "Fraccionamiento contractual bajo tope",
      description: "Descripción de prueba",
      category: "procurement",
      severity: "high",
      public_safe: true,
      reviewer_only: false,
      entity_id: "8601",
      entity_key: "8601",
      entity_label: "Company",
      scope_key: "scope-1",
      scope_type: "contract",
      dedup_key: "signal:1",
      score: 5,
      identity_confidence: 1,
      identity_match_type: "EXACT_COMPANY_NIT",
      identity_quality: "exact",
      evidence_count: 2,
      evidence_bundle_id: "bundle-1",
      evidence_refs: ["https://example.com/ref"],
      data: {},
      sources: [{ database: "neo4j_public" }],
      evidence_items: [
        {
          item_id: "item-1",
          source_id: "neo4j_public",
          record_id: null,
          url: "https://example.com/ref",
          label: "Referencia",
          item_type: "reference",
          node_ref: "Document:https://example.com/ref",
          observed_at: "2026-03-31T00:00:00Z",
          public_safe: true,
          identity_match_type: "EXACT_COMPANY_NIT",
          identity_quality: "exact",
        },
      ],
      created_at: "2026-03-31T00:00:00Z",
      first_seen_at: "2026-03-31T00:00:00Z",
      last_seen_at: "2026-03-31T00:00:00Z",
    },
  ],
};

describe("Signals", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders signal registry and detail", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : String(input.url);
      if (url.includes("/api/v1/signals/split_contracts_below_threshold")) {
        return Promise.resolve(new Response(JSON.stringify(DETAIL_RESPONSE), { status: 200, headers: { "Content-Type": "application/json" } }));
      }
      return Promise.resolve(new Response(JSON.stringify(LIST_RESPONSE), { status: 200, headers: { "Content-Type": "application/json" } }));
    });

    render(
      <MemoryRouter initialEntries={["/app/signals/split_contracts_below_threshold"]}>
        <Routes>
          <Route path="/app/signals/:signalId" element={<Signals />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(screen.getByText(/Señales publicables/i)).toBeInTheDocument());
    await waitFor(() => expect(screen.getAllByText(/Fraccionamiento contractual bajo tope/i).length).toBeGreaterThan(0));
    expect(screen.getByText(/Muestras materializadas/i)).toBeInTheDocument();
    expect(screen.getByText(/Referencia/i)).toBeInTheDocument();
  });
});
