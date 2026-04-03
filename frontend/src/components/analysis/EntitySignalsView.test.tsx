import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";

import "@/i18n";

import { EntitySignalsView } from "./EntitySignalsView";

const SIGNALS_RESPONSE = {
  entity_id: "8601",
  entity_key: "8601",
  total: 1,
  last_run_id: "run-1",
  last_refreshed_at: "2026-03-31T00:00:00Z",
  stale: false,
  signals: [
    {
      hit_id: "hit-1",
      run_id: "run-1",
      signal_id: "judicial_case_contract_overlap",
      signal_version: 1,
      title: "Cruce judicial con contratación",
      description: "La entidad aparece tanto en contratación como en un expediente judicial público.",
      category: "judicial",
      severity: "high" as const,
      public_safe: true,
      reviewer_only: false,
      entity_id: "8601",
      entity_key: "8601",
      entity_label: "Company",
      scope_key: "radicado-123",
      scope_type: "judicial_case",
      dedup_key: "signal:test",
      score: 0.9,
      identity_confidence: 1,
      identity_match_type: "EXACT_COMPANY_NIT",
      identity_quality: "exact",
      evidence_count: 2,
      evidence_bundle_id: "bundle:hit-1",
      evidence_refs: ["radicado-123"],
      data: {},
      sources: [{ database: "neo4j_public" }],
      evidence_items: [
        {
          item_id: "item-1",
          source_id: "neo4j_public",
          record_id: "radicado-123",
          url: "https://example.test/radicado-123",
          label: "Radicado 123",
          item_type: "document",
          node_ref: "JudicialCase:radicado-123",
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

describe("EntitySignalsView", () => {
  it("renders materialized signal evidence and refresh action", () => {
    const onRefresh = vi.fn();

    render(
      <MemoryRouter>
        <EntitySignalsView
          signals={SIGNALS_RESPONSE}
          loading={false}
          error={null}
          onRefresh={onRefresh}
        />
      </MemoryRouter>,
    );

    expect(screen.getByRole("heading", { name: /Persisted signals for review/i })).toBeInTheDocument();
    expect(screen.getByText(/Cruce judicial con contratación/i)).toBeInTheDocument();
    expect(screen.getByText(/Radicado 123/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Refresh signals/i }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it("renders empty state when there are no signals", () => {
    render(
      <MemoryRouter>
        <EntitySignalsView
          signals={{
            entity_id: "8601",
            entity_key: "8601",
            total: 0,
            last_run_id: null,
            last_refreshed_at: null,
            stale: true,
            signals: [],
          }}
          loading={false}
          error={null}
          onRefresh={() => {}}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/No materialized signals yet/i)).toBeInTheDocument();
  });
});
