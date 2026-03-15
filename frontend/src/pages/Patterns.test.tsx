import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

vi.mock("@/api/client", () => ({
  listPatterns: vi.fn(),
  getEntityPatterns: vi.fn(),
}));

import { getEntityPatterns, listPatterns } from "@/api/client";
import { Patterns } from "./Patterns";

const mockListPatterns = vi.mocked(listPatterns);
const mockGetEntityPatterns = vi.mocked(getEntityPatterns);

const samplePatterns = [
  {
    id: "p1",
    name_pt: "Padrão A",
    name_en: "Pattern A",
    description_pt: "Descrição A",
    description_en: "Description A",
  },
  {
    id: "p2",
    name_pt: "Padrão B",
    name_en: "Pattern B",
    description_pt: "Descrição B",
    description_en: "Description B",
  },
];

const sampleResults = [
  {
    pattern_id: "p1",
    pattern_name: "Pattern A",
    description: "Result A",
    data: { count: 5 },
    entity_ids: ["e1"],
    sources: [{ database: "SECOP Integrado" }],
  },
  {
    pattern_id: "p2",
    pattern_name: "Pattern B",
    description: "Result B",
    data: { count: 3 },
    entity_ids: ["e2"],
    sources: [{ database: "SIGEP" }],
  },
];

function renderPatterns(entityId?: string) {
  const path = entityId ? `/patterns/${entityId}` : "/patterns";
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/patterns/:entityId?" element={<Patterns />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("Patterns", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListPatterns.mockResolvedValue({ patterns: samplePatterns });
    mockGetEntityPatterns.mockResolvedValue({
      entity_id: "e1",
      patterns: sampleResults,
      total: 2,
    });
  });

  it("renders pattern list from API", async () => {
    renderPatterns();

    await waitFor(() => {
      expect(screen.getByText("Pattern A")).toBeInTheDocument();
      expect(screen.getByText("Pattern B")).toBeInTheDocument();
    });
  });

  it("shows select entity hint when no entityId", async () => {
    renderPatterns();

    await waitFor(() => {
      expect(screen.getByText(/select an entity/i)).toBeInTheDocument();
    });
  });

  it("shows pattern results when entityId present", async () => {
    renderPatterns("e1");

    await waitFor(() => {
      expect(screen.getByText("Result A")).toBeInTheDocument();
      expect(screen.getByText("Result B")).toBeInTheDocument();
    });
  });

  it("shows error on pattern list load failure", async () => {
    mockListPatterns.mockRejectedValueOnce(new Error("fail"));

    renderPatterns();

    await waitFor(() => {
      expect(screen.getByText(/failed to load/i)).toBeInTheDocument();
    });
  });

  it("shows error on entity pattern analysis failure", async () => {
    mockGetEntityPatterns.mockRejectedValueOnce(new Error("fail"));

    renderPatterns("e1");

    await waitFor(() => {
      expect(screen.getByText(/failed to run/i)).toBeInTheDocument();
    });
  });

  it("filters results by active pattern", async () => {
    const user = userEvent.setup();
    renderPatterns("e1");

    // Wait for patterns and results to load
    await waitFor(() => {
      expect(screen.getByText("Result A")).toBeInTheDocument();
      expect(screen.getByText("Result B")).toBeInTheDocument();
    });

    // Click pattern A button in sidebar to filter
    const patternButtons = screen.getAllByRole("button");
    const patternAButton = patternButtons.find((btn) =>
      btn.textContent?.includes("Pattern A"),
    )!;
    await user.click(patternAButton);

    expect(screen.getByText("Result A")).toBeInTheDocument();
    expect(screen.queryByText("Result B")).not.toBeInTheDocument();
  });
});
