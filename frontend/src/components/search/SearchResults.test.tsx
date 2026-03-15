import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import "@/i18n";

import type { SearchResult } from "@/api/client";
import { SearchResults } from "./SearchResults";

const sampleResults: SearchResult[] = [
  {
    id: "e1",
    name: "Ana Torres",
    type: "person",
    document: "***3456",
    sources: [{ database: "SIGEP" }, { database: "SECOP Integrado" }],
    score: 1.0,
  },
  {
    id: "e2",
    name: "Constructora Andina",
    type: "company",
    sources: [{ database: "SECOP Integrado" }],
    score: 0.9,
  },
];

function renderResults(results: SearchResult[]) {
  return render(
    <MemoryRouter>
      <SearchResults results={results} />
    </MemoryRouter>,
  );
}

describe("SearchResults", () => {
  it("shows no results message when empty", () => {
    renderResults([]);
    expect(screen.getByText(/no results found/i)).toBeInTheDocument();
  });

  it("renders result items with names", () => {
    renderResults(sampleResults);
    expect(screen.getByText("Ana Torres")).toBeInTheDocument();
    expect(screen.getByText("Constructora Andina")).toBeInTheDocument();
  });

  it("renders type badges", () => {
    renderResults(sampleResults);
    expect(screen.getByText("Person")).toBeInTheDocument();
    expect(screen.getByText("Company")).toBeInTheDocument();
  });

  it("links to graph page for each result", () => {
    renderResults(sampleResults);
    const links = screen.getAllByRole("link");
    expect(links[0]).toHaveAttribute("href", "/app/analysis/e1");
    expect(links[1]).toHaveAttribute("href", "/app/analysis/e2");
  });

  it("shows source badges", () => {
    renderResults(sampleResults);
    expect(screen.getByText("SIGEP")).toBeInTheDocument();
    expect(screen.getAllByText("SECOP Integrado")).toHaveLength(2);
  });

  it("shows document when available", () => {
    renderResults(sampleResults);
    expect(screen.getByText("***3456")).toBeInTheDocument();
  });
});
