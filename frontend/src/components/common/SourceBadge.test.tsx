import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { SourceBadge } from "./SourceBadge";

describe("SourceBadge", () => {
  it("renders a friendly label for known source ids", () => {
    render(<SourceBadge source="secop_ii_contracts" />);
    expect(screen.getByText("SECOP II contracts")).toBeInTheDocument();
  });

  it("falls back to a humanized label for unknown sources", () => {
    render(<SourceBadge source="custom_source_name" />);
    expect(screen.getByText("Custom Source Name")).toBeInTheDocument();
  });
});
