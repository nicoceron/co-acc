import { render, screen, fireEvent } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { PatternCard } from "./PatternCard";
import "../../i18n";

const mockPattern = {
  id: "sanctioned_supplier_record",
  name_es: "Proveedor con historial de sanción",
  name_en: "Supplier with sanction history",
  description_es: "Proveedor con sanciones registradas en fuentes públicas",
  description_en: "Supplier with sanctions recorded in public sources",
};

describe("PatternCard", () => {
  it("renders pattern name and description in English", () => {
    render(<PatternCard pattern={mockPattern} />);
    expect(screen.getByText("Supplier with sanction history")).toBeDefined();
    expect(screen.getByText("Supplier with sanctions recorded in public sources")).toBeDefined();
  });

  it("renders pattern id", () => {
    render(<PatternCard pattern={mockPattern} />);
    expect(screen.getByText("sanctioned_supplier_record")).toBeDefined();
  });

  it("calls onClick with pattern id", () => {
    const onClick = vi.fn();
    render(<PatternCard pattern={mockPattern} onClick={onClick} />);
    fireEvent.click(screen.getByRole("button"));
    expect(onClick).toHaveBeenCalledWith("sanctioned_supplier_record");
  });

  it("applies active class when active", () => {
    const { container } = render(<PatternCard pattern={mockPattern} active />);
    const button = container.querySelector("button");
    expect(button?.className).toContain("active");
  });
});
