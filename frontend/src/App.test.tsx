import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

describe("App", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", undefined);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the atlas landing page", async () => {
    render(
      <MemoryRouter>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /riesgos documentales/i })).toBeInTheDocument();
    });
  });

  it("renders the workspace search route", async () => {
    render(
      <MemoryRouter initialEntries={["/app/search"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /^buscar empresas$/i })).toBeInTheDocument();
    });
  });

  it("renders the workspace patterns route", async () => {
    render(
      <MemoryRouter initialEntries={["/app/patterns"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /^patrones de riesgo$/i })).toBeInTheDocument();
    });

    expect(screen.getByRole("heading", { name: /qué quieres revisar/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /patrones con casos/i })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: /catálogo completo/i }));

    expect(screen.getByRole("heading", { name: /catálogo técnico completo/i })).toBeInTheDocument();
    expect(screen.getByRole("searchbox", { name: /buscar en el catálogo/i })).toBeInTheDocument();
  });

  it("renders the prioritized contracts route without fixtures", async () => {
    render(
      <MemoryRouter initialEntries={["/priorizados"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /contratos priorizados/i })).toBeInTheDocument();
    });
  });

  it("renders the public methodology route", async () => {
    render(
      <MemoryRouter initialEntries={["/metodologia"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /construye una señal/i })).toBeInTheDocument();
    });
  });
});
