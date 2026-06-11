import { render, screen, waitFor } from "@testing-library/react";
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
      expect(screen.getByRole("heading", { name: /datos publicos de colombia/i })).toBeInTheDocument();
    });
  });

  it("renders the workspace search route", async () => {
    render(
      <MemoryRouter initialEntries={["/app/search"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /buscar en el grafo/i })).toBeInTheDocument();
    });
  });

  it("renders the workspace patterns route", async () => {
    render(
      <MemoryRouter initialEntries={["/app/patterns"]}>
        <App />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: /patrones/i })).toBeInTheDocument();
    });
  });
});
