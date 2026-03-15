import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

import "@/i18n";

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock("react-router", async () => {
  const actual = await vi.importActual<typeof import("react-router")>(
    "react-router",
  );
  return { ...actual, useNavigate: () => mockNavigate };
});

// Mock auth store
const mockLogin = vi.fn();
const mockRegister = vi.fn();
let mockStoreState = {
  login: mockLogin,
  register: mockRegister,
  loading: false,
  error: null as string | null,
  token: null as string | null,
};

vi.mock("@/stores/auth", () => ({
  useAuthStore: Object.assign(
    (selector?: (state: typeof mockStoreState) => unknown) =>
      selector ? selector(mockStoreState) : mockStoreState,
    {
      getState: () => mockStoreState,
    },
  ),
}));

import { Register } from "./Register";

function renderRegister() {
  return render(
    <MemoryRouter>
      <Register />
    </MemoryRouter>,
  );
}

describe("Register", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStoreState = {
      login: mockLogin,
      register: mockRegister,
      loading: false,
      error: null,
      token: null,
    };
  });

  it("renders registration form with all fields", () => {
    renderRegister();

    expect(screen.getByLabelText(/email/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/^password$/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/confirm password/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/invite code/i)).toBeInTheDocument();
  });

  it("shows email and password inputs with correct types", () => {
    renderRegister();

    const emailInput = screen.getByLabelText(/email/i);
    const passwordInput = screen.getByLabelText(/^password$/i);

    expect(emailInput).toHaveAttribute("type", "email");
    expect(passwordInput).toHaveAttribute("type", "password");
  });

  it("has submit button that calls register", async () => {
    const user = userEvent.setup();
    renderRegister();

    const submitBtn = screen.getByRole("button", { name: /register/i });
    expect(submitBtn).toBeInTheDocument();

    await user.type(screen.getByLabelText(/email/i), "test@example.com");
    await user.type(screen.getByLabelText(/^password$/i), "password123");
    await user.type(screen.getByLabelText(/confirm password/i), "password123");
    await user.type(screen.getByLabelText(/invite code/i), "INV-123");
    await user.click(submitBtn);

    expect(mockRegister).toHaveBeenCalledWith(
      "test@example.com",
      "password123",
      "INV-123",
    );
  });

  it("shows validation errors when submitting empty form", async () => {
    const user = userEvent.setup();
    renderRegister();

    await user.click(screen.getByRole("button", { name: /register/i }));

    expect(mockRegister).not.toHaveBeenCalled();
    expect(screen.getByText(/email is required/i)).toBeInTheDocument();
  });

  it("shows error from store", () => {
    mockStoreState.error = "auth.invalidInvite";
    renderRegister();

    expect(screen.getByText(/invalid invite code/i)).toBeInTheDocument();
  });

  it("disables submit button during loading", () => {
    mockStoreState.loading = true;
    renderRegister();

    const submitBtn = screen.getByRole("button", { name: /loading/i });
    expect(submitBtn).toBeDisabled();
  });

  it("has link to login page", () => {
    renderRegister();
    const link = screen.getByText(/have an account\? login/i);
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute("href", "/login");
  });
});
