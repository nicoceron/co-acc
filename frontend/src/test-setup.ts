import "@testing-library/jest-dom/vitest";

// Polyfill ResizeObserver for jsdom
if (typeof globalThis.ResizeObserver === "undefined") {
  globalThis.ResizeObserver = class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof globalThis.ResizeObserver;
}

if (typeof URL.createObjectURL === "undefined") {
  URL.createObjectURL = () => "blob:coacc-test";
}

if (typeof URL.revokeObjectURL === "undefined") {
  URL.revokeObjectURL = () => {};
}
