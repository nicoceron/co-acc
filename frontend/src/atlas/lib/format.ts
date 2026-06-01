import type { Severity } from "../data/prototype";

export function formatNumber(value: number): string {
  return new Intl.NumberFormat("es-CO").format(value);
}

export function compactNumber(value: number): string {
  return new Intl.NumberFormat("es-CO", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function severityClass(severity: Severity): string {
  if (severity === "critical") return "co-severity co-severity--critical";
  if (severity === "high") return "co-severity co-severity--high";
  if (severity === "medium") return "co-severity co-severity--medium";
  return "co-severity co-severity--low";
}

export function severityVar(severity: Severity): string {
  if (severity === "critical") return "var(--coacc-sev-critical)";
  if (severity === "high") return "var(--coacc-sev-high)";
  if (severity === "medium") return "var(--coacc-sev-medium)";
  return "var(--coacc-sev-low)";
}

export function normalizeRouteId(input: string | undefined): string {
  return input?.trim() || "";
}

export function toPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

export function searchIncludes(haystack: string, needle: string): boolean {
  return haystack.toLocaleLowerCase("es-CO").includes(needle.toLocaleLowerCase("es-CO"));
}
