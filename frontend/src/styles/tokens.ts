export const dataColors = {
  person: "#4EA8DE",
  company: "#E07A5F",
  contract: "#F2CC8F",
  sanction: "#E56B6F",
  signal: "#B8A9C9",
  finding: "#EC4899",
  regalia: "#2DD4BF",
  budget: "#84CC16",
  election: "#81B29A",
  health: "#0D9488",
  education: "#A855F7",
  environment: "#059669",
  mining: "#B45309",
  judicial: "#CA8A04",
  pep: "#D946EF",
  disclosure: "#7C3AED",
} as const;

export type DataEntityType = keyof typeof dataColors;

export const semanticColors = {
  success: "#22c55e",
  warning: "#eab308",
  danger: "#ef4444",
  info: "#3b82f6",
} as const;
