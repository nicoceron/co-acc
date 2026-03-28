import { formatSignalLabel } from "@/lib/evidence";
import type { MaterializedInvestigation, MaterializedLead, MaterializedValidationCase } from "@/lib/materialized";

export type ReviewTone = "high" | "medium" | "initial";

export interface ReviewBadge {
  label: string;
  tone: ReviewTone;
}

export const QUICK_GLOSSARY: Array<{ term: string; description: string }> = [
  {
    term: "SECOP",
    description: "Portal público donde aparecen procesos, contratos y documentos de contratación estatal.",
  },
  {
    term: "NIT",
    description: "Número de identificación tributaria usado para reconocer empresas y entidades.",
  },
  {
    term: "RUT",
    description: "Registro tributario que ayuda a verificar representantes legales, actividades y fechas.",
  },
  {
    term: "Convenio interadministrativo",
    description: "Contrato entre entidades públicas que a veces sirve como canal adicional de contratación.",
  },
  {
    term: "Ventana de sanción",
    description: "Periodo en el que una sanción estaba vigente o seguía abierta cuando apareció un contrato.",
  },
];

const PUBLIC_TEXT_REPLACEMENTS: Array<[RegExp, string]> = [
  [/\bofficial_case_bulletin_count\b/g, "boletines oficiales"],
  [/\bperson_sanction_count\b/g, "sanciones personales"],
  [/\boffice_count\b/g, "cargos públicos"],
  [/\bSAME_AS\b/g, "coincidencia de identidad"],
  [/\bPOSSIBLY_SAME_AS\b/g, "posible coincidencia de identidad"],
  [/\bSUPERVISA_PAGO\b/g, "supervisó pagos de"],
  [/\bCONTRATOU\b/g, "contrató con"],
  [/\bOFFICER_OF\b/g, "aparece como directivo de"],
  [/Lead generado desde capas oficiales:/g, "Señal automática basada en"],
  [/Lead generado desde registros abiertos:/g, "Señal automática basada en"],
];

function joinReadable(items: string[]): string {
  if (items.length === 0) return "";
  if (items.length === 1) return items[0]!;
  if (items.length === 2) return `${items[0]!} y ${items[1]!}`;
  return `${items.slice(0, -1).join(", ")} y ${items.at(-1)!}`;
}

function distinctNonEmpty(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))];
}

export function humanizePublicText(value: string | null | undefined): string {
  let text = String(value ?? "").trim();
  if (!text) return "";
  for (const [pattern, replacement] of PUBLIC_TEXT_REPLACEMENTS) {
    text = text.replace(pattern, replacement);
  }
  return text.replace(/\s{2,}/g, " ").trim();
}

export function getLeadPriorityBadge(score: number): ReviewBadge {
  if (score >= 18) return { label: "Prioridad alta", tone: "high" };
  if (score >= 12) return { label: "Prioridad media", tone: "medium" };
  return { label: "Revisión inicial", tone: "initial" };
}

export function getLeadConfidenceBadge(lead: MaterializedLead): ReviewBadge {
  const highConfidenceAlert = lead.alerts.some((alert) => String(alert.confidence_tier ?? "").toLowerCase() === "high");
  const documentedAlerts = lead.alerts.filter((alert) => (alert.source_list ?? []).length > 0).length;
  if (lead.matched_validation_titles.length > 0 || (lead.public_sources.length >= 2 && highConfidenceAlert)) {
    return { label: "Verificado con fuente pública", tone: "high" };
  }
  if (lead.public_sources.length > 0 || documentedAlerts > 0 || lead.graph_summary?.edge_count) {
    return { label: "Probable, falta cierre documental", tone: "medium" };
  }
  return { label: "Pista inicial", tone: "initial" };
}

export function isCorroboratedInvestigation(investigation: MaterializedInvestigation): boolean {
  return (
    investigation.status === "public_case"
    || (investigation.reported_sources?.length ?? 0) > 0
    || (investigation.reported_claims?.length ?? 0) > 0
  );
}

export function isFreshInvestigation(investigation: MaterializedInvestigation): boolean {
  return investigation.status === "generated_lead" && !isCorroboratedInvestigation(investigation);
}

export function getInvestigationPriorityBadge(investigation: MaterializedInvestigation): ReviewBadge {
  if (
    isCorroboratedInvestigation(investigation)
    || investigation.evidence.length >= 8
  ) {
    return { label: "Prioridad alta", tone: "high" };
  }
  if (investigation.evidence.length >= 4 || investigation.tags.length >= 2) {
    return { label: "Prioridad media", tone: "medium" };
  }
  return { label: "Revisión inicial", tone: "initial" };
}

export function getInvestigationConfidenceBadge(investigation: MaterializedInvestigation): ReviewBadge {
  if (investigation.status === "public_case" || (investigation.verified_open_data?.length ?? 0) >= 3) {
    return { label: "Verificado con fuente pública", tone: "high" };
  }
  if ((investigation.verified_open_data?.length ?? 0) > 0 || investigation.public_sources.length >= 3) {
    return { label: "Probable, falta cierre documental", tone: "medium" };
  }
  return { label: "Pista inicial", tone: "initial" };
}

export function getValidationConfidenceBadge(validationCase: MaterializedValidationCase): ReviewBadge {
  if (validationCase.matched) {
    return { label: "Caso conocido reproducido", tone: "high" };
  }
  return { label: "Aún no reproducido", tone: "initial" };
}

export function buildLeadRankingReason(lead: MaterializedLead): string {
  const reasons = distinctNonEmpty([
    ...lead.practice_labels.slice(0, 2).map((label) => formatSignalLabel(label)),
    lead.highlights[0],
  ]).slice(0, 3);
  const sentence = reasons.length > 0
    ? `Sube por ${joinReadable(reasons)}.`
    : "Sube por señales cruzadas en datos públicos.";
  return humanizePublicText(sentence);
}

export function buildInvestigationBasis(investigation: MaterializedInvestigation): string {
  const reasons = distinctNonEmpty([
    ...investigation.tags.slice(0, 2).map((tag) => formatSignalLabel(tag)),
    humanizePublicText(investigation.evidence[0]?.label),
  ]).slice(0, 3);
  const sentence = reasons.length > 0
    ? `Se sostiene en ${joinReadable(reasons)}.`
    : "Se sostiene en datos públicos cruzados.";
  return humanizePublicText(sentence);
}

export function buildValidationBasis(validationCase: MaterializedValidationCase): string {
  const reasons = distinctNonEmpty(
    validationCase.matched_signals.slice(0, 3).map((signal) => formatSignalLabel(signal)),
  );
  const sentence = reasons.length > 0
    ? `Coincide por ${joinReadable(reasons)}.`
    : "Coincide con señales ya documentadas públicamente.";
  return humanizePublicText(sentence);
}
