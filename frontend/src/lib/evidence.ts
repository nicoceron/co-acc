import type { MaterializedGraphPayload } from "@/lib/materialized";

const SIGNAL_LABELS: Record<string, string> = {
  budget_execution_discrepancy: "Facturación o pagos por delante de la ejecución",
  candidate_supplier_overlap: "Candidatura y contratación en la misma persona",
  company_capacity_mismatch: "Contratación muy superior al tamaño financiero reportado",
  company_donor_vendor_overlap: "Empresa donante y contratista",
  contract_suspension_stacking: "Suspensiones repetidas en contratos públicos",
  disclosure_risk_stack: "Declaraciones con referencias corporativas o conflictos",
  donor_official_vendor_loop: "Ruta donante-funcionario-proveedor",
  donor_supplier_overlap: "Donante que también aparece como proveedor",
  education_control_capture: "Control institucional concentrado con alias contractuales",
  funding_spike_then_awards: "Pico de recursos públicos antes de nuevos contratos",
  funding_overlap: "Cruce entre financiación política y contratación",
  interadministrative_channel_stacking: "Convenios interadministrativos apilados con contratación regular",
  invoice_execution_gap: "Facturas sin avance material suficiente",
  low_competition_bidding: "Baja competencia o invitación directa",
  official_case_bulletin_exposure: "Boletín oficial con exposición pública",
  official_case_bulletin_record: "Registro en boletín oficial",
  payment_supervision_risk_stack: "Supervisión de pagos sobre contratos riesgosos",
  public_money_channel_stacking: "Canales públicos múltiples sobre el mismo actor",
  public_official_supplier_overlap: "Proveedor con directivo o vínculo en cargo público",
  sanctioned_person_exposure_stack: "Sanciones oficiales con exposición pública",
  sanctioned_still_receiving: "Proveedor sancionado que siguió recibiendo contratos",
  sanctioned_health_operator_overlap: "Prestador de salud con sanciones",
  sanctioned_supplier_record: "Proveedor con antecedentes sancionatorios",
  sensitive_public_official_supplier_overlap: "Proveedor ligado a cargo sensible",
  split_contracts_below_threshold: "Paquetes repetidos de contratos bajo umbral",
  shared_officer_supplier_network: "Red compartida de directivos en proveedores",
};

const RELATION_LABELS: Record<string, string> = {
  ADJUDICOU_A: "adjudicó a",
  ADMINISTRA: "tiene como directivo a",
  CANDIDATO_EM: "fue candidato en",
  CONTRATOU: "contrató con",
  DONO_A: "donó a",
  GANO: "ganó",
  OFFICER_OF: "aparece como directivo en",
  POSSIBLY_SAME_AS: "podría ser la misma identidad que",
  PUBLIC_APPLICATION_TRACE: "comparte aplicación pública con",
  PUBLIC_INFRASTRUCTURE_TRACE: "comparte infraestructura pública con",
  PUBLIC_MARKETING_TRACE: "comparte rastro público de mercadeo con",
  PUBLIC_PORTFOLIO_TRACE: "comparte rastro público de portafolio con",
  PUBLIC_PROFILE_TRACE: "comparte rastro público de perfil con",
  PUBLIC_SUBDOMAIN_TRACE: "comparte subdominio público con",
  REFERENTE_A: "hace referencia a",
  REPRESENTA_LEGALMENTE: "representa legalmente a",
  ROLE_DOCUMENTED: "aparece documentado como",
  SAME_AS: "coincide con",
  SANCIONADA: "fue sancionada por",
  SUPERVISA_PAGO: "supervisó pagos de",
};

export interface ConnectionTrace {
  id: string;
  headline: string;
  detail?: string | null;
  focusNodeId?: string | null;
}

export function formatMoney(value: number): string {
  return new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatDate(value: string): string {
  return new Intl.DateTimeFormat("es-CO", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function formatSignalLabel(signalId: string): string {
  const normalized = signalId.trim();
  const underscored = normalized.toLowerCase().replace(/\s+/g, "_");
  return SIGNAL_LABELS[normalized] ?? SIGNAL_LABELS[underscored] ?? normalized.replaceAll("_", " ");
}

export function normalizeReadableText(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\s+/g, " ")
    .trim();
}

function formatRelationLabel(type: string): string {
  return RELATION_LABELS[type] ?? normalizeReadableText(type);
}

export function formatEdgeDetail(
  properties: Record<string, unknown> | undefined,
): string | null {
  if (!properties) return null;

  const evidenceRefs = Array.isArray(properties.evidence_refs)
    ? properties.evidence_refs.filter(Boolean).map(String).slice(0, 3)
    : [];
  if (evidenceRefs.length > 0) {
    return evidenceRefs.join(" · ");
  }

  for (const key of ["role", "match_reason", "object", "buyer_name", "municipality"]) {
    const value = properties[key];
    if (typeof value === "string" && value.trim()) {
      return normalizeReadableText(value);
    }
  }

  for (const key of ["total_value", "value"]) {
    const value = properties[key];
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      return formatMoney(value);
    }
  }

  return null;
}

export function buildConnectionTraces(
  graph: MaterializedGraphPayload | null | undefined,
  pivotId?: string | null,
): ConnectionTrace[] {
  if (!graph) return [];

  const activeId = pivotId ?? graph.center_id;
  const nodesById = new Map(graph.nodes.map((node) => [node.id, node]));
  const directEdges = graph.edges.filter((edge) => edge.source === activeId || edge.target === activeId);

  const traces: ConnectionTrace[] = [];
  const seen = new Set<string>();

  const sortedDirectEdges = [...directEdges].sort((a, b) => {
    const aOther = nodesById.get(a.source === activeId ? a.target : a.source);
    const bOther = nodesById.get(b.source === activeId ? b.target : b.source);
    return (
      Number((a.type !== "MANTIENE_A")) - Number((b.type !== "MANTIENE_A"))
      || Number((aOther?.type !== "education")) - Number((bOther?.type !== "education"))
      || Number(((a.properties?.total_value as number | undefined) ?? (a.properties?.value as number | undefined) ?? 0))
      - Number(((b.properties?.total_value as number | undefined) ?? (b.properties?.value as number | undefined) ?? 0))
    ) * -1;
  });

  for (const edge of sortedDirectEdges) {
    const otherId = edge.source === activeId ? edge.target : edge.source;
    const other = nodesById.get(otherId);
    const active = nodesById.get(activeId);
    if (!other || !active) continue;

    const directKey = `${activeId}:${otherId}:${edge.type}`;
    if (!seen.has(directKey)) {
      seen.add(directKey);
      traces.push({
        id: directKey,
        headline: `${active.label} — ${formatRelationLabel(edge.type)} — ${other.label}`,
        detail: formatEdgeDetail(edge.properties),
        focusNodeId: otherId,
      });
    }

    const secondaryEdges = graph.edges
      .filter((secondaryEdge) => (
        (secondaryEdge.source === otherId || secondaryEdge.target === otherId)
        && secondaryEdge.source !== activeId
        && secondaryEdge.target !== activeId
        && secondaryEdge.type !== "MANTIENE_A"
      ))
      .sort((a, b) => (
        Number(((b.properties?.total_value as number | undefined) ?? (b.properties?.value as number | undefined) ?? 0))
        - Number(((a.properties?.total_value as number | undefined) ?? (a.properties?.value as number | undefined) ?? 0))
      ));

    for (const secondaryEdge of secondaryEdges.slice(0, 2)) {
      const thirdId = secondaryEdge.source === otherId ? secondaryEdge.target : secondaryEdge.source;
      const third = nodesById.get(thirdId);
      if (!third) continue;
      const secondaryKey = `${activeId}:${otherId}:${thirdId}:${secondaryEdge.type}`;
      if (seen.has(secondaryKey)) continue;
      seen.add(secondaryKey);
      traces.push({
        id: secondaryKey,
        headline: `${active.label} — ${formatRelationLabel(edge.type)} — ${other.label} — ${formatRelationLabel(secondaryEdge.type)} — ${third.label}`,
        detail: formatEdgeDetail(secondaryEdge.properties) ?? formatEdgeDetail(edge.properties),
        focusNodeId: thirdId,
      });
    }
  }

  return traces.slice(0, 12);
}
