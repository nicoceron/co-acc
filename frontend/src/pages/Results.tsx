import { useDeferredValue, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ArrowLeft, ChevronRight, Link2, ScrollText, Search } from "lucide-react";
import { Link, useLocation, useParams } from "react-router";

import { GraphCanvas } from "@/components/graph/GraphCanvas";
import { formatPropertyLabel, humanizeIdentifier } from "@/lib/display";
import {
  buildConnectionTraces,
  formatDate,
  formatSignalLabel,
} from "@/lib/evidence";
import {
  buildInvestigationBasis,
  buildLeadRankingReason,
  buildValidationBasis,
  getInvestigationConfidenceBadge,
  getLeadConfidenceBadge,
  getValidationConfidenceBadge,
  humanizePublicText,
  isCorroboratedInvestigation,
  type ReviewBadge,
} from "@/lib/review";
import type {
  MaterializedCaseDetail,
  MaterializedInvestigation,
  MaterializedLead,
  MaterializedResultsPack,
  MaterializedValidationCase,
  MaterializedWatchlistCompany,
  MaterializedWatchlistPerson,
} from "@/lib/materialized";
import { loadMaterializedCase, loadMaterializedResultsPack } from "@/lib/materialized";

import styles from "./Results.module.css";

type DrilldownCase = MaterializedCaseDetail;
type QueueKind = "companies" | "people";
type QueueEntity = MaterializedWatchlistCompany | MaterializedWatchlistPerson;

interface CatalogDefinition {
  id: string;
  title: string;
  description: string;
  aliases: string[];
  planned?: boolean;
}

type CatalogItem =
  | {
      kind: "investigation";
      key: string;
      categoryId: string;
      riskScore: number;
      corroborated: boolean;
      investigation: MaterializedInvestigation;
    }
  | {
      kind: "validation";
      key: string;
      categoryId: string;
      riskScore: number;
      corroborated: true;
      validationCase: MaterializedValidationCase;
    }
  | {
      kind: "lead";
      key: string;
      categoryId: string;
      riskScore: number;
      corroborated: boolean;
      queueKind: QueueKind;
      row: QueueEntity;
    };

interface CatalogSection {
  definition: CatalogDefinition;
  items: CatalogItem[];
  corroboratedCount: number;
}

const BASE_CATALOG: CatalogDefinition[] = [
  {
    id: "elefante_blanco",
    title: "Elefante blanco / obra trabada",
    description: "Obras o contratos donde pagos, facturación o avance reportado no cierran bien con la ejecución.",
    aliases: ["elefante_blanco", "budget_execution_discrepancy", "Facturación o pagos por delante de la ejecución"],
  },
  {
    id: "microdesfalco_contable",
    title: "Microdesfalco contable",
    description: "Tesorería, nómina, conciliaciones y excepciones contables donde aparecen cheques, descuadres o edición sensible de registros.",
    aliases: [
      "microdesfalco_contable",
      "payroll_cheque_exception",
      "treasury_jsp7_gap",
      "deleted_crp_sequence",
    ],
  },
  {
    id: "vendedor_objetos_robados",
    title: "Vendedor de objetos robados",
    description: "Celulares primero: vendedor no autorizado, IMEI o equipos recuperados y zonas de hurto. Todavía sin casos publicados.",
    aliases: ["vendedor_objetos_robados"],
    planned: true,
  },
  {
    id: "proveedor_sancionado",
    title: "Proveedor sancionado",
    description: "Empresas con sanciones públicas o contratación que siguió corriendo dentro de la ventana sancionatoria.",
    aliases: [
      "proveedor_sancionado",
      "sanctioned_supplier_record",
      "sanctioned_still_receiving",
      "Proveedor con antecedentes sancionatorios",
      "Proveedor sancionado que siguió recibiendo contratos",
    ],
  },
  {
    id: "captura_contractual",
    title: "Proveedor con vínculo oficial",
    description: "Empresas o personas proveedoras conectadas con cargos públicos, cargos sensibles o redes oficiales.",
    aliases: [
      "captura_contractual",
      "public_official_supplier_overlap",
      "sensitive_public_official_supplier_overlap",
      "Proveedor con directivo o vínculo en cargo público",
      "Proveedor ligado a cargo sensible",
    ],
  },
  {
    id: "riesgo_politico_contractual",
    title: "Riesgo político-contractual",
    description: "Cruces entre candidatura, donaciones, campaña y contratación sobre los mismos actores.",
    aliases: [
      "riesgo_politico_contractual",
      "candidate_supplier_overlap",
      "donor_vendor_loop",
      "Candidatura y contratación en la misma persona",
      "Donante que también aparece como proveedor",
    ],
  },
  {
    id: "supervision",
    title: "Supervisión e interventoría",
    description: "Supervisores, interventores o responsables de pago vinculados a contratos con riesgo o sanción.",
    aliases: [
      "supervision_disciplinaria",
      "interventoria_pagos",
      "supervision_local",
      "supervision_pago_documental",
      "payment_supervision_risk_stack",
      "Supervisión de pagos sobre contratos riesgosos",
    ],
  },
  {
    id: "captura_educativa",
    title: "Captura educativa",
    description: "Instituciones educativas con control concentrado, alias contractuales o puentes societarios verificables.",
    aliases: ["captura_educativa"],
  },
  {
    id: "direccionamiento_contractual",
    title: "Direccionamiento contractual",
    description: "Procesos con señales de direccionamiento, baja competencia, fraccionamiento o ruteo político.",
    aliases: [
      "ungrd_direccionamiento",
      "low_competition_bidding",
      "split_contracts_below_threshold",
      "Baja competencia o invitación directa",
      "Paquetes repetidos de contratos bajo umbral",
    ],
  },
  {
    id: "dadivas_intermediacion",
    title: "Dádivas e intermediación política",
    description: "Pagos, favores o intermediación política detectados alrededor de casos públicos o boletines oficiales.",
    aliases: ["ungrd_dadivas", "ungrd_preacuerdo", "ungrd_intervencion_ministerio"],
  },
  {
    id: "conflictos_societarios",
    title: "Conflictos y referencias societarias",
    description: "Declaraciones públicas con referencias corporativas, conflictos o lazos societarios que ameritan revisión.",
    aliases: [
      "declared_conflict_references",
      "Declaraciones con referencias corporativas o conflictos",
    ],
  },
  {
    id: "capacidad_financiera",
    title: "Capacidad financiera insuficiente",
    description: "Contratación que parece demasiado grande para el tamaño financiero reportado del actor.",
    aliases: [
      "capacity_mismatch_contracts",
      "Contratación muy superior al tamaño financiero reportado",
    ],
  },
];

function formatNodeTypeLabel(type: string): string {
  const labels: Record<string, string> = {
    company: "Empresa",
    person: "Persona",
    buyer: "Comprador público",
    territory: "Territorio",
    education: "Institución educativa",
  };
  return labels[type] ?? humanizeIdentifier(type);
}

function normalizeEntityRef(value?: string | null): string {
  const text = String(value ?? "").trim();
  const digits = text.replace(/\D/g, "");
  return digits || text.toUpperCase();
}

function normalizeCatalogToken(value: string): string {
  return value
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function formatCatalogTitle(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "Señal sin clasificar";
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1);
}

function createFallbackCategory(seed: string): CatalogDefinition {
  return {
    id: `dynamic-${normalizeCatalogToken(seed).replaceAll(" ", "-") || "sin-clasificar"}`,
    title: formatCatalogTitle(formatSignalLabel(seed)),
    description: "Casos agrupados por esta señal tal como aparece hoy en los datos públicos publicados.",
    aliases: [seed],
  };
}

function resolveCategoryDefinitions(
  seeds: Array<string | null | undefined>,
  catalog: CatalogDefinition[],
): CatalogDefinition[] {
  const normalizedSeeds = seeds
    .map((seed) => String(seed ?? "").trim())
    .filter(Boolean)
    .map((seed) => ({ raw: seed, normalized: normalizeCatalogToken(seed) }));

  const matches = new Map<string, CatalogDefinition>();
  for (const seed of normalizedSeeds) {
    for (const definition of catalog) {
      if (definition.aliases.some((alias) => normalizeCatalogToken(alias) === seed.normalized)) {
        matches.set(definition.id, definition);
      }
    }
  }

  if (matches.size > 0) return [...matches.values()];

  return [createFallbackCategory(normalizedSeeds[0]?.raw ?? "Sin clasificar")];
}

function deriveInvestigationRiskScore(
  investigation: MaterializedInvestigation,
  linkedScore?: number,
): number {
  if (typeof linkedScore === "number") return linkedScore;
  let score = investigation.status === "public_case" ? 20 : 12;
  score += Math.min(investigation.evidence.length, 4);
  score += Math.min(investigation.verified_open_data?.length ?? 0, 3);
  return score;
}

function catalogSortValue(item: CatalogItem): number {
  return item.kind === "investigation" ? 3 : item.kind === "validation" ? 2 : 1;
}

function isFreshCatalogItem(item: CatalogItem): boolean {
  if (item.kind === "lead") return !item.corroborated;
  if (item.kind === "investigation") {
    return item.investigation.status === "generated_lead" && !item.corroborated;
  }
  return false;
}

function buildAlertPracticeLabels(
  alerts: Array<{ alert_type?: string | null }> | undefined,
): string[] {
  return [...new Set((alerts ?? [])
    .map((alert) => formatSignalLabel(String(alert.alert_type ?? "").trim()))
    .filter(Boolean))];
}

function buildLeadCategorySeeds(
  row: QueueEntity,
  featuredLead?: MaterializedLead | null,
): string[] {
  return [
    ...(row.alerts ?? []).map((alert) => String(alert.alert_type ?? "").trim()),
    ...(featuredLead?.practice_labels ?? []),
  ].filter(Boolean);
}

function buildIdentityKeys(entityId?: string | null, documentId?: string | null): string[] {
  return [...new Set([String(entityId ?? "").trim(), normalizeEntityRef(documentId)].filter(Boolean))];
}

function formatPublicCaseTitle(title: string): string {
  return title
    .replace("stalled-work / elefante blanco style signal", "obra trabada con señal de elefante blanco")
    .replace("official-supplier overlap", "traslape oficial con proveedor")
    .replace("sanctioned public operator", "operador público sancionado")
    .replace("candidate-supplier overlap", "traslape entre candidatura y contratación")
    .replace("sanctioned supplier still receiving contracts", "proveedor sancionado que siguió recibiendo contratos");
}

function buildLeadDrilldown(lead: MaterializedLead): DrilldownCase | null {
  if (!lead.graph) return null;
  return {
    id: `${lead.entity_type}:${lead.entity_id}`,
    title: lead.name,
    subtitle: `${lead.entity_type === "company" ? "Empresa" : "Persona"}${lead.document_id ? ` · ${lead.document_id}` : ""}`,
    summary: lead.primary_reason ?? null,
    graph: lead.graph,
    public_sources: lead.public_sources,
    tags: lead.practice_labels,
  };
}

function buildValidationDrilldown(validationCase: MaterializedValidationCase): DrilldownCase | null {
  if (!validationCase.graph) return null;
  return {
    id: `validation:${validationCase.case_id}`,
    title: formatPublicCaseTitle(validationCase.title),
    subtitle: `${validationCase.entity_name} · ${validationCase.entity_ref}`,
    summary: humanizePublicText(validationCase.summary),
    graph: validationCase.graph,
    public_sources: validationCase.public_sources,
    tags: validationCase.observed_signals.map((signal) => formatSignalLabel(signal)),
  };
}

function buildCategoryHref(categoryId: string, libraryMode: boolean): string {
  return `${libraryMode ? "/biblioteca" : "/casos"}/modalidad/${categoryId}`;
}

function getCounterpartHref(categoryId: string, libraryMode: boolean): string {
  return `${libraryMode ? "/casos" : "/biblioteca"}/modalidad/${categoryId}`;
}

function getModeRoot(libraryMode: boolean): string {
  return libraryMode ? "/biblioteca" : "/casos";
}

function normalizeLeadAlerts(alerts: QueueEntity["alerts"]) {
  return alerts.map((alert) => ({
    alert_type: alert.alert_type,
    label: formatSignalLabel(alert.alert_type),
    reason_text: alert.reason_text,
    confidence_tier: alert.confidence_tier,
    severity_score: alert.severity_score,
    source_list: alert.source_list,
  }));
}

function getReviewBadge(item: CatalogItem): ReviewBadge {
  if (item.kind === "investigation") return getInvestigationConfidenceBadge(item.investigation);
  if (item.kind === "validation") return getValidationConfidenceBadge(item.validationCase);
  return getLeadConfidenceBadge({
    entity_type: item.queueKind === "companies" ? "company" : "person",
    entity_id: item.row.entity_id,
    document_id: "document_id" in item.row ? item.row.document_id : null,
    name: item.row.name,
    risk_score: item.row.suspicion_score,
    signal_types: item.row.signal_types,
    primary_reason: item.row.alerts[0]?.reason_text ?? null,
    practice_labels: buildAlertPracticeLabels(item.row.alerts),
    highlights: [],
    alerts: normalizeLeadAlerts(item.row.alerts),
    matched_validation_titles: item.corroborated ? ["matched"] : [],
    public_sources: [],
    patterns: [],
  });
}

function getItemTypeLabel(item: CatalogItem): string {
  if (item.kind === "investigation") {
    return isCorroboratedInvestigation(item.investigation) ? "Caso corroborado" : "Pista nueva";
  }
  if (item.kind === "validation") return "Caso reproducido";
  return item.corroborated ? "Pista con contraste" : "Pista nueva";
}

function getItemTitle(item: CatalogItem): string {
  if (item.kind === "investigation") return item.investigation.title;
  if (item.kind === "validation") return formatPublicCaseTitle(item.validationCase.title);
  return item.row.name;
}

function getItemMeta(item: CatalogItem): string {
  if (item.kind === "investigation") {
    return [item.investigation.subject_name, item.investigation.subject_ref].filter(Boolean).join(" · ");
  }
  if (item.kind === "validation") {
    return [item.validationCase.entity_name, item.validationCase.entity_ref].filter(Boolean).join(" · ");
  }
  return [
    item.queueKind === "companies" ? "Empresa" : "Persona",
    "document_id" in item.row ? item.row.document_id : null,
  ].filter(Boolean).join(" · ");
}

function getItemSummary(item: CatalogItem): string {
  if (item.kind === "investigation") return humanizePublicText(item.investigation.summary);
  if (item.kind === "validation") return humanizePublicText(item.validationCase.summary);
  return humanizePublicText(item.row.alerts[0]?.reason_text ?? "Cruce detectado en datos públicos.");
}

function getItemReason(item: CatalogItem): string {
  if (item.kind === "investigation") return buildInvestigationBasis(item.investigation);
  if (item.kind === "validation") return buildValidationBasis(item.validationCase);
  return buildLeadRankingReason({
    entity_type: item.queueKind === "companies" ? "company" : "person",
    entity_id: item.row.entity_id,
    document_id: "document_id" in item.row ? item.row.document_id : null,
    name: item.row.name,
    risk_score: item.row.suspicion_score,
    signal_types: item.row.signal_types,
    primary_reason: item.row.alerts[0]?.reason_text ?? null,
    practice_labels: buildAlertPracticeLabels(item.row.alerts),
    highlights: [],
    alerts: normalizeLeadAlerts(item.row.alerts),
    matched_validation_titles: item.corroborated ? ["matched"] : [],
    public_sources: [],
    patterns: [],
  });
}

function getItemTags(item: CatalogItem): string[] {
  if (item.kind === "investigation") {
    return item.investigation.tags.slice(0, 2).map((tag) => formatSignalLabel(tag));
  }
  if (item.kind === "validation") {
    return item.validationCase.matched_signals.slice(0, 2).map((signal) => formatSignalLabel(signal));
  }
  return buildAlertPracticeLabels(item.row.alerts).slice(0, 2);
}

function getItemSearchText(item: CatalogItem): string {
  return [
    getItemTitle(item),
    getItemMeta(item),
    getItemSummary(item),
    getItemReason(item),
    ...getItemTags(item),
  ].join(" ").toLowerCase();
}

function matchesSearch(text: string, query: string): boolean {
  if (!query) return true;
  return text.includes(query);
}

function toneClassName(tone: ReviewBadge["tone"]): string {
  if (tone === "high") return styles.reviewHigh ?? "";
  if (tone === "medium") return styles.reviewMedium ?? "";
  return styles.reviewInitial ?? "";
}

function SavedCaseModal({ detail, onClose }: { detail: DrilldownCase | null; onClose: () => void }) {
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [layoutMode, setLayoutMode] = useState<"force" | "hierarchy">("force");

  const graph = detail?.graph ?? null;
  const enabledTypes = useMemo(() => new Set((graph?.nodes ?? []).map((node) => node.type)), [graph]);
  const enabledRelTypes = useMemo(() => new Set((graph?.edges ?? []).map((edge) => edge.type)), [graph]);

  useEffect(() => {
    if (!detail?.graph) {
      setSelectedNodeId(null);
      return;
    }
    setSelectedNodeId(detail.graph.center_id);
    setHoveredNodeId(null);
    setLayoutMode("force");
  }, [detail]);

  useEffect(() => {
    if (!detail) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [detail, onClose]);

  const selectedNode = useMemo(() => {
    if (!graph) return null;
    return graph.nodes.find((node) => node.id === (selectedNodeId ?? graph.center_id)) ?? null;
  }, [graph, selectedNodeId]);

  const selectedNodeIds = useMemo(() => new Set(selectedNodeId ? [selectedNodeId] : []), [selectedNodeId]);
  const traces = useMemo(
    () => buildConnectionTraces(graph, selectedNode?.id ?? graph?.center_id),
    [graph, selectedNode?.id],
  );

  if (!detail) return null;

  return (
    <div className={styles.modalBackdrop} onClick={onClose}>
      <div className={styles.modal} onClick={(event) => event.stopPropagation()}>
        <div className={styles.modalHeader}>
          <div>
            <p className={styles.sectionEyebrow}>Evidencia conectada</p>
            <h2 className={styles.modalTitle}>{detail.title}</h2>
            {detail.subtitle && <p className={styles.modalMeta}>{detail.subtitle}</p>}
            {detail.summary && <p className={styles.modalSummary}>{humanizePublicText(detail.summary)}</p>}
          </div>
          <button type="button" className={styles.secondaryAction} onClick={onClose}>
            Cerrar
          </button>
        </div>

        {!graph ? (
          <div className={styles.emptyPanel}>Este caso aún no tiene una red guardada de relaciones.</div>
        ) : (
          <div className={styles.modalLayout}>
            <section className={styles.modalNarrative}>
              {detail.tags.length > 0 && (
                <div className={styles.tagRow}>
                  {detail.tags.slice(0, 6).map((tag) => (
                    <span key={`${detail.id}-${tag}`} className={styles.tagChip}>{tag}</span>
                  ))}
                </div>
              )}

              <div className={styles.evidenceList}>
                {traces.length > 0 ? traces.map((trace) => (
                  <button
                    key={trace.id}
                    type="button"
                    className={styles.traceButton}
                    onClick={() => setSelectedNodeId(trace.focusNodeId ?? graph.center_id)}
                  >
                    <strong>{trace.headline}</strong>
                    {trace.detail && <span>{trace.detail}</span>}
                  </button>
                )) : (
                  <div className={styles.emptyPanel}>Esta red todavía no trae conexiones priorizadas.</div>
                )}
              </div>

              {detail.public_sources.length > 0 && (
                <div className={styles.sourcesCard}>
                  <div className={styles.cardHead}>
                    <ScrollText size={16} />
                    <span>Fuentes públicas</span>
                  </div>
                  <ol className={styles.sourcesList}>
                    {detail.public_sources.map((source) => (
                      <li key={`${detail.id}-${source}`}>
                        <a href={source} target="_blank" rel="noreferrer">
                          {source.replace(/^https?:\/\//, "")}
                        </a>
                      </li>
                    ))}
                  </ol>
                </div>
              )}
            </section>

            <section className={styles.modalExhibit}>
              <div className={styles.graphStage}>
                <GraphCanvas
                  data={{ nodes: graph.nodes, edges: graph.edges }}
                  centerId={graph.center_id}
                  enabledTypes={enabledTypes}
                  enabledRelTypes={enabledRelTypes}
                  hiddenNodeIds={new Set<string>()}
                  selectedNodeIds={selectedNodeIds}
                  hoveredNodeId={hoveredNodeId}
                  layoutMode={layoutMode}
                  onNodeClick={setSelectedNodeId}
                  onNodeDeselect={() => setSelectedNodeId(null)}
                  onNodeHover={setHoveredNodeId}
                  onNodeRightClick={() => {}}
                  onLayoutChange={setLayoutMode}
                  onFullscreen={() => {}}
                  sidebarCollapsed={false}
                />
              </div>

              <div className={styles.inspectorCard}>
                <div className={styles.cardHead}>
                  <Link2 size={16} />
                  <span>Actor seleccionado</span>
                </div>
                {selectedNode ? (
                  <>
                    <h3 className={styles.inspectorTitle}>{selectedNode.label}</h3>
                    <p className={styles.inspectorMeta}>
                      {formatNodeTypeLabel(selectedNode.type)}
                      {selectedNode.document_id ? ` · ${selectedNode.document_id}` : ""}
                    </p>
                    <div className={styles.propertyList}>
                      {Object.entries(selectedNode.properties ?? {}).slice(0, 14).map(([key, value]) => (
                        <div key={key} className={styles.propertyRow}>
                          <span>{formatPropertyLabel(key)}</span>
                          <strong>{String(value ?? "—")}</strong>
                        </div>
                      ))}
                    </div>
                  </>
                ) : (
                  <div className={styles.emptyPanel}>Selecciona una conexión o un nodo.</div>
                )}
              </div>
            </section>
          </div>
        )}
      </div>
    </div>
  );
}

function CategoryIndexCard({
  section,
  libraryMode,
}: {
  section: CatalogSection;
  libraryMode: boolean;
}) {
  const openCount = section.items.filter(isFreshCatalogItem).length;
  const previewItem = section.items[0] ?? null;

  return (
    <Link
      to={buildCategoryHref(section.definition.id, libraryMode)}
      className={styles.categoryCard}
      aria-label={`Abrir modalidad ${section.definition.title}`}
    >
      <div className={styles.categoryCardHead}>
        <p className={styles.cardEyebrow}>{libraryMode ? "Modalidad documentada" : "Modalidad activa"}</p>
        {section.definition.planned ? <span className={styles.plannedPill}>En construcción</span> : null}
      </div>

      <h2 className={styles.categoryCardTitle}>{section.definition.title}</h2>
      <p className={styles.categoryCardDescription}>{section.definition.description}</p>

      <div className={styles.categoryStats}>
        {!libraryMode ? <span>{openCount} abiertas</span> : null}
        <span>{section.corroboratedCount} verificadas</span>
      </div>

      {previewItem ? (
        <div className={styles.categoryPreview}>
          <span>{libraryMode ? "Caso destacado" : "Ahora mismo"}</span>
          <strong>{getItemTitle(previewItem)}</strong>
        </div>
      ) : (
        <div className={styles.categoryPreview}>
          <span>Estado</span>
          <strong>Todavía sin casos publicados</strong>
        </div>
      )}

      <div className={styles.categoryCardFoot}>
        <span>{libraryMode ? "Abrir biblioteca de esta modalidad" : "Abrir modalidad"}</span>
        <ChevronRight size={16} />
      </div>
    </Link>
  );
}

function CaseCard({
  item,
  featured = false,
  libraryMode,
  loading,
  canOpen,
  onOpenValidation,
  onOpenEntity,
}: {
  item: CatalogItem;
  featured?: boolean;
  libraryMode: boolean;
  loading: boolean;
  canOpen: boolean;
  onOpenValidation: (validationCase: MaterializedValidationCase) => void;
  onOpenEntity: (entityId: string) => void;
}) {
  const badge = getReviewBadge(item);
  const tags = getItemTags(item);
  const title = getItemTitle(item);
  const meta = getItemMeta(item);
  const summary = getItemSummary(item);
  const reason = getItemReason(item);
  const typeLabel = getItemTypeLabel(item);
  const cardClassName = featured ? `${styles.caseCard} ${styles.caseCardFeatured}` : styles.caseCard;

  let action: ReactNode = null;
  if (item.kind === "investigation") {
    action = (
      <Link
        to={`${getModeRoot(libraryMode)}/${item.investigation.slug}`}
        className={styles.primaryAction}
      >
        Abrir dossier
      </Link>
    );
  } else if (item.kind === "validation") {
    action = (
      <button type="button" className={styles.primaryAction} onClick={() => onOpenValidation(item.validationCase)}>
        Ver evidencia
      </button>
    );
  } else if (canOpen) {
    action = (
      <button
        type="button"
        className={styles.primaryAction}
        onClick={() => onOpenEntity(item.row.entity_id)}
        disabled={loading}
      >
        {loading ? "Abriendo…" : "Ver subgrafo"}
      </button>
    );
  } else {
    action = <span className={styles.inlineNote}>Sin subgrafo guardado</span>;
  }

  return (
    <article className={cardClassName}>
      <div className={styles.caseTopline}>
        <div>
          <p className={styles.cardEyebrow}>{typeLabel}</p>
          <div className={styles.reviewRow}>
            <span className={`${styles.reviewBadge} ${toneClassName(badge.tone)}`}>{badge.label}</span>
          </div>
        </div>
      </div>

      <h3 className={styles.caseTitle}>{title}</h3>
      {meta ? <p className={styles.caseMeta}>{meta}</p> : null}
      <p className={styles.caseSummary}>{summary}</p>
      <p className={styles.caseReason}>{reason}</p>

      <div className={styles.caseFooter}>
        {tags.length > 0 ? (
          <div className={styles.tagRow}>
            {tags.slice(0, featured ? 2 : 1).map((tag) => (
              <span key={`${item.key}-${tag}`} className={styles.tagChip}>{tag}</span>
            ))}
          </div>
        ) : <span />}
        {action}
      </div>
    </article>
  );
}

export function Results() {
  const location = useLocation();
  const { categoryId } = useParams();

  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeCase, setActiveCase] = useState<DrilldownCase | null>(null);
  const [caseLoadError, setCaseLoadError] = useState<string | null>(null);
  const [loadingCaseId, setLoadingCaseId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const deferredSearchQuery = useDeferredValue(searchQuery.trim().toLowerCase());
  const caseCacheRef = useRef(new Map<string, DrilldownCase>());

  const isLibraryMode = location.pathname.startsWith("/biblioteca")
    || location.pathname.startsWith("/investigations");

  useEffect(() => {
    const controller = new AbortController();
    loadMaterializedResultsPack(controller.signal)
      .then((payload) => setPack(payload))
      .catch((loadError) => setError(loadError instanceof Error ? loadError.message : "No fue posible cargar el lote materializado."));
    return () => controller.abort();
  }, []);

  useEffect(() => {
    setSearchQuery("");
    setCaseLoadError(null);
  }, [categoryId, isLibraryMode]);

  const matchedValidationCases = useMemo(
    () => pack?.validation.cases.filter((validationCase) => validationCase.matched) ?? [],
    [pack],
  );
  const investigations = useMemo(() => pack?.investigations ?? [], [pack]);
  const featuredLeadIndex = useMemo(() => {
    if (!pack) return new Map<string, MaterializedLead>();
    return new Map(
      [...pack.featured_companies, ...pack.featured_people].map((lead) => [lead.entity_id, lead]),
    );
  }, [pack]);

  const drilldownIndex = useMemo(() => {
    if (!pack) return new Map<string, DrilldownCase>();
    const index = new Map<string, DrilldownCase>();
    for (const lead of [...pack.featured_companies, ...pack.featured_people]) {
      const detail = buildLeadDrilldown(lead);
      if (detail) index.set(lead.entity_id, detail);
    }
    for (const validationCase of pack.validation.cases) {
      const detail = buildValidationDrilldown(validationCase);
      if (detail) index.set(validationCase.entity_id, detail);
    }
    return index;
  }, [pack]);

  const watchlistCaseIndex = useMemo(() => {
    if (!pack) return new Map<string, { case_file?: string | null }>();
    const index = new Map<string, { case_file?: string | null }>();
    for (const row of [...pack.watchlists.companies, ...pack.watchlists.people]) {
      index.set(row.entity_id, { case_file: row.case_file });
    }
    return index;
  }, [pack]);

  const catalogSections = useMemo(() => {
    if (!pack) return [];

    const definitions = new Map(BASE_CATALOG.map((definition) => [definition.id, definition]));
    const sections = new Map<string, CatalogSection>();
    const seenKeysBySection = new Map<string, Set<string>>();
    const watchlistRows: Array<{ queueKind: QueueKind; row: QueueEntity }> = [
      ...pack.watchlists.companies.map((row) => ({ queueKind: "companies" as const, row })),
      ...pack.watchlists.people.map((row) => ({ queueKind: "people" as const, row })),
    ];
    const riskByEntity = new Map<string, number>();
    const riskByRef = new Map<string, number>();

    for (const { row } of watchlistRows) {
      riskByEntity.set(row.entity_id, row.suspicion_score);
      if ("document_id" in row && row.document_id) {
        riskByRef.set(normalizeEntityRef(row.document_id), row.suspicion_score);
      }
    }

    function ensureSections(seeds: Array<string | null | undefined>): CatalogSection[] {
      return resolveCategoryDefinitions(seeds, [...definitions.values()]).map((definition) => {
        if (!definitions.has(definition.id)) definitions.set(definition.id, definition);
        const existing = sections.get(definition.id);
        if (existing) return existing;
        const created: CatalogSection = { definition, items: [], corroboratedCount: 0 };
        sections.set(definition.id, created);
        return created;
      });
    }

    function seenKeys(sectionId: string): Set<string> {
      const existing = seenKeysBySection.get(sectionId);
      if (existing) return existing;
      const created = new Set<string>();
      seenKeysBySection.set(sectionId, created);
      return created;
    }

    function registerKeys(sectionId: string, entityId?: string | null, documentId?: string | null): void {
      const keys = buildIdentityKeys(entityId, documentId);
      const bucket = seenKeys(sectionId);
      for (const key of keys) bucket.add(key);
    }

    function isCovered(sectionId: string, entityId?: string | null, documentId?: string | null): boolean {
      const keys = buildIdentityKeys(entityId, documentId);
      const bucket = seenKeys(sectionId);
      return keys.some((key) => bucket.has(key));
    }

    function lookupRisk(entityId?: string | null, documentId?: string | null): number | undefined {
      if (entityId && riskByEntity.has(entityId)) return riskByEntity.get(entityId);
      if (documentId) return riskByRef.get(normalizeEntityRef(documentId));
      return undefined;
    }

    const corroboratedKeys = new Set<string>();
    const addCorroboratedKeys = (entityId?: string | null, documentId?: string | null) => {
      for (const key of buildIdentityKeys(entityId, documentId)) corroboratedKeys.add(key);
    };

    for (const investigation of investigations) {
      if (isCorroboratedInvestigation(investigation)) {
        addCorroboratedKeys(investigation.entity_id, investigation.subject_ref);
      }
    }
    for (const validationCase of matchedValidationCases) {
      addCorroboratedKeys(validationCase.entity_id, validationCase.entity_ref);
    }
    for (const lead of featuredLeadIndex.values()) {
      if (lead.matched_validation_titles.length > 0) {
        addCorroboratedKeys(lead.entity_id, lead.document_id);
      }
    }

    for (const investigation of investigations) {
      for (const section of ensureSections([investigation.category, ...investigation.tags])) {
        section.items.push({
          kind: "investigation",
          key: `investigation:${investigation.slug}`,
          categoryId: section.definition.id,
          riskScore: deriveInvestigationRiskScore(
            investigation,
            lookupRisk(investigation.entity_id, investigation.subject_ref),
          ),
          corroborated: isCorroboratedInvestigation(investigation),
          investigation,
        });
        registerKeys(section.definition.id, investigation.entity_id, investigation.subject_ref);
      }
    }

    for (const validationCase of matchedValidationCases) {
      for (const section of ensureSections([
        validationCase.category,
        ...validationCase.matched_signals,
        ...validationCase.observed_signals,
      ])) {
        if (isCovered(section.definition.id, validationCase.entity_id, validationCase.entity_ref)) continue;
        section.items.push({
          kind: "validation",
          key: `validation:${validationCase.case_id}`,
          categoryId: section.definition.id,
          riskScore:
            lookupRisk(validationCase.entity_id, validationCase.entity_ref)
            ?? (18 + Math.min(validationCase.matched_signals.length, 4)),
          corroborated: true,
          validationCase,
        });
        registerKeys(section.definition.id, validationCase.entity_id, validationCase.entity_ref);
      }
    }

    for (const { queueKind, row } of watchlistRows) {
      const featuredLead = featuredLeadIndex.get(row.entity_id);
      const seeds = buildLeadCategorySeeds(row, featuredLead);
      if (seeds.length === 0) continue;
      const rowDocumentId = "document_id" in row ? row.document_id : null;
      const corroborated =
        buildIdentityKeys(row.entity_id, rowDocumentId).some((key) => corroboratedKeys.has(key))
        || Boolean(featuredLead?.matched_validation_titles.length);
      for (const section of ensureSections(seeds)) {
        if (isCovered(section.definition.id, row.entity_id, rowDocumentId)) continue;
        section.items.push({
          kind: "lead",
          key: `${queueKind}:${row.entity_id}`,
          categoryId: section.definition.id,
          riskScore: row.suspicion_score,
          corroborated,
          queueKind,
          row,
        });
        registerKeys(section.definition.id, row.entity_id, rowDocumentId);
      }
    }

    const baseOrder = new Map(BASE_CATALOG.map((definition, index) => [definition.id, index]));
    return [...definitions.values()]
      .map((definition) => {
        const existing = sections.get(definition.id) ?? { definition, items: [], corroboratedCount: 0 };
        const items = [...existing.items].sort((left, right) => (
          right.riskScore - left.riskScore
          || Number(right.corroborated) - Number(left.corroborated)
          || catalogSortValue(right) - catalogSortValue(left)
          || left.key.localeCompare(right.key, "es")
        ));
        return {
          definition,
          items,
          corroboratedCount: items.filter((item) => item.corroborated).length,
        } satisfies CatalogSection;
      })
      .sort((left, right) => {
        const leftOrder = baseOrder.get(left.definition.id);
        const rightOrder = baseOrder.get(right.definition.id);
        if (leftOrder !== undefined || rightOrder !== undefined) {
          return (leftOrder ?? 999) - (rightOrder ?? 999);
        }
        return (right.items[0]?.riskScore ?? 0) - (left.items[0]?.riskScore ?? 0);
      });
  }, [featuredLeadIndex, investigations, matchedValidationCases, pack]);

  const visibleCatalogSections = useMemo(
    () => catalogSections.filter((section) => section.items.length > 0 || section.definition.planned),
    [catalogSections],
  );

  const frontlineCatalogSections = useMemo(
    () => visibleCatalogSections
      .map((section) => ({
        ...section,
        items: [...section.items].sort((left, right) => (
          Number(isFreshCatalogItem(right)) - Number(isFreshCatalogItem(left))
          || right.riskScore - left.riskScore
          || Number(right.corroborated) - Number(left.corroborated)
          || catalogSortValue(right) - catalogSortValue(left)
          || left.key.localeCompare(right.key, "es")
        )),
      }))
      .filter((section) => section.definition.planned || section.items.some(isFreshCatalogItem)),
    [visibleCatalogSections],
  );

  const corroboratedLibraryCount = useMemo(
    () => visibleCatalogSections.reduce((total, section) => total + section.corroboratedCount, 0),
    [visibleCatalogSections],
  );

  const libraryCatalogSections = useMemo(
    () => visibleCatalogSections
      .map((section) => ({
        ...section,
        items: section.items.filter((item) => item.corroborated),
      }))
      .filter((section) => section.items.length > 0),
    [visibleCatalogSections],
  );

  const pageSections = isLibraryMode ? libraryCatalogSections : frontlineCatalogSections;
  const pageSectionsById = useMemo(
    () => new Map(pageSections.map((section) => [section.definition.id, section])),
    [pageSections],
  );

  const currentSection = categoryId ? pageSectionsById.get(categoryId) ?? null : null;

  const catalogSummary = useMemo(() => {
    const newItems = frontlineCatalogSections.reduce(
      (total, section) => total + section.items.filter(isFreshCatalogItem).length,
      0,
    );
    const activeCategories = frontlineCatalogSections.filter((section) => section.items.some(isFreshCatalogItem)).length;
    return { newItems, activeCategories };
  }, [frontlineCatalogSections]);

  const librarySummary = useMemo(() => {
    const corroboratedItems = libraryCatalogSections.reduce((total, section) => total + section.items.length, 0);
    const activeCategories = libraryCatalogSections.length;
    return { corroboratedItems, activeCategories };
  }, [libraryCatalogSections]);

  const filteredIndexSections = useMemo(() => {
    return pageSections.filter((section) => {
      if (!deferredSearchQuery) return true;
      const tokens = [
        section.definition.title,
        section.definition.description,
        ...section.items.slice(0, 3).map((item) => getItemTitle(item)),
      ].join(" ").toLowerCase();
      return matchesSearch(tokens, deferredSearchQuery);
    });
  }, [deferredSearchQuery, pageSections]);

  const filteredCategoryItems = useMemo(() => {
    if (!currentSection) return [];
    return currentSection.items.filter((item) => (
      matchesSearch(getItemSearchText(item), deferredSearchQuery)
    ));
  }, [currentSection, deferredSearchQuery]);

  const featuredCategoryItem = filteredCategoryItems[0] ?? null;
  const remainingCategoryItems = filteredCategoryItems.slice(1);

  async function openEntityCase(entityId: string): Promise<void> {
    const existing = drilldownIndex.get(entityId) ?? caseCacheRef.current.get(entityId);
    if (existing) {
      setCaseLoadError(null);
      setActiveCase(existing);
      return;
    }

    const caseFile = watchlistCaseIndex.get(entityId)?.case_file;
    if (!caseFile) {
      setCaseLoadError("Este caso todavía no tiene un subgrafo materializado para mostrar.");
      return;
    }

    setLoadingCaseId(entityId);
    setCaseLoadError(null);
    try {
      const detail = await loadMaterializedCase(caseFile);
      caseCacheRef.current.set(entityId, detail);
      setActiveCase(detail);
    } catch (loadError) {
      setCaseLoadError(loadError instanceof Error ? loadError.message : "No fue posible cargar el caso materializado.");
    } finally {
      setLoadingCaseId(null);
    }
  }

  if (!pack && !error) {
    return (
      <div className={styles.stateWrap}>
        <p className={styles.stateEyebrow}>Biblioteca de investigaciones</p>
        <h1 className={styles.stateTitle}>Cargando resultados públicos…</h1>
      </div>
    );
  }

  if (!pack) {
    return (
      <div className={styles.stateWrap}>
        <p className={styles.stateEyebrow}>Resultados no disponibles</p>
        <h1 className={styles.stateTitle}>No encontramos los resultados publicados.</h1>
        <p className={styles.stateText}>{error}</p>
      </div>
    );
  }

  if (categoryId && !currentSection) {
    return (
      <div className={styles.page}>
        <div className={styles.stateWrap}>
          <p className={styles.stateEyebrow}>Modalidad no encontrada</p>
          <h1 className={styles.stateTitle}>Esta modalidad no está publicada en esta vista.</h1>
          <p className={styles.stateText}>
            Puede que todavía no tenga casos suficientes en esta sección o que la ruta ya no exista.
          </p>
          <div className={styles.heroActions}>
            <Link to={getModeRoot(isLibraryMode)} className={styles.primaryAction}>
              Volver a modalidades
            </Link>
          </div>
        </div>
      </div>
    );
  }

  if (!currentSection) {
    return (
      <div className={styles.page}>
        <header className={styles.indexHero}>
          <div className={styles.heroCopy}>
            <p className={styles.heroEyebrow}>{isLibraryMode ? "Biblioteca pública" : "Directorio público"}</p>
            <h1 className={styles.heroTitle}>
              {isLibraryMode ? "Biblioteca por modalidad." : "Explora modalidades."}
            </h1>
            <p className={styles.heroLead}>
              {isLibraryMode
                ? "Casos ya corroborados, agrupados por modalidad para navegar sin perderse en el archivo."
                : "Cada modalidad abre una página propia. Primero eliges el tipo de hallazgo; después entras a sus casos."}
            </p>
            <div className={styles.heroMeta}>
              <span>Actualizado {formatDate(pack.generated_at_utc)}</span>
              {isLibraryMode ? (
                <>
                  <span>{librarySummary.corroboratedItems} casos verificados</span>
                  <span>{librarySummary.activeCategories} modalidades</span>
                </>
              ) : (
                <>
                  <span>{catalogSummary.newItems} pistas abiertas</span>
                  <span>{catalogSummary.activeCategories} modalidades activas</span>
                  <span>{corroboratedLibraryCount} casos verificados</span>
                </>
              )}
            </div>
          </div>

          <div className={styles.heroAside}>
            <p className={styles.heroAsideLabel}>Navegación</p>
            <p className={styles.heroAsideText}>
              Entra por modalidad, no por tablero. Cada página reúne un caso principal y sus hallazgos relacionados.
            </p>
            <div className={styles.heroActions}>
              <Link to={isLibraryMode ? "/casos" : "/biblioteca"} className={styles.secondaryAction}>
                {isLibraryMode ? "Ir a pistas nuevas" : "Ir a biblioteca"}
              </Link>
            </div>
          </div>
        </header>

        <section className={styles.section}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.sectionEyebrow}>{isLibraryMode ? "Explorar archivo" : "Explorar hallazgos"}</p>
              <h2>{isLibraryMode ? "Busca una modalidad ya corroborada." : "Busca una modalidad o un actor."}</h2>
            </div>
          </div>

          <label className={styles.searchBar}>
            <Search size={18} />
            <input
              type="search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder={isLibraryMode ? "Ejemplo: proveedor sancionado, FONDECUN" : "Ejemplo: elefante blanco, TransMilenio, supervisión"}
              aria-label={isLibraryMode ? "Buscar modalidad verificada" : "Buscar modalidad o actor"}
            />
          </label>

          {filteredIndexSections.length > 0 ? (
            <div className={styles.categoryGrid}>
              {filteredIndexSections.map((section) => (
                <CategoryIndexCard
                  key={section.definition.id}
                  section={section}
                  libraryMode={isLibraryMode}
                />
              ))}
            </div>
          ) : (
            <div className={styles.emptyPanel}>
              <strong>No encontramos una modalidad con ese filtro.</strong>
              <p>Prueba con una señal, un actor o una palabra más general.</p>
            </div>
          )}
        </section>

        <SavedCaseModal detail={activeCase} onClose={() => setActiveCase(null)} />
      </div>
    );
  }

  const currentOpenCount = currentSection.items.filter(isFreshCatalogItem).length;

  return (
    <div className={styles.page}>
      <header className={styles.categoryHero}>
        <div className={styles.breadcrumbRow}>
          <Link to={getModeRoot(isLibraryMode)} className={styles.backLink}>
            <ArrowLeft size={16} />
            Volver a modalidades
          </Link>
          <span className={styles.breadcrumbDivider}>/</span>
          <span className={styles.breadcrumbCurrent}>{currentSection.definition.title}</span>
        </div>

        <div className={styles.heroCopy}>
          <p className={styles.heroEyebrow}>{isLibraryMode ? "Biblioteca por modalidad" : "Modalidad investigada"}</p>
          <h1 className={styles.heroTitle}>{currentSection.definition.title}</h1>
          <p className={styles.heroLead}>{currentSection.definition.description}</p>
          <div className={styles.heroMeta}>
            {!isLibraryMode ? <span>{currentOpenCount} abiertas</span> : null}
            <span>{currentSection.corroboratedCount} verificadas</span>
            <span>Actualizado {formatDate(pack.generated_at_utc)}</span>
          </div>
        </div>

        <div className={styles.heroAside}>
          <p className={styles.heroAsideLabel}>Vista alterna</p>
          <p className={styles.heroAsideText}>
            {isLibraryMode
              ? "Cambia a pistas nuevas para ver qué sigue abierto en esta modalidad."
              : "Cambia a biblioteca para ver sólo los casos ya corroborados de esta modalidad."}
          </p>
          <div className={styles.heroActions}>
            <Link to={getCounterpartHref(currentSection.definition.id, isLibraryMode)} className={styles.secondaryAction}>
              {isLibraryMode ? "Ver pistas nuevas" : "Ver biblioteca de esta modalidad"}
            </Link>
          </div>
        </div>
      </header>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>Dentro de esta modalidad</p>
            <h2>{isLibraryMode ? "Filtra los casos ya corroborados." : "Filtra los casos y pistas publicados."}</h2>
          </div>
        </div>

        <label className={styles.searchBar}>
          <Search size={18} />
          <input
            type="search"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="Busca por actor, resumen o señal"
            aria-label="Buscar caso dentro de la modalidad"
          />
        </label>

        {caseLoadError ? <p className={styles.queueError}>{caseLoadError}</p> : null}

        {filteredCategoryItems.length === 0 ? (
          <div className={styles.emptyPanel}>
            <strong>No encontramos casos con ese filtro.</strong>
            <p>Prueba con menos palabras o vuelve a la modalidad completa.</p>
          </div>
        ) : (
          <div className={styles.storyGrid}>
            {featuredCategoryItem ? (
              <div className={styles.storyLead}>
                <div className={styles.storyLabelRow}>
                  <p className={styles.sectionEyebrow}>Caso principal</p>
                </div>
                <CaseCard
                  item={featuredCategoryItem}
                  featured
                  libraryMode={isLibraryMode}
                  loading={featuredCategoryItem.kind === "lead" && loadingCaseId === featuredCategoryItem.row.entity_id}
                  canOpen={featuredCategoryItem.kind !== "lead" || Boolean(
                    drilldownIndex.get(featuredCategoryItem.row.entity_id)
                    || watchlistCaseIndex.get(featuredCategoryItem.row.entity_id)?.case_file
                  )}
                  onOpenValidation={(validationCase) => {
                    const detail = buildValidationDrilldown(validationCase);
                    if (detail) setActiveCase(detail);
                  }}
                  onOpenEntity={(entityId) => {
                    void openEntityCase(entityId);
                  }}
                />
              </div>
            ) : null}

            <div className={styles.storyRail}>
              <div className={styles.storyLabelRow}>
                <p className={styles.sectionEyebrow}>Relacionados</p>
                <span className={styles.storyCount}>{remainingCategoryItems.length} más</span>
              </div>

              {remainingCategoryItems.length > 0 ? (
                <div className={styles.caseList}>
                  {remainingCategoryItems.map((item) => (
                    <CaseCard
                      key={item.key}
                      item={item}
                      libraryMode={isLibraryMode}
                      loading={item.kind === "lead" && loadingCaseId === item.row.entity_id}
                      canOpen={item.kind !== "lead" || Boolean(
                        drilldownIndex.get(item.row.entity_id)
                        || watchlistCaseIndex.get(item.row.entity_id)?.case_file
                      )}
                      onOpenValidation={(validationCase) => {
                        const detail = buildValidationDrilldown(validationCase);
                        if (detail) setActiveCase(detail);
                      }}
                      onOpenEntity={(entityId) => {
                        void openEntityCase(entityId);
                      }}
                    />
                  ))}
                </div>
              ) : (
                <div className={styles.emptyPanel}>
                  <strong>No hay más casos en esta modalidad.</strong>
                  <p>Por ahora esta línea sólo tiene un caso publicado.</p>
                </div>
              )}
            </div>
          </div>
        )}
      </section>

      <SavedCaseModal detail={activeCase} onClose={() => setActiveCase(null)} />
    </div>
  );
}
