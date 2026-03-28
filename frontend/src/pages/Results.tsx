import { useEffect, useMemo, useRef, useState } from "react";
import { Link2, ScrollText, ShieldCheck } from "lucide-react";
import { Link, useLocation } from "react-router";

import { GraphCanvas } from "@/components/graph/GraphCanvas";
import { formatPropertyLabel, humanizeIdentifier } from "@/lib/display";
import {
  buildConnectionTraces,
  formatDate,
  formatMoney,
  formatSignalLabel,
} from "@/lib/evidence";
import {
  buildInvestigationBasis,
  buildValidationBasis,
  getInvestigationConfidenceBadge,
  getInvestigationPriorityBadge,
  getLeadPriorityBadge,
  getValidationConfidenceBadge,
  humanizePublicText,
  QUICK_GLOSSARY,
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
    title: "Elefante blanco",
    description: "Obras o contratos donde pagos, facturación o avance reportado no cierran bien con la ejecución.",
    aliases: ["elefante_blanco", "budget_execution_discrepancy", "Facturación o pagos por delante de la ejecución"],
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

function signalCountLabel(count: number): string {
  return `${count} ${count === 1 ? "alerta" : "alertas"}`;
}

function badgeToneClass(tone: ReviewBadge["tone"]): string {
  if (tone === "high") return styles.reviewHigh ?? "";
  if (tone === "medium") return styles.reviewMedium ?? "";
  return styles.reviewInitial ?? "";
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

function resolveCategoryDefinition(
  seeds: Array<string | null | undefined>,
  catalog: CatalogDefinition[],
): CatalogDefinition {
  const normalizedSeeds = seeds
    .map((seed) => String(seed ?? "").trim())
    .filter(Boolean)
    .map((seed) => ({ raw: seed, normalized: normalizeCatalogToken(seed) }));

  for (const seed of normalizedSeeds) {
    const found = catalog.find((definition) => (
      definition.aliases.some((alias) => normalizeCatalogToken(alias) === seed.normalized)
    ));
    if (found) return found;
  }

  return createFallbackCategory(normalizedSeeds[0]?.raw ?? "Sin clasificar");
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

function summarizeQueueMetrics(entity: QueueEntity, kind: QueueKind): string[] {
  if (kind === "companies") {
    const company = entity as MaterializedWatchlistCompany;
    return [
      `${company.contract_count} contratos · ${formatMoney(company.contract_value)}`,
      company.sanction_count > 0 ? `${company.sanction_count} sanciones` : "",
      company.official_officer_count > 0 ? `${company.official_officer_count} cruces con cargo público` : "",
      company.execution_gap_contract_count > 0 ? `${company.execution_gap_contract_count} brechas de ejecución` : "",
    ].filter(Boolean);
  }

  const person = entity as MaterializedWatchlistPerson;
  return [
    (person.person_sanction_count ?? 0) > 0 ? `${person.person_sanction_count ?? 0} sanciones oficiales` : "",
    person.candidacy_count > 0 ? `${person.candidacy_count} candidaturas` : "",
    person.donation_count > 0 ? `${person.donation_count} donaciones` : "",
    person.supplier_contract_count > 0 ? `${person.supplier_contract_count} contratos como proveedor` : "",
    person.conflict_disclosure_count > 0 ? `${person.conflict_disclosure_count} conflictos declarados` : "",
  ].filter(Boolean);
}

function scoreTone(score: number): string {
  if (score >= 16) return styles.scoreCritical ?? "";
  if (score >= 10) return styles.scoreHot ?? "";
  return styles.scoreWarm ?? "";
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
            <p className={styles.sectionEyebrow}>Exhibición del caso</p>
            <h2 className={styles.modalTitle}>{detail.title}</h2>
            {detail.subtitle && <p className={styles.modalMeta}>{detail.subtitle}</p>}
            {detail.summary && <p className={styles.modalSummary}>{humanizePublicText(detail.summary)}</p>}
          </div>
          <button type="button" className={styles.closeButton} onClick={onClose}>
            Cerrar
          </button>
        </div>

        {!graph ? (
          <div className={styles.emptyState}>Este caso aún no tiene una red guardada de relaciones.</div>
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
                  <div className={styles.emptyState}>Esta red todavía no trae conexiones priorizadas.</div>
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
                  <div className={styles.emptyState}>Selecciona una conexión o un nodo.</div>
                )}
              </div>
            </section>
          </div>
        )}
      </div>
    </div>
  );
}

function InvestigationCard({ investigation }: { investigation: MaterializedInvestigation }) {
  const priority = getInvestigationPriorityBadge(investigation);
  const confidence = getInvestigationConfidenceBadge(investigation);
  const basis = buildInvestigationBasis(investigation);

  return (
    <article className={styles.investigationCard}>
      <p className={styles.cardEyebrow}>Dossier</p>
      <h3>{investigation.title}</h3>
      <p className={styles.cardMeta}>
        {investigation.subject_name}
        {investigation.subject_ref ? ` · ${investigation.subject_ref}` : ""}
      </p>
      <div className={styles.reviewRow}>
        <span className={`${styles.reviewBadge} ${badgeToneClass(priority.tone)}`}>{priority.label}</span>
        <span className={`${styles.reviewBadge} ${badgeToneClass(confidence.tone)}`}>{confidence.label}</span>
      </div>
      <p className={styles.reviewReason}>{basis}</p>
      <p className={styles.cardSummary}>{humanizePublicText(investigation.summary)}</p>
      <div className={styles.tagRow}>
        {investigation.tags.slice(0, 4).map((tag) => (
          <span key={`${investigation.slug}-${tag}`} className={styles.tagChip}>{formatSignalLabel(tag)}</span>
        ))}
      </div>
      <Link to={`/investigations/${investigation.slug}`} className={styles.inlineAction}>
        Abrir dossier
      </Link>
    </article>
  );
}

function ProofCaseCard({
  validationCase,
  onOpen,
}: {
  validationCase: MaterializedValidationCase;
  onOpen: () => void;
}) {
  const confidence = getValidationConfidenceBadge(validationCase);
  const basis = buildValidationBasis(validationCase);

  return (
    <article className={styles.proofCard}>
      <div className={styles.proofHead}>
        <span className={styles.verifiedPill}>Corroborado</span>
        <span className={styles.cardMeta}>Caso conocido</span>
      </div>
      <h3>{formatPublicCaseTitle(validationCase.title)}</h3>
      <p className={styles.cardMeta}>{validationCase.entity_name} · {validationCase.entity_ref}</p>
      <div className={styles.reviewRow}>
        <span className={`${styles.reviewBadge} ${badgeToneClass(confidence.tone)}`}>{confidence.label}</span>
      </div>
      <p className={styles.reviewReason}>{basis}</p>
      <p className={styles.cardSummary}>{humanizePublicText(validationCase.summary)}</p>
      <div className={styles.tagRow}>
        {validationCase.matched_signals.map((signal) => (
          <span key={`${validationCase.case_id}-${signal}`} className={styles.tagChip}>{formatSignalLabel(signal)}</span>
        ))}
      </div>
      <div className={styles.cardActions}>
        <button type="button" className={styles.detailButton} onClick={onOpen}>
          Ver red de relaciones
        </button>
      </div>
    </article>
  );
}

function QueueLeadCard({
  row,
  queueKind,
  corroborated,
  canOpen,
  loading,
  onOpen,
}: {
  row: QueueEntity;
  queueKind: QueueKind;
  corroborated: boolean;
  canOpen: boolean;
  loading: boolean;
  onOpen: () => void;
}) {
  const priority = getLeadPriorityBadge(row.suspicion_score);
  const corroborationBadge: ReviewBadge = corroborated
    ? { label: "Corroborado", tone: "high" }
    : { label: "Sin corroboración externa", tone: "initial" };
  const practices = buildAlertPracticeLabels(row.alerts).slice(0, 3);
  const primaryReason = row.alerts[0]?.reason_text ?? "Cruce detectado en datos públicos.";
  const metrics = summarizeQueueMetrics(row, queueKind);

  return (
    <article className={styles.queueCard}>
      <div className={styles.queueHead}>
        <div>
          <p className={styles.cardEyebrow}>
            {queueKind === "companies" ? "Empresa" : "Persona"}
            {"document_id" in row && row.document_id ? ` · ${row.document_id}` : ""}
          </p>
          <h3>{row.name}</h3>
        </div>
        <div className={`${styles.scorePill} ${scoreTone(row.suspicion_score)}`}>
          <span>{row.suspicion_score}</span>
          <small>{signalCountLabel(row.signal_types)}</small>
        </div>
      </div>

      <div className={styles.reviewRow}>
        <span className={`${styles.reviewBadge} ${badgeToneClass(priority.tone)}`}>{priority.label}</span>
        <span className={`${styles.reviewBadge} ${badgeToneClass(corroborationBadge.tone)}`}>
          {corroborationBadge.label}
        </span>
      </div>
      <p className={styles.reviewReason}>
        {practices.length > 0 ? `Sube por ${practices.slice(0, 2).join(" y ")}.` : "Sube por señales cruzadas en datos públicos."}
      </p>
      <p className={styles.cardSummary}>{primaryReason}</p>

      <div className={styles.tagRow}>
        {practices.map((practice) => (
          <span key={`${row.entity_id}-${practice}`} className={styles.tagChip}>{practice}</span>
        ))}
      </div>

      {metrics.length > 0 && (
        <ul className={styles.metricList}>
          {metrics.map((metric) => (
            <li key={`${row.entity_id}-${metric}`}>{metric}</li>
          ))}
        </ul>
      )}

      {canOpen ? (
        <div className={styles.cardActions}>
          <button type="button" className={styles.detailButton} onClick={onOpen} disabled={loading}>
            {loading ? "Abriendo…" : "Ver caso"}
          </button>
        </div>
      ) : null}
    </article>
  );
}

function CategoryShelf({
  section,
  expanded,
  onToggle,
  onOpenValidation,
  onOpenEntity,
  resolveLeadState,
}: {
  section: CatalogSection;
  expanded: boolean;
  onToggle: () => void;
  onOpenValidation: (validationCase: MaterializedValidationCase) => void;
  onOpenEntity: (entityId: string) => void;
  resolveLeadState: (entityId: string) => { canOpen: boolean; loading: boolean };
}) {
  const visibleItems = expanded ? section.items : section.items.slice(0, 3);

  return (
    <section id={`category-${section.definition.id}`} className={styles.categorySection}>
      <div className={styles.categoryHeader}>
        <div>
          <p className={styles.sectionEyebrow}>Categoría</p>
          <h2>{section.definition.title}</h2>
          <p className={styles.categoryDescription}>{section.definition.description}</p>
        </div>
        <div className={styles.categoryMeta}>
          <span>{section.items.length} casos</span>
          <span>{section.corroboratedCount} corroborados</span>
          {section.definition.planned ? <span>En preparación</span> : null}
        </div>
      </div>

      {section.items.length === 0 ? (
        <div className={styles.categoryEmpty}>
          <strong>Todavía no hay casos publicados.</strong>
          <p>
            Esta categoría ya tiene ruta de investigación, pero todavía no publicamos leads con suficiente cierre
            documental como para exponerlos aquí.
          </p>
        </div>
      ) : (
        <>
          <div className={styles.categoryGrid}>
            {visibleItems.map((item) => {
              if (item.kind === "investigation") {
                return <InvestigationCard key={item.key} investigation={item.investigation} />;
              }
              if (item.kind === "validation") {
                return (
                  <ProofCaseCard
                    key={item.key}
                    validationCase={item.validationCase}
                    onOpen={() => onOpenValidation(item.validationCase)}
                  />
                );
              }
              const leadState = resolveLeadState(item.row.entity_id);
              return (
                <QueueLeadCard
                  key={item.key}
                  row={item.row}
                  queueKind={item.queueKind}
                  corroborated={item.corroborated}
                  canOpen={leadState.canOpen}
                  loading={leadState.loading}
                  onOpen={() => { void onOpenEntity(item.row.entity_id); }}
                />
              );
            })}
          </div>

          {section.items.length > 3 && (
            <div className={styles.categoryFooter}>
              <button type="button" className={styles.moreButton} onClick={onToggle}>
                {expanded ? "Ocultar biblioteca" : `Ver biblioteca (${section.items.length})`}
              </button>
            </div>
          )}
        </>
      )}
    </section>
  );
}

export function Results() {
  const location = useLocation();
  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeCase, setActiveCase] = useState<DrilldownCase | null>(null);
  const [caseLoadError, setCaseLoadError] = useState<string | null>(null);
  const [loadingCaseId, setLoadingCaseId] = useState<string | null>(null);
  const [expandedCategories, setExpandedCategories] = useState<Record<string, boolean>>({});
  const caseCacheRef = useRef(new Map<string, DrilldownCase>());

  useEffect(() => {
    const controller = new AbortController();
    loadMaterializedResultsPack(controller.signal)
      .then((payload) => setPack(payload))
      .catch((loadError) => setError(loadError instanceof Error ? loadError.message : "No fue posible cargar el lote materializado."));
    return () => controller.abort();
  }, []);

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

    function ensureSection(seeds: Array<string | null | undefined>): CatalogSection {
      const definition = resolveCategoryDefinition(seeds, [...definitions.values()]);
      if (!definitions.has(definition.id)) definitions.set(definition.id, definition);
      const existing = sections.get(definition.id);
      if (existing) return existing;
      const created: CatalogSection = { definition, items: [], corroboratedCount: 0 };
      sections.set(definition.id, created);
      return created;
    }

    function seenKeys(sectionId: string): Set<string> {
      const existing = seenKeysBySection.get(sectionId);
      if (existing) return existing;
      const created = new Set<string>();
      seenKeysBySection.set(sectionId, created);
      return created;
    }

    function registerKeys(sectionId: string, entityId?: string | null, documentId?: string | null): string[] {
      const keys = buildIdentityKeys(entityId, documentId);
      const bucket = seenKeys(sectionId);
      for (const key of keys) bucket.add(key);
      return keys;
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
      if (
        investigation.status === "public_case"
        || (investigation.verified_open_data?.length ?? 0) > 0
        || investigation.public_sources.length > 0
      ) {
        addCorroboratedKeys(investigation.entity_id, investigation.subject_ref);
      }
    }
    for (const validationCase of matchedValidationCases) {
      addCorroboratedKeys(validationCase.entity_id, validationCase.entity_ref);
    }
    for (const lead of featuredLeadIndex.values()) {
      if (lead.matched_validation_titles.length > 0 || lead.public_sources.length > 0) {
        addCorroboratedKeys(lead.entity_id, lead.document_id);
      }
    }

    for (const investigation of investigations) {
      const section = ensureSection([investigation.category, ...investigation.tags]);
      section.items.push({
        kind: "investigation",
        key: `investigation:${investigation.slug}`,
        categoryId: section.definition.id,
        riskScore: deriveInvestigationRiskScore(
          investigation,
          lookupRisk(investigation.entity_id, investigation.subject_ref),
        ),
        corroborated:
          investigation.status === "public_case"
          || (investigation.verified_open_data?.length ?? 0) > 0
          || investigation.public_sources.length > 0,
        investigation,
      });
      registerKeys(section.definition.id, investigation.entity_id, investigation.subject_ref);
    }

    for (const validationCase of matchedValidationCases) {
      const section = ensureSection([
        validationCase.category,
        ...validationCase.matched_signals,
        ...validationCase.observed_signals,
      ]);
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

    for (const { queueKind, row } of watchlistRows) {
      const featuredLead = featuredLeadIndex.get(row.entity_id);
      const seeds = buildLeadCategorySeeds(row, featuredLead);
      if (seeds.length === 0) continue;
      const section = ensureSection(seeds);
      const rowDocumentId = "document_id" in row ? row.document_id : null;
      if (isCovered(section.definition.id, row.entity_id, rowDocumentId)) continue;
      const corroborated =
        buildIdentityKeys(row.entity_id, rowDocumentId).some((key) => corroboratedKeys.has(key))
        || Boolean(featuredLead?.matched_validation_titles.length)
        || Boolean(featuredLead?.public_sources.length);
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
  const isLibraryMode = location.pathname === "/investigations";

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
  const pageSections = isLibraryMode ? libraryCatalogSections : frontlineCatalogSections;

  async function openEntityCase(entityId: string): Promise<void> {
    const existing = drilldownIndex.get(entityId) ?? caseCacheRef.current.get(entityId);
    if (existing) {
      setCaseLoadError(null);
      setActiveCase(existing);
      return;
    }

    const caseFile = watchlistCaseIndex.get(entityId)?.case_file;
    if (!caseFile) {
      setCaseLoadError("Este caso aún no tiene un subgrafo materializado.");
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

  return (
    <div className={styles.page}>
      <header className={styles.hero}>
        <div className={styles.heroCopy}>
          <p className={styles.heroEyebrow}>Biblioteca pública de investigaciones</p>
          <h1 className={styles.heroTitle}>
            {isLibraryMode ? "Biblioteca corroborada para contrastar y citar." : "Casos ordenados por práctica, no por ruido."}
          </h1>
          <p className={styles.heroLead}>
            {isLibraryMode
              ? "Aquí quedan los casos que ya tienen mejor cierre público: dossiers, casos reproducidos y hallazgos con respaldo externo. Sirve para contrastar, citar y comparar contra las pistas nuevas."
              : "La portada está hecha para encontrar cosas nuevas. Por eso aquí salen primero las pistas abiertas de mayor riesgo por práctica; los casos ya corroborados quedan como respaldo y control, no como portada."}
          </p>
          <div className={styles.heroMeta}>
            <span>Generado {formatDate(pack.generated_at_utc)}</span>
            {isLibraryMode ? (
              <>
                <span>{librarySummary.corroboratedItems} casos corroborados</span>
                <span>{librarySummary.activeCategories} categorías documentadas</span>
              </>
            ) : (
              <>
                <span>{catalogSummary.newItems} pistas nuevas visibles</span>
                <span>{corroboratedLibraryCount} casos corroborados aparte</span>
              </>
            )}
          </div>
          <div className={styles.cardActions}>
            {pageSections[0] ? (
              <a href={`#category-${pageSections[0].definition.id}`} className={styles.primaryCta}>
                Explorar categorías
              </a>
            ) : null}
            <Link to={isLibraryMode ? "/results" : "/investigations"} className={styles.inlineAction}>
              {isLibraryMode ? "Volver a nuevas pistas" : "Ver biblioteca corroborada"}
            </Link>
          </div>
        </div>

        <aside className={styles.heroProof}>
          <div className={styles.heroProofHead}>
            <ShieldCheck size={16} />
            <span>Qué verás primero</span>
          </div>
          <div className={styles.heroProofGrid}>
            <div className={styles.heroProofStat}>
              <strong>{isLibraryMode ? librarySummary.activeCategories : catalogSummary.activeCategories}</strong>
              <span>{isLibraryMode ? "categorías documentadas" : "categorías con pistas nuevas"}</span>
            </div>
            <div className={styles.heroProofStat}>
              <strong>{isLibraryMode ? librarySummary.corroboratedItems : catalogSummary.newItems}</strong>
              <span>{isLibraryMode ? "casos corroborados" : "pistas nuevas publicadas"}</span>
            </div>
            <div className={styles.heroProofStat}>
              <strong>{isLibraryMode ? investigations.length : corroboratedLibraryCount}</strong>
              <span>{isLibraryMode ? "dossiers en biblioteca" : "casos corroborados aparte"}</span>
            </div>
            <div className={styles.heroProofStat}>
              <strong>{pack.stats.promoted_sources}</strong>
              <span>fuentes oficiales activas</span>
            </div>
          </div>
        </aside>
      </header>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>Cómo navegar</p>
            <h2>
              {isLibraryMode
                ? "Primero eliges la práctica. Después comparas qué ya quedó documentado."
                : "Primero eliges la práctica. Después miras si el caso ya está corroborado."}
            </h2>
          </div>
        </div>

        <div className={styles.guideGrid}>
          <article className={styles.guideCard}>
            <strong>{isLibraryMode ? "1. Escoge una práctica documentada" : "1. Escoge una categoría"}</strong>
            <p>
              {isLibraryMode
                ? "Aquí sólo quedan categorías con material corroborado que ya puedes revisar o citar con más seguridad."
                : "Elefante blanco, proveedor sancionado, supervisión y las demás prácticas con pistas nuevas visibles."}
            </p>
          </article>
          <article className={styles.guideCard}>
            <strong>{isLibraryMode ? "2. Usa esto como control" : "2. Mira qué tan nuevo es"}</strong>
            <p>
              {isLibraryMode
                ? "Esta biblioteca sirve para contrastar si una pista nueva ya tiene soporte público suficiente o si todavía está verde."
                : "La portada prioriza pistas iniciales. Si algo ya está corroborado, queda marcado pero no manda la pantalla."}
            </p>
          </article>
          <article className={styles.guideCard}>
            <strong>{isLibraryMode ? "3. Vuelve a descubrir" : "3. Usa lo corroborado como control"}</strong>
            <p>
              {isLibraryMode
                ? "Si quieres encontrar cosas nuevas en vez de revisar benchmark, vuelve a la portada de nuevas pistas."
                : "La biblioteca corroborada sigue disponible, pero la portada está hecha para descubrir, no para repetir benchmark."}
            </p>
          </article>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>Glosario rápido</p>
            <h2>Términos mínimos para leer un caso sin saber contratación pública</h2>
          </div>
        </div>
        <div className={styles.glossaryGrid}>
          {QUICK_GLOSSARY.map((item) => (
            <article key={item.term} className={styles.glossaryCard}>
              <strong>{item.term}</strong>
              <p>{item.description}</p>
            </article>
          ))}
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>Explorar por categoría</p>
            <h2>
              {isLibraryMode
                ? "Esta biblioteca está organizada por práctica para revisar rápido lo ya documentado."
                : "Estas categorías tienen pistas nuevas arriba. Lo corroborado queda atrás como referencia."}
            </h2>
          </div>
        </div>
        <div className={styles.catalogNav}>
          {pageSections.map((section) => (
            <a key={section.definition.id} href={`#category-${section.definition.id}`} className={styles.catalogNavCard}>
              <strong>{section.definition.title}</strong>
              <span>
                {isLibraryMode
                  ? `${section.items.length} corroborados`
                  : `${section.items.filter(isFreshCatalogItem).length} pistas nuevas`}
              </span>
              <small>
                {isLibraryMode ? "listos para contraste" : `${section.corroboratedCount} corroborados aparte`}
              </small>
            </a>
          ))}
        </div>
      </section>

      <section className={styles.section}>
        {caseLoadError && <p className={styles.queueError}>{caseLoadError}</p>}
        <div className={styles.catalogStack}>
          {pageSections.map((section) => (
            <CategoryShelf
              key={section.definition.id}
              section={section}
              expanded={Boolean(expandedCategories[section.definition.id])}
              onToggle={() => setExpandedCategories((current) => ({
                ...current,
                [section.definition.id]: !current[section.definition.id],
              }))}
              onOpenValidation={(validationCase) => {
                const detail = buildValidationDrilldown(validationCase);
                if (detail) setActiveCase(detail);
              }}
              onOpenEntity={(entityId) => {
                void openEntityCase(entityId);
              }}
              resolveLeadState={(entityId) => ({
                canOpen: Boolean(drilldownIndex.get(entityId) || watchlistCaseIndex.get(entityId)?.case_file),
                loading: loadingCaseId === entityId,
              })}
            />
          ))}
        </div>
      </section>

      <section className={styles.footerNote}>
        <p>
          {isLibraryMode
            ? "Esta biblioteca existe para contrastar, citar y revisar el material que ya alcanzó un cierre público más alto."
            : "La portada ahora sirve para cazar nuevas pistas. Los casos ya corroborados siguen existiendo, pero quedaron relegados a la biblioteca para que no tapen los hallazgos nuevos."}
        </p>
      </section>

      <SavedCaseModal detail={activeCase} onClose={() => setActiveCase(null)} />
    </div>
  );
}
