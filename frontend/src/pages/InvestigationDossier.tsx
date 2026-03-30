import { useEffect, useMemo, useState } from "react";
import { ArrowLeft, BadgeCheck, BookOpenText, FileSearch, Link2, Network, ScrollText } from "lucide-react";
import { Link, useLocation, useParams } from "react-router";

import { GraphCanvas } from "@/components/graph/GraphCanvas";
import { formatPropertyLabel, humanizeIdentifier } from "@/lib/display";
import { buildConnectionTraces, formatSignalLabel } from "@/lib/evidence";
import { loadMaterializedResultsPack, type MaterializedInvestigation } from "@/lib/materialized";
import {
  buildInvestigationBasis,
  getInvestigationConfidenceBadge,
  getInvestigationPriorityBadge,
  humanizePublicText,
  isCorroboratedInvestigation,
  QUICK_GLOSSARY,
  type ReviewBadge,
} from "@/lib/review";

import styles from "./InvestigationDossier.module.css";

function statusLabel(investigation: MaterializedInvestigation): string {
  return isCorroboratedInvestigation(investigation) ? "Verificado" : "Alerta";
}

const CATEGORY_TITLES: Record<string, string> = {
  elefante_blanco: "Elefante blanco / obra trabada",
  microdesfalco_contable: "Microdesfalco contable",
  vendedor_objetos_robados: "Vendedor de objetos robados",
  proveedor_sancionado: "Proveedor sancionado",
  captura_contractual: "Proveedor con vínculo oficial",
  riesgo_politico_contractual: "Riesgo político-contractual",
  supervision: "Supervisión e interventoría",
  captura_educativa: "Captura educativa",
  direccionamiento_contractual: "Direccionamiento contractual",
  dadivas_intermediacion: "Dádivas e intermediación política",
  conflictos_societarios: "Conflictos y referencias societarias",
  capacidad_financiera: "Capacidad financiera insuficiente",
};

function categoryTitle(category: string): string {
  return CATEGORY_TITLES[category] ?? categoryLabel(category);
}

function collectionMeta(
  pathname: string,
  investigation?: MaterializedInvestigation | null,
): { href: string; label: string } {
  const isLibraryPath = pathname.startsWith("/biblioteca") || pathname.startsWith("/investigations");
  const root = isLibraryPath ? "/biblioteca" : "/casos";
  if (investigation?.category) {
    return {
      href: `${root}/modalidad/${investigation.category}`,
      label: `Volver a ${categoryTitle(investigation.category)}`,
    };
  }
  return {
    href: root,
    label: isLibraryPath ? "Volver a biblioteca" : "Volver a casos",
  };
}

function categoryLabel(category: string): string {
  const readable = category.replaceAll("_", " ");
  return readable.charAt(0).toUpperCase() + readable.slice(1);
}

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

function badgeToneClass(tone: ReviewBadge["tone"]): string {
  if (tone === "high") return styles.reviewHigh ?? "";
  if (tone === "medium") return styles.reviewMedium ?? "";
  return styles.reviewInitial ?? "";
}

function splitBoardItem(item: string): { label: string | null; body: string } {
  const normalized = humanizePublicText(item);
  const parts = normalized.split("·");
  if (parts.length < 2) {
    return { label: null, body: normalized.trim() };
  }
  return {
    label: humanizePublicText(parts[0]?.trim() || null) || null,
    body: parts.slice(1).join("·").trim(),
  };
}

export function InvestigationDossier() {
  const { slug } = useParams<{ slug: string }>();
  const location = useLocation();
  const [investigation, setInvestigation] = useState<MaterializedInvestigation | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [layoutMode, setLayoutMode] = useState<"force" | "hierarchy">("force");

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    loadMaterializedResultsPack(controller.signal)
      .then((pack) => {
        const found = (pack.investigations ?? []).find((item) => item.slug === slug) ?? null;
        setInvestigation(found);
        setError(found ? null : "No encontramos este dossier en el lote publicado.");
      })
      .catch((loadError) => {
        setError(loadError instanceof Error ? loadError.message : "No fue posible cargar el dossier.");
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [slug]);

  useEffect(() => {
    if (!investigation?.graph) {
      setSelectedNodeId(null);
      return;
    }
    setSelectedNodeId(investigation.graph.center_id);
    setHoveredNodeId(null);
    setLayoutMode("force");
  }, [investigation]);

  const graph = investigation?.graph ?? null;
  const enabledTypes = useMemo(
    () => new Set((graph?.nodes ?? []).map((node) => node.type)),
    [graph],
  );
  const enabledRelTypes = useMemo(
    () => new Set((graph?.edges ?? []).map((edge) => edge.type)),
    [graph],
  );
  const selectedNode = useMemo(() => {
    if (!graph) return null;
    return graph.nodes.find((node) => node.id === (selectedNodeId ?? graph.center_id)) ?? null;
  }, [graph, selectedNodeId]);
  const selectedNodeIds = useMemo(
    () => new Set(selectedNodeId ? [selectedNodeId] : []),
    [selectedNodeId],
  );
  const traces = useMemo(
    () => buildConnectionTraces(graph, selectedNode?.id ?? graph?.center_id),
    [graph, selectedNode?.id],
  );
  const priority = useMemo(
    () => (investigation ? getInvestigationPriorityBadge(investigation) : null),
    [investigation],
  );
  const confidence = useMemo(
    () => (investigation ? getInvestigationConfidenceBadge(investigation) : null),
    [investigation],
  );
  const basis = useMemo(
    () => (investigation ? buildInvestigationBasis(investigation) : null),
    [investigation],
  );
  const keyActors = useMemo(() => {
    if (!graph) return [];
    return graph.nodes
      .filter((node) => node.id !== graph.center_id && node.type !== "education")
      .sort((a, b) => {
        const aScore = Number(a.type === "person") + Number(a.type === "company") + Number(Boolean(a.document_id));
        const bScore = Number(b.type === "person") + Number(b.type === "company") + Number(Boolean(b.document_id));
        return bScore - aScore || a.label.localeCompare(b.label, "es");
      })
      .slice(0, 10);
  }, [graph]);
  const backNavigation = useMemo(
    () => collectionMeta(location.pathname, investigation),
    [investigation, location.pathname],
  );

  if (loading) {
    return (
      <div className={styles.stateWrap}>
        <p className={styles.stateEyebrow}>Dossier público</p>
        <h1 className={styles.stateTitle}>Cargando investigación…</h1>
      </div>
    );
  }

  if (!investigation) {
    return (
      <div className={styles.stateWrap}>
        <p className={styles.stateEyebrow}>Dossier no disponible</p>
        <h1 className={styles.stateTitle}>No encontramos esta investigación.</h1>
        <p className={styles.stateText}>{error}</p>
        <Link to={backNavigation.href} className={styles.backLink}>
          <ArrowLeft size={16} />
          {backNavigation.label}
        </Link>
      </div>
    );
  }

  return (
    <div className={styles.page}>
      <div className={styles.frame}>
        <div className={styles.topBar}>
          <Link to={backNavigation.href} className={styles.backLink}>
            <ArrowLeft size={16} />
            {backNavigation.label}
          </Link>
          <span className={styles.statusPill}>{statusLabel(investigation)}</span>
        </div>

        <header className={styles.hero}>
          <div className={styles.heroMain}>
            <p className={styles.eyebrow}>Dossier público</p>
            <h1 className={styles.title}>{investigation.title}</h1>
            <p className={styles.subjectLine}>
              {investigation.subject_name}
              {investigation.subject_ref ? ` · ${investigation.subject_ref}` : ""}
            </p>
            <p className={styles.summary}>{humanizePublicText(investigation.summary)}</p>
            {investigation.why_it_matters && (
              <div className={styles.whyBlock}>
                <strong>Por qué vale la pena mirar esto</strong>
                <p>{humanizePublicText(investigation.why_it_matters)}</p>
              </div>
            )}
            <div className={styles.reviewRow}>
              {priority ? (
                <strong className={`${styles.reviewBadge} ${badgeToneClass(priority.tone)}`}>{priority.label}</strong>
              ) : null}
              {confidence ? (
                <strong className={`${styles.reviewBadge} ${badgeToneClass(confidence.tone)}`}>{confidence.label}</strong>
              ) : null}
            </div>
            <p className={styles.reviewReason}>{basis}</p>
            <div className={styles.tagRow}>
              <span className={styles.metaPill}>{categoryLabel(investigation.category)}</span>
              {investigation.tags.slice(0, 4).map((tag) => (
                <span key={`${investigation.slug}-${tag}`} className={styles.tagChip}>
                  {formatSignalLabel(tag)}
                </span>
              ))}
            </div>
          </div>

          <aside className={styles.heroSide}>
            <div className={styles.statCard}>
              <span>hallazgos explicados</span>
              <strong>{investigation.findings.length}</strong>
            </div>
            <div className={styles.statCard}>
              <span>datos y documentos</span>
              <strong>{investigation.evidence.length}</strong>
            </div>
            <div className={styles.statCard}>
              <span>fuentes públicas</span>
              <strong>{investigation.public_sources.length}</strong>
            </div>
            {graph && (
              <div className={styles.statCard}>
                <span>red guardada</span>
                <strong>{graph.nodes.length} nodos</strong>
              </div>
            )}
          </aside>
        </header>

        <section className={styles.storyCard}>
          <div className={styles.sectionHead}>
            <ScrollText size={16} />
            <span>Qué sabemos</span>
          </div>
          <ol className={styles.findingsList}>
            {investigation.findings.map((finding) => (
              <li key={`${investigation.slug}-${finding}`}>{humanizePublicText(finding)}</li>
            ))}
          </ol>
        </section>

        {(investigation.reported_claims?.length ||
          investigation.verified_open_data?.length ||
          investigation.open_questions?.length) && (
          <section className={styles.verificationBoard}>
            {investigation.reported_claims?.length ? (
              <article className={`${styles.verificationCard} ${styles.claimsCard}`}>
                <div className={styles.sectionHead}>
                  <Link2 size={16} />
                  <span>Ya reportado</span>
                </div>
                <p className={styles.boardTitle}>Lo que ya se publicó afuera</p>
                <ul className={styles.boardList}>
                  {investigation.reported_claims.map((item) => {
                    const parsed = splitBoardItem(item);
                    return (
                      <li key={`${investigation.slug}-claim-${item}`} className={styles.boardListItem}>
                        {parsed.label ? <strong className={styles.boardItemLabel}>{parsed.label}</strong> : null}
                        <span>{parsed.body}</span>
                      </li>
                    );
                  })}
                </ul>
                {investigation.reported_sources?.length ? (
                  <div className={styles.boardSources}>
                    <p className={styles.boardSourcesTitle}>Fuentes externas usadas en esta sección</p>
                    <ol className={styles.boardSourceList}>
                      {investigation.reported_sources.map((source) => (
                        <li key={`${investigation.slug}-reported-source-${source}`}>
                          <a href={source} target="_blank" rel="noreferrer">
                            {source.replace(/^https?:\/\//, "")}
                          </a>
                        </li>
                      ))}
                    </ol>
                  </div>
                ) : null}
              </article>
            ) : null}

            {investigation.verified_open_data?.length ? (
              <article className={`${styles.verificationCard} ${styles.verifiedCard}`}>
                <div className={styles.sectionHead}>
                  <BadgeCheck size={16} />
                  <span>Lo que sabemos</span>
                </div>
                <p className={styles.boardTitle}>Confirmado por documentos y datos públicos</p>
                <ul className={styles.boardList}>
                  {investigation.verified_open_data.map((item) => {
                    const parsed = splitBoardItem(item);
                    return (
                      <li key={`${investigation.slug}-verified-${item}`} className={styles.boardListItem}>
                        {parsed.label ? <strong className={styles.boardItemLabel}>{parsed.label}</strong> : null}
                        <span>{parsed.body}</span>
                      </li>
                    );
                  })}
                </ul>
              </article>
            ) : null}

            {investigation.open_questions?.length ? (
              <article className={`${styles.verificationCard} ${styles.openCard}`}>
                <div className={styles.sectionHead}>
                  <BookOpenText size={16} />
                  <span>Lo que falta</span>
                </div>
                <p className={styles.boardTitle}>Huecos documentales que siguen abiertos</p>
                <ul className={styles.boardList}>
                  {investigation.open_questions.map((item) => {
                    const parsed = splitBoardItem(item);
                    return (
                      <li key={`${investigation.slug}-open-${item}`} className={styles.boardListItem}>
                        {parsed.label ? <strong className={styles.boardItemLabel}>{parsed.label}</strong> : null}
                        <span>{parsed.body}</span>
                      </li>
                    );
                  })}
                </ul>
              </article>
            ) : null}
          </section>
        )}

        <section className={styles.railCard}>
          <div className={styles.sectionHead}>
            <BadgeCheck size={16} />
            <span>Los documentos</span>
          </div>
          <div className={styles.evidenceGrid}>
            {investigation.evidence.map((item) => (
              <article key={`${investigation.slug}-${item.label}`} className={styles.evidenceCard}>
                <span>{humanizePublicText(item.label)}</span>
                <strong>{item.value}</strong>
                {item.detail && <small>{humanizePublicText(item.detail)}</small>}
              </article>
            ))}
          </div>
        </section>

        <section className={styles.railCard}>
          <div className={styles.sectionHead}>
            <FileSearch size={16} />
            <span>Actores en esta red</span>
          </div>
          <div className={styles.actorList}>
            {keyActors.map((actor) => (
              <button
                key={`${investigation.slug}-${actor.id}`}
                type="button"
                className={styles.actorChip}
                onClick={() => setSelectedNodeId(actor.id)}
              >
                <span>{actor.label}</span>
                <small>{formatNodeTypeLabel(actor.type)}</small>
              </button>
            ))}
          </div>
        </section>

        <section className={styles.exhibitCard}>
          <div className={styles.sectionHead}>
            <Network size={16} />
            <span>Red de relaciones</span>
          </div>
          <p className={styles.exhibitLead}>
            Primero la historia, luego la red. Aquí puedes seguir actores y conexiones guardadas del caso.
          </p>

          {graph ? (
            <>
              <div className={styles.traceList}>
                {traces.map((trace) => (
                  <button
                    key={trace.id}
                    type="button"
                    className={styles.traceButton}
                    onClick={() => setSelectedNodeId(trace.focusNodeId ?? graph.center_id)}
                  >
                    <strong>{trace.headline}</strong>
                    {trace.detail && <span>{trace.detail}</span>}
                  </button>
                ))}
              </div>

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
            </>
          ) : (
            <div className={styles.emptyState}>Grafo en construcción.</div>
          )}
        </section>

        <section className={styles.inspectorCard}>
          <div className={styles.sectionHead}>
            <Link2 size={16} />
            <span>Actor seleccionado</span>
          </div>

          {selectedNode ? (
            <>
              <h2 className={styles.inspectorTitle}>{selectedNode.label}</h2>
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
            <div className={styles.emptyState}>Selecciona un actor o una conexión destacada.</div>
          )}
        </section>

        {investigation.public_sources.length > 0 && (
          <section className={styles.sourcesCard}>
            <div className={styles.sectionHead}>
              <BookOpenText size={16} />
              <span>Fuentes públicas</span>
            </div>
            <ol className={styles.sourceList}>
              {investigation.public_sources.map((source) => (
                <li key={`${investigation.slug}-${source}`}>
                  <a href={source} target="_blank" rel="noreferrer">
                    {source.replace(/^https?:\/\//, "")}
                  </a>
                </li>
              ))}
            </ol>
          </section>
        )}

        <section className={styles.glossaryStrip}>
          <div className={styles.sectionHead}>
            <BookOpenText size={16} />
            <span>Glosario</span>
          </div>
          <div className={styles.glossaryGrid}>
            {QUICK_GLOSSARY.map((item) => (
              <article key={`${investigation.slug}-${item.term}`} className={styles.glossaryCard}>
                <strong>{item.term}</strong>
                <p>{item.description}</p>
              </article>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
