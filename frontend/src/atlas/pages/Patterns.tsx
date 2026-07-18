import {
  ArrowRight,
  BookOpen,
  Check,
  ChevronDown,
  GitBranch,
  Search,
  ShieldCheck,
} from "lucide-react";
import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { Link } from "react-router";

import { DataStatus, EmptyState, Pill, Rule, SeverityBadge } from "../components/ui";
import type { Severity } from "../data/prototype";
import { formatNumber } from "../lib/format";
import { useAtlasOverview, type PatternSummary } from "../lib/useAtlasData";

const SEVERITY_FILTERS: ("all" | Severity)[] = ["all", "low", "medium", "high", "critical"];
const INITIAL_VISIBLE_PATTERNS = 8;

type PatternView = "explore" | "catalog";
type PatternFamilyId = "procurement" | "sanctions" | "integrity" | "projects" | "assets" | "cross" | "other";

interface PatternFamily {
  id: PatternFamilyId;
  label: string;
  description: string;
  categories: string[];
  accent: string;
}

const PATTERN_FAMILIES: PatternFamily[] = [
  {
    id: "procurement",
    label: "Contratación pública",
    description: "Competencia, adjudicación, modificaciones y concentración contractual.",
    categories: ["procurement", "ordering", "services"],
    accent: "var(--coacc-ochre)",
  },
  {
    id: "sanctions",
    label: "Antecedentes y sanciones",
    description: "Cruces de proveedores y personas con registros disciplinarios o judiciales.",
    categories: ["sanctions", "judicial"],
    accent: "var(--coacc-coral)",
  },
  {
    id: "integrity",
    label: "Relaciones e integridad",
    description: "Conflictos, vínculos políticos, gobierno corporativo y elecciones.",
    categories: ["conflict", "governance", "election"],
    accent: "var(--coacc-iris)",
  },
  {
    id: "projects",
    label: "Proyectos y regalías",
    description: "Ejecución, interventoría y consistencia documental de proyectos públicos.",
    categories: ["projects"],
    accent: "var(--coacc-lime)",
  },
  {
    id: "assets",
    label: "Bienes, ambiente y energía",
    description: "Cruces con propiedad, licencias ambientales y registros energéticos.",
    categories: ["property", "environment", "energy"],
    accent: "var(--coacc-cyan)",
  },
  {
    id: "cross",
    label: "Cruces entre fuentes",
    description: "Coincidencias que aparecen al conectar distintas fuentes públicas.",
    categories: ["cross_sector", "cross_detector"],
    accent: "var(--coacc-moss)",
  },
  {
    id: "other",
    label: "Otros controles",
    description: "Otras definiciones documentales incluidas en el registro público.",
    categories: [],
    accent: "var(--coacc-fg-2)",
  },
];

const SEVERITY_RANK: Record<Severity, number> = {
  low: 0,
  medium: 1,
  high: 2,
  critical: 3,
};

function displaySignalId(signalId: string): string {
  return signalId.replace(/_review_only$/, "");
}

function familyForCategory(category: string): PatternFamily {
  return PATTERN_FAMILIES.find((family) => family.categories.includes(category))
    ?? PATTERN_FAMILIES[PATTERN_FAMILIES.length - 1]!;
}

function strongestSeverity(patterns: PatternSummary[]): Severity {
  return patterns.reduce<Severity>((current, pattern) => (
    SEVERITY_RANK[pattern.severity] > SEVERITY_RANK[current] ? pattern.severity : current
  ), "low");
}

function matchesCatalogSearch(pattern: PatternSummary, query: string): boolean {
  if (!query) return true;
  const searchable = [
    pattern.id,
    pattern.title,
    pattern.desc,
    pattern.category,
    ...pattern.signalIds,
    ...pattern.sourcesRequired,
  ].join(" ").toLocaleLowerCase("es-CO");
  return searchable.includes(query.toLocaleLowerCase("es-CO"));
}

export function PatternsPage() {
  const { data, status } = useAtlasOverview();
  const [view, setView] = useState<PatternView>("explore");
  const [selectedFamily, setSelectedFamily] = useState<PatternFamilyId | null>(null);
  const [visibleLimit, setVisibleLimit] = useState(INITIAL_VISIBLE_PATTERNS);
  const [severity, setSeverity] = useState<"all" | Severity>("all");
  const [query, setQuery] = useState("");

  const patternsWithCases = useMemo(() => (
    data.patterns
      .filter((pattern) => pattern.materialized && pattern.hits > 0)
      .sort((a, b) => b.hits - a.hits || a.title.localeCompare(b.title, "es-CO"))
  ), [data.patterns]);

  const familySummaries = useMemo(() => PATTERN_FAMILIES.map((family) => {
    const patterns = patternsWithCases.filter((pattern) => familyForCategory(pattern.category).id === family.id);
    return {
      ...family,
      patterns,
      hits: patterns.reduce((sum, pattern) => sum + pattern.hits, 0),
      severity: strongestSeverity(patterns),
    };
  }).filter((family) => family.patterns.length > 0), [patternsWithCases]);

  const exploredPatterns = useMemo(() => (
    selectedFamily
      ? patternsWithCases.filter((pattern) => familyForCategory(pattern.category).id === selectedFamily)
      : patternsWithCases
  ), [patternsWithCases, selectedFamily]);

  const catalogPatterns = useMemo(() => data.patterns
    .filter((pattern) => severity === "all" || pattern.severity === severity)
    .filter((pattern) => matchesCatalogSearch(pattern, query.trim()))
    .sort((a, b) => b.hits - a.hits || a.title.localeCompare(b.title, "es-CO")), [data.patterns, query, severity]);

  useEffect(() => {
    setVisibleLimit(INITIAL_VISIBLE_PATTERNS);
  }, [selectedFamily]);

  const selectedFamilySummary = familySummaries.find((family) => family.id === selectedFamily);
  const materializedCount = data.patterns.filter((pattern) => pattern.materialized).length;
  const totalHits = data.patterns.reduce((sum, pattern) => sum + pattern.hits, 0);

  return (
    <main className="co-container co-patterns">
      <header className="co-workspace-head">
        <div>
          <Rule accent>Workspace · Patrones</Rule>
          <h1>Patrones de riesgo</h1>
          <p>
            Explora coincidencias documentales por tema y abre los casos con su evidencia.
            No constituyen una acusación.
          </p>
        </div>
        <div className="co-action-row">
          <DataStatus status={status} />
          <Pill tone="accent">
            <ShieldCheck size={13} />
            evidencia trazable
          </Pill>
        </div>
      </header>

      <div className="co-pattern-summary" aria-label="Resumen de patrones">
        <span><strong>{formatNumber(totalHits)}</strong> coincidencias documentales</span>
        <span><strong>{formatNumber(patternsWithCases.length)}</strong> patrones con casos</span>
        <span><strong>{formatNumber(data.patterns.length)}</strong> definiciones registradas</span>
      </div>

      <div className="co-pattern-view-switch" role="tablist" aria-label="Vista de patrones">
        <button
          aria-selected={view === "explore"}
          className={view === "explore" ? "active" : ""}
          role="tab"
          type="button"
          onClick={() => setView("explore")}
        >
          <ShieldCheck size={16} />
          Explorar detecciones
        </button>
        <button
          aria-selected={view === "catalog"}
          className={view === "catalog" ? "active" : ""}
          role="tab"
          type="button"
          onClick={() => setView("catalog")}
        >
          <BookOpen size={16} />
          Catálogo completo ({formatNumber(data.patterns.length)})
        </button>
      </div>

      {view === "explore" ? (
        <div role="tabpanel" className="co-pattern-explore">
          <section className="co-pattern-section" aria-labelledby="pattern-families-title">
            <div className="co-pattern-section__head">
              <div>
                <Rule>01 · Elige un tema</Rule>
                <h2 id="pattern-families-title">¿Qué quieres revisar?</h2>
                <p>Agrupamos los patrones técnicos en temas fáciles de recorrer.</p>
              </div>
              {selectedFamily ? (
                <button className="co-button co-button--ghost" type="button" onClick={() => setSelectedFamily(null)}>
                  Ver todos los temas
                </button>
              ) : null}
            </div>

            <div className="co-pattern-family-grid">
              {familySummaries.map((family) => {
                const active = selectedFamily === family.id;
                return (
                  <button
                    aria-pressed={active}
                    className={`co-pattern-family${active ? " co-pattern-family--active" : ""}`}
                    key={family.id}
                    style={{ "--pattern-accent": family.accent } as CSSProperties}
                    type="button"
                    onClick={() => setSelectedFamily(active ? null : family.id)}
                  >
                    <span className="co-pattern-family__top">
                      <SeverityBadge severity={family.severity} />
                      {active ? <span className="co-pattern-family__selected"><Check size={14} /> Seleccionado</span> : null}
                    </span>
                    <strong>{family.label}</strong>
                    <span>{family.description}</span>
                    <span className="co-pattern-family__metrics">
                      <b>{formatNumber(family.hits)}</b> coincidencias · {family.patterns.length} patrones
                    </span>
                  </button>
                );
              })}
            </div>
          </section>

          <section className="co-pattern-section" aria-labelledby="patterns-with-cases-title">
            <div className="co-pattern-section__head">
              <div>
                <Rule>02 · Revisa los casos</Rule>
                <h2 id="patterns-with-cases-title">
                  {selectedFamilySummary?.label ?? "Patrones con casos"}
                </h2>
                <p>
                  {selectedFamilySummary
                    ? `${selectedFamilySummary.patterns.length} patrones en este tema, ordenados por número de coincidencias.`
                    : "Los patrones con más coincidencias aparecen primero. Elige un tema para reducir la lista."}
                </p>
              </div>
              <span className="co-pattern-section__count">{exploredPatterns.length} con casos</span>
            </div>

            <div className="co-pattern-list">
              {exploredPatterns.slice(0, visibleLimit).map((pattern) => {
                const family = familyForCategory(pattern.category);
                const primarySignal = pattern.signalIds[0];
                return (
                  <article className="co-pattern-card" key={pattern.id}>
                    <div className="co-pattern-card__content">
                      <div className="co-pattern-card__eyebrow">
                        <SeverityBadge severity={pattern.severity} />
                        <span>{family.label}</span>
                      </div>
                      <h3>{pattern.title}</h3>
                      <p>{pattern.desc || "Definición documental registrada para revisión."}</p>
                    </div>

                    <dl className="co-pattern-card__metrics">
                      <div>
                        <dt>Coincidencias</dt>
                        <dd>{formatNumber(pattern.hits)}</dd>
                      </div>
                      <div>
                        <dt>Confianza</dt>
                        <dd>{pattern.confidence == null ? "—" : `${pattern.confidence.toFixed(1)}%`}</dd>
                      </div>
                    </dl>

                    <div className="co-pattern-card__actions">
                      {primarySignal ? (
                        <Link className="co-button co-button--primary" to={`/app/signals/${displaySignalId(primarySignal)}`}>
                          Ver casos y evidencia
                          <ArrowRight size={15} />
                        </Link>
                      ) : <span className="co-muted">Detalle no disponible</span>}
                      <details>
                        <summary>
                          Detalles técnicos
                          <ChevronDown size={15} />
                        </summary>
                        <div className="co-pattern-card__technical">
                          <span><strong>ID</strong><code>{pattern.id}</code></span>
                          <span><strong>Fuentes</strong>{pattern.sourcesRequired.join(" · ") || "No declaradas"}</span>
                          <span>
                            <strong>Señales</strong>
                            <span className="co-chip-cloud">
                              {pattern.signalIds.map((signalId) => (
                                <Link className="co-mono" key={signalId} to={`/app/signals/${displaySignalId(signalId)}`}>
                                  {displaySignalId(signalId)}
                                </Link>
                              ))}
                            </span>
                          </span>
                        </div>
                      </details>
                    </div>
                  </article>
                );
              })}
            </div>

            {!exploredPatterns.length ? (
              <EmptyState title="Sin patrones con casos" body="Este tema todavía no tiene coincidencias materializadas." />
            ) : null}

            {visibleLimit < exploredPatterns.length ? (
              <button
                className="co-button co-pattern-show-more"
                type="button"
                onClick={() => setVisibleLimit(exploredPatterns.length)}
              >
                Mostrar {exploredPatterns.length - visibleLimit} patrones más
              </button>
            ) : null}
          </section>
        </div>
      ) : (
        <section className="co-pattern-catalog" role="tabpanel" aria-labelledby="pattern-catalog-title">
          <div className="co-pattern-section__head">
            <div>
              <Rule>Auditoría · Registro público</Rule>
              <h2 id="pattern-catalog-title">Catálogo técnico completo</h2>
              <p>Las {data.patterns.length} definiciones permanecen disponibles, incluidas las que todavía no tienen resultados.</p>
            </div>
          </div>

          <div className="co-pattern-catalog__controls">
            <label className="co-pattern-search">
              <Search size={16} />
              <input
                aria-label="Buscar en el catálogo de patrones"
                placeholder="Buscar patrón, señal o fuente"
                type="search"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
            <div className="co-filter-bar" aria-label="Filtrar catálogo por severidad">
              {SEVERITY_FILTERS.map((item) => (
                <button className={severity === item ? "active" : ""} key={item} type="button" onClick={() => setSeverity(item)}>
                  {item === "all" ? "todas" : item}
                </button>
              ))}
              <span>{catalogPatterns.length} de {data.patterns.length}</span>
            </div>
          </div>

          <div className="co-table-wrap">
            <table className="co-table co-pattern-catalog__table">
              <thead>
                <tr>
                  <th>Patrón</th>
                  <th>Riesgo</th>
                  <th>Resultado</th>
                  <th>Señales</th>
                  <th>Fuentes</th>
                </tr>
              </thead>
              <tbody>
                {catalogPatterns.map((pattern) => (
                  <tr className={pattern.materialized ? "" : "co-pattern-row--registered"} key={pattern.id}>
                    <td data-label="Patrón">
                      <strong>{pattern.title}</strong>
                      <span className="co-row-sub">{pattern.desc || "Sin descripción"}</span>
                      <span className="co-row-sub co-mono">{pattern.id}</span>
                    </td>
                    <td data-label="Riesgo">
                      <SeverityBadge severity={pattern.severity} />
                      <span className="co-row-sub">{pattern.category}</span>
                    </td>
                    <td data-label="Resultado">
                      <strong className="co-mono">{formatNumber(pattern.hits)} hits</strong>
                      <span className="co-row-sub">
                        confianza {pattern.confidence == null ? "—" : `${pattern.confidence.toFixed(1)}%`}
                      </span>
                      <Pill tone={pattern.materialized ? "moss" : "neutral"}>
                        {pattern.materialized ? "materialized" : "registered_only"}
                      </Pill>
                    </td>
                    <td data-label="Señales">
                      <span className="co-chip-cloud">
                        {pattern.signalIds.map((signalId) => (
                          <Link className="co-mono" key={signalId} to={`/app/signals/${displaySignalId(signalId)}`}>
                            {displaySignalId(signalId)}
                          </Link>
                        ))}
                      </span>
                    </td>
                    <td data-label="Fuentes">
                      <span className="co-mono">{pattern.sourcesRequired.join(" · ") || "no declarada"}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!catalogPatterns.length ? (
              <EmptyState title="Sin resultados" body="No hay patrones para la búsqueda y severidad seleccionadas." />
            ) : null}
          </div>
        </section>
      )}

      <div className="co-inline-metrics co-pattern-footer-metrics">
        <span><GitBranch size={14} /> {formatNumber(data.patterns.length)} patrones registrados</span>
        <span>{formatNumber(materializedCount)} materializados</span>
        <span>{formatNumber(data.signals.length)} señales en catálogo</span>
      </div>
    </main>
  );
}
