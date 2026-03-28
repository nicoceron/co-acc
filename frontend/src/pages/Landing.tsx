import { useCallback, useEffect, useMemo, useState } from "react";
import { BadgeCheck, BookOpenText, Link2, Network, ShieldCheck } from "lucide-react";
import { Link } from "react-router";

import { type StatsResponse, getStats } from "@/api/client";
import { formatSignalLabel } from "@/lib/evidence";
import { loadMaterializedResultsPack, type MaterializedInvestigation, type MaterializedResultsPack } from "@/lib/materialized";
import { isCorroboratedInvestigation, isFreshInvestigation } from "@/lib/review";

import styles from "./Landing.module.css";

function useReveal() {
  const setRef = useCallback((node: HTMLElement | null) => {
    if (!node) return;
    const cls = styles.revealed ?? "revealed";
    const prefersReduced = typeof window.matchMedia === "function"
      && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced || typeof IntersectionObserver === "undefined") {
      node.classList.add(cls);
      return;
    }

    const observer = new IntersectionObserver(([entry]) => {
      if (entry?.isIntersecting) {
        node.classList.add(cls);
        observer.disconnect();
      }
    }, { threshold: 0.16 });

    observer.observe(node);
  }, []);

  return setRef;
}

function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(Math.round(n / 100_000) / 10).toFixed(1)}M`;
  if (n >= 1_000) return `${(Math.round(n / 100) / 10).toFixed(1)}K`;
  return String(n);
}

interface SourceDef {
  name: string;
  description: string;
  countFn: (s: StatsResponse) => number | null;
}

const DATA_SOURCES: SourceDef[] = [
  { name: "SECOP Integrado", description: "Contratación pública y trazabilidad de procesos.", countFn: (s) => s.contract_count },
  { name: "SECOP II Procesos", description: "Diseño del proceso, competencia y adjudicación.", countFn: (s) => s.contract_count },
  { name: "SECOP II Contratos", description: "Contratos, adiciones, suspensiones y ejecución.", countFn: (s) => s.contract_count },
  { name: "SECOP Proveedores", description: "Normalización de contratistas y empresas.", countFn: (s) => s.company_count },
  { name: "SECOP Sanciones", description: "Antecedentes sancionatorios y ventanas activas.", countFn: (s) => s.sanction_count },
  { name: "SIGEP", description: "Servidores públicos y ocupación institucional.", countFn: (s) => s.person_count },
  { name: "Puestos Sensibles", description: "Cargos expuestos a mayor riesgo de captura.", countFn: (s) => s.person_count },
  { name: "Activos Ley 2013", description: "Declaraciones patrimoniales y conflictos.", countFn: (s) => s.person_count },
  { name: "Conflictos", description: "Referencias corporativas y conflictos declarados.", countFn: (s) => s.person_count },
  { name: "SGR Gastos", description: "Obras, regalías y posibles elefantes blancos.", countFn: (s) => s.finance_count },
  { name: "REPS Salud", description: "Prestadores de salud y cruces sancionatorios.", countFn: (s) => s.health_count },
  { name: "MEN Matrícula", description: "Instituciones y control educativo enlazable.", countFn: (s) => s.education_count },
  { name: "Cuentas Claras", description: "Financiación política conectada con contratación.", countFn: (s) => s.election_count },
];

function InvestigationFeature({
  investigation,
  featured = false,
}: {
  investigation: MaterializedInvestigation;
  featured?: boolean;
}) {
  const isFresh = isFreshInvestigation(investigation);

  return (
    <article className={`${styles.investigationCard} ${featured ? styles.investigationCardFeatured : ""}`}>
      <p className={styles.cardEyebrow}>{isFresh ? "Pista nueva" : "Caso corroborado"}</p>
      <h3>{investigation.title}</h3>
      <p className={styles.cardMeta}>
        {investigation.subject_name}
        {investigation.subject_ref ? ` · ${investigation.subject_ref}` : ""}
      </p>
      <p className={styles.cardSummary}>{investigation.summary}</p>
      <div className={styles.tagRow}>
        {investigation.tags.slice(0, featured ? 4 : 2).map((tag) => (
          <span key={`${investigation.slug}-${tag}`} className={styles.tagChip}>{formatSignalLabel(tag)}</span>
        ))}
      </div>
      <Link to={`/investigations/${investigation.slug}`} className={styles.inlineAction}>
        {isFresh ? "Abrir pista" : "Abrir caso"}
      </Link>
    </article>
  );
}

export function Landing() {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);

  const proofRef = useReveal();
  const investigationsRef = useReveal();
  const methodologyRef = useReveal();
  const sourcesRef = useReveal();

  useEffect(() => {
    getStats().then(setStats).catch(() => {});
    const controller = new AbortController();
    loadMaterializedResultsPack(controller.signal).then(setPack).catch(() => {});
    return () => controller.abort();
  }, []);

  const investigations = useMemo(() => pack?.investigations ?? [], [pack]);
  const freshInvestigations = useMemo(
    () => investigations.filter((investigation) => isFreshInvestigation(investigation)),
    [investigations],
  );
  const corroboratedInvestigations = useMemo(
    () => investigations.filter((investigation) => isCorroboratedInvestigation(investigation)),
    [investigations],
  );
  const featuredInvestigation = freshInvestigations[0] ?? null;
  const supportingInvestigations = freshInvestigations.slice(1, 4);
  const libraryPreview = corroboratedInvestigations.slice(0, 4);
  const proofCases = useMemo(
    () => (pack?.validation.cases ?? []).filter((item) => item.matched).slice(0, 4),
    [pack],
  );

  return (
    <div className={styles.page}>
      <section className={styles.hero}>
        <div className={styles.heroCopy}>
          <p className={styles.badge}>Investigación pública con fuentes verificables</p>
          <h1 className={styles.title}>
            Primero descubrir. Después corroborar. No al revés.
          </h1>
          <p className={styles.subtitle}>
            CO-ACC cruza contratación, sanciones, cargos públicos, financiación política, educación y otras fuentes
            oficiales para producir hallazgos verificables. La lectura pública ahora separa dos flujos: portada para
            encontrar pistas nuevas y biblioteca aparte para revisar lo ya corroborado.
          </p>
          <div className={styles.heroActions}>
            <Link to="/results" className={styles.primaryCta}>
              Ver pistas nuevas
            </Link>
            <Link to="/investigations" className={styles.secondaryCta}>
              Biblioteca corroborada
            </Link>
          </div>
          <p className={styles.disclaimer}>
            Los hallazgos son señales investigativas basadas en datos públicos. No sustituyen una decisión judicial ni
            atribuyen responsabilidad penal por sí solos.
          </p>
        </div>

        <aside className={styles.heroEvidence}>
          <div className={styles.metricCard}>
            <span>casos corroborados</span>
            <strong>{pack ? String(pack.validation.matched) : "—"}</strong>
          </div>
          <div className={styles.metricCard}>
            <span>contratos visibles</span>
            <strong>{stats ? formatCount(stats.contract_count) : "—"}</strong>
          </div>
          <div className={styles.metricCard}>
            <span>fuentes oficiales activas</span>
            <strong>{pack?.stats.promoted_sources ?? "—"}</strong>
          </div>
          <div className={styles.metricCard}>
            <span>pistas priorizadas</span>
            <strong>{pack ? formatCount(pack.summary.company_watchlist_count + pack.summary.people_watchlist_count) : "—"}</strong>
          </div>
        </aside>
      </section>

      <section ref={investigationsRef} className={`${styles.section} ${styles.reveal}`}>
        <div className={styles.sectionHead}>
          <div>
            <p className={styles.sectionEyebrow}>Portada</p>
            <h2>La portada pública ya no empieza por casos viejos: empieza por hallazgos nuevos.</h2>
          </div>
          <Link to="/results" className={styles.inlineAction}>
            Ir a pistas nuevas
          </Link>
        </div>

        <div className={styles.frontPageGrid}>
          {featuredInvestigation ? (
            <div className={styles.leadStory}>
              <InvestigationFeature investigation={featuredInvestigation} featured />
            </div>
          ) : (
            <article className={`${styles.investigationCard} ${styles.investigationCardFeatured}`}>
              <p className={styles.cardEyebrow}>Portada de descubrimiento</p>
              <h3>Este lote todavía no trae una pista nueva publicada con cierre suficiente.</h3>
              <p className={styles.cardSummary}>
                La portada sigue reservada para descubrimiento. Cuando un hallazgo nuevo pase el umbral mínimo de
                legibilidad pública, aparecerá aquí antes que los casos corroborados.
              </p>
              <Link to="/results" className={styles.inlineAction}>
                Ir a pistas nuevas
              </Link>
            </article>
          )}

          {supportingInvestigations.length > 0 ? (
            <div className={styles.supportingStories}>
              {supportingInvestigations.map((investigation) => (
                <InvestigationFeature key={investigation.slug} investigation={investigation} />
              ))}
            </div>
          ) : null}
        </div>
      </section>

      <section ref={proofRef} className={`${styles.section} ${styles.reveal}`}>
        <div className={styles.sectionHead}>
          <div>
            <p className={styles.sectionEyebrow}>Biblioteca corroborada</p>
            <h2>Lo corroborado queda aparte para contrastar, citar y medir si las pistas nuevas van bien encaminadas.</h2>
          </div>
          <div className={styles.proofBadge}>
            <BadgeCheck size={16} />
            <span>{libraryPreview.length > 0 ? libraryPreview.length : proofCases.length} controles visibles</span>
          </div>
        </div>

        <div className={styles.proofGrid}>
          {libraryPreview.length > 0 ? libraryPreview.map((investigation) => (
            <InvestigationFeature key={investigation.slug} investigation={investigation} />
          )) : proofCases.map((caseItem) => (
            <article key={caseItem.case_id} className={styles.proofCard}>
              <p className={styles.cardEyebrow}>{caseItem.category.replaceAll("_", " ")}</p>
              <h3>{caseItem.title}</h3>
              <p className={styles.cardMeta}>{caseItem.entity_name} · {caseItem.entity_ref}</p>
              <p className={styles.cardSummary}>{caseItem.summary}</p>
              <div className={styles.tagRow}>
                {caseItem.matched_signals.slice(0, 3).map((signal) => (
                  <span key={`${caseItem.case_id}-${signal}`} className={styles.tagChip}>{formatSignalLabel(signal)}</span>
                ))}
              </div>
            </article>
          ))}
        </div>
      </section>

      <section id="metodologia" ref={methodologyRef} className={`${styles.section} ${styles.reveal}`}>
        <div className={styles.sectionHead}>
          <div>
            <p className={styles.sectionEyebrow}>Metodología</p>
            <h2>Cómo se convierte una base pública dispersa en una investigación legible</h2>
          </div>
        </div>

        <div className={styles.methodologyGrid}>
          <article className={styles.methodCard}>
            <Network size={18} />
            <h3>1. Normalización</h3>
            <p>Unificamos cédulas, NIT, BPIN, contratos, convenios, geografía y control fiscal para que el grafo conecte actores que los portales publican por separado.</p>
          </article>
          <article className={styles.methodCard}>
            <Link2 size={18} />
            <h3>2. Cruces y patrones</h3>
            <p>Cruzamos contratación, sanciones, financiación política, educación, cargos públicos y ejecución contractual para detectar patrones que se repiten.</p>
          </article>
          <article className={styles.methodCard}>
            <ShieldCheck size={18} />
            <h3>3. Validación</h3>
            <p>Contrastamos el sistema con casos públicos conocidos para comprobar que las conexiones reproducen hechos verificables antes de publicarlas como evidencia útil.</p>
          </article>
          <article className={styles.methodCard}>
            <BookOpenText size={18} />
            <h3>4. Dossiers</h3>
            <p>El resultado no se deja como lista cruda. Cada caso destacado se sirve como dossier con hallazgos, evidencia, nodos y fuentes públicas citables.</p>
          </article>
        </div>
      </section>

      <section ref={sourcesRef} className={`${styles.section} ${styles.reveal}`}>
        <div className={styles.sectionHead}>
          <div>
            <p className={styles.sectionEyebrow}>Registro de fuentes</p>
            <h2>Fuentes oficiales activas en esta publicación</h2>
          </div>
        </div>

        <div className={styles.sourcesGrid}>
          {DATA_SOURCES.map((source) => {
            const count = stats ? source.countFn(stats) : null;
            return (
              <article key={source.name} className={styles.sourceCard}>
                <div className={styles.sourceHead}>
                  <strong>{source.name}</strong>
                  <span>{count != null ? formatCount(count) : "—"}</span>
                </div>
                <p>{source.description}</p>
              </article>
            );
          })}
        </div>
      </section>
    </div>
  );
}
