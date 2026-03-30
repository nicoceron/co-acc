import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router";

import { type StatsResponse, getStats } from "@/api/client";
import { HeroGraph } from "@/components/landing/HeroGraph";
import { NetworkAnimation } from "@/components/landing/NetworkAnimation";
import { StatsBar } from "@/components/landing/StatsBar";
import { formatSignalLabel } from "@/lib/evidence";
import {
  loadMaterializedResultsPack,
  type MaterializedInvestigation,
  type MaterializedResultsPack,
} from "@/lib/materialized";
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
    }, { threshold: 0.15 });

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
  { name: "SECOP Integrado", description: "Contratos, adjudicaciones y trazas de compra pública.", countFn: (s) => s.contract_count },
  { name: "SECOP Sanciones", description: "Sanciones y ventanas donde la contratación siguió viva.", countFn: (s) => s.sanction_count },
  { name: "SIGEP", description: "Servidores públicos y cargos enlazables con contratación.", countFn: (s) => s.person_count },
  { name: "Ley 2013", description: "Activos, intereses y referencias declaradas por funcionarios.", countFn: (s) => s.person_count },
  { name: "SGR", description: "Regalías, gasto y red flags de ejecución territorial.", countFn: (s) => s.finance_count },
  { name: "Cuentas Claras", description: "Financiación política conectada con proveedores y campañas.", countFn: (s) => s.election_count },
  { name: "MEN", description: "Control educativo, matrícula y puentes institucionales.", countFn: (s) => s.education_count },
  { name: "REPS Salud", description: "Prestadores, sanciones y redes del sector salud.", countFn: (s) => s.health_count },
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
      <p className={styles.cardEyebrow}>{isFresh ? "Hallazgo abierto" : "Caso corroborado"}</p>
      <h3>{investigation.title}</h3>
      <p className={styles.cardMeta}>
        {investigation.subject_name}
        {investigation.subject_ref ? ` · ${investigation.subject_ref}` : ""}
      </p>
      <p className={styles.cardSummary}>{investigation.summary}</p>
      <div className={styles.tagRow}>
        {investigation.tags.slice(0, featured ? 4 : 2).map((tag) => (
          <span key={`${investigation.slug}-${tag}`} className={styles.tagChip}>
            {formatSignalLabel(tag)}
          </span>
        ))}
      </div>
      <Link to={`/casos/${investigation.slug}`} className={styles.inlineAction}>
        {isFresh ? "Abrir hallazgo" : "Abrir caso"}
      </Link>
    </article>
  );
}

export function Landing() {
  const findingsRef = useReveal();
  const methodRef = useReveal();
  const sourcesRef = useReveal();

  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);

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
  const supportingInvestigations = freshInvestigations.slice(1, 3);
  const libraryPreview = corroboratedInvestigations.slice(0, 3);

  return (
    <>
      <section className={styles.hero}>
        <NetworkAnimation />

        <div className={styles.heroContent}>
          <div className={styles.heroLeft}>
            <span className={styles.badge}>Investigación pública con datos oficiales de Colombia</span>
            <h1 className={styles.title}>Hallazgos nuevos sobre corrupción en Colombia.</h1>
            <p className={styles.subtitle}>
              Rastreando la contratación pública en Colombia. Cruzamos datos oficiales para detectar redes de
              corrupción, elefantes blancos y captura del Estado.
            </p>
            <div className={styles.heroActions}>
              <Link to="/casos" className={styles.cta}>
                Abrir casos
              </Link>
              <Link to="/biblioteca" className={styles.secondaryCta}>
                Ver verificados
              </Link>
            </div>
            <p className={styles.disclaimer}>
              Hallazgos y casos se apoyan en fuentes públicas. La plataforma muestra señales investigativas, no fallos judiciales.
            </p>
          </div>

          <div className={styles.heroRight}>
            <HeroGraph />
          </div>
        </div>
      </section>

      <StatsBar />

      <section className={styles.findings}>
        <div ref={findingsRef} className={`${styles.findingsInner} ${styles.reveal}`}>
          <div className={styles.sectionHead}>
            <div>
              <span className={styles.sectionLabel}>Abrir primero</span>
              <h2 className={styles.sectionHeading}>Pistas nuestras antes que archivo.</h2>
            </div>
            <Link to="/casos" className={styles.inlineAction}>
              Ver todos los casos
            </Link>
          </div>

          <div className={styles.frontPageGrid}>
            {featuredInvestigation ? (
              <div className={styles.leadStory}>
                <InvestigationFeature investigation={featuredInvestigation} featured />
              </div>
            ) : (
              <article className={`${styles.investigationCard} ${styles.investigationCardFeatured}`}>
                <p className={styles.cardEyebrow}>Hallazgo abierto</p>
                <h3>Todavía no hay una pista nueva con cierre público suficiente para ocupar la portada.</h3>
                <p className={styles.cardSummary}>
                  Cuando aparezca una pista nueva con lectura pública suficiente, entra aquí primero.
                </p>
                <Link to="/casos" className={styles.inlineAction}>
                  Abrir casos
                </Link>
              </article>
            )}

            <div className={styles.sideColumn}>
              {supportingInvestigations.length > 0 ? (
                <div className={styles.supportingStories}>
                  {supportingInvestigations.map((investigation) => (
                    <InvestigationFeature key={investigation.slug} investigation={investigation} />
                  ))}
                </div>
              ) : null}

              {libraryPreview.length > 0 ? (
                <div className={styles.controlPanel}>
                  <div className={styles.controlHead}>
                    <span>Ya verificados</span>
                  </div>
                  <p>Casos cerrados que sirven para contraste y contexto.</p>
                  <div className={styles.supportingStories}>
                    {libraryPreview.slice(0, 2).map((investigation) => (
                      <InvestigationFeature key={investigation.slug} investigation={investigation} />
                    ))}
                  </div>
                  <Link to="/biblioteca" className={styles.inlineAction}>
                    Abrir biblioteca
                  </Link>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </section>

      <section id="metodologia" className={styles.howItWorks}>
        <div ref={methodRef} className={`${styles.howItWorksInner} ${styles.reveal}`}>
          <span className={styles.sectionLabel}>Método</span>
          <h2 className={styles.sectionHeading}>Cómo pasamos de tablas oficiales a casos legibles.</h2>
          <div className={styles.stepsGrid}>
            <div className={styles.step}>
              <span className={styles.stepNumber}>1</span>
              <span className={styles.stepTitle}>Conectamos fuentes</span>
              <span className={styles.stepDesc}>Unimos contratos, personas, empresas, cargos, sanciones y otras capas oficiales.</span>
            </div>
            <div className={styles.step}>
              <span className={styles.stepNumber}>2</span>
              <span className={styles.stepTitle}>Agrupamos por práctica</span>
              <span className={styles.stepDesc}>La salida no es una lista cruda: cada caso cae en una práctica entendible.</span>
            </div>
            <div className={styles.step}>
              <span className={styles.stepNumber}>3</span>
              <span className={styles.stepTitle}>Bajamos a dossier</span>
              <span className={styles.stepDesc}>Cada caso fuerte termina en hallazgos, documentos, fuentes y red de relaciones.</span>
            </div>
          </div>
        </div>
      </section>

      <section className={styles.sources}>
        <div ref={sourcesRef} className={`${styles.sourcesInner} ${styles.reveal}`}>
          <span className={styles.sectionLabel}>Fuentes</span>
          <h2 className={styles.sectionHeading}>Base pública activa.</h2>
          <div className={styles.sourcesGrid}>
            {DATA_SOURCES.map((source) => {
              const count = stats ? source.countFn(stats) : null;
              return (
                <div key={source.name} className={styles.sourceCard}>
                  <div className={styles.sourceHead}>
                    <strong>{source.name}</strong>
                    <span>{count != null ? formatCount(count) : "—"}</span>
                  </div>
                  <p>{source.description}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <footer className={styles.footer}>
        <div className={styles.footerInner}>
          <div className={styles.footerTop}>
            <Link to="/casos" className={styles.footerLink}>Casos</Link>
            <Link to="/biblioteca" className={styles.footerLink}>Biblioteca</Link>
            <a href="#metodologia" className={styles.footerLink}>Metodología</a>
          </div>
          <div className={styles.footerDivider} />
          <span className={styles.footerBrand}>CO·ACC</span>
          <p className={styles.footerDisclaimer}>
            Investigación pública con datos oficiales de Colombia.
          </p>
        </div>
      </footer>
    </>
  );
}
