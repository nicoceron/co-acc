import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router";

import { ChoroplethMunicipal } from "@/components/maps/ChoroplethMunicipal";
import { formatSignalLabel } from "@/lib/evidence";
import {
  loadMaterializedResultsPack,
  type MaterializedInvestigation,
  type MaterializedTerritorialHit,
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

const DATA_SOURCES = [
  "SECOP Integrado",
  "SECOP Sanciones",
  "SIGEP",
  "Ley 2013",
  "SGR",
  "Cuentas Claras",
  "MEN",
  "REPS Salud",
];

function sectorKey(value: string) {
  return value.trim().toLowerCase();
}

function investigationMatchesSector(investigation: MaterializedInvestigation, sector: string | null) {
  if (!sector) return true;
  const selected = sectorKey(sector);
  return [investigation.category, ...investigation.tags].some((value) => sectorKey(value) === selected);
}

function SectorChips({
  sectors,
  selected,
  onSelect,
}: {
  sectors: string[];
  selected: string | null;
  onSelect: (sector: string | null) => void;
}) {
  if (sectors.length === 0) return null;
  return (
    <div className={styles.sectorChips} aria-label="Filtrar por sector">
      <button
        type="button"
        className={`${styles.sectorChip} ${selected === null ? styles.sectorChipActive : ""}`}
        onClick={() => onSelect(null)}
      >
        Todos
      </button>
      {sectors.map((sector) => (
        <button
          key={sector}
          type="button"
          className={`${styles.sectorChip} ${selected === sector ? styles.sectorChipActive : ""}`}
          onClick={() => onSelect(sector)}
        >
          {formatSignalLabel(sector)}
        </button>
      ))}
    </div>
  );
}

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

  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);
  const [selectedSector, setSelectedSector] = useState<string | null>(null);
  const [municipalityFilter, setMunicipalityFilter] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    loadMaterializedResultsPack(controller.signal).then(setPack).catch(() => {});
    return () => controller.abort();
  }, []);

  const sectors = useMemo(() => {
    if (pack?.active_sectors?.length) return pack.active_sectors;
    const derived = new Set<string>();
    for (const investigation of pack?.investigations ?? []) {
      if (investigation.category) derived.add(investigation.category);
      for (const tag of investigation.tags) derived.add(tag);
    }
    return Array.from(derived).slice(0, 8);
  }, [pack]);
  const territorialHits: MaterializedTerritorialHit[] = useMemo(() => {
    if (pack?.territorial_hits?.length) return pack.territorial_hits;
    return (pack?.watchlists.territories ?? []).map((territory) => ({
      divipola: territory.territory_id,
      municipality: territory.municipality ?? territory.territory_name,
      department: territory.department,
      sector: "procurement",
      hits: Math.max(1, Math.round(territory.suspicion_score)),
    }));
  }, [pack]);
  const filteredTerritorialHits = useMemo(
    () => territorialHits.filter((row) => {
      const sectorOk = !selectedSector || !row.sector || sectorKey(row.sector) === sectorKey(selectedSector);
      const municipalityOk = !municipalityFilter
        || row.divipola === municipalityFilter
        || row.municipality === municipalityFilter;
      return sectorOk && municipalityOk;
    }),
    [municipalityFilter, selectedSector, territorialHits],
  );
  const investigations = useMemo(
    () => (pack?.investigations ?? []).filter((investigation) => (
      investigationMatchesSector(investigation, selectedSector)
    )),
    [pack, selectedSector],
  );
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
  const heroHighlights = freshInvestigations.slice(0, 3);

  return (
    <>
      <section className={styles.hero}>
        <div className={styles.heroContent}>
          <div className={styles.heroLeft}>
            <span className={styles.badge}>Investigación pública con datos oficiales de Colombia</span>
            <h1 className={styles.title}>Hallazgos propios, no archivo de escándalos.</h1>
            <p className={styles.subtitle}>
              Cruzamos contratación, sanciones, cargos públicos y documentos oficiales para publicar pistas nuevas
              sobre corrupción en Colombia antes de que se vuelvan lugar común.
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

          <aside className={styles.heroLedger}>
            <p className={styles.ledgerEyebrow}>Radar inmediato</p>
            <ol className={styles.ledgerList}>
              {heroHighlights.length > 0 ? heroHighlights.map((investigation, index) => (
                <li key={investigation.slug} className={styles.ledgerItem}>
                  <span className={styles.ledgerIndex}>{String(index + 1).padStart(2, "0")}</span>
                  <div className={styles.ledgerBody}>
                    <Link to={`/casos/${investigation.slug}`} className={styles.ledgerLink}>
                      {investigation.title}
                    </Link>
                    <p className={styles.ledgerMeta}>
                      {investigation.subject_name}
                      {investigation.tags[0] ? ` · ${formatSignalLabel(investigation.tags[0])}` : ""}
                    </p>
                  </div>
                </li>
              )) : (
                <li className={styles.ledgerEmpty}>
                  La próxima pista nueva con suficiente soporte público aparecerá aquí primero.
                </li>
              )}
            </ol>
          </aside>
        </div>
      </section>

      <section className={styles.sectorAtlas}>
        <div className={styles.sectorAtlasInner}>
          <div className={styles.sectionHead}>
            <div>
              <span className={styles.sectionLabel}>Territorio</span>
              <h2 className={styles.sectionHeading}>Lectura por sector y municipio.</h2>
            </div>
            {municipalityFilter ? (
              <button
                type="button"
                className={styles.inlineAction}
                onClick={() => setMunicipalityFilter(null)}
              >
                Limpiar municipio
              </button>
            ) : null}
          </div>
          <SectorChips sectors={sectors} selected={selectedSector} onSelect={setSelectedSector} />
          <div className={styles.sectorMapShell}>
            <ChoroplethMunicipal
              data={filteredTerritorialHits}
              onMunicipalityClick={(divipola) => setMunicipalityFilter(divipola)}
            />
          </div>
        </div>
      </section>

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
          <h2 className={styles.sectionHeading}>Cómo pasamos de registros oficiales a casos.</h2>
          <p className={styles.methodLead}>
            Cruzamos contratación, cargos públicos, sanciones, financiación política y expedientes documentales.
            Después agrupamos todo por práctica para que la lectura empiece en el hallazgo y no en la tabla.
          </p>
        </div>
      </section>

      <section className={styles.sources}>
        <div ref={sourcesRef} className={`${styles.sourcesInner} ${styles.reveal}`}>
          <span className={styles.sectionLabel}>Fuentes</span>
          <h2 className={styles.sectionHeading}>Base pública activa.</h2>
          <div className={styles.sourceLine}>
            {DATA_SOURCES.map((source) => (
              <span key={source} className={styles.sourcePill}>{source}</span>
            ))}
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
            Investigación pública con datos oficiales de Colombia: {DATA_SOURCES.join(", ")}.
          </p>
        </div>
      </footer>
    </>
  );
}
