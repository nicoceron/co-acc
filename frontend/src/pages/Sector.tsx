import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";

import { SectorBreakdown } from "@/components/charts/SectorBreakdown";
import { SeverityDonut, type SeverityDatum } from "@/components/charts/SeverityDonut";
import { ChoroplethMunicipal } from "@/components/maps/ChoroplethMunicipal";
import { HeatmapConcentration } from "@/components/maps/HeatmapConcentration";
import { CaseDataTable, type CaseTableRow } from "@/components/tables/CaseDataTable";
import { formatSignalLabel } from "@/lib/evidence";
import {
  loadMaterializedResultsPack,
  type MaterializedInvestigation,
  type MaterializedResultsPack,
} from "@/lib/materialized";

import styles from "./Sector.module.css";

function matchesSector(
  investigation: MaterializedInvestigation,
  sectorId: string,
): boolean {
  const needle = sectorId.toLowerCase();
  return [investigation.category, ...investigation.tags].some(
    (value) => value.toLowerCase() === needle,
  );
}

function severityFromEvidence(evidence: MaterializedInvestigation["evidence"]): string {
  const severity = evidence
    .map((entry) => entry.label.toLowerCase())
    .find((label) => ["critical", "high", "medium", "low"].includes(label));
  return severity ?? "medium";
}

export function Sector() {
  const { sectorId = "" } = useParams();
  const [pack, setPack] = useState<MaterializedResultsPack | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    loadMaterializedResultsPack(controller.signal).then(setPack).catch(() => {});
    return () => controller.abort();
  }, []);

  const investigations = useMemo(
    () => (pack?.investigations ?? []).filter((inv) => matchesSector(inv, sectorId)),
    [pack, sectorId],
  );

  const territorialHits = useMemo(() => {
    const all = pack?.territorial_hits ?? [];
    const scoped = all.filter((hit) =>
      hit.sector ? hit.sector.toLowerCase() === sectorId.toLowerCase() : true,
    );
    return scoped.length > 0 ? scoped : all;
  }, [pack, sectorId]);

  const severityData: SeverityDatum[] = useMemo(() => {
    const buckets: Record<string, number> = {
      critical: 0,
      high: 0,
      medium: 0,
      low: 0,
    };
    investigations.forEach((inv) => {
      const sev = severityFromEvidence(inv.evidence);
      buckets[sev] = (buckets[sev] ?? 0) + 1;
    });
    return Object.entries(buckets)
      .filter(([, hits]) => hits > 0)
      .map(([severity, hits]) => ({ severity, hits }));
  }, [investigations]);

  const sectorBreakdown = useMemo(
    () =>
      (pack?.practice_summary ?? [])
        .slice(0, 8)
        .map((entry) => ({ sector: entry.label, hits: entry.count })),
    [pack],
  );

  const tableRows: CaseTableRow[] = useMemo(
    () =>
      investigations.slice(0, 12).map((inv) => ({
        title: inv.title,
        sector: inv.category,
        severity: severityFromEvidence(inv.evidence),
        hits: inv.evidence.length,
      })),
    [investigations],
  );

  const sectorLabel = formatSignalLabel(sectorId);

  return (
    <main className={styles.sectorPage}>
      <span className={styles.eyebrow}>Sector</span>
      <h1 className={styles.title}>{sectorLabel}</h1>
      <p className={styles.summary}>
        Casos y señales agregadas para el sector <strong>{sectorLabel}</strong>. Vista territorial,
        severidad, y ranking derivados del archivo público.
      </p>

      <section className={styles.stack}>
        <header className={styles.stackHead}>
          <span className={styles.stackEyebrow}>Vista territorial</span>
          <h2>Concentración por municipio</h2>
        </header>
        {territorialHits.length > 0 ? (
          <>
            <div className={styles.mapShell}>
              <ChoroplethMunicipal data={territorialHits} />
            </div>
            <HeatmapConcentration data={territorialHits} />
          </>
        ) : (
          <p className={styles.empty}>Sin hits territoriales para este sector todavía.</p>
        )}
      </section>

      <section className={styles.split}>
        <div className={styles.stack}>
          <header className={styles.stackHead}>
            <span className={styles.stackEyebrow}>Severidad</span>
            <h2>Distribución de hallazgos</h2>
          </header>
          {severityData.length > 0 ? (
            <SeverityDonut data={severityData} />
          ) : (
            <p className={styles.empty}>Aún no hay hallazgos públicos para graficar severidad.</p>
          )}
        </div>
        <div className={styles.stack}>
          <header className={styles.stackHead}>
            <span className={styles.stackEyebrow}>Conteo por sector</span>
            <h2>Comparativa nacional</h2>
          </header>
          {sectorBreakdown.length > 0 ? (
            <SectorBreakdown data={sectorBreakdown} />
          ) : (
            <p className={styles.empty}>Sin datos agregados por sector.</p>
          )}
        </div>
      </section>

      <section className={styles.stack}>
        <header className={styles.stackHead}>
          <span className={styles.stackEyebrow}>Casos</span>
          <h2>Investigaciones recientes</h2>
        </header>
        {tableRows.length > 0 ? (
          <div className={styles.tableShell}>
            <CaseDataTable rows={tableRows} />
          </div>
        ) : (
          <p className={styles.empty}>Sin casos activos en este sector.</p>
        )}
        <div className={styles.grid}>
          {investigations.map((investigation) => (
            <article key={investigation.slug} className={styles.item}>
              <Link to={`/casos/${investigation.slug}`}>{investigation.title}</Link>
              <p>{investigation.summary}</p>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
