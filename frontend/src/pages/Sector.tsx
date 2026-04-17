import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";

import { formatSignalLabel } from "@/lib/evidence";
import {
  loadMaterializedResultsPack,
  type MaterializedResultsPack,
} from "@/lib/materialized";

import styles from "./Sector.module.css";

function matchesSector(category: string, tags: string[], sectorId: string) {
  const needle = sectorId.toLowerCase();
  return [category, ...tags].some((value) => value.toLowerCase() === needle);
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
    () => (pack?.investigations ?? []).filter((investigation) => (
      matchesSector(investigation.category, investigation.tags, sectorId)
    )),
    [pack, sectorId],
  );

  return (
    <main className={styles.sectorPage}>
      <span className={styles.eyebrow}>Sector</span>
      <h1 className={styles.title}>{formatSignalLabel(sectorId)}</h1>
      <p className={styles.summary}>
        Casos y hallazgos filtrados por la misma señal sectorial usada en la portada pública.
      </p>
      <div className={styles.grid}>
        {investigations.map((investigation) => (
          <article key={investigation.slug} className={styles.item}>
            <Link to={`/casos/${investigation.slug}`}>{investigation.title}</Link>
            <p>{investigation.summary}</p>
          </article>
        ))}
      </div>
    </main>
  );
}
