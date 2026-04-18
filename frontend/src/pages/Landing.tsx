import { Link } from "react-router";

import styles from "./Landing.module.css";

const CAPABILITIES = [
  {
    title: "Contratación pública",
    meta: "5.6M filas · SECOP I+II+TVEC",
    body: "Todo el registro nacional de contratación — adjudicaciones, modificaciones, suspensiones, ejecución financiera — normalizado en un lago columnar y consultable por entidad, proveedor, región y fecha.",
  },
  {
    title: "Declaraciones y cargos",
    meta: "SIGEP · Ley 2013 · PACO",
    body: "Servidores públicos, posiciones sensibles, declaraciones de bienes y conflictos, sanciones disciplinarias — cruzables contra adjudicaciones contractuales.",
  },
  {
    title: "Regalías y proyectos",
    meta: "SGR · BPIN · DNP",
    body: "Proyectos de inversión territorial con ejecución presupuestal, ligados a municipios, sectores y proveedores ejecutores.",
  },
  {
    title: "Sectoriales PIDA",
    meta: "30 datasets anticorrupción",
    body: "Salud, ambiente, minas, educación, estadísticas — los 30 datasets priorizados en la Hoja de Ruta 2025, con su custodio institucional mapeado.",
  },
];

const STATS = [
  { k: "5.6M", v: "filas en SECOP Integrado" },
  { k: "43", v: "señales de cruce registradas" },
  { k: "112", v: "pipelines colombianos" },
  { k: "30", v: "datasets PIDA mapeados" },
];

export function Landing() {
  return (
    <div className={styles.root}>
      <section className={styles.hero}>
        <div className={styles.heroInner}>
          <span className={styles.kicker}>
            <span className={styles.kickerDot} />
            open graph infrastructure · colombia
          </span>
          <h1 className={styles.heroTitle}>
            El grafo abierto de los datos públicos de Colombia.
          </h1>
          <p className={styles.heroSub}>
            co/acc normaliza los registros oficiales fragmentados de Colombia — contratación,
            cargos, declaraciones, regalías, sanciones — en un lago consultable y un grafo de
            evidencia. Sin puntajes, sin acusaciones: solo el contexto documental cruzado.
          </p>
          <div className={styles.heroActions}>
            <Link to="/casos" className={styles.ctaPrimary}>
              Explorar casos
            </Link>
            <Link to="/app" className={styles.ctaSecondary}>
              Entrar al workspace
            </Link>
          </div>
        </div>

        <dl className={styles.stats}>
          {STATS.map((s) => (
            <div key={s.v} className={styles.stat}>
              <dt className={styles.statKey}>{s.k}</dt>
              <dd className={styles.statValue}>{s.v}</dd>
            </div>
          ))}
        </dl>
      </section>

      <section className={styles.capabilities}>
        <header className={styles.sectionHead}>
          <span className={styles.sectionKicker}>capacidades</span>
          <h2 className={styles.sectionTitle}>Qué contiene el grafo</h2>
        </header>

        <div className={styles.grid}>
          {CAPABILITIES.map((c) => (
            <article key={c.title} className={styles.card}>
              <span className={styles.cardMeta}>{c.meta}</span>
              <h3 className={styles.cardTitle}>{c.title}</h3>
              <p className={styles.cardBody}>{c.body}</p>
            </article>
          ))}
        </div>
      </section>

      <section className={styles.disclaimer}>
        <p>
          <strong>Contexto documental, no acusación.</strong> co/acc reproduce y cruza registros
          públicos oficiales para habilitar escrutinio ciudadano. Las señales son patrones
          estadísticos sobre datos del Estado — no afirmaciones sobre personas específicas.
        </p>
      </section>
    </div>
  );
}
