import { ArrowRight, Database, FileText, Network, Search } from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router";

import { AtlasMap, BarChart } from "../components/visuals";
import { DataStatus, Eyebrow, Frame, MetricTile, MiniBar, Pill, Rule, Sparkline } from "../components/ui";
import { atlasFixture, type FeedItem } from "../data/prototype";
import { formatNumber, toPercent } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

function FeedTicker({ feed }: { feed: FeedItem[] }) {
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => setIndex((value) => (value + 1) % feed.length), 2400);
    return () => window.clearInterval(timer);
  }, [feed.length]);

  const item = feed[index] ?? feed[0] ?? atlasFixture.feed[0]!;
  return (
    <div className="co-feed-ticker" key={`${item.signal}-${index}`}>
      <span>{item.t}</span>
      <em>{item.signal}</em>
      <strong>{item.entity}</strong>
      <span>{item.note}</span>
    </div>
  );
}

function Pillar({
  icon,
  n,
  title,
  body,
}: {
  icon: ReactNode;
  n: string;
  title: string;
  body: string;
}) {
  return (
    <article className="co-pillar">
      <span>{icon}</span>
      <div>
        <em>{n}</em>
        <h3>{title}</h3>
        <p>{body}</p>
      </div>
    </article>
  );
}

export function Landing() {
  const { data, status } = useAtlasOverview();
  const sectorBars = useMemo(() => {
    return data.sectors
      .map((sector) => ({
        label: sector.label,
        value: sector.hits,
        display: `${formatNumber(sector.hits)} hits`,
        color: sector.accent,
      }))
      .sort((a, b) => b.value - a.value);
  }, [data.sectors]);

  return (
    <main>
      <section className="co-container co-hero">
        <div className="co-hero__copy">
          <Rule accent>Atlas abierto · Colombia · 32 departamentos · 16 fuentes</Rule>
          <h1>
            Los datos publicos de Colombia, cartografiados.
          </h1>
          <p>
            Un atlas vivo de registros oficiales cruzados en un grafo de identidad:
            contratacion, sanciones, declaraciones, regalias y evidencia documental.
          </p>
          <div className="co-action-row">
            <Link className="co-button co-button--primary" to="/app">
              Abrir observatorio
              <ArrowRight size={16} />
            </Link>
            <Link className="co-button" to="/casos">Ver casos</Link>
            <Pill tone="accent">AGPL v3</Pill>
          </div>
          <Frame className="co-live-strip">
            <Pill tone="accent">senal en vivo</Pill>
            <FeedTicker feed={data.feed} />
            <Link className="co-button co-button--ghost" to="/app">ver todo</Link>
          </Frame>
        </div>

        <Frame coord="MAPA · L-01" className="co-hero__map">
          <div className="co-frame-title">
            <div>
              <h2>Hits por departamento</h2>
              <span>ultimas 24h · severidad cartografica</span>
            </div>
            <DataStatus status={status} />
          </div>
          <AtlasMap departamentos={data.departamentos} height={460} />
          <div className="co-legend">
            <span><i className="low" /> bajo</span>
            <span><i className="medium" /> medio</span>
            <span><i className="high" /> alto</span>
            <span><i className="critical" /> critico</span>
          </div>
        </Frame>
      </section>

      <section className="co-container co-metrics-band">
        <Rule>Indicadores del grafo</Rule>
        <div className="co-metrics-grid">
          <MetricTile label="Registros indexados" value={data.metrics.records} sub="+184k esta semana" spark={data.seriesHits ?? atlasFixture.seriesHits} />
          <MetricTile label="Nodos en grafo" value={data.metrics.nodes} sub="personas, empresas, contratos" spark={atlasFixture.seriesHits.map((value) => value * 0.7)} />
          <MetricTile label="Senales activas" value={data.metrics.signals} sub="publicas + reviewer" spark={atlasFixture.seriesHits.map((value, index) => value * 0.5 + index)} />
          <MetricTile label="Fuentes operativas" value={data.metrics.sources} sub="watermark y cobertura" spark={atlasFixture.seriesSourcesOk} />
        </div>
      </section>

      <section className="co-container co-two-col">
        <div>
          <Eyebrow accent>Que es</Eyebrow>
          <h2>Un grafo de evidencia. No un oraculo.</h2>
        </div>
        <div className="co-pillars">
          <Pillar
            icon={<Database size={19} />}
            n="01"
            title="Lago abierto"
            body="Fuentes oficiales normalizadas en un lago columnar particionado por fecha y fuente."
          />
          <Pillar
            icon={<Network size={19} />}
            n="02"
            title="Identidad enlazada"
            body="Personas, empresas, contratos, sanciones y declaraciones unidos por un grafo versionado."
          />
          <Pillar
            icon={<Search size={19} />}
            n="03"
            title="Senales reproducibles"
            body="Patrones materializados con evidencia, score, fuente y fecha de observacion."
          />
          <Pillar
            icon={<FileText size={19} />}
            n="04"
            title="Public safe"
            body="La superficie publica conserva contexto documental y reduce exposicion innecesaria."
          />
        </div>
      </section>

      <section className="co-container co-two-col">
        <div>
          <Eyebrow accent>Sectores</Eyebrow>
          <h2>Cobertura por sector.</h2>
          <p className="co-muted">Distribucion de senales materializadas por sector y entidades asociadas.</p>
          <Link className="co-button" to="/sectores">
            Explorar sectores
            <ArrowRight size={16} />
          </Link>
        </div>
        <Frame coord="SECTORES · S-01">
          <BarChart items={sectorBars} />
        </Frame>
      </section>

      <section className="co-container co-two-col">
        <div>
          <Eyebrow accent>Fuentes</Eyebrow>
          <h2>Registros oficiales, cruzados.</h2>
          <p className="co-muted">Watermark, hash documental y cobertura operativa por fuente.</p>
        </div>
        <div className="co-table-wrap">
          <table className="co-table">
            <thead>
              <tr>
                <th>codigo</th>
                <th>fuente</th>
                <th>descripcion</th>
                <th>cobertura</th>
                <th>filas</th>
              </tr>
            </thead>
            <tbody>
              {data.sources.map((source) => (
                <tr key={source.code}>
                  <td className="co-mono">{source.code}</td>
                  <td>{source.name}</td>
                  <td>{source.desc}</td>
                  <td>
                    <div className="co-coverage">
                      <MiniBar value={source.cov} color={source.cov > 0.75 ? "var(--coacc-moss)" : source.cov > 0.6 ? "var(--coacc-accent)" : "var(--coacc-coral)"} />
                      <span>{toPercent(source.cov)}</span>
                    </div>
                  </td>
                  <td className="co-num">{source.rows}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="co-container">
        <Frame coord="CTA · Z-99" className="co-cta">
          <div>
            <h2>Empieza por una entidad.</h2>
            <p>Busca un NIT, una razon social o una persona y abre contratos, sanciones, senales y evidencia documental.</p>
          </div>
          <div className="co-action-row">
            <Link className="co-button co-button--primary" to="/app/search">
              Buscar en el grafo
              <Search size={16} />
            </Link>
            <Sparkline data={atlasFixture.seriesHits} width={140} />
          </div>
        </Frame>
      </section>
    </main>
  );
}
