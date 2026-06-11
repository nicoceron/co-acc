import { Database, GitBranch, Search } from "lucide-react";
import { Link } from "react-router";

import { AtlasMap } from "../components/visuals";
import { DataStatus, EmptyState, Frame, MetricTile, Panel, Rule, SeverityBadge } from "../components/ui";
import { formatNumber, toPercent } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

export function Dashboard() {
  const { data, status } = useAtlasOverview();
  const signalsWithHits = data.signals.filter((signal) => signal.hits > 0).length;
  const topPatterns = data.patterns.filter((pattern) => pattern.hits > 0).slice(0, 6);

  return (
    <main className="co-container co-container--wide co-dashboard">
      <header className="co-workspace-head">
        <div>
          <Rule accent>Workspace · Observatorio en vivo</Rule>
          <h1>Estado del grafo</h1>
          <p>Snapshot operativo · stream activo · datos public_safe por defecto</p>
        </div>
        <div className="co-action-row">
          <DataStatus status={status} />
          <Link className="co-button" to="/app/search">
            <Search size={16} />
            Buscar
          </Link>
          <Link className="co-button" to="/app/patterns">
            <GitBranch size={16} />
            Patrones
          </Link>
          <Link className="co-button co-button--primary" to="/app/signals">Catalogo de senales</Link>
        </div>
      </header>

      <div className="co-dashboard__grid">
        <Frame coord="FEED · F-01" className="co-dashboard__feed">
          <div className="co-frame-title">
            <div>
              <h2>Hits recientes</h2>
              <span>12 ultimos · evidencia y entidad</span>
            </div>
            <DataStatus status={status} />
          </div>
          {data.feed.length ? (
            <div className="co-event-feed">
              {data.feed.map((item) => (
                <article className="co-event-feed__row" key={`${item.t}-${item.entity}-${item.signal}`}>
                  <time className="co-mono">{item.t}</time>
                  <div>
                    <Link to={`/app/signals/${item.signal}`} className="co-mono co-event-feed__signal">{item.signal}</Link>
                    <span>{item.note}</span>
                  </div>
                  <SeverityBadge severity={item.severity} />
                  <Link to="/app/entity/ent_co_900412118" className="co-event-feed__entity">{item.entity}</Link>
                </article>
              ))}
            </div>
          ) : (
            <EmptyState title="Sin hits recientes" body="No hay senales materializadas para alimentar el feed." />
          )}
        </Frame>

        <aside className="co-dashboard__side">
          <Frame coord="ATLAS · A-02">
            <div className="co-frame-title">
              <div>
                <h2>Hits · 24h</h2>
                <span>32 departamentos</span>
              </div>
            </div>
            <AtlasMap departamentos={data.departamentos} height={270} compact />
          </Frame>

          <Panel title="Indicadores" meta={<Database size={14} />}>
            <div className="co-mini-metrics">
              <MetricTile label="hits recientes" value={formatNumber(data.feed.length)} sub="feed materializado" spark={data.seriesHits} />
              <MetricTile
                label="senales con hits"
                value={formatNumber(signalsWithHits)}
                sub="catalogo vivo"
                spark={data.seriesHits.map((value) => value * 0.6)}
              />
              <MetricTile label="contratos" value={data.metrics.contracts} sub="contratacion indexada" />
              <MetricTile label="sanciones" value={data.metrics.sanctions} sub="sanciones enlazables" />
            </div>
          </Panel>
        </aside>
      </div>

      <section className="co-dashboard__sources">
        <Rule>Patrones materializados</Rule>
        <div className="co-table-wrap">
          <table className="co-table">
            <thead>
              <tr>
                <th>patron</th>
                <th>severidad</th>
                <th>categoria</th>
                <th>hits</th>
                <th>senal</th>
              </tr>
            </thead>
            <tbody>
              {topPatterns.map((pattern) => {
                const primarySignal = pattern.signalIds[0];
                return (
                  <tr key={pattern.id}>
                    <td>
                      {primarySignal ? (
                        <Link to={`/app/signals/${primarySignal}`}>{pattern.title}</Link>
                      ) : (
                        <span>{pattern.title}</span>
                      )}
                      <span className="co-row-sub">{pattern.id}</span>
                    </td>
                    <td><SeverityBadge severity={pattern.severity} /></td>
                    <td className="co-mono">{pattern.category}</td>
                    <td className="co-num">{formatNumber(pattern.hits)}</td>
                    <td className="co-mono">{primarySignal || "registry"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {!topPatterns.length ? (
            <EmptyState title="Sin patrones materializados" body="La API no entrego patrones con hits para esta vista." />
          ) : null}
        </div>
      </section>

      <section className="co-dashboard__sources">
        <Rule>Salud de fuentes</Rule>
        <div className="co-table-wrap">
          <table className="co-table">
            <thead>
              <tr>
                <th>codigo</th>
                <th>fuente</th>
                <th>cobertura</th>
                <th>estado</th>
                <th>actualizado</th>
              </tr>
            </thead>
            <tbody>
              {data.sources.slice(0, 10).map((source) => (
                <tr key={source.code}>
                  <td className="co-mono">{source.code}</td>
                  <td>{source.name}</td>
                  <td>
                    <div className="co-coverage">
                      <span className="co-minibar"><span style={{ width: toPercent(source.cov) }} /></span>
                      <span>{toPercent(source.cov)}</span>
                    </div>
                  </td>
                  <td className="co-num">{source.rows}</td>
                  <td className="co-mono">hace {2 + (source.code.length % 9)} h</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!data.sources.length ? (
            <EmptyState title="Sin fuentes cargadas" body="La API no entrego salud de fuentes para esta vista." />
          ) : null}
        </div>
      </section>
    </main>
  );
}
