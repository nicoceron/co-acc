import { Database, Search } from "lucide-react";
import { Link } from "react-router";

import { AtlasMap } from "../components/visuals";
import { DataStatus, Frame, MetricTile, Panel, Rule, SeverityBadge } from "../components/ui";
import { formatNumber, toPercent } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

export function Dashboard() {
  const { data, status } = useAtlasOverview();

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
          <div className="co-table-wrap">
            <table className="co-table">
              <thead>
                <tr>
                  <th>cuando</th>
                  <th>senal</th>
                  <th>severidad</th>
                  <th>entidad</th>
                  <th>nota</th>
                </tr>
              </thead>
              <tbody>
                {data.feed.map((item) => (
                  <tr key={`${item.t}-${item.entity}`}>
                    <td className="co-mono">{item.t}</td>
                    <td>
                      <Link to={`/app/signals/${item.signal}`} className="co-mono">{item.signal}</Link>
                    </td>
                    <td><SeverityBadge severity={item.severity} /></td>
                    <td>
                      <Link to="/app/entity/ent_co_900412118">{item.entity}</Link>
                    </td>
                    <td>{item.note}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
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
              <MetricTile label="hits 24h" value={formatNumber(1248)} sub="+12% vs media" spark={data.seriesHits} />
              <MetricTile label="entidades nuevas" value={formatNumber(328)} sub="cruzadas hoy" spark={data.seriesHits.map((value) => value * 0.6)} />
              <MetricTile label="contratos" value={data.metrics.contracts} sub="contratacion indexada" />
              <MetricTile label="sanciones" value={data.metrics.sanctions} sub="sanciones enlazables" />
            </div>
          </Panel>
        </aside>
      </div>

      <section className="co-dashboard__sources">
        <Rule>Salud de fuentes</Rule>
        <div className="co-table-wrap">
          <table className="co-table">
            <thead>
              <tr>
                <th>codigo</th>
                <th>fuente</th>
                <th>cobertura</th>
                <th>filas</th>
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
        </div>
      </section>
    </main>
  );
}
