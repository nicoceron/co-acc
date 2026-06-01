import { ArrowLeft, FileText, Network } from "lucide-react";
import { Link, useParams } from "react-router";

import { RelationGraph } from "../components/visuals";
import { Frame, MetricTile, Pill, Rule, SeverityBadge } from "../components/ui";
import { atlasFixture } from "../data/prototype";
import { formatNumber } from "../lib/format";

export function EntityPage() {
  const { entityId } = useParams();
  const entity = atlasFixture.entityExample;

  return (
    <main className="co-container co-container--wide co-entity">
      <Link className="co-button co-button--ghost" to="/app/search">
        <ArrowLeft size={16} />
        Busqueda
      </Link>

      <header className="co-entity-head">
        <div className="co-action-row">
          <Pill tone="accent">{entity.type}</Pill>
          {entity.pep ? <Pill tone="coral">PEP</Pill> : null}
          <span className="co-mono">{entityId || entity.id}</span>
        </div>
        <h1>{entity.name}</h1>
        <p>
          <span>{entity.doc}</span>
          <span>desde {entity.firstSeen}</span>
          <span>ultimo {entity.lastSeen}</span>
          <span>{entity.sources.join(" · ")}</span>
        </p>
      </header>

      <div className="co-metrics-grid co-metrics-grid--compact">
        <MetricTile label="contratos" value={formatNumber(entity.contracts)} sub="2017-2026" />
        <MetricTile label="valor adjudicado" value={entity.valueCop} sub="agregado nominal" />
        <MetricTile label="senales activas" value={String(entity.signals.length)} sub={entity.signals.map((signal) => signal.id).join(" · ")} />
        <MetricTile label="sanciones / pep" value={`${entity.sanctions} / ${entity.pepLinks}`} sub="SIRI · Ley 2013" />
      </div>

      <div className="co-entity__grid">
        <Frame coord="GRAFO · G-01">
          <div className="co-frame-title">
            <div>
              <h2>Vecindad de identidad</h2>
              <span>1 salto · {entity.relations.length} nodos</span>
            </div>
            <Network size={16} />
          </div>
          <RelationGraph center={entity.name} relations={entity.relations} />
        </Frame>

        <Frame coord="SENALES · S-01">
          <div className="co-frame-title">
            <div>
              <h2>Senales materializadas</h2>
              <span>{entity.signals.length} hits</span>
            </div>
            <FileText size={16} />
          </div>
          <div className="co-signal-cards">
            {entity.signals.map((signal) => (
              <Link key={signal.id} className="co-signal-card" to={`/app/signals/${signal.id}`}>
                <div>
                  <SeverityBadge severity={signal.sev} />
                  <span className="co-mono">{signal.id}</span>
                </div>
                <h3>{signal.title}</h3>
                <p>{signal.ev} evidencias · score {signal.score.toFixed(2)}</p>
              </Link>
            ))}
          </div>
        </Frame>
      </div>

      <section className="co-timeline-section">
        <Rule>Linea de tiempo documental</Rule>
        <Frame coord="TIMELINE · T-01">
          <ol className="co-timeline">
            {entity.timeline.map((row) => (
              <li key={`${row.d}-${row.label}`}>
                <time>{row.d}</time>
                <span />
                <div>
                  <Pill>{row.k}</Pill>
                  <p>{row.label}</p>
                </div>
              </li>
            ))}
          </ol>
        </Frame>
      </section>
    </main>
  );
}
