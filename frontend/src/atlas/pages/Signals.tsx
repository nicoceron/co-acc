import { ArrowLeft, GitBranch, ShieldCheck } from "lucide-react";
import { useMemo, useState } from "react";
import { Link, useParams } from "react-router";

import { Frame, Pill, Rule, SeverityBadge } from "../components/ui";
import { atlasFixture, type Severity, type SignalDetail } from "../data/prototype";
import { formatNumber } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

const SEVERITY_FILTERS: ("all" | Severity)[] = ["all", "low", "medium", "high", "critical"];

function fallbackDetail(signalId: string): SignalDetail {
  return {
    version: "registry",
    scope: "entity",
    runner: `registry:${signalId}`,
    policy: { public: true, reviewer: false, identity: ["nit"], dedup: ["entity_id", "source_id"] },
    sourcesRequired: ["SECOP-II", "RUES"],
    entityTypes: ["empresa", "contrato"],
    sampleHits: atlasFixture.signalDetail["S-014"]?.sampleHits ?? [],
  };
}

export function SignalsPage() {
  const { signalId } = useParams();
  const { data } = useAtlasOverview();
  const [severity, setSeverity] = useState<"all" | Severity>("all");
  const [publicOnly, setPublicOnly] = useState(false);

  const filtered = useMemo(() => {
    return data.signals
      .filter((signal) => severity === "all" || signal.severity === severity)
      .filter((signal) => !publicOnly || signal.public);
  }, [data.signals, publicOnly, severity]);

  if (signalId) {
    const signal = data.signals.find((item) => item.id === signalId) ?? atlasFixture.signals.find((item) => item.id === signalId) ?? atlasFixture.signals[0]!;
    const detail = atlasFixture.signalDetail[signal.id] ?? fallbackDetail(signal.id);

    return (
      <main className="co-container co-signal-detail">
        <Link className="co-button co-button--ghost" to="/app/signals">
          <ArrowLeft size={16} />
          Senales
        </Link>
        <header className="co-page-head">
          <Rule accent>{signal.id} · v{detail.version}</Rule>
          <h1>{signal.title}</h1>
          <p>{signal.desc}</p>
          <div className="co-action-row">
            <SeverityBadge severity={signal.severity} />
            <Pill tone={signal.public ? "moss" : "neutral"}>{signal.public ? "public" : "reviewer"}</Pill>
            <Pill>{formatNumber(signal.hits)} hits</Pill>
          </div>
        </header>

        <div className="co-signal-detail__grid">
          <Frame coord="POLITICA · P-01">
            <div className="co-spec-list">
              <span><strong>scope</strong>{detail.scope}</span>
              <span><strong>runner</strong>{detail.runner}</span>
              <span><strong>identidad</strong>{detail.policy.identity.join(" · ")}</span>
              <span><strong>dedup</strong>{detail.policy.dedup.join(" · ")}</span>
            </div>
          </Frame>
          <Frame coord="FUENTES · S-02">
            <div className="co-chip-cloud">
              {detail.sourcesRequired.map((source) => <Pill key={source}>{source}</Pill>)}
            </div>
            <div className="co-chip-cloud co-chip-cloud--spaced">
              {detail.entityTypes.map((type) => <Pill key={type} tone="accent">{type}</Pill>)}
            </div>
          </Frame>
        </div>

        <Frame coord="MUESTRA · H-03">
          <div className="co-frame-title">
            <div>
              <h2>Hits de muestra</h2>
              <span>confianza, evidencia y score</span>
            </div>
            <GitBranch size={16} />
          </div>
          <div className="co-table-wrap">
            <table className="co-table">
              <thead>
                <tr>
                  <th>entidad / proceso</th>
                  <th>confianza</th>
                  <th>evidencia</th>
                  <th>score</th>
                </tr>
              </thead>
              <tbody>
                {detail.sampleHits.map((hit) => (
                  <tr key={hit.label}>
                    <td>{hit.label}</td>
                    <td className="co-num">{hit.conf.toFixed(2)}</td>
                    <td className="co-num">{hit.ev}</td>
                    <td className="co-num">{hit.score.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Frame>
      </main>
    );
  }

  return (
    <main className="co-container co-signals">
      <header className="co-workspace-head">
        <div>
          <Rule accent>Workspace · Catalogo</Rule>
          <h1>Senales</h1>
          <p>Patrones documentales versionados, materializados y enlazables.</p>
        </div>
        <Pill tone="accent">
          <ShieldCheck size={13} />
          public_safe
        </Pill>
      </header>

      <div className="co-filter-bar">
        {SEVERITY_FILTERS.map((item) => (
          <button className={severity === item ? "active" : ""} key={item} type="button" onClick={() => setSeverity(item)}>
            {item === "all" ? "todas" : item}
          </button>
        ))}
        <button className={publicOnly ? "active" : ""} type="button" onClick={() => setPublicOnly((value) => !value)}>
          solo publicas
        </button>
        <span>{filtered.length} de {data.signals.length}</span>
      </div>

      <div className="co-table-wrap">
        <table className="co-table">
          <thead>
            <tr>
              <th>id</th>
              <th>severidad</th>
              <th>senal</th>
              <th>categoria</th>
              <th>hits</th>
              <th>publico</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((signal) => (
              <tr key={signal.id}>
                <td>
                  <Link className="co-mono" to={`/app/signals/${signal.id}`}>{signal.id}</Link>
                </td>
                <td><SeverityBadge severity={signal.severity} /></td>
                <td>
                  <Link to={`/app/signals/${signal.id}`}>{signal.title}</Link>
                  <span className="co-row-sub">{signal.desc}</span>
                </td>
                <td className="co-mono">{signal.category}</td>
                <td className="co-num">{formatNumber(signal.hits)}</td>
                <td>{signal.public ? <Pill tone="moss">public</Pill> : <Pill>reviewer</Pill>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </main>
  );
}
