import { ArrowLeft, Gauge, GitBranch } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";

import { getSignal, type SignalDetailResponse } from "@/api/client";

import { EmptyState, Frame, Pill, Rule, SeverityBadge } from "../components/ui";
import { atlasFixture, type Severity, type SignalDetail } from "../data/prototype";
import { formatNumber } from "../lib/format";
import { allowAtlasFixtures, useAtlasOverview } from "../lib/useAtlasData";

const SEVERITY_FILTERS: ("all" | Severity)[] = ["all", "low", "medium", "high", "critical"];

function displaySignalId(signalId: string): string {
  return signalId.replace(/_review_only$/, "");
}

function fallbackDetail(signalId: string): SignalDetail {
  return {
    version: "registry",
    scope: "entity",
    runner: `registry:${signalId}`,
    policy: { public: true, identity: ["nit"], dedup: ["entity_id", "source_id"] },
    sourcesRequired: ["SECOP-II", "RUES"],
    entityTypes: ["empresa", "contrato"],
    sampleHits: atlasFixture.signalDetail["S-014"]?.sampleHits ?? [],
  };
}

function apiDetailToViewModel(response: SignalDetailResponse): SignalDetail {
  const { definition } = response;
  return {
    version: String(definition.version),
    scope: definition.scope_type,
    runner: definition.runner ? `${definition.runner.kind}:${definition.runner.ref}` : `registry:${definition.id}`,
    policy: {
      public: definition.public_safe,
      identity: definition.requires_identity,
      dedup: definition.dedup_fields,
    },
    sourcesRequired: definition.sources_required,
    entityTypes: definition.entity_types,
    sampleHits: response.sample_hits.map((hit) => ({
      label: [hit.entity_key, hit.scope_key].filter(Boolean).join(" · "),
      conf: hit.confidence_index,
      ev: hit.evidence_count,
      score: hit.score,
      identity: hit.confidence_components.identity,
      traceability: hit.confidence_components.evidence_traceability,
      corroboration: hit.confidence_components.source_corroboration,
    })),
  };
}

export function SignalsPage() {
  const { signalId } = useParams();
  const { data } = useAtlasOverview();
  const fixturesAllowed = allowAtlasFixtures();
  const [severity, setSeverity] = useState<"all" | Severity>("all");
  const [apiDetail, setApiDetail] = useState<SignalDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setApiDetail(null);
    if (!signalId || typeof globalThis.fetch !== "function") {
      setDetailLoading(false);
      return () => {
        cancelled = true;
      };
    }
    setDetailLoading(true);
    getSignal(signalId)
      .then((response) => {
        if (!cancelled) setApiDetail(apiDetailToViewModel(response));
      })
      .catch(() => {
        if (!cancelled) setApiDetail(null);
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [signalId]);

  const filtered = useMemo(() => {
    return data.signals
      .filter((signal) => severity === "all" || signal.severity === severity);
  }, [data.signals, severity]);

  if (signalId) {
    const signal = data.signals.find((item) => displaySignalId(item.id) === signalId || item.id === signalId)
      ?? (fixturesAllowed ? atlasFixture.signals.find((item) => item.id === signalId) : undefined)
      ?? (fixturesAllowed ? atlasFixture.signals[0] : undefined);

    if (!signal) {
      return (
        <main className="co-container co-signal-detail">
          <Link className="co-button co-button--ghost" to="/app/signals">
            <ArrowLeft size={16} />
            Senales
          </Link>
          <EmptyState title="Senal no disponible" body="No hay definicion materializada con ese identificador." />
        </main>
      );
    }

    const fixtureDetail = fixturesAllowed ? atlasFixture.signalDetail[signal.id] : undefined;
    const detail: SignalDetail = apiDetail ?? fixtureDetail ?? fallbackDetail(signal.id);

    return (
      <main className="co-container co-signal-detail">
        <Link className="co-button co-button--ghost" to="/app/signals">
          <ArrowLeft size={16} />
          Senales
        </Link>
        <header className="co-page-head">
          <Rule accent>{displaySignalId(signal.id)} · v{detail.version}</Rule>
          <h1>{signal.title}</h1>
          <p>{signal.desc}</p>
          <div className="co-action-row">
            <SeverityBadge severity={signal.severity} />
            <Pill tone="accent"><Gauge size={13} /> confianza {signal.confidence?.toFixed(1) ?? "—"}%</Pill>
            <Pill>{formatNumber(signal.hits)} hits</Pill>
          </div>
        </header>

        <div className="co-signal-detail__grid">
          <Frame coord="METODO · P-01">
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
              <span>calidad de evidencia separada del score de riesgo</span>
            </div>
            <GitBranch size={16} />
          </div>
          <div className="co-table-wrap">
            <table className="co-table">
              <thead>
                <tr>
                  <th>entidad / proceso</th>
                  <th>indice</th>
                  <th>identidad</th>
                  <th>trazabilidad</th>
                  <th>corroboracion</th>
                  <th>evidencia</th>
                  <th>riesgo</th>
                </tr>
              </thead>
              <tbody>
                {detail.sampleHits.map((hit) => (
                  <tr key={hit.label}>
                    <td>{hit.label}</td>
                    <td className="co-num">{hit.conf.toFixed(1)}%</td>
                    <td className="co-num">{(hit.identity * 100).toFixed(0)}%</td>
                    <td className="co-num">{(hit.traceability * 100).toFixed(0)}%</td>
                    <td className="co-num">{(hit.corroboration * 100).toFixed(0)}%</td>
                    <td className="co-num">{hit.ev}</td>
                    <td className="co-num">{hit.score.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {detailLoading && !detail.sampleHits.length ? (
              <EmptyState title="Cargando muestra" body="Consultando hits materializados para esta senal." />
            ) : !detail.sampleHits.length ? (
              <EmptyState title="Sin hits de muestra" body="Esta senal no tiene hits de muestra disponibles en la API." />
            ) : null}
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
          <p>Todos los patrones documentales, con confianza trazable y evidencia enlazada.</p>
        </div>
        <Pill tone="accent">
          <Gauge size={13} />
          confidence_index
        </Pill>
      </header>

      <div className="co-filter-bar">
        {SEVERITY_FILTERS.map((item) => (
          <button className={severity === item ? "active" : ""} key={item} type="button" onClick={() => setSeverity(item)}>
            {item === "all" ? "todas" : item}
          </button>
        ))}
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
              <th>estado</th>
              <th>confianza</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((signal) => (
              <tr key={signal.id}>
                <td>
                  <Link className="co-mono" to={`/app/signals/${displaySignalId(signal.id)}`}>{displaySignalId(signal.id)}</Link>
                </td>
                <td><SeverityBadge severity={signal.severity} /></td>
                <td>
                  <Link to={`/app/signals/${displaySignalId(signal.id)}`}>{signal.title}</Link>
                  <span className="co-row-sub">{signal.desc}</span>
                </td>
                <td className="co-mono">{signal.category}</td>
                <td className="co-num">{formatNumber(signal.hits)}</td>
                <td>
                  <Pill tone={signal.materialized ? "moss" : "neutral"}>
                    {signal.materialized ? "materialized" : "registry"}
                  </Pill>
                </td>
                <td className="co-num">{signal.confidence == null ? "—" : `${signal.confidence.toFixed(1)}%`}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!filtered.length ? (
          <EmptyState title="Sin senales" body="No hay senales para los filtros activos." />
        ) : null}
      </div>
    </main>
  );
}
