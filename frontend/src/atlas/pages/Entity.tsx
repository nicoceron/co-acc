import { ArrowLeft, ExternalLink, FileText, Network } from "lucide-react";
import { Link, useParams } from "react-router";

import type { EntityDetail } from "@/api/client";

import { RelationGraph } from "../components/visuals";
import { DataStatus, EmptyState, Frame, MetricTile, Pill, Rule, SeverityBadge } from "../components/ui";
import { atlasFixture } from "../data/prototype";
import { compactNumber, formatNumber } from "../lib/format";
import { allowAtlasFixtures, useAtlasEntity } from "../lib/useAtlasData";

type EntityProperty = string | number | boolean | null | undefined;

function firstProperty(entity: EntityDetail, keys: string[]): EntityProperty {
  for (const key of keys) {
    const value = entity.properties[key];
    if (value !== null && value !== undefined && String(value).trim()) {
      return value;
    }
  }
  return undefined;
}

function textProperty(entity: EntityDetail, keys: string[], fallback: string): string {
  const value = firstProperty(entity, keys);
  return value === undefined ? fallback : String(value);
}

function numberProperty(entity: EntityDetail, keys: string[]): number {
  const value = firstProperty(entity, keys);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function formatCop(value: number): string {
  if (!value) return "$0";
  return new Intl.NumberFormat("es-CO", {
    currency: "COP",
    maximumFractionDigits: 1,
    notation: "compact",
    style: "currency",
  }).format(value);
}

function sourceLabel(entity: EntityDetail): string {
  const labels = entity.sources.map((source) => source.database).filter(Boolean);
  return labels.length ? labels.join(" · ") : "sin fuentes";
}

function FixtureEntityView({ entityId }: { entityId?: string }) {
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

export function EntityPage() {
  const { entityId } = useParams();
  const fixturesAllowed = allowAtlasFixtures();
  const entityState = useAtlasEntity(entityId);

  if (entityState.status === "loading") {
    return (
      <main className="co-container co-container--wide co-entity">
        <Link className="co-button co-button--ghost" to="/app/search">
          <ArrowLeft size={16} />
          Busqueda
        </Link>
        <EmptyState title="Cargando entidad" body="Consultando detalle, senales y evidencia en la API." />
      </main>
    );
  }

  if (!entityState.entity) {
    if (fixturesAllowed && entityState.status === "fixture") {
      return <FixtureEntityView entityId={entityId} />;
    }
    return (
      <main className="co-container co-container--wide co-entity">
        <Link className="co-button co-button--ghost" to="/app/search">
          <ArrowLeft size={16} />
          Busqueda
        </Link>
        <EmptyState
          title={entityId ? "Entidad no disponible" : "Selecciona una entidad"}
          body={entityState.error || "Busca un NIT, cedula o entidad y abre un resultado vivo de la API."}
        />
      </main>
    );
  }

  const entity = entityState.entity;
  const name = textProperty(entity, ["name", "razon_social", "nombre", "display_name"], entity.id);
  const documentId = textProperty(entity, ["document_id", "nit", "cedula", "numero_documento"], entity.id);
  const role = textProperty(entity, ["role"], entity.type);
  const contractCount = numberProperty(entity, ["contract_count"]);
  const contractValue = numberProperty(entity, ["total_contract_value"]);
  const sourceRows = numberProperty(entity, ["source_row_count"]);

  return (
    <main className="co-container co-container--wide co-entity">
      <Link className="co-button co-button--ghost" to="/app/search">
        <ArrowLeft size={16} />
        Busqueda
      </Link>

      <header className="co-entity-head">
        <div className="co-action-row">
          <Pill tone="accent">{entity.entity_label || entity.type}</Pill>
          {entity.is_pep ? <Pill tone="coral">PEP</Pill> : null}
          <Pill>{role}</Pill>
          <DataStatus status={entityState.status} />
          <span className="co-mono">{entity.id}</span>
        </div>
        <h1>{name}</h1>
        <p>
          <span>{documentId}</span>
          <span>{entity.identity_quality || "identidad"}</span>
          <span>{entity.exposure_tier || "confidence_indexed"}</span>
          <span>{sourceLabel(entity)}</span>
        </p>
      </header>

      <div className="co-metrics-grid co-metrics-grid--compact">
        <MetricTile label="contratos" value={formatNumber(contractCount)} sub="contratacion enlazada" />
        <MetricTile label="valor adjudicado" value={formatCop(contractValue)} sub="agregado nominal" />
        <MetricTile label="senales activas" value={formatNumber(entityState.signals.length)} sub="hits materializados" />
        <MetricTile label="evidencia" value={formatNumber(entityState.totalDocuments)} sub={`${compactNumber(sourceRows)} filas fuente`} />
      </div>

      <div className="co-entity__grid">
        <Frame coord="EVIDENCIA · E-01">
          <div className="co-frame-title">
            <div>
              <h2>Evidencia documental</h2>
              <span>{entityState.evidenceBundles.length} paquetes · {entityState.totalDocuments} documentos</span>
            </div>
            <FileText size={16} />
          </div>
          <div className="co-evidence-list">
            {entityState.evidenceBundles.map((bundle) => (
              <article key={bundle.id}>
                <FileText size={17} />
                <div>
                  <strong>{bundle.title}</strong>
                  <span>{bundle.relation_summary || bundle.description || bundle.reference || bundle.bundle_type}</span>
                  <span>{bundle.source || bundle.document_kinds.join(" · ") || `${bundle.document_count} docs`}</span>
                  {bundle.documents.map((document) => (
                    <span key={document.id}>
                      {document.url ? (
                        <a href={document.url} target="_blank" rel="noreferrer">
                          {document.title} <ExternalLink size={12} />
                        </a>
                      ) : document.title}
                      {" · "}{document.source || "fuente no atribuida"}
                      {" · "}{document.uploaded_at?.slice(0, 10) || "fecha no disponible"}
                      {" · "}{document.identity_match_type || "enlace no especificado"}
                      {" · "}{document.row_selector || document.file_selector || "selector no disponible"}
                    </span>
                  ))}
                </div>
              </article>
            ))}
          </div>
          {!entityState.evidenceBundles.length ? (
            <EmptyState title="Sin paquetes de evidencia" body="No hay evidencia documental enlazada para esta entidad." />
          ) : null}
        </Frame>

        <Frame coord="SENALES · S-01">
          <div className="co-frame-title">
            <div>
              <h2>Senales materializadas</h2>
              <span>{entityState.signals.length} hits</span>
            </div>
            <Network size={16} />
          </div>
          <div className="co-signal-cards">
            {entityState.signals.map((signal) => (
              <Link key={signal.hit_id} className="co-signal-card" to={`/app/signals/${signal.signal_id}`}>
                <div>
                  <SeverityBadge severity={signal.severity} />
                  <span className="co-mono">{signal.signal_id}</span>
                </div>
                <h3>{signal.title}</h3>
                <p>
                  confianza {signal.confidence_index.toFixed(1)}% · riesgo {signal.score.toFixed(2)}
                  {" · "}{signal.evidence_count} evidencias
                </p>
                <span className="co-row-sub">
                  {signal.last_seen_at?.slice(0, 10) || "fecha no disponible"}
                </span>
              </Link>
            ))}
          </div>
          {!entityState.signals.length ? (
            <EmptyState title="Sin senales materializadas" body="La ultima corrida no produjo hits para esta entidad." />
          ) : null}
        </Frame>
      </div>

      <section className="co-timeline-section">
        <Rule>Linea de tiempo documental</Rule>
        <Frame coord="TIMELINE · T-01">
          <ol className="co-timeline">
            {entityState.timeline.map((row) => (
              <li key={row.id}>
                <time>{row.date.slice(0, 10)}</time>
                <span />
                <div>
                  <Pill>{row.entity_type}</Pill>
                  <p>{row.label}</p>
                </div>
              </li>
            ))}
          </ol>
          {!entityState.timeline.length ? (
            <EmptyState title="Sin linea de tiempo" body="No hay eventos temporales materializados para esta entidad." />
          ) : null}
        </Frame>
      </section>
    </main>
  );
}
