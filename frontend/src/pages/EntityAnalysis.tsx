import { Fragment } from "react";
import { Link, useParams } from "react-router";
import { clsx } from "clsx";

import {
  getEntity,
  getEntityEvidenceTrail,
  getEntitySignals,
  type EntityDetail,
  type EntityEvidenceTrailResponse,
  type EntitySignalsResponse,
} from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const SEV_TONE: Record<string, string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

const KEY_LABELS: Record<string, string> = {
  razon_social: "razón social",
  document_id: "documento",
  nit: "NIT",
  created_at: "creado",
  updated_at: "actualizado",
};

export function EntityAnalysis() {
  const { entityId } = useParams<{ entityId: string }>();

  const entityQ = useAsync(
    () => (entityId ? getEntity(entityId) : Promise.reject(new Error("no id"))),
    [entityId],
  );
  const signalsQ = useAsync(
    () => (entityId ? getEntitySignals(entityId) : Promise.reject(new Error("no id"))),
    [entityId],
  );
  const evidenceQ = useAsync(
    () => (entityId ? getEntityEvidenceTrail(entityId) : Promise.reject(new Error("no id"))),
    [entityId],
  );

  if (!entityId) return <Shell><Err msg="Falta el id." /></Shell>;
  if (entityQ.loading) return <Shell><Loading /></Shell>;
  if (entityQ.error || !entityQ.data) {
    return (
      <Shell>
        <Back />
        <Err msg={entityQ.error?.message ?? "no encontrada"} />
      </Shell>
    );
  }

  const entity = entityQ.data;
  const displayName =
    (entity.properties.razon_social as string | undefined) ||
    (entity.properties.name as string | undefined) ||
    (entity.properties.nombre as string | undefined) ||
    entity.id;

  const documentId =
    (entity.properties.document_id as string | undefined) ||
    (entity.properties.nit as string | undefined) ||
    (entity.properties.document as string | undefined);

  return (
    <Shell>
      <Back />
      <header className="mt-6 pb-10">
        <div className="flex items-center gap-3 font-mono text-[12px]">
          <span className="text-ink-400">{entity.type}</span>
          {entity.is_pep && (
            <span className="rounded-full border border-amber-300/30 bg-amber-300/5 px-2 py-0.5 text-[10px] uppercase tracking-wider text-amber-300">
              PEP
            </span>
          )}
        </div>
        <h1 className="mt-3 max-w-[24ch] text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          {displayName}
        </h1>
        <div className="mt-5 flex flex-wrap gap-5 font-mono text-[12px] text-ink-400">
          <span>{entity.id}</span>
          {documentId && <span className="text-ink-100">{documentId}</span>}
          {entity.sources.length > 0 && (
            <span>{entity.sources.map((s) => s.database).join(" · ")}</span>
          )}
        </div>
      </header>

      <div className="grid gap-6 md:grid-cols-2">
        <Properties entity={entity} />
        {entity.sources.length > 0 && (
          <Panel title={`Fuentes · ${entity.sources.length}`}>
            <div className="flex flex-wrap gap-1.5">
              {entity.sources.map((s, i) => (
                <span
                  key={`${s.database}-${i}`}
                  className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 font-mono text-[11px] text-ink-200"
                >
                  {s.database}
                </span>
              ))}
            </div>
          </Panel>
        )}

        {signalsQ.loading ? (
          <Panel title="Señales"><Dim>cargando…</Dim></Panel>
        ) : signalsQ.error ? (
          <Panel title="Señales"><Err msg={signalsQ.error.message} /></Panel>
        ) : signalsQ.data ? (
          <Signals data={signalsQ.data} />
        ) : null}

        {evidenceQ.loading ? (
          <Panel title="Evidencia"><Dim>cargando…</Dim></Panel>
        ) : evidenceQ.error ? (
          <Panel title="Evidencia"><Err msg={evidenceQ.error.message} /></Panel>
        ) : evidenceQ.data ? (
          <Evidence data={evidenceQ.data} />
        ) : null}
      </div>
    </Shell>
  );
}

function Properties({ entity }: { entity: EntityDetail }) {
  const entries = Object.entries(entity.properties).filter(
    ([k]) => k !== "razon_social" && k !== "name" && k !== "nombre",
  );
  return (
    <Panel title={`Propiedades · ${entries.length}`}>
      {entries.length === 0 ? (
        <Dim>sin propiedades en public_safe</Dim>
      ) : (
        <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2 font-mono text-[12px]">
          {entries.slice(0, 14).map(([k, v]) => (
            <Fragment key={k}>
              <dt className="text-ink-500">{KEY_LABELS[k] ?? k.replace(/_/g, " ")}</dt>
              <dd className="truncate text-ink-100">{formatValue(v)}</dd>
            </Fragment>
          ))}
        </dl>
      )}
    </Panel>
  );
}

function Signals({ data }: { data: EntitySignalsResponse }) {
  const items = data.signals ?? [];
  return (
    <Panel title={`Señales · ${data.total}`}>
      {items.length === 0 ? (
        <Dim>ninguna señal activa</Dim>
      ) : (
        <ul className="space-y-3">
          {items.slice(0, 8).map((hit) => (
            <li key={hit.hit_id} className="border-b border-white/5 pb-3 last:border-0 last:pb-0">
              <Link to={`/app/signals/${hit.signal_id}`} className="group block">
                <div className="flex items-center gap-2 font-mono text-[11px]">
                  <span className={clsx("uppercase", SEV_TONE[hit.severity] ?? "text-ink-400")}>
                    {hit.severity}
                  </span>
                  <span className="text-ink-500">{hit.signal_id}</span>
                  <span className="ml-auto text-lime-300">{hit.score.toFixed(2)}</span>
                </div>
                <div className="mt-1 text-[13px] text-ink-100 group-hover:text-lime-300">
                  {hit.title}
                </div>
                <div className="mt-0.5 font-mono text-[11px] text-ink-500">
                  {hit.evidence_count} ev · conf {hit.identity_confidence.toFixed(2)}
                </div>
              </Link>
            </li>
          ))}
        </ul>
      )}
      {data.stale && (
        <div className="mt-3 font-mono text-[11px] text-amber-300/80">
          señales no recorridas recientemente
        </div>
      )}
    </Panel>
  );
}

function Evidence({ data }: { data: EntityEvidenceTrailResponse }) {
  return (
    <Panel title={`Evidencia · ${data.total_bundles} · ${data.total_documents} docs`}>
      {data.bundles.length === 0 ? (
        <Dim>sin paquetes documentales</Dim>
      ) : (
        <ul className="space-y-3">
          {data.bundles.slice(0, 6).map((b) => (
            <li key={b.id} className="border-b border-white/5 pb-3 last:border-0 last:pb-0">
              <div className="flex items-baseline justify-between gap-4 font-mono text-[11px]">
                <span className="text-ink-400">{b.bundle_type}</span>
                <span className="text-lime-300">{b.document_count} docs</span>
              </div>
              <div className="mt-1 text-[13px] text-ink-100">{b.title}</div>
              {b.relation_summary && (
                <div className="mt-0.5 text-[12px] text-ink-400">{b.relation_summary}</div>
              )}
              <div className="mt-1 font-mono text-[11px] text-ink-500">
                {b.reference ? `${b.reference} · ` : ""}
                {b.source ?? "sin fuente"}
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">{children}</div>;
}

function Back() {
  return (
    <Link to="/app/search" className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50">
      ← Búsqueda
    </Link>
  );
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-lg border border-white/5 bg-white/[0.01] p-5">
      <div className="mb-4 font-mono text-[11px] uppercase tracking-[0.12em] text-ink-400">
        {title}
      </div>
      {children}
    </section>
  );
}

function Dim({ children }: { children: React.ReactNode }) {
  return <span className="font-mono text-[12px] text-ink-500">{children}</span>;
}

function Loading() {
  return (
    <div className="flex items-center gap-2 font-mono text-[13px] text-ink-400">
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400" />
      cargando entidad…
    </div>
  );
}

function Err({ msg }: { msg: string }) {
  return (
    <p className="mt-4 rounded-md border border-rose-400/30 bg-rose-400/5 px-4 py-3 font-mono text-[13px] text-rose-300">
      {msg}
    </p>
  );
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined || v === "") return "—";
  if (typeof v === "boolean") return v ? "sí" : "no";
  return String(v);
}
