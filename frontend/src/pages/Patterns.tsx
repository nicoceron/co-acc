import { Fragment, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { clsx } from "clsx";

import {
  getEntityPatterns,
  listPatterns,
  type PatternResult,
} from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const TIER_TONE: Record<string, string> = {
  critical: "text-rose-300",
  high: "text-orange-300",
  medium: "text-amber-300",
  low: "text-sky-300",
};

export function Patterns() {
  const { entityId } = useParams();
  if (entityId) return <PatternMatches entityId={entityId} />;
  return <PatternRegistry />;
}

function PatternRegistry() {
  const state = useAsync(() => listPatterns(), []);
  const navigate = useNavigate();
  const [input, setInput] = useState("");

  function handleSubmit(e: { preventDefault: () => void }) {
    e.preventDefault();
    const trimmed = input.trim();
    if (trimmed) navigate(`/app/patterns/${encodeURIComponent(trimmed)}`);
  }

  return (
    <Shell>
      <header className="flex flex-wrap items-baseline justify-between gap-4 pb-8">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Workspace · Patterns
          </div>
          <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
            Patrones documentales
          </h1>
          <p className="mt-4 max-w-[62ch] text-[14px] leading-relaxed text-ink-400">
            Reglas versionadas que cruzan fuentes para detectar concentración, ventanas cortas, co-licitación, captura territorial y otras anomalías.
          </p>
        </div>
        {state.data && (
          <div className="font-mono text-[12px] text-ink-400">
            {state.data.patterns.length} registrados
          </div>
        )}
      </header>

      <form
        onSubmit={handleSubmit}
        className="mb-10 flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.02] p-1.5 focus-within:border-lime-300/40"
      >
        <span className="pl-3 font-mono text-[11px] uppercase tracking-wider text-ink-500">
          Analizar
        </span>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="id de entidad, NIT, o cédula"
          className="flex-1 bg-transparent px-3 py-2 text-[14px] text-ink-50 placeholder:text-ink-500 focus:outline-none"
        />
        <button
          type="submit"
          disabled={!input.trim()}
          className="rounded-md bg-lime-300 px-4 py-2 text-[13px] font-medium text-ink-950 transition hover:bg-lime-400 disabled:opacity-50"
        >
          Ejecutar
        </button>
      </form>

      {state.loading ? (
        <Loading>cargando registro…</Loading>
      ) : state.error ? (
        <Err msg={state.error.message} />
      ) : !state.data || state.data.patterns.length === 0 ? (
        <Empty>Sin patrones registrados.</Empty>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {state.data.patterns.map((p) => (
            <article
              key={p.id}
              className="flex flex-col gap-2 rounded-lg border border-white/5 bg-white/[0.01] p-5"
            >
              <span className="font-mono text-[11px] uppercase tracking-[0.1em] text-lime-300">
                {p.id}
              </span>
              <span className="text-[15px] font-medium text-ink-50">{p.name_es}</span>
              <p className="line-clamp-4 text-[13px] leading-relaxed text-ink-400">
                {p.description_es}
              </p>
            </article>
          ))}
        </div>
      )}
    </Shell>
  );
}

function PatternMatches({ entityId }: { entityId: string }) {
  const state = useAsync(() => getEntityPatterns(entityId, "es"), [entityId]);

  return (
    <Shell>
      <Link
        to="/app/patterns"
        className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50"
      >
        ← Patrones
      </Link>

      <header className="mt-6 pb-10">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-lime-300">
          {entityId}
        </div>
        <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          Patrones detectados
        </h1>
        <p className="mt-4 max-w-[60ch] text-[14px] leading-relaxed text-ink-400">
          Resultados del registro completo contra esta entidad. Cada coincidencia enlaza fuentes oficiales.
        </p>
        {state.data && (
          <div className="mt-6 flex flex-wrap items-center gap-5 font-mono text-[12px] text-ink-400">
            <span>{state.data.total} coincidencias</span>
            <Link
              to={`/app/analysis/${entityId}`}
              className="text-lime-300 hover:text-lime-400"
            >
              ver entidad →
            </Link>
          </div>
        )}
      </header>

      {state.loading ? (
        <Loading>ejecutando patrones…</Loading>
      ) : state.error ? (
        <Err msg={state.error.message} />
      ) : !state.data || state.data.patterns.length === 0 ? (
        <Empty>No se detectaron patrones para esta entidad.</Empty>
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {state.data.patterns.map((m, idx) => (
            <MatchCard key={`${m.pattern_id}-${idx}`} m={m} />
          ))}
        </div>
      )}
    </Shell>
  );
}

function MatchCard({ m }: { m: PatternResult }) {
  return (
    <article className="flex flex-col gap-3 rounded-lg border border-white/5 bg-white/[0.01] p-5">
      <div className="flex items-center justify-between font-mono text-[11px]">
        <span className={clsx("uppercase", TIER_TONE[m.intelligence_tier ?? ""] ?? "text-ink-400")}>
          {m.intelligence_tier ?? "pattern"}
        </span>
        <span className="text-ink-500">{m.pattern_id}</span>
      </div>
      <div className="text-[15px] font-medium text-ink-50">{m.pattern_name}</div>
      <p className="text-[13px] leading-relaxed text-ink-400">{m.description}</p>

      {m.sources.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {m.sources.map((s) => (
            <span
              key={s.database}
              className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 font-mono text-[10px] text-ink-200"
            >
              {s.database}
            </span>
          ))}
        </div>
      )}

      <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 border-t border-white/5 pt-3 font-mono text-[11px]">
        <dt className="text-ink-500">entidades</dt>
        <dd className="text-ink-100">{m.entity_ids.length}</dd>
        {Object.entries(m.data)
          .slice(0, 3)
          .map(([k, v]) => (
            <Fragment key={k}>
              <dt className="text-ink-500">{k}</dt>
              <dd className="truncate text-ink-100">
                {typeof v === "object" ? JSON.stringify(v) : String(v)}
              </dd>
            </Fragment>
          ))}
      </dl>
    </article>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">{children}</div>;
}

function Loading({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-2 font-mono text-[13px] text-ink-400">
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400" />
      {children}
    </div>
  );
}

function Err({ msg }: { msg: string }) {
  return (
    <p className="rounded-md border border-rose-400/30 bg-rose-400/5 px-4 py-3 font-mono text-[13px] text-rose-300">
      {msg}
    </p>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-white/10 px-5 py-10 text-center text-[13px] text-ink-400">
      {children}
    </div>
  );
}
