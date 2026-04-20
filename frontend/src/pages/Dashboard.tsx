import { Link } from "react-router";
import { clsx } from "clsx";

import {
  getPublicMeta,
  listSignals,
  type PublicMetaResponse,
  type SignalListItem,
} from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const SEV_TONE: Record<SignalListItem["severity"], string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

export function Dashboard() {
  const metaQ = useAsync(getPublicMeta, []);
  const signalsQ = useAsync(listSignals, []);
  const meta = metaQ.data;
  const signals = signalsQ.data;

  const top = (signals?.signals ?? [])
    .slice()
    .sort((a, b) => b.hit_count - a.hit_count)
    .slice(0, 10);

  return (
    <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">
      <header className="flex flex-wrap items-baseline justify-between gap-4 pb-8">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Overview
          </div>
          <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
            Estado del grafo
          </h1>
        </div>
        {meta && (
          <div className="flex items-center gap-2 text-[12px] text-ink-400">
            <span className="h-1.5 w-1.5 rounded-full bg-lime-400" />
            <span className="font-mono">{meta.mode}</span>
          </div>
        )}
      </header>

      <section className="grid grid-cols-2 gap-px overflow-hidden rounded-lg border border-white/5 bg-white/5 md:grid-cols-4">
        <Kpi label="Entidades" value={meta ? compact(meta.total_nodes) : "—"} sub={meta ? `${compact(meta.total_relationships)} relaciones` : "—"} />
        <Kpi label="Contratos" value={meta ? compact(meta.contract_count) : "—"} sub="SECOP I/II/TVEC" />
        <Kpi label="Empresas" value={meta ? compact(meta.company_count) : "—"} sub="RUES" />
        <Kpi label="Sanciones" value={meta ? compact(meta.sanction_count) : "—"} sub="SIRI · FURAG" />
      </section>

      {meta && <SourceHealth meta={meta} />}

      <section className="mt-12">
        <div className="flex items-baseline justify-between pb-4">
          <div>
            <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
              Señales activas
            </div>
            <h2 className="mt-1 text-xl font-medium text-ink-50">
              Top por hits · {signals?.signals.length ?? 0} registradas
            </h2>
          </div>
          <Link to="/app/signals" className="text-[13px] text-lime-300 hover:text-lime-400">
            Ver catálogo →
          </Link>
        </div>

        {signalsQ.loading ? (
          <Skeleton rows={6} />
        ) : top.length === 0 ? (
          <Empty>Sin señales materializadas.</Empty>
        ) : (
          <div className="overflow-hidden rounded-lg border border-white/5">
            <table className="w-full">
              <tbody className="divide-y divide-white/5">
                {top.map((s) => (
                  <tr key={s.id}>
                    <td className="w-16 px-5 py-3.5">
                      <span className={clsx("font-mono text-[11px] uppercase", SEV_TONE[s.severity])}>
                        {s.severity}
                      </span>
                    </td>
                    <td className="py-3.5">
                      <Link to={`/app/signals/${s.id}`} className="block text-[14px] text-ink-50 hover:text-lime-300">
                        {s.title}
                      </Link>
                    </td>
                    <td className="hidden px-5 py-3.5 text-[12px] text-ink-400 md:table-cell">
                      {s.category}
                    </td>
                    <td className="px-5 py-3.5 text-right font-mono text-[13px] text-ink-200">
                      {compact(s.hit_count)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {metaQ.error && (
        <p className="mt-6 rounded-md border border-rose-400/30 bg-rose-400/5 px-4 py-3 font-mono text-[13px] text-rose-300">
          {metaQ.error.message}
        </p>
      )}
    </div>
  );
}

function Kpi({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-ink-950 px-5 py-6">
      <div className="text-[11px] uppercase tracking-[0.12em] text-ink-400">{label}</div>
      <div className="mt-2 font-mono text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
        {value}
      </div>
      {sub && <div className="mt-1 font-mono text-[11px] text-ink-500">{sub}</div>}
    </div>
  );
}

function SourceHealth({ meta }: { meta: PublicMetaResponse }) {
  const h = meta.source_health;
  const total = Math.max(h.implemented_sources, 1);
  const healthy = (h.healthy_sources / total) * 100;
  const stale = (h.stale_sources / total) * 100;
  return (
    <section className="mt-12">
      <div className="flex items-baseline justify-between pb-4">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Cobertura
          </div>
          <h2 className="mt-1 text-xl font-medium text-ink-50">
            {h.loaded_sources} / {h.implemented_sources} fuentes cargadas
          </h2>
        </div>
        <div className="font-mono text-[12px] text-ink-400">
          catálogo {h.data_sources}
        </div>
      </div>

      <div className="flex h-1 w-full overflow-hidden rounded-full bg-white/5">
        <div className="h-full bg-lime-400" style={{ width: `${healthy}%` }} />
        <div className="h-full bg-amber-400/70" style={{ width: `${stale}%` }} />
      </div>

      <dl className="mt-4 grid grid-cols-2 gap-6 md:grid-cols-4">
        <Stat label="Frescas" value={h.healthy_sources} tone="text-lime-300" />
        <Stat label="Rezagadas" value={h.stale_sources} tone="text-amber-300" />
        <Stat label="Implementadas" value={h.implemented_sources} />
        <Stat label="Catálogo" value={h.data_sources} />
      </dl>
    </section>
  );
}

function Stat({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return (
    <div>
      <div className="font-mono text-[11px] uppercase tracking-[0.1em] text-ink-500">{label}</div>
      <div className={clsx("mt-1 font-mono text-xl font-medium", tone ?? "text-ink-100")}>
        {value}
      </div>
    </div>
  );
}

function Skeleton({ rows }: { rows: number }) {
  return (
    <div className="overflow-hidden rounded-lg border border-white/5">
      <div className="divide-y divide-white/5">
        {Array.from({ length: rows }).map((_, i) => (
          <div key={i} className="h-12 animate-pulse bg-white/[0.015]" />
        ))}
      </div>
    </div>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-white/10 px-5 py-10 text-center text-[13px] text-ink-400">
      {children}
    </div>
  );
}

function compact(n: number): string {
  if (n >= 1_000_000) {
    const m = n / 1_000_000;
    return `${m >= 10 ? m.toFixed(0) : m.toFixed(1)}M`;
  }
  if (n >= 1_000) {
    const k = n / 1_000;
    return `${k >= 10 ? k.toFixed(0) : k.toFixed(1)}K`;
  }
  return String(n);
}
