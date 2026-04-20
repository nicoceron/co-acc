import { Link, useParams } from "react-router";
import { clsx } from "clsx";

import { listSignals, type SignalListItem } from "@/api/client";
import { useAsync } from "@/hooks/useAsync";
import { SECTORS, SECTOR_ORDER, type SectorDef } from "@/config/sectors";

const SEV_TONE: Record<SignalListItem["severity"], string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

export function Sector() {
  const { sectorId } = useParams();
  if (!sectorId) return <Index />;
  const def = SECTORS[sectorId];
  if (!def) return <NotFound id={sectorId} />;
  return <Detail def={def} />;
}

function Index() {
  const { data } = useAsync(listSignals, []);
  const counts = signalsByCategory(data?.signals ?? []);

  return (
    <Shell>
      <header className="pb-10">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
          Sectores
        </div>
        <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          Cobertura sectorial
        </h1>
        <p className="mt-4 max-w-[60ch] text-[15px] leading-relaxed text-ink-400">
          Ocho dominios que mapean los 177 datasets priorizados y los 30 anticorrupción de la PIDA.
        </p>
      </header>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {SECTOR_ORDER.map((id) => {
          const def = SECTORS[id];
          if (!def) return null;
          const sigs = def.signal_categories.reduce(
            (acc, cat) => acc + (counts.get(cat)?.signals ?? 0),
            0,
          );
          const hits = def.signal_categories.reduce(
            (acc, cat) => acc + (counts.get(cat)?.hits ?? 0),
            0,
          );
          return (
            <Link
              key={def.id}
              to={`/sector/${def.id}`}
              className="group flex flex-col gap-3 rounded-lg border border-white/5 bg-white/[0.01] p-5 transition hover:border-white/15 hover:bg-white/[0.02]"
            >
              <div className="font-mono text-[11px] uppercase tracking-[0.1em] text-lime-300">
                {def.id}
              </div>
              <div className="text-lg font-medium text-ink-50 group-hover:text-lime-300">
                {def.name}
              </div>
              <p className="line-clamp-3 text-[13px] leading-relaxed text-ink-400">
                {def.description}
              </p>
              <div className="mt-auto grid grid-cols-3 gap-3 border-t border-white/5 pt-4 font-mono text-[11px]">
                <Mini k="pida" v={def.pida_datasets} />
                <Mini k="señales" v={sigs} />
                <Mini k="hits" v={compact(hits)} />
              </div>
            </Link>
          );
        })}
      </div>
    </Shell>
  );
}

function Detail({ def }: { def: SectorDef }) {
  const { data, loading } = useAsync(listSignals, []);
  const all = data?.signals ?? [];
  const sectorSigs = all
    .filter((s) => def.signal_categories.includes(s.category))
    .sort((a, b) => b.hit_count - a.hit_count)
    .slice(0, 12);

  return (
    <Shell>
      <Link to="/sector" className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50">
        ← Sectores
      </Link>

      <header className="mt-6 pb-10">
        <div className="font-mono text-[11px] uppercase tracking-[0.1em] text-lime-300">
          {def.id}
        </div>
        <h1 className="mt-2 max-w-[22ch] text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          {def.name}
        </h1>
        <p className="mt-4 max-w-[60ch] text-[15px] leading-relaxed text-ink-300">
          {def.description}
        </p>
        <div className="mt-6 flex flex-wrap items-center gap-4 font-mono text-[12px] text-ink-400">
          <span>{def.custodian}</span>
          <span className="rounded-full border border-lime-300/30 bg-lime-300/5 px-2 py-0.5 text-[10px] uppercase tracking-wider text-lime-300">
            {def.pida_datasets} datasets PIDA
          </span>
        </div>
      </header>

      <div className="grid gap-6 md:grid-cols-2">
        <Panel title={`Fuentes · ${def.sources.length}`}>
          <ul className="space-y-2">
            {def.sources.map((s) => (
              <li key={s.id} className="flex items-baseline justify-between gap-3 border-b border-white/5 pb-2 last:border-0 last:pb-0">
                <span className="text-[13px] text-ink-100">{s.name}</span>
                {s.note && (
                  <span className="font-mono text-[11px] text-ink-500">{s.note}</span>
                )}
              </li>
            ))}
          </ul>
        </Panel>

        <Panel title={`Señales · ${sectorSigs.length}`}>
          {loading ? (
            <Dim>cargando…</Dim>
          ) : sectorSigs.length === 0 ? (
            <Dim>sin señales materializadas</Dim>
          ) : (
            <ul className="space-y-2">
              {sectorSigs.map((s) => (
                <li key={s.id}>
                  <Link
                    to={`/app/signals/${s.id}`}
                    className="group flex items-center gap-3 border-b border-white/5 py-2 last:border-0"
                  >
                    <span className={clsx("w-16 font-mono text-[10px] uppercase", SEV_TONE[s.severity])}>
                      {s.severity}
                    </span>
                    <span className="flex-1 text-[13px] text-ink-100 group-hover:text-lime-300">
                      {s.title}
                    </span>
                    <span className="font-mono text-[12px] text-lime-300">
                      {compact(s.hit_count)}
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>
    </Shell>
  );
}

function NotFound({ id }: { id: string }) {
  return (
    <Shell>
      <Link to="/sector" className="text-[13px] text-ink-400 hover:text-ink-50">
        ← Sectores
      </Link>
      <h1 className="mt-6 text-3xl font-medium text-ink-50">{id}</h1>
      <p className="mt-3 text-[14px] text-ink-400">Sector no encontrado.</p>
    </Shell>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">{children}</div>;
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

function Mini({ k, v }: { k: string; v: number | string }) {
  return (
    <div>
      <div className="text-lime-300">{v}</div>
      <div className="text-[10px] uppercase tracking-wider text-ink-500">{k}</div>
    </div>
  );
}

function Dim({ children }: { children: React.ReactNode }) {
  return <span className="font-mono text-[12px] text-ink-500">{children}</span>;
}

function signalsByCategory(signals: SignalListItem[]) {
  const m = new Map<string, { signals: number; hits: number }>();
  for (const s of signals) {
    const prev = m.get(s.category) ?? { signals: 0, hits: 0 };
    prev.signals += 1;
    prev.hits += s.hit_count;
    m.set(s.category, prev);
  }
  return m;
}

function compact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}
