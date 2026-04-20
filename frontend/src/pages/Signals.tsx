import { useMemo, useState } from "react";
import { Link, useParams } from "react-router";
import { clsx } from "clsx";

import {
  getSignal,
  listSignals,
  type SignalDetailResponse,
  type SignalListItem,
  type SignalListResponse,
} from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const SEVERITIES = ["low", "medium", "high", "critical"] as const;
type Severity = (typeof SEVERITIES)[number];

const SEV_TONE: Record<Severity, string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

export function Signals() {
  const { signalId } = useParams<{ signalId?: string }>();
  const listQ = useAsync(listSignals, []);
  const detailQ = useAsync(
    () => (signalId ? getSignal(signalId) : Promise.resolve(null)),
    [signalId],
  );

  if (signalId) {
    if (detailQ.loading) return <Shell>{<Loading />}</Shell>;
    if (detailQ.error) return <Shell><Err msg={detailQ.error.message} /></Shell>;
    if (detailQ.data) return <Detail data={detailQ.data} />;
  }

  if (listQ.loading) return <Shell>{<Loading />}</Shell>;
  if (listQ.error) return <Shell><Err msg={listQ.error.message} /></Shell>;
  if (listQ.data) return <Catalog data={listQ.data} />;
  return null;
}

function Catalog({ data }: { data: SignalListResponse }) {
  const [sev, setSev] = useState<"all" | Severity>("all");
  const [onlyPublic, setOnlyPublic] = useState(false);
  const [q, setQ] = useState("");

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return data.signals
      .filter((s) => sev === "all" || s.severity === sev)
      .filter((s) => !onlyPublic || s.public_safe)
      .filter((s) =>
        !needle
          ? true
          : `${s.id} ${s.title} ${s.description} ${s.category}`.toLowerCase().includes(needle),
      )
      .sort((a, b) => b.hit_count - a.hit_count);
  }, [data.signals, sev, onlyPublic, q]);

  return (
    <Shell>
      <div className="flex flex-wrap items-baseline justify-between gap-4 pb-8">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Catálogo
          </div>
          <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
            Señales
          </h1>
        </div>
        <div className="flex gap-4 font-mono text-[12px] text-ink-400">
          <span>{data.signals.length} registradas</span>
          <span>v{data.registry_version}</span>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2 pb-6">
        <Pill active={sev === "all"} onClick={() => setSev("all")}>
          Todas
        </Pill>
        {SEVERITIES.map((s) => (
          <Pill key={s} active={sev === s} onClick={() => setSev(s)}>
            {s}
          </Pill>
        ))}
        <span className="mx-2 h-4 w-px bg-white/10" />
        <Pill active={onlyPublic} onClick={() => setOnlyPublic((v) => !v)}>
          Solo públicas
        </Pill>
        <div className="ml-auto">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Buscar…"
            className="w-64 rounded-md border border-white/10 bg-transparent px-3 py-1.5 text-[13px] text-ink-100 placeholder:text-ink-500 focus:border-lime-300/40 focus:outline-none"
          />
        </div>
      </div>

      {filtered.length === 0 ? (
        <Empty>Sin coincidencias.</Empty>
      ) : (
        <div className="overflow-hidden rounded-lg border border-white/5">
          <table className="w-full">
            <tbody className="divide-y divide-white/5">
              {filtered.map((s) => (
                <Row key={s.id} s={s} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Shell>
  );
}

function Row({ s }: { s: SignalListItem }) {
  return (
    <tr className="group transition hover:bg-white/[0.02]">
      <td className="w-20 px-5 py-4 align-top">
        <span className={clsx("font-mono text-[11px] uppercase", SEV_TONE[s.severity])}>
          {s.severity}
        </span>
      </td>
      <td className="py-4 align-top">
        <Link to={`/app/signals/${s.id}`} className="block">
          <div className="text-[14px] text-ink-50 group-hover:text-lime-300">{s.title}</div>
          <div className="mt-0.5 line-clamp-1 text-[12px] text-ink-400">{s.description}</div>
        </Link>
      </td>
      <td className="hidden px-5 py-4 align-top font-mono text-[11px] text-ink-500 md:table-cell">
        {s.category}
      </td>
      <td className="px-5 py-4 text-right align-top font-mono text-[13px] text-ink-200">
        {compact(s.hit_count)}
      </td>
      <td className="hidden w-24 px-5 py-4 text-right align-top md:table-cell">
        {s.public_safe ? (
          <span className="font-mono text-[10px] uppercase tracking-wide text-lime-300">
            public
          </span>
        ) : (
          <span className="font-mono text-[10px] uppercase tracking-wide text-ink-500">
            reviewer
          </span>
        )}
      </td>
    </tr>
  );
}

function Detail({ data }: { data: SignalDetailResponse }) {
  const def = data.definition;
  return (
    <Shell>
      <Link to="/app/signals" className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50">
        ← Catálogo
      </Link>

      <header className="mt-6 pb-10">
        <div className="flex flex-wrap items-center gap-3 text-[12px]">
          <span className={clsx("font-mono uppercase", SEV_TONE[def.severity])}>
            {def.severity}
          </span>
          <span className="font-mono text-ink-500">{def.category}</span>
          {def.public_safe && (
            <span className="rounded-full border border-lime-300/30 bg-lime-300/5 px-2 py-0.5 font-mono text-[10px] uppercase tracking-wider text-lime-300">
              public
            </span>
          )}
        </div>
        <h1 className="mt-4 max-w-[24ch] text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          {def.title}
        </h1>
        <p className="mt-4 max-w-[60ch] text-[15px] leading-relaxed text-ink-300">
          {def.description}
        </p>
        <div className="mt-6 flex flex-wrap gap-5 font-mono text-[12px] text-ink-500">
          <span>{def.id}</span>
          <span>v{def.version}</span>
          <span>scope · {def.scope_type}</span>
          {def.runner && <span>runner · {def.runner.kind}:{def.runner.ref}</span>}
        </div>
      </header>

      <div className="grid gap-6 md:grid-cols-2">
        <Panel title="Fuentes">
          {def.sources_required.length === 0 ? (
            <Dim>sin fuentes</Dim>
          ) : (
            <Tags items={def.sources_required} />
          )}
        </Panel>

        <Panel title="Entidades">
          {def.entity_types.length === 0 ? <Dim>universo abierto</Dim> : <Tags items={def.entity_types} />}
        </Panel>

        <Panel title="Política">
          <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2 font-mono text-[12px]">
            <Kv k="public_safe" v={def.public_safe ? "sí" : "no"} />
            <Kv k="reviewer_only" v={def.reviewer_only ? "sí" : "no"} />
            <Kv k="identity" v={def.requires_identity.join(", ") || "—"} />
            <Kv k="dedup" v={def.dedup_fields.join(", ") || "—"} />
            {def.pattern_id && <Kv k="pattern" v={def.pattern_id} />}
          </dl>
        </Panel>

        <Panel title={`Hits · ${data.sample_hits.length}`}>
          {data.sample_hits.length === 0 ? (
            <Dim>sin hits materializados</Dim>
          ) : (
            <ul className="space-y-3">
              {data.sample_hits.slice(0, 6).map((hit) => (
                <li key={hit.hit_id} className="flex items-baseline justify-between gap-4 border-b border-white/5 pb-3 last:border-0 last:pb-0">
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-[13px] text-ink-100">
                      {hit.entity_label || hit.entity_key}
                    </div>
                    <div className="mt-0.5 font-mono text-[11px] text-ink-500">
                      {hit.evidence_count} ev · conf {hit.identity_confidence.toFixed(2)}
                    </div>
                  </div>
                  <span className="font-mono text-[12px] text-lime-300">
                    {hit.score.toFixed(2)}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>
    </Shell>
  );
}

function Shell({ children }: { children: React.ReactNode }) {
  return <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">{children}</div>;
}

function Pill({
  children,
  active,
  onClick,
}: {
  children: React.ReactNode;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={clsx(
        "rounded-full border px-3 py-1 font-mono text-[11px] uppercase tracking-wider transition",
        active
          ? "border-lime-300/40 bg-lime-300/10 text-lime-300"
          : "border-white/10 text-ink-400 hover:border-white/20 hover:text-ink-100",
      )}
    >
      {children}
    </button>
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

function Tags({ items }: { items: string[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {items.map((t) => (
        <span
          key={t}
          className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 font-mono text-[11px] text-ink-200"
        >
          {t}
        </span>
      ))}
    </div>
  );
}

function Kv({ k, v }: { k: string; v: string }) {
  return (
    <>
      <dt className="text-ink-500">{k}</dt>
      <dd className="text-ink-100">{v}</dd>
    </>
  );
}

function Dim({ children }: { children: React.ReactNode }) {
  return <span className="font-mono text-[12px] text-ink-500">{children}</span>;
}

function Loading() {
  return (
    <div className="flex items-center gap-2 font-mono text-[13px] text-ink-400">
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-lime-400" />
      cargando…
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
