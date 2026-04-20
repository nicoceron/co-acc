import { useMemo, useState } from "react";
import { Link, useParams } from "react-router";
import { clsx } from "clsx";

import {
  getCase,
  listCases,
  refreshCase,
  type CaseResponse,
  type CaseSummary,
  type SignalHit,
} from "@/api/client";
import { IS_PUBLIC_MODE } from "@/config/runtime";
import { useAsync } from "@/hooks/useAsync";

type StatusFilter = "all" | "draft" | "review" | "published" | "archived";

const STATUS_FILTERS: { value: StatusFilter; label: string }[] = [
  { value: "all", label: "Todos" },
  { value: "draft", label: "Borradores" },
  { value: "review", label: "Revisión" },
  { value: "published", label: "Publicados" },
  { value: "archived", label: "Archivados" },
];

const STATUS_TONE: Record<string, string> = {
  draft: "text-ink-400",
  review: "text-amber-300",
  published: "text-lime-300",
  archived: "text-ink-500",
};

const SEV_TONE: Record<string, string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

export function Cases() {
  const { caseId } = useParams();
  if (caseId) return <CaseDetail caseId={caseId} />;
  return <CaseList />;
}

function CaseList() {
  const state = useAsync(() => listCases(1, 100), []);
  const [status, setStatus] = useState<StatusFilter>("all");
  const [query, setQuery] = useState("");

  const isAuthGate = state.error instanceof Error && /401|403|unauthor/i.test(state.error.message);

  const filtered = useMemo<CaseSummary[]>(() => {
    const cases = state.data?.cases ?? [];
    const q = query.trim().toLowerCase();
    return cases.filter((c) => {
      if (status !== "all" && c.status !== status) return false;
      if (!q) return true;
      return (
        c.title.toLowerCase().includes(q) ||
        (c.description ?? "").toLowerCase().includes(q) ||
        c.id.toLowerCase().includes(q)
      );
    });
  }, [state.data, status, query]);

  const total = state.data?.total ?? 0;
  const publishedCount = (state.data?.cases ?? []).filter((c) => c.status === "published").length;

  return (
    <Shell>
      <header className="flex flex-wrap items-baseline justify-between gap-4 pb-8">
        <div>
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Workspace · Casos
          </div>
          <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
            Casos e investigaciones
          </h1>
          <p className="mt-4 max-w-[60ch] text-[14px] leading-relaxed text-ink-400">
            Investigaciones que combinan señales cruzadas, evidencia y narrativa. Los publicados pasan al archivo.
          </p>
        </div>
        {!isAuthGate && state.data && (
          <div className="flex gap-4 font-mono text-[12px] text-ink-400">
            <span>{total} total</span>
            <span>{publishedCount} publicados</span>
          </div>
        )}
      </header>

      {isAuthGate ? (
        <AuthGate />
      ) : (
        <>
          <div className="flex flex-wrap items-center gap-2 pb-6">
            {STATUS_FILTERS.map((f) => (
              <Pill key={f.value} active={status === f.value} onClick={() => setStatus(f.value)}>
                {f.label}
              </Pill>
            ))}
            <div className="ml-auto">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Buscar…"
                className="w-64 rounded-md border border-white/10 bg-transparent px-3 py-1.5 text-[13px] text-ink-100 placeholder:text-ink-500 focus:border-lime-300/40 focus:outline-none"
              />
            </div>
          </div>

          {state.loading ? (
            <Loading>cargando casos…</Loading>
          ) : state.error ? (
            <Err msg={state.error.message} />
          ) : filtered.length === 0 ? (
            <Empty>
              {query || status !== "all"
                ? "Sin resultados para los filtros actuales."
                : "Aún no hay casos registrados."}
            </Empty>
          ) : (
            <div className="overflow-hidden rounded-lg border border-white/5">
              <table className="w-full">
                <tbody className="divide-y divide-white/5">
                  {filtered.map((c) => (
                    <CaseRow key={c.id} c={c} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </Shell>
  );
}

function CaseRow({ c }: { c: CaseSummary }) {
  return (
    <tr className="group transition hover:bg-white/[0.02]">
      <td className="w-28 px-5 py-4 align-top">
        <span className={clsx("font-mono text-[11px] uppercase", STATUS_TONE[c.status] ?? "text-ink-400")}>
          {c.status}
        </span>
        {c.stale && (
          <span className="ml-2 font-mono text-[10px] uppercase text-amber-300/80">stale</span>
        )}
      </td>
      <td className="py-4 align-top">
        <Link to={`/app/cases/${c.id}`} className="block">
          <div className="text-[14px] text-ink-50 group-hover:text-lime-300">{c.title}</div>
          {c.description && (
            <div className="mt-0.5 line-clamp-1 text-[12px] text-ink-400">{c.description}</div>
          )}
        </Link>
      </td>
      <td className="hidden w-56 px-5 py-4 text-right align-top font-mono text-[11px] text-ink-500 md:table-cell">
        <span>{c.entity_ids.length} ent</span>
        <span className="mx-2">·</span>
        <span>{c.signal_count} sig</span>
        <span className="mx-2">·</span>
        <span className="text-lime-300">{c.public_signal_count} pub</span>
      </td>
      <td className="hidden px-5 py-4 text-right align-top font-mono text-[11px] text-ink-500 lg:table-cell">
        {formatDate(c.updated_at)}
      </td>
    </tr>
  );
}

function CaseDetail({ caseId }: { caseId: string }) {
  const state = useAsync(() => getCase(caseId), [caseId]);
  const [refreshing, setRefreshing] = useState(false);
  const [localCase, setLocalCase] = useState<CaseResponse | null>(null);

  const isAuthGate = state.error instanceof Error && /401|403|unauthor/i.test(state.error.message);
  const c = localCase ?? state.data;

  async function handleRefresh() {
    if (!c) return;
    setRefreshing(true);
    try {
      const refreshed = await refreshCase(c.id);
      setLocalCase(refreshed);
    } catch (err) {
      console.error(err);
    } finally {
      setRefreshing(false);
    }
  }

  return (
    <Shell>
      <Link to="/app/cases" className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50">
        ← Casos
      </Link>

      {isAuthGate ? (
        <div className="mt-6">
          <AuthGate />
        </div>
      ) : state.loading ? (
        <div className="mt-6">
          <Loading>cargando caso…</Loading>
        </div>
      ) : state.error ? (
        <div className="mt-6">
          <Err msg={state.error.message} />
        </div>
      ) : c ? (
        <>
          <header className="mt-6 pb-10">
            <div className="flex flex-wrap items-center gap-3 text-[12px]">
              <span className={clsx("font-mono uppercase", STATUS_TONE[c.status] ?? "text-ink-400")}>
                {c.status}
              </span>
              {c.stale && (
                <span className="font-mono text-[10px] uppercase text-amber-300/80">stale</span>
              )}
              <span className="font-mono text-ink-500">{c.id}</span>
            </div>
            <h1 className="mt-4 max-w-[30ch] text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
              {c.title}
            </h1>
            {c.description && (
              <p className="mt-4 max-w-[60ch] text-[15px] leading-relaxed text-ink-300">
                {c.description}
              </p>
            )}
            <div className="mt-6 flex flex-wrap items-center gap-5 font-mono text-[12px] text-ink-400">
              <span>{c.entity_ids.length} entidades</span>
              <span>{c.signal_count} señales</span>
              <span className="text-lime-300">{c.public_signal_count} públicas</span>
              <span>actualizado {formatDate(c.updated_at)}</span>
              <button
                type="button"
                onClick={handleRefresh}
                disabled={refreshing}
                className="ml-auto rounded-md border border-lime-300/40 bg-lime-300/5 px-3 py-1 text-[12px] text-lime-300 transition hover:bg-lime-300/10 disabled:opacity-50"
              >
                {refreshing ? "refrescando…" : "refrescar señales"}
              </button>
            </div>
          </header>

          <div className="grid gap-6 md:grid-cols-2">
            <Panel title={`Señales · ${c.signals.length}`}>
              {c.signals.length === 0 ? (
                <Dim>sin señales asociadas</Dim>
              ) : (
                <ul className="space-y-3">
                  {c.signals.slice(0, 20).map((s) => (
                    <SignalRowItem key={s.hit_id} s={s} />
                  ))}
                </ul>
              )}
            </Panel>

            <Panel title={`Línea de tiempo · ${c.events.length}`}>
              {c.events.length === 0 ? (
                <Dim>sin eventos</Dim>
              ) : (
                <ul className="space-y-2">
                  {c.events.slice(0, 24).map((e) => (
                    <li key={e.id} className="flex items-baseline gap-4 border-b border-white/5 pb-2 last:border-0 last:pb-0">
                      <span className="w-24 font-mono text-[11px] text-ink-500">{formatDate(e.date)}</span>
                      <span className="flex-1 text-[13px] text-ink-100">{e.label}</span>
                      <span className="font-mono text-[11px] text-ink-400">{e.type}</span>
                    </li>
                  ))}
                </ul>
              )}
            </Panel>

            <div className="md:col-span-2">
              <Panel title={`Evidencia · ${c.evidence_bundles.length}`}>
                {c.evidence_bundles.length === 0 ? (
                  <Dim>sin paquetes</Dim>
                ) : (
                  <ul className="grid gap-3 md:grid-cols-2">
                    {c.evidence_bundles.slice(0, 12).map((b) => (
                      <li key={b.bundle_id} className="rounded-md border border-white/5 p-3">
                        <div className="text-[13px] text-ink-100">{b.headline}</div>
                        <div className="mt-1 font-mono text-[11px] text-ink-500">
                          {b.evidence_items.length} docs
                        </div>
                        {b.source_list.length > 0 && (
                          <div className="mt-2 flex flex-wrap gap-1">
                            {b.source_list.map((src) => (
                              <span
                                key={src}
                                className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 font-mono text-[10px] text-ink-300"
                              >
                                {src}
                              </span>
                            ))}
                          </div>
                        )}
                      </li>
                    ))}
                  </ul>
                )}
              </Panel>
            </div>
          </div>
        </>
      ) : null}
    </Shell>
  );
}

function SignalRowItem({ s }: { s: SignalHit }) {
  return (
    <li>
      <Link
        to={`/app/signals/${s.signal_id}`}
        className="group block border-b border-white/5 pb-3 last:border-0 last:pb-0"
      >
        <div className="flex items-center gap-3 font-mono text-[11px]">
          <span className={clsx("uppercase", SEV_TONE[s.severity] ?? "text-ink-400")}>
            {s.severity}
          </span>
          <span className="ml-auto text-lime-300">{(s.score ?? 0).toFixed(2)}</span>
          <span className="text-ink-500">{s.evidence_count} ev</span>
        </div>
        <div className="mt-1 text-[13px] text-ink-100 group-hover:text-lime-300">{s.title}</div>
        <div className="mt-0.5 font-mono text-[11px] text-ink-500">
          {s.entity_label ?? s.entity_key}
          {s.last_seen_at && ` · ${formatDate(s.last_seen_at)}`}
        </div>
      </Link>
    </li>
  );
}

function AuthGate() {
  return (
    <div className="mt-6 rounded-lg border border-white/5 bg-white/[0.01] p-8">
      <div className="font-mono text-[11px] uppercase tracking-[0.12em] text-amber-300">
        reviewer_only
      </div>
      <h2 className="mt-3 max-w-[30ch] text-xl font-medium text-ink-50">
        Esta sección requiere autenticación de revisor
      </h2>
      <p className="mt-3 max-w-[60ch] text-[14px] leading-relaxed text-ink-400">
        Los casos combinan señales públicas y privadas. En modo{" "}
        <code className="font-mono text-ink-200">public_safe</code> sólo se exponen casos
        publicados al archivo{" "}
        <Link to="/casos" className="text-lime-300 hover:text-lime-400">
          /casos
        </Link>
        .
      </p>
      {!IS_PUBLIC_MODE && (
        <Link to="/login" className="mt-5 inline-block text-[13px] text-lime-300 hover:text-lime-400">
          Iniciar sesión →
        </Link>
      )}
    </div>
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

function Dim({ children }: { children: React.ReactNode }) {
  return <span className="font-mono text-[12px] text-ink-500">{children}</span>;
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

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("es-CO", { year: "numeric", month: "short", day: "2-digit" });
}
