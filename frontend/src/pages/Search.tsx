import { useCallback, useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { clsx } from "clsx";

import { searchEntities, type SearchResponse, type SearchResult } from "@/api/client";

const TYPES = [
  { value: "all", label: "Todos" },
  { value: "person", label: "Personas" },
  { value: "company", label: "Empresas" },
  { value: "contract", label: "Contratos" },
  { value: "sanction", label: "Sanciones" },
];

const TYPE_TONE: Record<string, string> = {
  person: "text-data-person",
  company: "text-data-company",
  contract: "text-data-contract",
  sanction: "text-data-sanction",
};

export function Search() {
  const [params, setParams] = useSearchParams();
  const q = params.get("q") ?? "";
  const type = params.get("type") ?? "all";

  const [draft, setDraft] = useState(q);
  const [typeDraft, setTypeDraft] = useState(type);
  const [res, setRes] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<Error | null>(null);

  useEffect(() => {
    setDraft(q);
    setTypeDraft(type);
  }, [q, type]);

  const run = useCallback((query: string, t: string) => {
    const trimmed = query.trim();
    if (!trimmed) {
      setRes(null);
      setErr(null);
      return;
    }
    setLoading(true);
    setErr(null);
    searchEntities(trimmed, t)
      .then(setRes)
      .catch((e: unknown) => {
        setErr(e instanceof Error ? e : new Error(String(e)));
        setRes(null);
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    run(q, type);
  }, [q, type, run]);

  function submit(e: { preventDefault: () => void }) {
    e.preventDefault();
    const next = new URLSearchParams();
    if (draft.trim()) next.set("q", draft.trim());
    if (typeDraft !== "all") next.set("type", typeDraft);
    setParams(next);
  }

  return (
    <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
        Search
      </div>
      <h1 className="mt-2 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
        Buscar en el grafo
      </h1>

      <form
        onSubmit={submit}
        className="mt-8 flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.02] p-1.5 focus-within:border-lime-300/40"
      >
        <input
          autoFocus
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="NIT, cédula, razón social, nombre…"
          className="flex-1 bg-transparent px-3 py-2 text-[15px] text-ink-50 placeholder:text-ink-500 focus:outline-none"
        />
        <select
          value={typeDraft}
          onChange={(e) => setTypeDraft(e.target.value)}
          className="rounded-md bg-transparent px-2 py-2 font-mono text-[12px] text-ink-200 focus:outline-none"
        >
          {TYPES.map((t) => (
            <option key={t.value} value={t.value} className="bg-ink-900">
              {t.label}
            </option>
          ))}
        </select>
        <button
          type="submit"
          disabled={loading || !draft.trim()}
          className="rounded-md bg-lime-300 px-4 py-2 text-[13px] font-medium text-ink-950 transition hover:bg-lime-400 disabled:opacity-50"
        >
          {loading ? "…" : "Buscar"}
        </button>
      </form>

      {err && (
        <p className="mt-6 rounded-md border border-rose-400/30 bg-rose-400/5 px-4 py-3 font-mono text-[13px] text-rose-300">
          {err.message}
        </p>
      )}

      {!q && <Tips />}

      {q && res && (
        <div className="mt-10">
          <div className="flex items-baseline justify-between pb-4 font-mono text-[12px] text-ink-400">
            <span>
              <span className="text-ink-50">{res.total}</span> resultados
            </span>
            <span>
              {res.results.length} mostrados · pág {res.page}
            </span>
          </div>
          {res.results.length === 0 ? (
            <Empty>Sin resultados. Prueba otra grafía.</Empty>
          ) : (
            <div className="overflow-hidden rounded-lg border border-white/5">
              <table className="w-full">
                <tbody className="divide-y divide-white/5">
                  {res.results.map((r) => (
                    <ResultRow key={r.id} r={r} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {q && loading && !res && (
        <p className="mt-6 font-mono text-[13px] text-ink-400">buscando…</p>
      )}
    </div>
  );
}

function ResultRow({ r }: { r: SearchResult }) {
  const source = r.sources[0]?.database;
  return (
    <tr className="group transition hover:bg-white/[0.02]">
      <td className="w-24 px-5 py-4">
        <span
          className={clsx(
            "font-mono text-[11px] uppercase",
            TYPE_TONE[r.type.toLowerCase()] ?? "text-ink-400",
          )}
        >
          {r.type}
        </span>
      </td>
      <td className="py-4">
        <Link to={`/app/analysis/${r.id}`} className="block">
          <div className="text-[14px] text-ink-50 group-hover:text-lime-300">{r.name}</div>
          <div className="mt-0.5 flex items-center gap-3 font-mono text-[11px] text-ink-500">
            {r.document && <span>{r.document}</span>}
            {source && <span>{source}</span>}
          </div>
        </Link>
      </td>
      <td className="px-5 py-4 text-right">
        <span className="font-mono text-[13px] text-ink-200">{r.score.toFixed(2)}</span>
      </td>
    </tr>
  );
}

function Tips() {
  return (
    <div className="mt-12 grid gap-4 md:grid-cols-2">
      {[
        ["NIT / cédula", "Con o sin puntos. El sistema normaliza antes de indexar."],
        ["Razón social", "Coincidencia parcial. Usa comillas para frase exacta."],
        ["Filtro por tipo", "Limita el espacio cuando sabes qué buscas."],
        ["Modo public_safe", "Personas privadas no aparecen. Solo PEPs y entidades jurídicas."],
      ].map(([k, v]) => (
        <div key={k} className="rounded-lg border border-white/5 p-5">
          <div className="font-mono text-[11px] uppercase tracking-[0.12em] text-lime-300">
            {k}
          </div>
          <p className="mt-2 text-[13px] leading-relaxed text-ink-400">{v}</p>
        </div>
      ))}
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
