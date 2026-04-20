import { Link } from "react-router";
import { clsx } from "clsx";

import { getPublicMeta, listSignals } from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const SEV_TONE: Record<string, string> = {
  low: "text-sky-300",
  medium: "text-amber-300",
  high: "text-orange-300",
  critical: "text-rose-300",
};

export function Casos() {
  const metaQ = useAsync(getPublicMeta, []);
  const signalsQ = useAsync(listSignals, []);

  const signals = (signalsQ.data?.signals ?? [])
    .filter((s) => s.public_safe && s.hit_count > 0)
    .sort((a, b) => b.hit_count - a.hit_count)
    .slice(0, 6);

  const meta = metaQ.data;

  return (
    <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">
      <header className="pb-10">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
          Archivo · Casos publicados
        </div>
        <h1 className="mt-2 max-w-[22ch] text-3xl font-medium tracking-tight text-ink-50 md:text-5xl">
          Dossiers documentales públicos
        </h1>
        <p className="mt-5 max-w-[62ch] text-[15px] leading-relaxed text-ink-400">
          Cruces de contratación, declaraciones, sanciones, regalías y ambientales con evidencia trazable al documento original.
        </p>
        {meta && (
          <div className="mt-8 grid grid-cols-2 gap-px overflow-hidden rounded-lg border border-white/5 bg-white/5 md:grid-cols-4">
            <Stat v={compact(meta.contract_count)} k="contratos" />
            <Stat v={compact(meta.company_count)} k="empresas" />
            <Stat v={compact(meta.sanction_count)} k="sanciones" />
            <Stat
              v={`${meta.source_health.loaded_sources}/${meta.source_health.data_sources}`}
              k="fuentes activas"
            />
          </div>
        )}
      </header>

      <div className="mb-12 flex items-start gap-3 rounded-lg border border-white/5 bg-white/[0.01] px-5 py-4">
        <span className="mt-1.5 h-1.5 w-1.5 rounded-full bg-lime-400" />
        <p className="text-[13px] leading-relaxed text-ink-300">
          El archivo está en curaduría. Mientras tanto, inspecciona las{" "}
          <Link to="/app/signals" className="text-lime-300 hover:text-lime-400">
            señales públicas activas
          </Link>{" "}
          que alimentan los próximos dossiers, o explora la{" "}
          <Link to="/sector" className="text-lime-300 hover:text-lime-400">
            cobertura sectorial
          </Link>
          .
        </p>
      </div>

      <section className="pb-16">
        <div className="pb-6">
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Señales · base documental
          </div>
          <h2 className="mt-2 text-xl font-medium text-ink-50">Qué estamos rastreando</h2>
          <p className="mt-2 max-w-[60ch] text-[14px] leading-relaxed text-ink-400">
            Señales públicas con más hits en la última corrida. Cada hit es un candidato a caso.
          </p>
        </div>

        {signalsQ.loading ? (
          <p className="font-mono text-[13px] text-ink-400">cargando…</p>
        ) : signals.length === 0 ? (
          <Empty>Aún no hay señales públicas con hits materializados.</Empty>
        ) : (
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {signals.map((s) => (
              <Link
                key={s.id}
                to={`/app/signals/${s.id}`}
                className="group flex flex-col gap-3 rounded-lg border border-white/5 bg-white/[0.01] p-5 transition hover:border-white/15 hover:bg-white/[0.02]"
              >
                <div className="flex items-center justify-between font-mono text-[11px]">
                  <span className={clsx("uppercase", SEV_TONE[s.severity] ?? "text-ink-400")}>
                    {s.severity}
                  </span>
                  <span className="rounded-full border border-lime-300/30 bg-lime-300/5 px-2 py-0.5 text-[10px] uppercase tracking-wider text-lime-300">
                    público
                  </span>
                </div>
                <div className="text-[15px] text-ink-50 group-hover:text-lime-300">{s.title}</div>
                <p className="line-clamp-3 text-[13px] leading-relaxed text-ink-400">
                  {s.description}
                </p>
                <div className="mt-auto flex items-baseline justify-between border-t border-white/5 pt-3 font-mono text-[11px]">
                  <span className="text-lime-300">{compact(s.hit_count)} hits</span>
                  <span className="text-ink-500">{s.category}</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </section>

      <section className="pb-16">
        <div className="pb-6">
          <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
            Metodología
          </div>
          <h2 className="mt-2 text-xl font-medium text-ink-50">Cómo se construye un caso</h2>
        </div>

        <div className="grid gap-px overflow-hidden rounded-lg border border-white/5 bg-white/5 md:grid-cols-3">
          <Method
            n="01"
            k="Rastreo"
            title="Normalización masiva"
            body="Ingestamos SECOP I/II, SIGEP, SIRI, SIREC, SGR/BPIN y fuentes sectoriales en un lago DuckDB particionado. Cada registro conserva su referencia al documento original."
          />
          <Method
            n="02"
            k="Cruce"
            title="Señales cruzadas"
            body="Patrones versionados detectan coincidencias entre sanciones, adjudicaciones, declaraciones y ejecución. Cada hit trae identidad, evidencia y ruta reproducible."
          />
          <Method
            n="03"
            k="Curaduría"
            title="Edición y publicación"
            body="Revisores enlazan señales, arman la línea de tiempo, verifican identidades y publican el dossier con enlaces permanentes a la evidencia."
          />
        </div>
      </section>
    </div>
  );
}

function Stat({ v, k }: { v: string; k: string }) {
  return (
    <div className="bg-ink-950 px-5 py-5">
      <div className="font-mono text-2xl font-medium text-ink-50 md:text-3xl">{v}</div>
      <div className="mt-1 font-mono text-[11px] uppercase tracking-wider text-ink-500">{k}</div>
    </div>
  );
}

function Method({ n, k, title, body }: { n: string; k: string; title: string; body: string }) {
  return (
    <article className="bg-ink-950 p-6">
      <div className="flex items-center gap-3 font-mono text-[11px] uppercase tracking-[0.12em]">
        <span className="text-lime-300">{n}</span>
        <span className="text-ink-500">· {k}</span>
      </div>
      <div className="mt-4 text-[16px] font-medium text-ink-50">{title}</div>
      <p className="mt-2 text-[13px] leading-relaxed text-ink-400">{body}</p>
    </article>
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
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}
