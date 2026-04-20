import { Link, useParams } from "react-router";

export function CasoDetail() {
  const { slug } = useParams();
  return (
    <div className="mx-auto max-w-[1400px] px-6 py-10 md:px-10">
      <Link to="/casos" className="inline-flex items-center gap-1 text-[13px] text-ink-400 hover:text-ink-50">
        ← Casos
      </Link>

      <header className="mt-6 pb-10">
        <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-ink-400">
          Dossier no publicado
        </div>
        <h1 className="mt-2 max-w-[24ch] text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
          {slug ?? "Caso"}
        </h1>
        <p className="mt-4 max-w-[60ch] text-[15px] leading-relaxed text-ink-400">
          Este caso aún no tiene dossier editorial publicado. Mientras el equipo prepara las primeras entregas, puedes revisar las señales activas o la cobertura sectorial.
        </p>
      </header>

      <div className="flex items-start gap-3 rounded-lg border border-white/5 bg-white/[0.01] px-5 py-4">
        <span className="mt-1.5 h-1.5 w-1.5 rounded-full bg-lime-400" />
        <p className="text-[13px] leading-relaxed text-ink-300">
          Los dossiers enlazan señales cruzadas con evidencia oficial. Explora{" "}
          <Link to="/app/signals" className="text-lime-300 hover:text-lime-400">
            señales activas
          </Link>{" "}
          o la{" "}
          <Link to="/sector" className="text-lime-300 hover:text-lime-400">
            cobertura sectorial
          </Link>{" "}
          mientras se publican.
        </p>
      </div>
    </div>
  );
}
