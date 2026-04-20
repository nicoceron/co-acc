import { Link } from "react-router";

import { getPublicMeta, type PublicMetaResponse } from "@/api/client";
import { useAsync } from "@/hooks/useAsync";

const SOURCES: { name: string; desc: string; metric?: keyof MetricMap }[] = [
  { name: "SECOP I · II · TVEC", desc: "Contratación pública", metric: "contracts" },
  { name: "RUES", desc: "Registro empresarial", metric: "companies" },
  { name: "SIRI", desc: "Sanciones disciplinarias", metric: "sanctions" },
  { name: "SIGEP", desc: "Servidores públicos" },
  { name: "Ley 2013", desc: "Declaración de bienes" },
  { name: "SGR · BPIN", desc: "Regalías e inversión" },
  { name: "ANLA · SIAC", desc: "Licencias ambientales" },
  { name: "ANH · ANM", desc: "Hidrocarburos y minería" },
  { name: "CGR · SIREC", desc: "Responsabilidad fiscal" },
  { name: "ICFES · MinEducación", desc: "Educación" },
];

type MetricMap = {
  contracts: number;
  companies: number;
  sanctions: number;
};

export function Landing() {
  const { data } = useAsync(getPublicMeta, []);

  return (
    <div className="mx-auto max-w-[1400px] px-6 md:px-10">
      <section className="flex min-h-[calc(100vh-3.5rem)] flex-col justify-center py-20">
        <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-ink-400">
          <span className="h-1.5 w-1.5 rounded-full bg-lime-400" />
          Open graph · Colombia
        </div>

        <h1 className="mt-8 max-w-[22ch] text-[56px] font-medium leading-[1.02] tracking-[-0.03em] text-ink-50 md:text-[84px]">
          Los datos públicos<br />
          de Colombia,<br />
          <span className="text-ink-400">conectados.</span>
        </h1>

        <p className="mt-8 max-w-[56ch] text-[17px] leading-relaxed text-ink-300">
          Un lago y un grafo abiertos sobre los registros oficiales fragmentados —
          contratación, cargos, sanciones, regalías. Sin puntajes. Sin acusaciones.
          Solo el contexto documental cruzado.
        </p>

        <div className="mt-10 flex flex-wrap items-center gap-3">
          <Link
            to="/app/search"
            className="group inline-flex items-center gap-2 rounded-full bg-lime-300 px-5 py-2.5 text-[13px] font-medium text-ink-950 transition hover:bg-lime-400"
          >
            Explorar el grafo
            <span className="transition group-hover:translate-x-0.5">→</span>
          </Link>
          <a
            href="https://github.com/nicoceron/co-acc"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-2 rounded-full border border-white/10 px-5 py-2.5 text-[13px] text-ink-200 transition hover:border-white/20 hover:text-ink-50"
          >
            Ver código ↗
          </a>
        </div>
      </section>

      <section className="grid grid-cols-2 gap-x-6 gap-y-10 border-y border-white/5 py-10 md:grid-cols-4">
        <Metric value={data ? compact(data.contract_count) : "—"} label="Contratos indexados" />
        <Metric value={data ? compact(data.company_count) : "—"} label="Empresas" />
        <Metric value={data ? compact(data.sanction_count) : "—"} label="Sanciones" />
        <Metric
          value={
            data ? `${data.source_health.loaded_sources}/${data.source_health.data_sources}` : "—"
          }
          label="Fuentes activas"
        />
      </section>

      <section className="py-24 md:py-32">
        <div className="grid gap-12 md:grid-cols-[280px_1fr] md:gap-16">
          <div>
            <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-lime-300">
              01 · Qué
            </span>
            <h2 className="mt-4 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
              Grafo de evidencia, no oráculo.
            </h2>
          </div>
          <div className="space-y-8 text-[15px] leading-relaxed text-ink-300 md:pt-2">
            <p>
              Normaliza SECOP, SIGEP, SIRI, RUES, Ley&nbsp;2013, SGR/BPIN, ANLA, ANH y diez
              fuentes más en un lago columnar. Un grafo de identidad enlaza personas,
              empresas, contratos, sanciones y declaraciones — con fuente y fecha en cada
              nodo.
            </p>
            <p className="text-ink-400">
              43 señales de cruce — concentración de proveedores, ventanas cortas, co-licitación,
              PEP adjudicado — cada hit con su paquete documental. El usuario decide qué significa.
            </p>
          </div>
        </div>
      </section>

      <section className="border-t border-white/5 py-24 md:py-32">
        <div className="grid gap-12 md:grid-cols-[280px_1fr] md:gap-16">
          <div>
            <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-lime-300">
              02 · Fuentes
            </span>
            <h2 className="mt-4 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
              Registros oficiales cruzados.
            </h2>
          </div>

          <div className="overflow-hidden rounded-lg border border-white/5">
            <table className="w-full">
              <tbody className="divide-y divide-white/5">
                {SOURCES.map((s) => (
                  <tr key={s.name} className="transition hover:bg-white/[0.02]">
                    <td className="w-[45%] px-5 py-4 text-[14px] text-ink-100">{s.name}</td>
                    <td className="px-5 py-4 text-[13px] text-ink-400">{s.desc}</td>
                    <td className="px-5 py-4 text-right font-mono text-[13px] text-lime-300">
                      {sourceMetric(s.metric, data)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="border-t border-white/5 py-24 md:py-32">
        <div className="grid gap-12 md:grid-cols-[280px_1fr] md:gap-16">
          <div>
            <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-lime-300">
              03 · Cómo
            </span>
            <h2 className="mt-4 text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
              De la gaceta al grafo.
            </h2>
          </div>
          <ol className="space-y-10 md:pt-2">
            {[
              {
                title: "Ingestión reproducible",
                body: "112 pipelines con watermark, hash y URL pública por registro. Lago columnar particionado.",
              },
              {
                title: "Identidad enlazada",
                body: "Neo4j como columna de relaciones. Cada nodo lleva su fuente, fecha y documento.",
              },
              {
                title: "Consulta pública",
                body: "public_safe por defecto: sin documentos de personas privadas. Reviewer desbloquea el workspace.",
              },
            ].map((step, i) => (
              <li key={step.title} className="grid grid-cols-[40px_1fr] gap-4">
                <span className="font-mono text-[13px] text-ink-500">0{i + 1}</span>
                <div>
                  <div className="text-[15px] font-medium text-ink-50">{step.title}</div>
                  <p className="mt-1 text-[14px] leading-relaxed text-ink-400">{step.body}</p>
                </div>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <footer className="flex flex-col gap-6 border-t border-white/5 py-10 md:flex-row md:items-center md:justify-between">
        <div className="font-mono text-[12px] text-ink-500">
          AGPL v3 · public_safe · auditable
        </div>
        <div className="flex gap-5 text-[13px] text-ink-400">
          <Link to="/casos" className="hover:text-ink-50">Casos</Link>
          <Link to="/sector" className="hover:text-ink-50">Sectores</Link>
          <Link to="/app" className="hover:text-ink-50">Workspace</Link>
          <a href="https://github.com/nicoceron/co-acc" target="_blank" rel="noreferrer" className="hover:text-ink-50">
            GitHub
          </a>
        </div>
      </footer>
    </div>
  );
}

function Metric({ value, label }: { value: string; label: string }) {
  return (
    <div>
      <div className="font-mono text-3xl font-medium tracking-tight text-ink-50 md:text-4xl">
        {value}
      </div>
      <div className="mt-1.5 text-[12px] text-ink-400">{label}</div>
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

function sourceMetric(key: keyof MetricMap | undefined, meta: PublicMetaResponse | null): string {
  if (!meta || !key) return "—";
  const map: MetricMap = {
    contracts: meta.contract_count,
    companies: meta.company_count,
    sanctions: meta.sanction_count,
  };
  return compact(map[key]);
}
