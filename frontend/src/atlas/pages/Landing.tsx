import { ArrowRight, Database, Gauge, Search, ShieldCheck } from "lucide-react";
import type { ReactNode } from "react";
import { Link } from "react-router";

import { DataStatus, Eyebrow, Frame, MetricTile, Pill, Rule } from "../components/ui";
import { useAtlasOverview } from "../lib/useAtlasData";

function Pillar({ icon, title, body }: { icon: ReactNode; title: string; body: string }) {
  return (
    <article className="co-pillar">
      <span>{icon}</span>
      <div>
        <h3>{title}</h3>
        <p>{body}</p>
      </div>
    </article>
  );
}

function displayDate(value?: string | null): string {
  if (!value) return "no disponible";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value.slice(0, 10);
  return new Intl.DateTimeFormat("es-CO", {
    dateStyle: "medium",
    timeZone: "America/Bogota",
  }).format(parsed);
}

export function Landing() {
  const { data, status } = useAtlasOverview();
  const materialized = data.signals.filter((signal) => signal.materialized).length;

  return (
    <main>
      <section className="co-container co-hero">
        <div className="co-hero__copy">
          <Rule accent>Contratación pública · Colombia · datos abiertos</Rule>
          <h1>Riesgos documentales que cualquiera puede verificar.</h1>
          <p>
            co/acc cruza registros públicos, muestra patrones de riesgo con su evidencia y
            prioriza contratos inusuales para revisión humana.
          </p>
          <div className="co-action-row">
            <Link className="co-button co-button--primary" to="/app/patterns">
              Ver patrones
              <ArrowRight size={16} />
            </Link>
            <Link className="co-button" to="/app/signals">Ver señales</Link>
            <DataStatus status={status} />
          </div>
        </div>

        <Frame coord="ALCANCE · MVP-01" className="co-hero__map">
          <Eyebrow accent>Problema y respuesta</Eyebrow>
          <h2>Los registros oficiales están fragmentados.</h2>
          <p>
            El lago de datos normaliza fuentes ya operativas y materializa cruces
            reproducibles. Cada hit conserva su fuente, fecha, selector y calidad de enlace.
          </p>
          <div className="co-spec-list">
            <span><strong>definiciones</strong>{data.signals.length || "—"}</span>
            <span><strong>materializadas</strong>{materialized || "—"}</span>
            <span><strong>fuentes reportadas</strong>{data.metrics.sources || "—"}</span>
          </div>
        </Frame>
      </section>

      <section className="co-container co-metrics-band">
        <Rule>Estado de actualización</Rule>
        <div className="co-metrics-grid co-metrics-grid--compact">
          <MetricTile label="datos" value={displayDate(data.refreshes.data)} sub="última operación del lago" />
          <MetricTile label="señales" value={displayDate(data.refreshes.signals)} sub="última materialización" />
          <MetricTile label="modelo" value={displayDate(data.refreshes.model)} sub="último scoring por lotes" />
          <MetricTile label="estado" value={status} sub="sin sustitución por fixtures" />
        </div>
      </section>

      <section className="co-container co-two-col">
        <div>
          <Eyebrow accent>Recorrido mínimo</Eyebrow>
          <h2>De la señal a la fuente oficial.</h2>
          <p className="co-muted">
            Confianza documental y fuerza de riesgo son conceptos distintos. Ninguno prueba
            corrupción ni reemplaza una investigación.
          </p>
        </div>
        <div className="co-pillars">
          <Pillar
            icon={<Database size={19} />}
            title="Catálogo completo"
            body="Todas las definiciones registradas aparecen, incluso cuando aún no tienen hits."
          />
          <Pillar
            icon={<Gauge size={19} />}
            title="Confianza explicada"
            body="Identidad, trazabilidad y corroboración forman un índice documental de 0 a 100."
          />
          <Pillar
            icon={<Search size={19} />}
            title="Evidencia verificable"
            body="Empresas, contratos, fechas, identificadores y enlaces oficiales permanecen unidos."
          />
          <Pillar
            icon={<ShieldCheck size={19} />}
            title="Señales, no acusaciones"
            body="El resultado prioriza revisión; no determina culpabilidad ni legalidad."
          />
        </div>
      </section>

      <section className="co-container">
        <Frame coord="NAVEGACIÓN · MVP-02" className="co-cta">
          <div>
            <h2>Explora los datos vivos.</h2>
            <p>La aplicación no muestra métricas de demostración cuando la API no responde.</p>
          </div>
          <div className="co-action-row">
            <Link className="co-button" to="/app/search">Buscar empresa o NIT</Link>
            <Link className="co-button" to="/priorizados">Contratos priorizados</Link>
            <Link className="co-button" to="/metodologia">Metodología</Link>
            <Pill tone="accent">contexto documental, no acusación</Pill>
          </div>
        </Frame>
      </section>
    </main>
  );
}
