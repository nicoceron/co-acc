import { Database, Gauge, ShieldCheck } from "lucide-react";

import { Frame, Pill, Rule } from "../components/ui";
import { useAtlasOverview } from "../lib/useAtlasData";

export function MethodologyPage() {
  const { data } = useAtlasOverview();

  return (
    <main className="co-container co-signal-detail">
      <header className="co-page-head">
        <Rule accent>Metodología pública · MVP</Rule>
        <h1>Cómo se construye una señal.</h1>
        <p>
          co/acc organiza documentos públicos en patrones verificables. Una señal es una
          invitación a revisar evidencia; no es una sentencia ni una acusación.
        </p>
      </header>

      <div className="co-signal-detail__grid">
        <Frame coord="DATOS · M-01">
          <Database size={18} />
          <h2>Lago y procedencia</h2>
          <p>
            Contratos YAML limitan cada extracción. Los registros se guardan en Parquet y
            DuckDB crea features, hits y paquetes de evidencia reproducibles.
          </p>
          <Pill>{data.metrics.sources} fuentes reportadas por la API</Pill>
        </Frame>
        <Frame coord="IDENTIDAD · M-02">
          <ShieldCheck size={18} />
          <h2>Enlaces de identidad</h2>
          <p>
            Los NIT exactos normalizados son el enlace preferido. Los enlaces probabilísticos
            se identifican como tales y reducen la confianza; nunca se presentan como certeza.
          </p>
        </Frame>
      </div>

      <Frame coord="CONFIANZA · M-03">
        <Gauge size={18} />
        <h2>Índice de confianza documental</h2>
        <p className="co-mono">
          100 × (0.55 × identidad + 0.25 × trazabilidad + 0.20 × corroboración)
        </p>
        <p>
          Identidad evalúa la calidad del enlace; trazabilidad, la evidencia enlazada;
          corroboración, las fuentes distintas. El índice no es probabilidad de corrupción.
          Una definición sin materialización no recibe un valor inventado.
        </p>
      </Frame>

      <Frame coord="MODELO · M-04">
        <h2>Isolation Forest por lotes</h2>
        <p>
          Un único Isolation Forest con semilla registrada puntúa contratos de 0 a 1. Usa
          valor, comparables por comprador y modalidad, concentración, historial reciente,
          participación del gasto, comportamiento temporal y banderas contractuales
          disponibles. Las tres mayores desviaciones se muestran en lenguaje común.
        </p>
        <p>
          PACO y sanciones sirven como etiquetas débiles de evaluación, no como verdad
          jurídica. Se reportan tasa base, precision@100 y @1.000, average precision, ROC AUC
          y comparación con ranking aleatorio; el holdout separa proveedores completos cuando
          existe NIT canónico.
        </p>
      </Frame>

      <Frame coord="LÍMITES · M-05">
        <h2>Actualización y límites conocidos</h2>
        <p>
          Las fechas visibles provienen de los manifiestos del lago, de señales y del modelo.
          Una fuente sin URL directa conserva su identificador y selector. Datos incompletos,
          enlaces probabilísticos y pocas observaciones reducen la confianza.
        </p>
        <div className="co-action-row">
          <a className="co-button" href="https://github.com/nicoceron/co-acc/blob/main/docs/confidence_index.md">Índice de confianza</a>
          <a className="co-button" href="https://github.com/nicoceron/co-acc/blob/main/docs/ai/anomaly_model.md">Model card</a>
          <a className="co-button" href="https://github.com/nicoceron/co-acc/blob/main/docs/architecture/overview.md">Arquitectura</a>
        </div>
      </Frame>
    </main>
  );
}
