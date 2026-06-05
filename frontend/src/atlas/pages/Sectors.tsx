import { ArrowRight } from "lucide-react";
import { Link } from "react-router";

import { AtlasMap, BarChart } from "../components/visuals";
import { EmptyState, Eyebrow, Frame, MiniBar, Rule } from "../components/ui";
import { formatNumber } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

export function Sectors() {
  const { data } = useAtlasOverview();
  const maxSectorHits = Math.max(1, ...data.sectors.map((item) => item.hits));
  const bars = data.sectors
    .map((sector) => ({
      label: sector.label,
      value: sector.hits,
      display: `${formatNumber(sector.hits)} hits · ${sector.contracts} contratos`,
      color: sector.accent,
    }))
    .sort((a, b) => b.value - a.value);

  return (
    <main className="co-container co-sectors">
      <Rule accent>Sectores · cobertura operacional</Rule>
      <header className="co-page-head co-page-head--wide">
        <h1>Riesgo documental por dominio publico.</h1>
        <p>Un corte por sector ayuda a separar patrones de contratacion, sancion, regalías y licenciamiento.</p>
      </header>

      <div className="co-sectors__grid">
        <Frame coord="SECTORES · B-01">
          <BarChart items={bars} />
        </Frame>
        <Frame coord="ATLAS · A-03">
          <AtlasMap departamentos={data.departamentos} height={340} compact />
        </Frame>
      </div>

      <section className="co-sector-cards">
        {data.sectors.map((sector) => (
          <Link className="co-sector-card" key={sector.id} to="/app/search">
            <span style={{ background: sector.accent }} />
            <div>
              <Eyebrow>{sector.id}</Eyebrow>
              <h2>{sector.label}</h2>
              <p>{formatNumber(sector.hits)} hits · {sector.contracts} contratos</p>
              <MiniBar value={sector.hits / maxSectorHits} color={sector.accent} />
            </div>
            <ArrowRight size={16} />
          </Link>
        ))}
        {!data.sectors.length ? (
          <EmptyState title="Sin sectores materializados" body="No hay agregados de senales por categoria para esta vista." />
        ) : null}
      </section>
    </main>
  );
}
