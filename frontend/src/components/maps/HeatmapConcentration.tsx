import type { TerritorialHit } from "./ChoroplethMunicipal";

export function HeatmapConcentration({ data }: { data: TerritorialHit[] }) {
  const maxHits = Math.max(1, ...data.map((row) => row.hits));
  return (
    <div>
      {data.slice(0, 8).map((row) => (
        <div key={`${row.divipola ?? row.municipality}-${row.sector ?? "all"}`}>
          <span>{row.municipality}</span>
          <meter min={0} max={maxHits} value={row.hits} />
          <strong>{row.hits}</strong>
        </div>
      ))}
    </div>
  );
}
