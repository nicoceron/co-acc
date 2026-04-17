import type { TerritorialHit } from "./ChoroplethMunicipal";

const rootStyle: React.CSSProperties = {
  display: "grid",
  gap: "0.5rem",
  padding: "0.25rem 0",
};

const rowStyle: React.CSSProperties = {
  display: "grid",
  gridTemplateColumns: "minmax(0, 1fr) minmax(6rem, 12rem) auto",
  alignItems: "center",
  gap: "0.75rem",
  padding: "0.45rem 0",
  borderBottom: "1px solid var(--border)",
  fontFamily: "var(--font-sans)",
  fontSize: "0.82rem",
};

export function HeatmapConcentration({ data }: { data: TerritorialHit[] }) {
  const maxHits = Math.max(1, ...data.map((row) => row.hits));
  return (
    <div style={rootStyle}>
      {data.slice(0, 8).map((row) => (
        <div key={`${row.divipola ?? row.municipality}-${row.sector ?? "all"}`} style={rowStyle}>
          <span style={{ color: "var(--text-primary)" }}>
            {row.municipality}
            {row.department ? (
              <span style={{ color: "var(--text-muted)" }}> — {row.department}</span>
            ) : null}
          </span>
          <meter min={0} max={maxHits} value={row.hits} style={{ width: "100%" }} />
          <strong style={{ color: "var(--accent)" }}>{row.hits}</strong>
        </div>
      ))}
    </div>
  );
}
