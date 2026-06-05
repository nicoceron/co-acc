import { useState } from "react";

import { EmptyState } from "./ui";
import type { Departamento, EntityExample } from "../data/prototype";
import { severityVar } from "../lib/format";

export function AtlasMap({
  departamentos,
  height = 430,
  compact = false,
}: {
  departamentos: Departamento[];
  height?: number;
  compact?: boolean;
}) {
  const [hover, setHover] = useState<Departamento | null>(null);
  const width = compact ? 390 : 540;
  const pad = 28;
  const maxHits = Math.max(1, ...departamentos.map((item) => item.hits));
  const contours = Array.from({ length: 7 }, (_, index) => {
    const radius = 58 + index * 28;
    return `M ${width / 2} ${height / 2} m -${radius} 0 a ${radius} ${radius * 1.15} 0 1 0 ${radius * 2} 0 a ${radius} ${radius * 1.15} 0 1 0 -${radius * 2} 0`;
  });

  return (
    <div className="co-map" style={{ aspectRatio: `${width}/${height}` }}>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Mapa de actividad por departamento">
        <defs>
          <pattern id="co-map-hatch" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line x1="0" y1="0" x2="0" y2="4" stroke="var(--coacc-line-2)" strokeWidth="0.5" />
          </pattern>
        </defs>

        <g stroke="var(--coacc-line-1)" strokeWidth="0.4">
          {Array.from({ length: 9 }, (_, index) => (
            <line
              key={`x-${index}`}
              x1={pad + index * ((width - 2 * pad) / 8)}
              y1={pad}
              x2={pad + index * ((width - 2 * pad) / 8)}
              y2={height - pad}
            />
          ))}
          {Array.from({ length: 9 }, (_, index) => (
            <line
              key={`y-${index}`}
              x1={pad}
              y1={pad + index * ((height - 2 * pad) / 8)}
              x2={width - pad}
              y2={pad + index * ((height - 2 * pad) / 8)}
            />
          ))}
        </g>

        <g fill="none" stroke="var(--coacc-line-2)" strokeWidth="0.5" opacity="0.58">
          {contours.map((path, index) => (
            <path key={path} d={path} strokeDasharray={index % 2 ? "1.5 2.5" : "0"} />
          ))}
        </g>

        <path
          d={`M ${width * 0.30} ${height * 0.10} C ${width * 0.10} ${height * 0.30}, ${width * 0.18} ${height * 0.55}, ${width * 0.28} ${height * 0.78} C ${width * 0.34} ${height * 0.94}, ${width * 0.55} ${height * 0.96}, ${width * 0.66} ${height * 0.84} C ${width * 0.84} ${height * 0.74}, ${width * 0.86} ${height * 0.50}, ${width * 0.74} ${height * 0.32} C ${width * 0.66} ${height * 0.20}, ${width * 0.50} ${height * 0.06}, ${width * 0.30} ${height * 0.10} Z`}
          fill="url(#co-map-hatch)"
          stroke="var(--coacc-line-3)"
          strokeWidth="0.7"
        />

        <g className="co-map__label">
          <text x={pad} y={pad - 9}>12N · 78W</text>
          <text x={width - pad} y={pad - 9} textAnchor="end">12N · 67W</text>
          <text x={pad} y={height - pad + 15}>1S · 78W</text>
          <text x={width - pad} y={height - pad + 15} textAnchor="end">1S · 67W</text>
        </g>

        {departamentos.map((item) => {
          const cx = pad + item.x * (width - 2 * pad);
          const cy = pad + item.y * (height - 2 * pad);
          const radius = 2 + (item.hits / maxHits) * (compact ? 8 : 12);
          const color = severityVar(item.sev);
          const isHover = hover?.code === item.code;
          return (
            <g
              key={item.code}
              className="co-map__point"
              onMouseEnter={() => setHover(item)}
              onMouseLeave={() => setHover(null)}
            >
              <circle cx={cx} cy={cy} r={radius + 4} fill={color} opacity="0.14" />
              <circle cx={cx} cy={cy} r={radius} fill={color} opacity="0.88" />
              <circle cx={cx} cy={cy} r={radius} fill="none" stroke={color} strokeWidth="0.6" />
              {(isHover || (!compact && item.hits > 150)) ? (
                <text x={cx + radius + 5} y={cy + 3} className="co-map__tag">
                  {item.code} · {item.hits}
                </text>
              ) : null}
            </g>
          );
        })}

        <g stroke="var(--coacc-accent)" strokeWidth="0.65" opacity="0.78">
          <line x1={width / 2 - 7} y1={height * 0.55} x2={width / 2 + 7} y2={height * 0.55} />
          <line x1={width / 2} y1={height * 0.55 - 7} x2={width / 2} y2={height * 0.55 + 7} />
          <text x={width / 2 + 9} y={height * 0.55 - 7} className="co-map__tag co-map__tag--accent">
            BOG · 04N · 74W
          </text>
        </g>
      </svg>

      {hover ? (
        <div className="co-map__hover">
          <span>{hover.code}</span>
          <strong>{hover.name}</strong>
          <em>{hover.sev} · {hover.hits} hits</em>
        </div>
      ) : null}
      {!departamentos.length ? (
        <div className="co-map__empty">
          Sin datos geograficos materializados
        </div>
      ) : null}
    </div>
  );
}

export function BarChart({
  items,
}: {
  items: {
    label: string;
    value: number;
    display: string;
    color: string;
  }[];
}) {
  if (!items.length) {
    return <EmptyState title="Sin datos materializados" body="No hay agregados disponibles para esta vista." />;
  }
  const max = Math.max(1, ...items.map((item) => item.value));
  return (
    <div className="co-bars">
      {items.map((item) => (
        <div className="co-bars__row" key={item.label}>
          <span>{item.label}</span>
          <span className="co-bars__track">
            <span style={{ width: `${(item.value / max) * 100}%`, background: item.color }} />
          </span>
          <em>{item.display}</em>
        </div>
      ))}
    </div>
  );
}

export function RelationGraph({
  center,
  relations,
}: {
  center: string;
  relations: EntityExample["relations"];
}) {
  const width = 620;
  const height = 360;
  const cx = width / 2;
  const cy = height / 2;
  const radius = Math.min(width, height) * 0.36;
  const nodes = relations.map((relation, index) => {
    const angle = (index / relations.length) * Math.PI * 2 - Math.PI / 2;
    return {
      ...relation,
      x: cx + Math.cos(angle) * radius,
      y: cy + Math.sin(angle) * radius,
    };
  });
  const colorFor = (type: EntityExample["relations"][number]["type"]) => {
    if (type === "persona") return "var(--coacc-data-person)";
    if (type === "empresa") return "var(--coacc-data-company)";
    return "var(--coacc-data-contract)";
  };

  return (
    <svg className="co-relation" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Grafo de relaciones">
      {[0.25, 0.5, 0.75, 1].map((scale) => (
        <circle key={scale} cx={cx} cy={cy} r={radius * scale} fill="none" stroke="var(--coacc-line-1)" strokeDasharray="1.5 3" />
      ))}
      {nodes.map((node) => (
        <line
          key={`edge-${node.id}`}
          x1={cx}
          y1={cy}
          x2={node.x}
          y2={node.y}
          stroke="var(--coacc-line-3)"
          strokeWidth={0.7 + node.weight * 1.4}
          opacity={0.58 + node.weight * 0.35}
        />
      ))}
      <circle cx={cx} cy={cy} r="23" fill="var(--coacc-accent-soft)" stroke="var(--coacc-accent)" strokeWidth="1.2" />
      <text x={cx} y={cy + 4} textAnchor="middle" className="co-relation__center">CENTRO</text>
      <text x={cx} y={cy + 46} textAnchor="middle" className="co-relation__title">{center}</text>

      {nodes.map((node) => {
        const color = colorFor(node.type);
        return (
          <g key={node.id}>
            <circle cx={node.x} cy={node.y} r={9 + node.weight * 4} fill={color} opacity="0.2" />
            <circle cx={node.x} cy={node.y} r={6 + node.weight * 3} fill={color} />
            <text x={node.x} y={node.y - 15} textAnchor="middle" className="co-relation__label">{node.label}</text>
            <text x={node.x} y={node.y + 20} textAnchor="middle" className="co-relation__role">{node.role}</text>
          </g>
        );
      })}
    </svg>
  );
}
