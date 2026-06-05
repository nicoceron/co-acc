import type { ReactNode } from "react";

import type { Severity } from "../data/prototype";
import type { AtlasDataStatus } from "../lib/useAtlasData";
import { severityClass, toPercent } from "../lib/format";

export function Brand() {
  return (
    <span className="co-brand" aria-label="co/acc">
      <svg width="24" height="24" viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" strokeWidth="1" />
        <circle cx="12" cy="12" r="5.8" fill="none" stroke="currentColor" strokeDasharray="2 2" strokeWidth="1" />
        <path d="M2 12h20M12 2v20" stroke="currentColor" strokeWidth="0.7" />
        <circle cx="15.2" cy="8.8" r="1.5" fill="currentColor" />
      </svg>
      <span className="co-brand__name">
        <span>co</span>
        <span className="co-brand__slash">/</span>
        <span>acc</span>
      </span>
      <span className="co-brand__tag">atlas v3</span>
    </span>
  );
}

export function Rule({ children, accent = false }: { children: ReactNode; accent?: boolean }) {
  return <div className={accent ? "co-rule co-rule--accent" : "co-rule"}>{children}</div>;
}

export function Eyebrow({ children, accent = false }: { children: ReactNode; accent?: boolean }) {
  return <div className={accent ? "co-eyebrow co-eyebrow--accent" : "co-eyebrow"}>{children}</div>;
}

export function Frame({
  children,
  coord,
  className = "",
}: {
  children: ReactNode;
  coord?: string;
  className?: string;
}) {
  return (
    <section className={`co-frame ${className}`}>
      {coord ? <span className="co-frame__coord">{coord}</span> : null}
      {children}
    </section>
  );
}

export function Panel({
  title,
  meta,
  children,
}: {
  title?: string;
  meta?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="co-panel">
      {title ? (
        <header className="co-panel__head">
          <span>{title}</span>
          {meta ? <span>{meta}</span> : null}
        </header>
      ) : null}
      {children}
    </section>
  );
}

export function Sparkline({
  data,
  width = 180,
  height = 30,
  stroke = "var(--coacc-accent)",
}: {
  data: number[];
  width?: number;
  height?: number;
  stroke?: string;
}) {
  const points = data.length ? data : [0, 0];
  const max = Math.max(...points);
  const min = Math.min(...points);
  const dx = width / Math.max(1, points.length - 1);
  const normalize = (value: number) => height - ((value - min) / Math.max(1, max - min)) * (height - 4) - 2;
  const line = points
    .map((value, index) => `${index === 0 ? "M" : "L"} ${(index * dx).toFixed(1)} ${normalize(value).toFixed(1)}`)
    .join(" ");
  const fill = `${line} L ${width} ${height} L 0 ${height} Z`;

  return (
    <svg className="co-spark" width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true">
      <path d={fill} fill={stroke} fillOpacity="0.12" />
      <path d={line} fill="none" stroke={stroke} strokeWidth="1.4" />
      <circle cx={width} cy={normalize(points[points.length - 1] ?? 0)} r="2.4" fill={stroke} />
    </svg>
  );
}

export function MiniBar({
  value,
  color = "var(--coacc-accent)",
}: {
  value: number;
  color?: string;
}) {
  return (
    <span className="co-minibar" aria-label={toPercent(value)}>
      <span style={{ width: toPercent(value), background: color }} />
    </span>
  );
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={severityClass(severity)}>{severity}</span>;
}

export function Pill({ children, tone = "neutral" }: { children: ReactNode; tone?: "neutral" | "accent" | "moss" | "coral" }) {
  return <span className={`co-pill co-pill--${tone}`}>{children}</span>;
}

export function MetricTile({
  label,
  value,
  sub,
  spark,
}: {
  label: string;
  value: string;
  sub: string;
  spark?: number[];
}) {
  return (
    <div className="co-metric">
      <div className="co-eyebrow">{label}</div>
      <strong>{value}</strong>
      <span>{sub}</span>
      {spark ? <Sparkline data={spark} /> : null}
    </div>
  );
}

export function EmptyState({ title, body }: { title: string; body?: string }) {
  return (
    <div className="co-empty-state">
      <strong>{title}</strong>
      {body ? <span>{body}</span> : null}
    </div>
  );
}

export function DataStatus({ status }: { status: AtlasDataStatus }) {
  const label =
    status === "live" ? "api live"
      : status === "partial" ? "api parcial"
        : status === "loading" ? "sync"
          : status === "unavailable" ? "sin api"
            : status === "idle" ? "idle" : "fixture";
  const tone = status === "live" ? "moss" : status === "loading" || status === "partial" ? "accent" : "neutral";
  return (
    <Pill tone={tone}>
      <span className="co-pulse" />
      {label}
    </Pill>
  );
}
