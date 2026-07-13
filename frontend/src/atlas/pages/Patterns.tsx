import { Gauge, GitBranch } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router";

import { DataStatus, EmptyState, Pill, Rule, SeverityBadge } from "../components/ui";
import type { Severity } from "../data/prototype";
import { formatNumber } from "../lib/format";
import { useAtlasOverview } from "../lib/useAtlasData";

const SEVERITY_FILTERS: ("all" | Severity)[] = ["all", "low", "medium", "high", "critical"];

function displaySignalId(signalId: string): string {
  return signalId.replace(/_review_only$/, "");
}

export function PatternsPage() {
  const { data, status } = useAtlasOverview();
  const [severity, setSeverity] = useState<"all" | Severity>("all");
  const [materializedOnly, setMaterializedOnly] = useState(false);

  const filtered = useMemo(() => {
    return data.patterns
      .filter((pattern) => severity === "all" || pattern.severity === severity)
      .filter((pattern) => !materializedOnly || pattern.materialized)
      .sort((a, b) => b.hits - a.hits || a.title.localeCompare(b.title, "es-CO"));
  }, [data.patterns, materializedOnly, severity]);

  const materializedCount = data.patterns.filter((pattern) => pattern.materialized).length;
  const totalHits = data.patterns.reduce((sum, pattern) => sum + pattern.hits, 0);

  return (
    <main className="co-container co-signals">
      <header className="co-workspace-head">
        <div>
          <Rule accent>Workspace · Patrones</Rule>
          <h1>Patrones</h1>
          <p>{formatNumber(totalHits)} hits materializados · {materializedCount} patrones con datos</p>
        </div>
        <div className="co-action-row">
          <DataStatus status={status} />
          <Pill tone="accent">
            <Gauge size={13} />
            todos · confidence_index
          </Pill>
        </div>
      </header>

      <div className="co-filter-bar">
        {SEVERITY_FILTERS.map((item) => (
          <button className={severity === item ? "active" : ""} key={item} type="button" onClick={() => setSeverity(item)}>
            {item === "all" ? "todas" : item}
          </button>
        ))}
        <button className={materializedOnly ? "active" : ""} type="button" onClick={() => setMaterializedOnly((value) => !value)}>
          materializados
        </button>
        <span>{filtered.length} de {data.patterns.length}</span>
      </div>

      <div className="co-table-wrap">
        <table className="co-table">
          <thead>
            <tr>
              <th>patron</th>
              <th>severidad</th>
              <th>categoria</th>
              <th>hits</th>
              <th>confianza</th>
              <th>senales</th>
              <th>fuentes</th>
              <th>estado</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((pattern) => {
              const primarySignal = pattern.signalIds[0];
              return (
                <tr key={pattern.id}>
                  <td>
                    {primarySignal ? (
                      <Link to={`/app/signals/${displaySignalId(primarySignal)}`}>{pattern.title}</Link>
                    ) : (
                      <span>{pattern.title}</span>
                    )}
                    <span className="co-row-sub">{pattern.desc || pattern.id}</span>
                    <span className="co-row-sub co-mono">{pattern.id}</span>
                  </td>
                  <td><SeverityBadge severity={pattern.severity} /></td>
                  <td className="co-mono">{pattern.category}</td>
                  <td className="co-num">{formatNumber(pattern.hits)}</td>
                  <td className="co-num">{pattern.confidence == null ? "—" : `${pattern.confidence.toFixed(1)}%`}</td>
                  <td>
                    <span className="co-mono">{pattern.signalIds.slice(0, 2).map(displaySignalId).join(" · ") || "registry"}</span>
                  </td>
                  <td>
                    <span className="co-mono">{pattern.sourcesRequired.slice(0, 3).join(" · ") || "fuente"}</span>
                  </td>
                  <td>
                    <Pill tone={pattern.materialized ? "moss" : "neutral"}>
                      {pattern.materialized ? "materialized" : "registry"}
                    </Pill>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {!filtered.length ? (
          <EmptyState title="Sin patrones" body="No hay patrones para los filtros activos." />
        ) : null}
      </div>

      <div className="co-inline-metrics">
        <span><GitBranch size={14} /> {formatNumber(data.patterns.length)} patrones registrados</span>
        <span>{formatNumber(materializedCount)} con hits</span>
        <span>{formatNumber(data.signals.length)} senales en catalogo</span>
      </div>
    </main>
  );
}
