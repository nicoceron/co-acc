import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

export interface SeverityDatum {
  severity: "critical" | "high" | "medium" | "low" | string;
  hits: number;
}

const COLORS: Record<string, string> = {
  critical: "#dc2626",
  high: "#ea580c",
  medium: "#ca8a04",
  low: "#0f766e",
};

export function SeverityDonut({ data }: { data: SeverityDatum[] }) {
  return (
    <ResponsiveContainer width="100%" height={220}>
      <PieChart>
        <Pie data={data} dataKey="hits" nameKey="severity" innerRadius={58} outerRadius={88} paddingAngle={2}>
          {data.map((entry) => (
            <Cell key={entry.severity} fill={COLORS[entry.severity] ?? "var(--accent)"} />
          ))}
        </Pie>
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
  );
}
