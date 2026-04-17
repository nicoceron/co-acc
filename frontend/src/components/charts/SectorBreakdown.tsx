import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export interface SectorDatum {
  sector: string;
  hits: number;
}

export function SectorBreakdown({ data }: { data: SectorDatum[] }) {
  return (
    <ResponsiveContainer width="100%" height={240}>
      <BarChart data={data} margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
        <CartesianGrid stroke="rgba(255,255,255,0.08)" vertical={false} />
        <XAxis dataKey="sector" tickLine={false} axisLine={false} minTickGap={18} />
        <YAxis tickLine={false} axisLine={false} width={36} />
        <Tooltip />
        <Bar dataKey="hits" fill="var(--accent)" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
