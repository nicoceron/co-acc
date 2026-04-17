import {
  flexRender,
  getCoreRowModel,
  type ColumnDef,
  useReactTable,
} from "@tanstack/react-table";

export interface CaseTableRow {
  title: string;
  sector: string;
  severity: string;
  hits: number;
}

const columns: Array<ColumnDef<CaseTableRow>> = [
  { accessorKey: "title", header: "Caso" },
  { accessorKey: "sector", header: "Sector" },
  { accessorKey: "severity", header: "Severidad" },
  { accessorKey: "hits", header: "Señales" },
];

export function CaseDataTable({ rows }: { rows: CaseTableRow[] }) {
  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <table>
      <thead>
        {table.getHeaderGroups().map((group) => (
          <tr key={group.id}>
            {group.headers.map((header) => (
              <th key={header.id}>
                {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
              </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody>
        {table.getRowModel().rows.map((row) => (
          <tr key={row.id}>
            {row.getVisibleCells().map((cell) => (
              <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
