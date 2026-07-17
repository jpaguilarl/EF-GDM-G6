import { useMemo } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";

export interface DataTableColumn {
  key: string;
  label: string;
  render?: (value: unknown, row: Record<string, unknown>) => React.ReactNode;
}

interface DataTableProps {
  rows: Record<string, unknown>[];
  total: number;
  pageSize: number;
  page: number;
  onPageChange: (page: number) => void;
  loading?: boolean;
  columns?: DataTableColumn[];
}

export function DataTable({
  rows,
  total,
  pageSize,
  page,
  onPageChange,
  loading = false,
  columns: explicitColumns,
}: DataTableProps) {
  const totalPages = Math.ceil(total / pageSize);
  const columns = useMemo(() => {
    if (explicitColumns) return explicitColumns;
    if (rows.length === 0) return [];
    return Object.keys(rows[0]).map((key) => ({
      key,
      label: key,
      render: (value: unknown) =>
        key === "start_timestamp" || key === "end_timestamp"
          ? new Date(String(value)).toLocaleString()
          : String(value ?? ""),
    }));
  }, [explicitColumns, rows]);

  return (
    <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
      <div className="text-caption text-on-surface-variant">
        {total.toLocaleString()} registros
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-20 text-on-surface-variant">Cargando...</div>
      ) : rows.length === 0 ? (
        <div className="flex items-center justify-center py-20 text-on-surface-variant italic">Sin registros</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-body-sm">
            <thead>
              <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase tracking-wide">
                {columns.map((col) => (
                  <th key={col.key} className="px-3 py-2 text-left whitespace-nowrap">{col.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr
                  key={i}
                  className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"} hover:bg-surface-container-low transition-colors`}
                >
                  {columns.map((col) => (
                    <td key={col.key} className="px-3 py-2 whitespace-nowrap tabular-nums">
                      {col.render ? col.render(row[col.key], row) : String(row[col.key] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-4 pt-2">
          <button
            onClick={() => onPageChange(Math.max(0, page - 1))}
            disabled={page === 0}
            className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted disabled:opacity-30 disabled:cursor-not-allowed"
          >
            <ChevronLeft className="w-4 h-4" /> Anterior
          </button>
          <span className="text-body-md text-on-surface-variant">
            Página {page + 1} de {totalPages}
          </span>
          <button
            onClick={() => onPageChange(Math.min(totalPages - 1, page + 1))}
            disabled={page >= totalPages - 1}
            className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted disabled:opacity-30 disabled:cursor-not-allowed"
          >
            Siguiente <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
}
