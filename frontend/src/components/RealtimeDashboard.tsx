import { useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { useRealtimeStream } from "../hooks/useRealtimeStream";
import type { RealtimeViewConfig } from "../lib/types";
import { SummaryCard } from "./SummaryCard";
import { Activity } from "lucide-react";

interface RealtimeDashboardProps {
  viewConfig: RealtimeViewConfig;
  filters?: Record<string, string>;
}

const COLORS = ["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a", "#2e7d32", "#f57c00", "#6a1b9a"];

export function RealtimeDashboard({ viewConfig, filters }: RealtimeDashboardProps) {
  const { rows, status, lastEventAt } = useRealtimeStream(viewConfig.key, viewConfig, filters);

  const totalValue = useMemo(
    () => rows.reduce((s, r) => s + (Number(r[viewConfig.valueField]) || 0), 0),
    [rows, viewConfig.valueField],
  );

  const topCategories = useMemo(() => {
    const grouped: Record<string, number> = {};
    for (const row of rows) {
      const cat = String(row[viewConfig.categoryField] ?? "?");
      grouped[cat] = (grouped[cat] || 0) + (Number(row[viewConfig.valueField]) || 0);
    }
    return Object.entries(grouped)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 15);
  }, [rows, viewConfig.categoryField, viewConfig.valueField]);

  const statusPill = () => {
    switch (status) {
      case "connected": return "bg-green-600";
      case "connecting":
      case "reconnecting": return "bg-amber-500";
      case "closed": return "bg-gray-400";
    }
  };

  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <div className="space-y-4">
      {/* Status + KPIs */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-surface-muted border border-border-subtle">
          <span className={`w-2 h-2 rounded-full ${statusPill()}`} />
          <span className="text-caption text-on-surface-variant capitalize">{status}</span>
        </div>
        {lastEventAt && (
          <span className="text-caption text-on-surface-variant">
            Último evento: {new Date(lastEventAt).toLocaleTimeString()}
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <SummaryCard label="Filas en Vivo" value={rows.length} icon={<Activity className="w-5 h-5" />} accent="primary" />
        <SummaryCard
          label={`Total ${viewConfig.valueField}`}
          value={Math.round(totalValue * 100) / 100}
          accent="secondary"
        />
        <SummaryCard label="Vista" value={viewConfig.label} accent="warning" />
      </div>

      {/* Chart */}
      {topCategories.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">
            Top {topCategories.length} por {viewConfig.categoryField}
          </h4>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={topCategories} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="name" tick={{ fontSize: 10 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="value" fill={COLORS[0]} radius={[4, 4, 0, 0]} name={viewConfig.valueField} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Table */}
      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
        <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">
          Datos en Vivo ({rows.length} filas)
        </h4>
        {rows.length === 0 ? (
          <div className="flex items-center justify-center py-16 text-on-surface-variant italic">
            {status === "connected" ? "Sin datos en vivo" : "Conectando..."}
          </div>
        ) : (
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="w-full text-body-sm">
              <thead className="sticky top-0 bg-surface-container-lowest">
                <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase tracking-wide">
                  {columns.map((col) => (
                    <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 500).map((row, i) => (
                  <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "" : "bg-surface-muted"}`}>
                    {columns.map((col) => (
                      <td key={col} className="px-3 py-1.5 whitespace-nowrap tabular-nums">
                        {String(row[col] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
