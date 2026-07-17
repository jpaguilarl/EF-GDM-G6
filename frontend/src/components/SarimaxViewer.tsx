import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { Calendar, Layers, TrendingUp, Table2, ChevronDown, ChevronUp } from "lucide-react";
import { apiGet } from "../lib/api";
import type { SarimaxSummary } from "../lib/types";
import { SummaryCard } from "./SummaryCard";

interface SarimaxRow {
  [key: string]: unknown;
}

export function SarimaxViewer() {
  const [showTable, setShowTable] = useState(false);
  const [borough, setBorough] = useState<string>("");
  const [serviceId, setServiceId] = useState<string>("");

  const { data: summary, isLoading: summaryLoading } = useQuery<SarimaxSummary>({
    queryKey: ["sarimax-summary"],
    queryFn: () => apiGet<SarimaxSummary>("/ml/sarimax/summary"),
  });

  const uniqueBoroughs = summary?.combos
    ? [...new Set(summary.combos.map(c => c.borough))].sort()
    : [];

  const availableServices = summary?.combos
    ? [...new Set(
        summary.combos
          .filter(c => !borough || c.borough === borough)
          .map(c => c.service_id)
      )].sort()
    : [];

  const { data, isLoading } = useQuery<{ rows: SarimaxRow[]; total: number }>({
    queryKey: ["sarimax", borough, serviceId],
    queryFn: () => {
      let url = "/ml/sarimax/forecast?limit=500&offset=0";
      if (borough) url += `&borough=${encodeURIComponent(borough)}`;
      if (serviceId) url += `&service_id=${encodeURIComponent(serviceId)}`;
      return apiGet<{ rows: SarimaxRow[]; total: number }>(url);
    },
    enabled: true,
  });

  const displayData = data?.rows ?? [];
  const hasData = displayData.length > 0;
  const sample = hasData ? displayData[0] : {} as SarimaxRow;
  const keys = Object.keys(sample);
  const timeCol = keys.find(
    (k) => k.toLowerCase().includes("time") || k.toLowerCase().includes("date") || k.toLowerCase().includes("fecha") || k.toLowerCase().includes("pickup") || k.toLowerCase().includes("ds")
  ) || keys[0] || "pickup_hour";
  const numCols = keys.filter((k) => k !== timeCol && typeof sample[k] === "number").slice(0, 5);

  return (
    <div className="space-y-6">
      {/* Summary KPIs */}
      {summaryLoading ? (
        <div className="text-center py-4 text-on-surface-variant">Cargando resumen...</div>
      ) : summary ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <SummaryCard label="Pronósticos" value={summary.total_rows.toLocaleString()} icon={<TrendingUp className="w-5 h-5" />} accent="primary" />
          <SummaryCard label="Combinaciones" value={summary.combos.length} icon={<Layers className="w-5 h-5" />} accent="secondary" />
          {summary.date_range?.min_dt && (
            <>
              <SummaryCard label="Desde" value={new Date(summary.date_range.min_dt).toLocaleDateString()} icon={<Calendar className="w-5 h-5" />} accent="primary" />
              <SummaryCard label="Hasta" value={new Date(summary.date_range.max_dt).toLocaleDateString()} icon={<Calendar className="w-5 h-5" />} accent="primary" />
            </>
          )}
        </div>
      ) : null}

      {/* Filter Controls */}
      <div className="flex flex-wrap gap-4 items-end">
        <div className="space-y-1">
          <label className="text-caption text-on-surface-variant uppercase tracking-wide">Borough</label>
          <select
            value={borough}
            onChange={(e) => { setBorough(e.target.value); setServiceId(""); }}
            className="block px-3 py-2 rounded-DEFAULT border border-border-subtle bg-surface-container-lowest text-on-surface text-sm min-w-[160px]"
          >
            <option value="">Todos</option>
            {uniqueBoroughs.map((b) => (
              <option key={b} value={b}>{b}</option>
            ))}
          </select>
        </div>
        <div className="space-y-1">
          <label className="text-caption text-on-surface-variant uppercase tracking-wide">Servicio</label>
          <select
            value={serviceId}
            onChange={(e) => setServiceId(e.target.value)}
            className="block px-3 py-2 rounded-DEFAULT border border-border-subtle bg-surface-container-lowest text-on-surface text-sm min-w-[160px]"
          >
            <option value="">Todos</option>
            {availableServices.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        {(borough || serviceId) && (
          <button
            onClick={() => { setBorough(""); setServiceId(""); }}
            className="px-3 py-2 rounded-DEFAULT text-label-md text-on-surface-variant border border-border-subtle hover:bg-surface-muted transition-colors"
          >
            Limpiar filtros
          </button>
        )}
      </div>

      {/* Chart */}
      {isLoading ? (
        <div className="text-center py-10 text-on-surface-variant">Cargando pronóstico...</div>
      ) : hasData ? (
        <>
          <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
            <h3 className="text-headline-md text-secondary mb-4">
              Pronóstico SARIMAX
              {(borough || serviceId) && (
                <span className="text-body-sm text-on-surface-variant ml-2">
                  — {borough && `${borough}`}{serviceId && ` / ${serviceId.toUpperCase()}`}
                </span>
              )}
            </h3>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={displayData as any[]} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis dataKey={timeCol} tick={{ fontSize: 11 }} angle={-45} textAnchor="end" height={60} />
                <YAxis />
                <Tooltip />
                <Legend />
                {numCols.map((col, i) => (
                  <Line
                    key={col}
                    type="monotone"
                    dataKey={col}
                    stroke={["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a"][i % 5]}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                    name={col}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Toggle Table */}
          <div className="flex justify-center">
            <button
              onClick={() => setShowTable(!showTable)}
              className="flex items-center gap-2 px-4 py-2 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted transition-colors"
            >
              <Table2 className="w-4 h-4" />
              {showTable ? "Ocultar datos" : "Ver datos"}
              {showTable ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>
          </div>

          {/* Detail Table (collapsible) */}
          {showTable && (
            <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
              <div className="overflow-x-auto max-h-96 overflow-y-auto">
                <table className="w-full text-body-sm">
                  <thead>
                    <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase sticky top-0 bg-surface-container-lowest">
                      {keys.map((col) => (
                        <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {displayData.map((row, i) => (
                      <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}>
                        {keys.map((col) => (
                          <td key={col} className="px-3 py-2 tabular-nums">{String(row[col] ?? "")}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      ) : (
        <div className="py-10 text-center text-on-surface-variant italic">Sin datos de pronóstico</div>
      )}
    </div>
  );
}
