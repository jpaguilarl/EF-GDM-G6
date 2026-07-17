import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceArea, ReferenceLine } from "recharts";
import { Calendar, Layers, TrendingUp, Table2, ChevronDown, ChevronUp } from "lucide-react";
import { apiGet } from "../lib/api";
import type { SarimaxSummary, SarimaxForecastRow } from "../lib/types";
import { SummaryCard } from "./SummaryCard";

interface ChartRow {
  pickup_hour: string;
  trip_count: number | null;
  yhat_historico: number | null;
  yhat_pronostico: number | null;
}

export function SarimaxViewer() {
  const [showTable, setShowTable] = useState(false);
  const [borough, setBorough] = useState<string>("");
  const [serviceId, setServiceId] = useState<string>("");
  const [startDate, setStartDate] = useState<string>("2024-01-01");
  const [endDate, setEndDate] = useState<string>("2026-06-30");

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

  const { data, isLoading } = useQuery<{ rows: SarimaxForecastRow[]; total: number }>({
    queryKey: ["sarimax", borough, serviceId, startDate, endDate],
    queryFn: () => {
      let url = "/ml/sarimax/forecast?limit=5000&offset=0&grain=daily";
      if (borough) url += `&borough=${encodeURIComponent(borough)}`;
      if (serviceId) url += `&service_id=${encodeURIComponent(serviceId)}`;
      if (startDate) url += `&start_date=${encodeURIComponent(startDate)}`;
      if (endDate) url += `&end_date=${encodeURIComponent(endDate)}`;
      return apiGet<{ rows: SarimaxForecastRow[]; total: number }>(url);
    },
    enabled: true,
  });

  const displayData = data?.rows ?? [];
  const hasData = displayData.length > 0;

  const chartData: ChartRow[] = useMemo(() => displayData.map(r => ({
    pickup_hour: r.pickup_hour,
    trip_count: r.trip_count,
    yhat_historico: r.forecast_type === "actual" ? r.yhat : null,
    yhat_pronostico: r.forecast_type === "forecast" ? r.yhat : null,
  })), [displayData]);

  const boundaryTs = useMemo(() => {
    if (!hasData) return null;
    // First day where forecast yhat overtakes historical yhat
    for (const row of chartData) {
      if (row.yhat_pronostico !== null &&
          (row.yhat_historico === null || row.yhat_pronostico >= row.yhat_historico)) {
        return row.pickup_hour;
      }
    }
    return null;
  }, [chartData, hasData]);

  const minTs = hasData ? displayData[0].pickup_hour : "";
  const maxTs = hasData ? displayData[displayData.length - 1].pickup_hour : "";

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
        <div className="space-y-1">
          <label className="text-caption text-on-surface-variant uppercase tracking-wide">Desde</label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="block px-3 py-2 rounded-DEFAULT border border-border-subtle bg-surface-container-lowest text-on-surface text-sm min-w-[160px]"
          />
        </div>
        <div className="space-y-1">
          <label className="text-caption text-on-surface-variant uppercase tracking-wide">Hasta</label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="block px-3 py-2 rounded-DEFAULT border border-border-subtle bg-surface-container-lowest text-on-surface text-sm min-w-[160px]"
          />
        </div>
        {(borough || serviceId || startDate || endDate) && (
          <button
            onClick={() => { setBorough(""); setServiceId(""); setStartDate("2024-01-01"); setEndDate("2026-06-30"); }}
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
              <LineChart data={chartData} margin={{ top: 20, right: 20, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis
                  dataKey="pickup_hour"
                  tick={{ fontSize: 11 }}
                  angle={-45}
                  textAnchor="end"
                  height={60}
                  tickFormatter={(v: string) => {
                    const d = new Date(v);
                    return d.toLocaleDateString("es-ES", { day: "2-digit", month: "short", year: "2-digit" });
                  }}
                />
                <YAxis />
                <Tooltip
                  labelFormatter={(v) => {
                    if (typeof v !== "string") return "";
                    const d = new Date(v);
                    return d.toLocaleDateString("es-ES", { day: "2-digit", month: "long", year: "numeric" });
                  }}
                />
                <Legend />
                {/* Background bands for sections */}
                {boundaryTs && (
                  <>
                    <ReferenceArea x1={minTs} x2={boundaryTs} fill="#f3f4f6" fillOpacity={0.5} label={{ value: "Histórico", position: "insideTopLeft", fill: "#6b7280", fontSize: 12 }} />
                    <ReferenceArea x1={boundaryTs} x2={maxTs} fill="#dbeafe" fillOpacity={0.5} label={{ value: "Pronóstico", position: "insideTopLeft", fill: "#1d4ed8", fontSize: 12 }} />
                    <ReferenceLine x={boundaryTs} stroke="#1d4ed8" strokeDasharray="4 4" />
                  </>
                )}
                {/* No boundary — all data is one type */}
                {!boundaryTs && hasData && (
                  displayData[0].forecast_type === "forecast" ? (
                    <ReferenceArea x1={minTs} x2={maxTs} fill="#dbeafe" fillOpacity={0.5} label={{ value: "Pronóstico", position: "insideTopLeft", fill: "#1d4ed8", fontSize: 12 }} />
                  ) : (
                    <ReferenceArea x1={minTs} x2={maxTs} fill="#f3f4f6" fillOpacity={0.5} label={{ value: "Histórico", position: "insideTopLeft", fill: "#6b7280", fontSize: 12 }} />
                  )
                )}
                <Line
                  type="monotone"
                  dataKey="trip_count"
                  stroke="#1f2937"
                  strokeWidth={1.5}
                  dot={false}
                  connectNulls={false}
                  name="Viajes reales"
                />
                <Line
                  type="monotone"
                  dataKey="yhat_historico"
                  stroke="#a00003"
                  strokeWidth={2}
                  dot={false}
                  connectNulls={false}
                  name="Histórico (yhat)"
                />
                <Line
                  type="monotone"
                  dataKey="yhat_pronostico"
                  stroke="#415f8e"
                  strokeDasharray="5 5"
                  strokeWidth={2}
                  dot={false}
                  connectNulls={false}
                  name="Pronóstico (yhat)"
                />
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
                      {hasData && Object.keys(displayData[0]).map((col) => (
                        <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {displayData.map((row, i) => (
                      <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}>
                        {hasData && Object.keys(displayData[0]).map((col) => (
                          <td key={col} className="px-3 py-2 tabular-nums">{String(row[col as keyof SarimaxForecastRow] ?? "")}</td>
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
