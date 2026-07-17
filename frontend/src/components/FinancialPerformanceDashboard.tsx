import { useMemo } from "react";
import {
  BarChart, Bar, RadialBarChart, RadialBar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from "recharts";
import { SummaryCard } from "./SummaryCard";
import type { MartSummary } from "../lib/types";

interface Props {
  summary?: MartSummary | null;
  isLoading?: boolean;
}

const SERVICE_LABELS: Record<string, string> = {
  yellow: "Yellow Taxi",
  green: "Green Taxi",
  fhvhv: "Apps (Uber/Lyft)",
  fhv: "FHV Clásico",
};

const BAR_COLORS = {
  fare: "#004aa0",
  tips: "#2e7d32",
  tolls: "#f57c00",
  congestion: "#6a1b9a",
};

function formatM(v: number | null | undefined): string {
  if (v == null) return "—";
  return `$${(v / 1_000_000).toFixed(2)}M`;
}

function formatNum(v: number): string {
  return v.toLocaleString();
}

function formatPct(v: number | null | undefined): string {
  if (v == null) return "N/A";
  return `${(v * 100).toFixed(1)}%`;
}

function getGaugeColor(ratio: number | null | undefined): string {
  if (ratio == null) return "#9CA3AF";
  if (ratio >= 0.85) return "#22C55E";
  if (ratio >= 0.80) return "#F59E0B";
  return "#EF4444";
}

function FinancialPerformanceDashboard({ summary, isLoading }: Props) {
  const barData = useMemo(() => {
    if (!summary?.by_service) return [];
    return summary.by_service
      .filter((s): s is { service_id: string; fare: number; tips: number; tolls: number; congestion: number } & Record<string, unknown> =>
        "fare" in s
      )
      .map((s) => ({
        service_id: s.service_id,
        fare: (s.fare as number) ?? 0,
        tips: (s.tips as number) ?? 0,
        tolls: (s.tolls as number) ?? 0,
        congestion: (s.congestion as number) ?? 0,
      }));
  }, [summary?.by_service]);

  const ratio = summary?.total?.ratio_pago_conductor;
  const ingresoBruto = summary?.total?.ingreso_bruto;
  const margenPromedio = summary?.total?.margen_promedio;

  const gaugeData = useMemo(
    () => [{ name: "ratio", value: ratio != null ? ratio * 100 : 0, fill: getGaugeColor(ratio) }],
    [ratio],
  );

  if (isLoading) {
    return <div className="text-center py-20 text-on-surface-variant">Cargando dashboard...</div>;
  }

  if (!summary) {
    return <div className="text-center py-20 text-on-surface-variant">Sin datos disponibles</div>;
  }

  return (
    <div className="space-y-6">
      {/* KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCard
          label="Ingreso Bruto Total"
          value={formatM(ingresoBruto)}
          accent="primary"
          sublabel={ingresoBruto != null ? `$${formatNum(Math.round(ingresoBruto))}` : undefined}
        />
        <SummaryCard
          label="Margen Promedio Plataforma"
          value={formatPct(margenPromedio)}
          accent="secondary"
          sublabel="Apps HVFHV"
        />
        <SummaryCard
          label="Ratio Pago Conductor"
          value={formatPct(ratio)}
          accent={ratio != null && ratio >= 0.85 ? "primary" : ratio != null && ratio >= 0.80 ? "warning" : "error"}
          sublabel="Apps HVFHV"
        />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Gauge */}
        <ChartCard title="Ratio Pago Conductor">
          <div className="relative h-[220px]">
            <ResponsiveContainer width="100%" height={200}>
              <RadialBarChart
                cx="50%"
                cy="80%"
                innerRadius="60%"
                outerRadius="90%"
                barSize={18}
                data={gaugeData}
                startAngle={180}
                endAngle={0}
              >
                <RadialBar
                  dataKey="value"
                  cornerRadius={4}
                  background={{ fill: "#E5E7EB" }}
                />
              </RadialBarChart>
            </ResponsiveContainer>
            <div className="absolute inset-x-0 bottom-6 text-center pointer-events-none">
              <div className="text-headline-lg font-bold tabular-nums">
                {formatPct(ratio)}
              </div>
              <div className="text-caption text-on-surface-variant mt-0.5">
                {ratio != null
                  ? ratio >= 0.85
                    ? "Saludable"
                    : ratio >= 0.80
                      ? "En observación"
                      : "Crítico"
                  : "Sin datos"}
              </div>
            </div>
          </div>
          <div className="flex justify-center gap-6 text-caption text-on-surface-variant mt-2">
            <span className="flex items-center gap-1">
              <span className="inline-block w-2.5 h-2.5 rounded-sm bg-green-500" /> &ge;85%
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-2.5 h-2.5 rounded-sm bg-amber-500" /> 80–85%
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-2.5 h-2.5 rounded-sm bg-red-500" /> &lt;80%
            </span>
          </div>
        </ChartCard>

        {/* Grouped BarChart */}
        <ChartCard title="Componentes de Tarifa por Servicio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                dataKey="service_id"
                tickFormatter={(v: string) => SERVICE_LABELS[v] ?? v}
                tick={{ fontSize: 10 }}
              />
              <YAxis tickFormatter={(v: number) => formatM(v)} tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: number) => formatM(v)} />
              <Legend />
              <Bar dataKey="fare" fill={BAR_COLORS.fare} radius={[4, 4, 0, 0]} name="Tarifa" />
              <Bar dataKey="tips" fill={BAR_COLORS.tips} radius={[4, 4, 0, 0]} name="Propinas" />
              <Bar dataKey="tolls" fill={BAR_COLORS.tolls} radius={[4, 4, 0, 0]} name="Peajes" />
              <Bar dataKey="congestion" fill={BAR_COLORS.congestion} radius={[4, 4, 0, 0]} name="Congestión" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Matrix table */}
      {summary.matrix && summary.matrix.length > 0 && (
        <ChartCard title="Matriz de Ingresos por Período y Servicio">
          <div className="overflow-x-auto max-h-[480px] overflow-y-auto">
            <table className="w-full text-body-sm">
              <thead>
                <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase tracking-wide sticky top-0 bg-surface-container-lowest">
                  <th className="px-3 py-2 text-left">Año</th>
                  <th className="px-3 py-2 text-left">Mes</th>
                  <th className="px-3 py-2 text-left">Tipo de Servicio</th>
                  <th className="px-3 py-2 text-right">Ingreso Total</th>
                  <th className="px-3 py-2 text-right">Margen Promedio</th>
                </tr>
              </thead>
              <tbody>
                {summary.matrix.map((row, i) => (
                  <tr
                    key={i}
                    className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"} hover:bg-surface-container-low transition-colors`}
                  >
                    <td className="px-3 py-2 tabular-nums">{row.year}</td>
                    <td className="px-3 py-2 tabular-nums">{String(row.month).padStart(2, "0")}</td>
                    <td className="px-3 py-2">{SERVICE_LABELS[row.service_id] ?? row.service_id}</td>
                    <td className="px-3 py-2 text-right tabular-nums">{formatM(row.ingreso_total)}</td>
                    <td className="px-3 py-2 text-right tabular-nums text-on-surface-variant">
                      {row.margen_promedio != null ? formatPct(row.margen_promedio) : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>
      )}
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
      <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">{title}</h4>
      {children}
    </div>
  );
}

export { FinancialPerformanceDashboard };
