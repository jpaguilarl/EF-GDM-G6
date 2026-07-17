import { useMemo } from "react";
import { AlertTriangle, Clock, Activity } from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as ReTooltip,
  ResponsiveContainer,
} from "recharts";
import { SummaryCard } from "./SummaryCard";
import type { MartSummary } from "../lib/types";

interface Props {
  summary?: MartSummary | null;
  isLoading?: boolean;
}

function formatNum(v: number): string {
  return v.toLocaleString();
}

function flowToColor(v: number, absMax: number): string {
  if (absMax === 0) return "#f0f0f0";
  const ratio = Math.max(-1, Math.min(1, v / absMax));
  if (ratio >= 0) {
    const p = ratio;
    return `rgb(${Math.round(255 - 213 * p)},${Math.round(255 - 98 * p)},${Math.round(255 - 112 * p)})`;
  } else {
    const p = Math.abs(ratio);
    return `rgb(${Math.round(255 - 25 * p)},${Math.round(255 - 198 * p)},${Math.round(255 - 185 * p)})`;
  }
}

function ChartCard({ title, className, children }: { title: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={`bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 ${className ?? ""}`}>
      <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">{title}</h4>
      {children}
    </div>
  );
}

export function SupplyDemandDashboard({ summary, isLoading }: Props) {
  const rows = useMemo(() => {
    if (!summary?.by_zone_hour?.length) return [];
    const seen = new Set<number>();
    const result: { location_id: number; zone: string; borough: string }[] = [];
    for (const r of summary.by_zone_hour) {
      if (!seen.has(r.location_id)) {
        seen.add(r.location_id);
        result.push({ location_id: r.location_id, zone: r.zone, borough: r.borough });
      }
    }
    return result;
  }, [summary?.by_zone_hour]);

  const cellValue = useMemo(() => {
    const map = new Map<string, number>();
    if (!summary?.by_zone_hour) return map;
    for (const r of summary.by_zone_hour) {
      map.set(`${r.location_id}-${r.hour}`, r.flujo_neto_oferta);
    }
    return map;
  }, [summary?.by_zone_hour]);

  const absMax = useMemo(() => {
    if (!summary?.by_zone_hour?.length) return 0;
    return Math.max(1, ...summary.by_zone_hour.map((r) => Math.abs(r.flujo_neto_oferta)));
  }, [summary?.by_zone_hour]);

  const HOURS = Array.from({ length: 21 }, (_, i) => i);

  const globalFlow = summary?.global_net_flow ?? 0;
  const criticalZones = summary?.critical_zones_count ?? 0;
  const avgWait = summary?.avg_wait_min;

  if (isLoading) {
    return <div className="text-center py-20 text-on-surface-variant">Cargando dashboard...</div>;
  }

  if (!summary) {
    return <div className="text-center py-20 text-on-surface-variant">Sin datos disponibles</div>;
  }

  return (
    <div className="space-y-6">
      {/* KPI row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCard
          label="Zonas Críticas"
          value={criticalZones}
          icon={<AlertTriangle className="w-5 h-5" />}
          accent="error"
          sublabel={`${summary.deficit_ratio ?? 0}% periodos en déficit`}
        />
        <SummaryCard
          label="Tiempo Espera (Apps)"
          value={avgWait != null ? `${avgWait.toFixed(1)} min` : "N/A"}
          icon={<Clock className="w-5 h-5" />}
          accent="secondary"
          sublabel={avgWait == null ? "Sin datos de espera" : "HVFHV (Uber/Lyft)"}
        />
        <SummaryCard
          label="Flujo Neto Global"
          value={formatNum(globalFlow)}
          icon={<Activity className="w-5 h-5" />}
          accent={globalFlow >= 0 ? "primary" : "error"}
          sublabel={globalFlow >= 0 ? "Superávit de oferta" : "Déficit de oferta"}
        />
      </div>

      {/* Heatmap */}
      <ChartCard title="Mapa de Calor: Flujo Neto por Zona y Hora">
        <div className="overflow-x-auto">
          <div
            className="grid gap-px min-w-[600px]"
            style={{
              gridTemplateColumns: `200px repeat(${HOURS.length}, minmax(36px, 1fr))`,
            }}
          >
            <div className="text-caption text-on-surface-variant px-2 py-1 font-medium" />
            {HOURS.map((h) => (
              <div
                key={h}
                className="text-caption text-on-surface-variant text-center px-1 py-1 font-medium"
              >
                {String(h).padStart(2, "0")}h
              </div>
            ))}

            {rows.map((row) => (
              <div key={row.location_id} className="contents">
                <div
                  className="text-caption text-on-surface-variant px-2 py-1 truncate flex items-center"
                  title={`${row.zone} (${row.borough})`}
                >
                  {row.zone}
                </div>
                {HOURS.map((h) => {
                  const v = cellValue.get(`${row.location_id}-${h}`) ?? 0;
                  const bg = flowToColor(v, absMax);
                  const lightText = Math.abs(v) > absMax * 0.6;
                  return (
                    <div
                      key={`${row.location_id}-${h}`}
                      className="h-7 flex items-center justify-center text-[10px] font-mono cursor-default transition-opacity hover:opacity-80"
                      style={{ backgroundColor: bg, color: lightText ? "#fff" : "#333" }}
                      title={`${row.zone} — ${String(h).padStart(2, "0")}:00\nFlujo: ${formatNum(v)}`}
                    >
                      {v !== 0 && Math.abs(v) >= absMax * 0.12 ? formatNum(v) : ""}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      </ChartCard>

      {/* Top 10 charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard title="Top 10 Desiertos de Servicio">
          <ResponsiveContainer width="100%" height={350}>
            <BarChart
              data={[...(summary.top_deficit_zones ?? [])].reverse()}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" horizontal={false} />
              <XAxis type="number" domain={["dataMin", 0]} tick={{ fontSize: 11 }} tickFormatter={(v: number) => formatNum(Math.abs(v))} />
              <YAxis type="category" dataKey="zone" tick={{ fontSize: 10 }} width={140} />
              <ReTooltip formatter={(v: any) => [formatNum(Math.abs(Number(v))), "Déficit"]} />
              <Bar dataKey="flujo_neto_oferta" fill="#E63946" radius={[0, 4, 4, 0]} name="Flujo Neto" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top 10 Acumulación de Vehículos">
          <ResponsiveContainer width="100%" height={350}>
            <BarChart
              data={summary.top_surplus_zones as any[]}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 20, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" horizontal={false} />
              <XAxis type="number" domain={[0, "dataMax"]} tick={{ fontSize: 11 }} tickFormatter={formatNum} />
              <YAxis type="category" dataKey="zone" tick={{ fontSize: 10 }} width={140} />
              <ReTooltip formatter={(v: any) => [formatNum(Number(v)), "Superávit"]} />
              <Bar dataKey="flujo_neto_oferta" fill="#2A9D8F" radius={[0, 4, 4, 0]} name="Flujo Neto" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}
