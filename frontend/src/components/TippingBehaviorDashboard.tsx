import { useMemo } from "react";
import { Info, Percent, DollarSign, TrendingDown } from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
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

const SERVICE_ORDER = ["yellow", "green", "fhvhv", "fhv"];

const GENEROSITY_ORDER = ["Sin Propina", "Baja", "Estándar", "Alta"];

const GENEROSITY_COLORS: Record<string, string> = {
  "Sin Propina": "#E63946",
  "Baja": "#F3C613",
  "Estándar": "#2A9D8F",
  "Alta": "#5F9EA0",
};

function formatPct(v: number): string {
  return `${(v ?? 0).toFixed(1)}%`;
}

function formatNum(v: number): string {
  return v.toLocaleString();
}

function TippingBehaviorDashboard({ summary, isLoading }: Props) {
  const kpi = useMemo(() => summary?.total ?? {}, [summary?.total]);

  const boroughCompare = useMemo(() => {
    if (!summary?.by_borough_origin || !summary?.by_borough_destination) return [];
    const destMap: Record<string, number> = {};
    for (const d of summary.by_borough_destination as any[]) {
      destMap[d.do_borough] = d.pct_propina;
    }
    return (summary.by_borough_origin as any[])
      .map((o: any) => ({
        borough: o.pu_borough,
        origen: o.pct_propina,
        destino: destMap[o.pu_borough] ?? null,
      }))
      .filter((d) => d.borough != null);
  }, [summary?.by_borough_origin, summary?.by_borough_destination]);

  const stackData = useMemo(() => {
    if (!summary?.generosity_by_service) return [];
    const grouped: Record<string, Record<string, number>> = {};
    for (const row of summary.generosity_by_service as any[]) {
      if (!grouped[row.service_id]) grouped[row.service_id] = {};
      grouped[row.service_id][row.categoria_generosidad] =
        (grouped[row.service_id][row.categoria_generosidad] ?? 0) + row.viajes;
    }
    return SERVICE_ORDER
      .filter((s) => grouped[s])
      .map((s) => ({
        service_id: s,
        "Sin Propina": grouped[s]["Sin Propina"] ?? 0,
        "Baja": grouped[s]["Baja"] ?? 0,
        "Estándar": grouped[s]["Estándar"] ?? 0,
        "Alta": grouped[s]["Alta"] ?? 0,
      }));
  }, [summary?.generosity_by_service]);

  if (isLoading) {
    return <div className="text-center py-20 text-on-surface-variant">Cargando dashboard...</div>;
  }

  if (!summary) {
    return <div className="text-center py-20 text-on-surface-variant">Sin datos disponibles</div>;
  }

  return (
    <div className="space-y-6">
      {/* Explanatory banner */}
      <div className="bg-surface-container-lowest border border-border-subtle border-l-4 border-l-amber-500 rounded-DEFAULT p-4 flex items-start gap-3">
        <Info className="w-5 h-5 text-amber-600 shrink-0 mt-0.5" />
        <div>
          <p className="text-body-sm text-on-surface-variant leading-relaxed">
            Las propinas se registran únicamente en pagos con tarjeta de crédito; los pagos en efectivo no reflejan propinas reales.
            Los KPIs se calculan sobre viajes con tarjeta de crédito o plataformas (HVFHV).
          </p>
        </div>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCard
          label="% Propina Promedio"
          value={formatPct(kpi.pct_propina_promedio as number)}
          accent="primary"
          icon={<Percent className="w-5 h-5" />}
          sublabel="solo pagos con tarjeta"
        />
        <SummaryCard
          label="Propina Prom. por Milla"
          value={`$${(kpi.propina_prom_por_milla ?? 0).toFixed(4)}`}
          accent="secondary"
          icon={<DollarSign className="w-5 h-5" />}
          sublabel="por milla, solo tarjeta"
        />
        <SummaryCard
          label="% Viajes Sin Propina"
          value={formatPct(kpi.pct_viajes_sin_propina as number)}
          accent="warning"
          icon={<TrendingDown className="w-5 h-5" />}
          sublabel={`de ${formatNum(kpi.viajes as number)} viajes con tarjeta`}
        />
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard title="% Propina Origen vs Destino por Borough">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={boroughCompare} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="borough" tick={{ fontSize: 10 }} />
              <YAxis tickFormatter={(v: any) => `${v}%`} domain={[0, "dataMax"]} tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: any) => [`${v != null ? v.toFixed(1) : "N/A"}%`]} />
              <Legend />
              <Bar dataKey="origen" fill="#a00003" radius={[4, 4, 0, 0]} name="% Propina Origen" />
              <Bar dataKey="destino" fill="#415f8e" radius={[4, 4, 0, 0]} name="% Propina Destino" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Distribución de Propinas por Tipo de Servicio">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={stackData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }} stackOffset="expand">
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="service_id" tickFormatter={(v: string) => SERVICE_LABELS[v] ?? v} tick={{ fontSize: 10 }} />
              <YAxis tickFormatter={(v: any) => `${(v ?? 0) * 100}%`} domain={[0, 1]} tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: any) => `${((v ?? 0) * 100).toFixed(0)}%`} />
              <Legend />
              {GENEROSITY_ORDER.map((cat) => (
                <Bar key={cat} dataKey={cat} stackId="g" fill={GENEROSITY_COLORS[cat]} name={cat} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}

function ChartCard({ title, className, children }: { title: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={`bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 ${className ?? ""}`}>
      <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">{title}</h4>
      {children}
    </div>
  );
}

export { TippingBehaviorDashboard };
