import { useMemo } from "react";
import {
  ScatterChart, Scatter, PieChart, Pie, Cell, ComposedChart, Bar, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ZAxis,
  ResponsiveContainer,
} from "recharts";
import { SummaryCard } from "./SummaryCard";
import type { MartSummary } from "../lib/types";

interface Props {
  summary?: MartSummary | null;
  isLoading?: boolean;
}

const BLOCK_COLORS: Record<string, string> = {
  Madrugada: "#2E4057",
  Mañana: "#F3C613",
  Mediodía: "#E68A2E",
  Tarde: "#2A9D8F",
  Noche: "#415f8e",
  Medianoche: "#6a1b9a",
};

const FALLBACK_COLORS = ["#a00003", "#004aa0", "#5c403b", "#916f6a", "#2e7d32"];

function blockColor(block: string): string {
  return BLOCK_COLORS[block] ?? FALLBACK_COLORS[0];
}

function formatNum(v: number): string {
  return v.toLocaleString();
}

function OperationalProfileDashboard({ summary, isLoading }: Props) {
  const scatterGroups = useMemo(() => {
    if (!summary?.scatter) return [];
    const grouped: Record<string, { bloque_horario: string; service_id: string; duracion: number; distancia: number }[]> = {};
    for (const pt of summary.scatter) {
      if (!grouped[pt.bloque_horario]) grouped[pt.bloque_horario] = [];
      grouped[pt.bloque_horario].push(pt);
    }
    return Object.entries(grouped).map(([block, pts], idx) => ({
      block,
      data: pts,
      color: blockColor(block) ?? FALLBACK_COLORS[idx % FALLBACK_COLORS.length],
    }));
  }, [summary?.scatter]);

  const pieData = useMemo(() => {
    const sh = summary?.shared_efficiency;
    if (!sh || !sh.viajes) return [];
    const sinMatch = sh.viajes - sh.viajes_match;
    return [
      { name: "Match", value: Math.max(sh.viajes_match, 0), color: "#2A9D8F" },
      { name: "Sin Match", value: Math.max(sinMatch, 0), color: "#E63946" },
    ];
  }, [summary?.shared_efficiency]);

  const composedData = useMemo(() => {
    if (!summary?.by_block) return [];
    return summary.by_block.map((b) => ({
      bloque_horario: b.bloque_horario,
      distancia_total: b.distancia_total ?? 0,
      velocidad_promedio: b.velocidad_promedio,
    }));
  }, [summary?.by_block]);

  const total = summary?.total || {};
  const duracion = total.duracion_promedio as number | undefined;
  const velocidad = total.velocidad_promedio as number | undefined;
  const tasa = total.tasa_ocupacion as number | null | undefined;

  if (isLoading) {
    return <div className="text-center py-20 text-on-surface-variant">Cargando dashboard...</div>;
  }

  if (!summary) {
    return <div className="text-center py-20 text-on-surface-variant">Sin datos disponibles</div>;
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCard
          label="Duración Promedio de Viaje"
          value={duracion != null ? `${duracion.toFixed(1)} min` : "N/A"}
          accent="primary"
        />
        <SummaryCard
          label="Velocidad Promedio"
          value={velocidad != null ? `${velocidad.toFixed(1)} mph` : "N/A"}
          accent="secondary"
        />
        <SummaryCard
          label="Tasa de Ocupación Compartida"
          value={tasa != null ? `${(tasa * 100).toFixed(1)}%` : "N/A"}
          accent="warning"
          sublabel={tasa == null ? "Solo disponible para FHVHV (apps)" : "Viajes con match / total"}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        <ChartCard title="Duración vs Distancia por Franja Horaria" className="lg:col-span-3">
          {scatterGroups.length > 0 ? (
            <ResponsiveContainer width="100%" height={320}>
              <ScatterChart margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis dataKey="duracion" name="Duración" unit=" min" tick={{ fontSize: 10 }} type="number" domain={[0, "dataMax"]} />
                <YAxis dataKey="distancia" name="Distancia" unit=" mi" tick={{ fontSize: 10 }} type="number" domain={[0, "dataMax"]} />
                <ZAxis range={[50, 50]} />
                <Tooltip
                  cursor={{ strokeDasharray: "3 3" }}
                  formatter={(value: any, name: string) => [Number(value).toFixed(1), name === "distancia" ? "Distancia (mi)" : "Duración (min)"]}
                />
                <Legend />
                {scatterGroups.map((g) => (
                  <Scatter key={g.block} data={g.data} name={g.block} fill={g.color} />
                ))}
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-[320px] text-on-surface-variant">Sin datos de dispersión</div>
          )}
        </ChartCard>

        <ChartCard title="Eficiencia de Viajes Compartidos" className="lg:col-span-2">
          {pieData.length > 0 ? (
            <div className="flex flex-col items-center">
              <ResponsiveContainer width="100%" height={260}>
                <PieChart>
                  <Pie
                    data={pieData}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    outerRadius={90}
                    label={({ name, value }) => `${name}: ${formatNum(value)}`}
                  >
                    {pieData.map((entry) => (
                      <Cell key={entry.name} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(v: any) => formatNum(Number(v))} />
                </PieChart>
              </ResponsiveContainer>
              <p className="text-caption text-on-surface-variant mt-2 text-center max-w-xs">
                Datos correspondientes a viajes FHVHV (Uber/Lyft). Yellow y Green Taxi no ofrecen viajes compartidos.
              </p>
            </div>
          ) : (
            <div className="flex items-center justify-center h-[260px] text-on-surface-variant">Sin datos de viajes compartidos</div>
          )}
        </ChartCard>
      </div>

      <ChartCard title="Distancia Total y Velocidad Promedio por Bloque Horario">
        {composedData.length > 0 ? (
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={composedData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="bloque_horario" tick={{ fontSize: 10 }} />
              <YAxis yAxisId="left" tick={{ fontSize: 10 }} name="Distancia Total" unit=" mi" />
              <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 10 }} name="Velocidad Promedio" unit=" mph" />
              <Tooltip />
              <Legend />
              <Bar yAxisId="left" dataKey="distancia_total" fill="#a00003" radius={[4, 4, 0, 0]} name="Distancia Total (mi)" />
              <Line yAxisId="right" type="monotone" dataKey="velocidad_promedio" stroke="#004aa0" strokeWidth={2} dot={{ r: 4 }} name="Velocidad Promedio (mph)" />
            </ComposedChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex items-center justify-center h-[320px] text-on-surface-variant">Sin datos por bloque horario</div>
        )}
      </ChartCard>
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

export { OperationalProfileDashboard };
