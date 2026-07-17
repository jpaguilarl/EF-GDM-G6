import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { MartSummary } from "../lib/types";

interface Props {
  summary?: MartSummary | null;
  isLoading?: boolean;
}

const CLASS_COLORS: Record<string, string> = {
  AX: "#d4af37",
  AY: "#d4a843",
  AZ: "#a8852e",
  BX: "#5fa3d1",
  BY: "#3d84b0",
  BZ: "#25628a",
  CX: "#a8a8a8",
  CY: "#7a7a7a",
  CZ: "#4a4a4a",
};

const ABC_ORDER = ["A", "B", "C"] as const;
const XYZ_ORDER = ["X", "Y", "Z"] as const;

function formatCurrency(v: number): string {
  return `$${(v / 1_000_000).toFixed(2)}M`;
}

function AbcXyzDashboard({ summary, isLoading }: Props) {
  const scatter: Record<string, unknown>[] = (summary as any)?.scatter ?? [];

  const paretoData = useMemo(() => {
    if (!scatter.length) return [];
    const sorted = [...scatter].sort(
      (a, b) => (b.ingresos_totales_zona as number) - (a.ingresos_totales_zona as number),
    );
    const total = sorted.reduce((s, r) => s + (r.ingresos_totales_zona as number), 0);
    const topN = 40;
    let cumSum = 0;
    const rows = sorted.slice(0, topN).map((r) => {
      cumSum += r.ingresos_totales_zona as number;
      return {
        zone: r.zone as string,
        ingresos: r.ingresos_totales_zona as number,
        acumulado_pct: +((cumSum / total) * 100).toFixed(1),
      };
    });
    const rest = sorted.slice(topN);
    if (rest.length > 0) {
      cumSum += rest.reduce((s, r) => s + (r.ingresos_totales_zona as number), 0);
      rows.push({
        zone: "Otras zonas",
        ingresos: rest.reduce((s, r) => s + (r.ingresos_totales_zona as number), 0),
        acumulado_pct: +((cumSum / total) * 100).toFixed(1),
      });
    }
    return rows;
  }, [scatter]);

  const matrix = useMemo(() => {
    const m: Record<string, Record<string, number>> = {};
    for (const abc of ABC_ORDER) {
      m[abc] = { X: 0, Y: 0, Z: 0 };
    }
    for (const row of scatter) {
      const abc = row.clase_abc as string;
      const xyz = row.clase_xyz as string;
      if (m[abc]?.[xyz] !== undefined) m[abc][xyz]++;
    }
    return m;
  }, [scatter]);

  const { data: geoData } = useQuery({
    queryKey: ["zones-geojson"],
    queryFn: () =>
      fetch("/api/v1/geo/zones").then((r) => {
        if (!r.ok) throw new Error("Failed to load GeoJSON");
        return r.json();
      }),
    staleTime: Infinity,
  });

  const zoneClassMap = useMemo(() => {
    const m: Record<number, { clase: string; ingresos: number; zone: string; borough: string }> = {};
    for (const row of scatter) {
      const locId = row.pu_location_id as number;
      if (locId != null) {
        m[locId] = {
          clase: ((row.clase_abc as string) ?? "") + ((row.clase_xyz as string) ?? ""),
          ingresos: row.ingresos_totales_zona as number,
          zone: row.zone as string,
          borough: row.borough as string,
        };
      }
    }
    return m;
  }, [scatter]);

  const geoStyle = (feature: any) => {
    const locId = feature.properties.LocationID;
    const info = zoneClassMap[locId];
    const cls = info?.clase;
    return {
      fillColor: cls && CLASS_COLORS[cls] ? CLASS_COLORS[cls] : "#d1d5db",
      weight: 1,
      opacity: 0.6,
      color: "#5c403b",
      fillOpacity: cls ? 0.8 : 0.15,
    };
  };

  const onEachGeo = (feature: any, layer: any) => {
    const locId = feature.properties.LocationID;
    const info = zoneClassMap[locId];
    const props = feature.properties;
    if (info) {
      layer.bindTooltip(
        `<strong>${props.zone}</strong><br/>${props.borough}<br/><strong>${info.clase}</strong> | ${formatCurrency(info.ingresos)}`,
        { sticky: true },
      );
    } else {
      layer.bindTooltip(
        `<strong>${props.zone}</strong><br/>${props.borough}<br/>Sin datos`,
        { sticky: true },
      );
    }
  };

  if (isLoading) {
    return <div className="text-center py-20 text-on-surface-variant">Cargando dashboard...</div>;
  }

  if (!summary || !scatter.length) {
    return <div className="text-center py-20 text-on-surface-variant">Sin datos disponibles</div>;
  }

  return (
    <div className="space-y-6">
      <ChartCard title="Pareto de Ingresos por Zona">
        <ResponsiveContainer width="100%" height={360}>
          <ComposedChart data={paretoData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
            <XAxis dataKey="zone" tick={{ fontSize: 9 }} angle={-45} textAnchor="end" height={80} />
            <YAxis yAxisId="left" tickFormatter={(v: any) => formatCurrency(Number(v))} tick={{ fontSize: 11 }} />
            <YAxis yAxisId="right" orientation="right" domain={[0, 100]} unit="%" tick={{ fontSize: 11 }} />
            <Tooltip
              formatter={(value: any, name: string) => {
                if (name === "ingresos") return [formatCurrency(Number(value)), "Ingresos"];
                return [`${value}%`, "% Acumulado"];
              }}
              labelFormatter={(label) => `Zona: ${label}`}
            />
            <ReferenceLine y={80} yAxisId="right" stroke="#d4af37" strokeWidth={2} label={{ value: "80% — Límite Clase A", position: "right", fontSize: 11, fill: "#d4af37" }} />
            <Bar yAxisId="left" dataKey="ingresos" fill="#a00003" radius={[2, 2, 0, 0]} name="ingresos" />
            <Line yAxisId="right" type="monotone" dataKey="acumulado_pct" stroke="#415f8e" strokeWidth={2} dot={false} name="acumulado_pct" />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>

      <ChartCard title="Matriz ABC/XYZ">
        <div className="grid grid-cols-4 gap-2 max-w-lg mx-auto">
          <div className="flex items-center justify-center p-2" />
          {XYZ_ORDER.map((col) => (
            <div key={col} className="text-label-md text-on-surface-variant text-center p-2 font-bold">
              {col}
            </div>
          ))}
          {ABC_ORDER.map((abc) => (
            <>
              <div className="text-label-md text-on-surface-variant flex items-center justify-center font-bold p-2">
                {abc}
              </div>
              {XYZ_ORDER.map((xyz) => {
                const cls = abc + xyz;
                const count = matrix[abc]?.[xyz] ?? 0;
                const isHighlight = cls === "AX";
                return (
                  <div
                    key={cls}
                    className={`rounded-DEFAULT p-3 text-center transition-colors ${
                      isHighlight
                        ? "bg-[#d4af37] text-white shadow-md ring-2 ring-[#d4af37] ring-offset-1"
                        : "text-white"
                    }`}
                    style={!isHighlight ? { backgroundColor: CLASS_COLORS[cls] ?? "#e5e7eb" } : undefined}
                  >
                    <div className="text-headline-md font-bold">{count}</div>
                    <div className="text-caption opacity-80">{cls}</div>
                  </div>
                );
              })}
            </>
          ))}
        </div>
        <div className="mt-3 text-center text-caption text-on-surface-variant">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-3 rounded-sm bg-[#d4af37]" />
            AX — Estratégico (altos ingresos, baja variabilidad)
          </span>
        </div>
      </ChartCard>

      <ChartCard title="Mapa de Clasificación ABC/XYZ por Zona">
        <div className="h-[500px] rounded overflow-hidden border border-border-subtle">
          {geoData ? (
            <MapContainer center={[40.71, -74.0]} zoom={10} zoomControl={false} className="h-full w-full">
              <TileLayer
                attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
              />
              <GeoJSON data={geoData} style={geoStyle} onEachFeature={onEachGeo} />
            </MapContainer>
          ) : (
            <div className="flex items-center justify-center h-full text-on-surface-variant">Cargando mapa...</div>
          )}
        </div>
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-caption text-on-surface-variant">
          {Object.entries(CLASS_COLORS).map(([cls, color]) => (
            <span key={cls} className="inline-flex items-center gap-1">
              <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: color }} />
              {cls}
            </span>
          ))}
          <span className="inline-flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-sm bg-gray-300" />
            Sin datos
          </span>
        </div>
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

export { AbcXyzDashboard };
