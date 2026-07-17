import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Clock } from "lucide-react";
import {
  AreaChart, Area, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from "recharts";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { SummaryCard } from "./SummaryCard";
import { ChartCard } from "./ChartCard";
import type { MartSummary } from "../lib/types";

interface Props {
  summary?: MartSummary | null;
  isLoading?: boolean;
}

const SERVICE_COLORS: Record<string, string> = {
  yellow: "#F3C613",
  green: "#2A9D8F",
  fhvhv: "#5F9EA0",
  fhv: "#E63946",
};

const SERVICE_LABELS: Record<string, string> = {
  yellow: "Yellow Taxi",
  green: "Green Taxi",
  fhvhv: "Apps (Uber/Lyft)",
  fhv: "FHV Clásico",
};

const SERVICE_ORDER = ["yellow", "green", "fhvhv", "fhv"];

function formatM(v: number): string {
  return `${(v / 1_000_000).toFixed(1)}M`;
}

function formatNum(v: number): string {
  return v.toLocaleString();
}

const GEO_URL = "/api/v1/geo/boroughs";

function DemandVolumeDashboard({ summary, isLoading }: Props) {
  const areaData = useMemo(() => {
    if (!summary?.by_hour) return [];
    const grouped: Record<number, Record<string, number>> = {};
    for (const row of summary.by_hour) {
      if (!grouped[row.pickup_hour]) grouped[row.pickup_hour] = {};
      grouped[row.pickup_hour][row.service_id] = (grouped[row.pickup_hour][row.service_id] ?? 0) + row.viajes;
    }
    return Object.entries(grouped)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([hour, svcs]) => ({
        hour: `${String(hour).padStart(2, "0")}:00`,
        yellow: svcs.yellow ?? 0,
        green: svcs.green ?? 0,
        fhvhv: svcs.fhvhv ?? 0,
        fhv: svcs.fhv ?? 0,
      }));
  }, [summary?.by_hour]);

  const donutData = useMemo(() => {
    if (!summary?.by_service) return [];
    let apps = 0;
    let taxis = 0;
    for (const row of summary.by_service) {
      if (row.service_id === "fhvhv" || row.service_id === "fhv") apps += row.viajes;
      else taxis += row.viajes;
    }
    return [
      { name: "Apps (Uber/Lyft)", value: apps, color: "#5F9EA0" },
      { name: "Taxis (Yellow/Green)", value: taxis, color: "#F3C613" },
    ];
  }, [summary?.by_service]);

  const esperaProm = useMemo(() => {
    const t = summary?.total;
    if (!t || !t.viajes_con_espera || !t.espera_total_min) return null;
    return (t.espera_total_min as number) / (t.viajes_con_espera as number);
  }, [summary?.total]);

  const boroughVolume = useMemo(() => {
    const m: Record<string, number> = {};
    if (!summary?.by_borough) return m;
    for (const row of summary.by_borough) m[row.pu_borough] = row.viajes;
    return m;
  }, [summary?.by_borough]);

  const maxBoroughVol = useMemo(() => Math.max(...Object.values(boroughVolume), 1), [boroughVolume]);

  const { data: geoData } = useQuery({
    queryKey: ["boroughs-geojson"],
    queryFn: () => fetch(GEO_URL).then((r) => { if (!r.ok) throw new Error("Failed to load GeoJSON"); return r.json(); }),
    staleTime: Infinity,
  });

  const geoStyle = (feature: any) => {
    const vol = boroughVolume[feature.properties.borough] ?? 0;
    const opacity = Math.max(0.05, Math.min(0.85, vol / maxBoroughVol));
    return {
      fillColor: "#a00003",
      weight: 1,
      opacity: 0.6,
      color: "#5c403b",
      fillOpacity: opacity,
    };
  };

  const onEachGeo = (feature: any, layer: any) => {
    const vol = boroughVolume[feature.properties.borough];
    layer.bindTooltip(
      `<strong>${feature.properties.borough}</strong><br/>${vol != null ? `${formatNum(vol)} viajes` : "Sin datos"}`,
      { sticky: true },
    );
  };

  const totalViajes = (summary?.total?.viajes as number | undefined) ?? 0;

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
          label="Total de Viajes"
          value={formatM(totalViajes)}
          accent="primary"
          sublabel={formatNum(totalViajes)}
        />
        <SummaryCard
          label="Espera Prom. (Apps)"
          value={esperaProm != null ? `${esperaProm.toFixed(1)} min` : "N/A"}
          icon={<Clock className="w-5 h-5" />}
          accent="secondary"
          sublabel={esperaProm == null ? "Sin datos de espera" : "HVFHV (Uber/Lyft)"}
        />
        <div
          className="bg-surface-container-lowest border border-border-subtle border-l-4 border-l-amber-500 rounded-DEFAULT p-4 flex items-start gap-3"
        >
          <div className="min-w-0 flex-1">
            <div className="text-caption text-on-surface-variant uppercase tracking-wide truncate">% Viajes en Apps vs Taxis</div>
            <div className="h-[80px]">
              {donutData.length > 0 && (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={donutData} dataKey="value" cx="50%" cy="50%" innerRadius={22} outerRadius={38} startAngle={90} endAngle={-270}>
                      {donutData.map((d, i) => (
                        <Cell key={i} fill={d.color} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(v: any) => formatM(Number(v))} />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </div>
            {donutData.length > 0 && (
              <div className="flex justify-around text-caption mt-1">
                {donutData.map((d) => (
                  <div key={d.name} className="flex items-center gap-1">
                    <span className="inline-block w-2 h-2 rounded-sm" style={{ backgroundColor: d.color }} />
                    <span className="text-on-surface-variant">{d.name}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-7 gap-6">
        <ChartCard title="Volumen de Viajes por Fecha" className="lg:col-span-4">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={summary.timeline as any[]} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="fecha_viaje" tick={{ fontSize: 10 }} />
              <YAxis tickFormatter={(v: any) => formatM(Number(v))} domain={[0, "dataMax"]} tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: any) => [formatM(Number(v)), "Viajes"]} />
              <Line type="monotone" dataKey="viajes" stroke="#a00003" strokeWidth={2} dot={false} name="Viajes" />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Viajes por Bloque Horario" className="lg:col-span-3">
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={areaData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="hour" tick={{ fontSize: 10 }} />
              <YAxis tickFormatter={(v: any) => formatM(Number(v))} domain={[0, 30_000_000]} tick={{ fontSize: 11 }} />
              <Tooltip formatter={(v: any) => formatM(Number(v))} />
              <Legend formatter={(value: string) => SERVICE_LABELS[value] ?? value} />
              {SERVICE_ORDER.map((s) => (
                <Area key={s} type="monotone" dataKey={s} stackId="1" stroke={SERVICE_COLORS[s]} fill={SERVICE_COLORS[s]} name={s} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Map */}
      <ChartCard title="Mapa de Calor por Borough">
        <div className="h-[400px] rounded overflow-hidden border border-border-subtle">
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
      </ChartCard>
    </div>
  );
}

export { DemandVolumeDashboard };
