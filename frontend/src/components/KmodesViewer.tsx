import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Cell,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar,
  LineChart, Line, FunnelChart, Funnel, LabelList, ResponsiveContainer,
} from "recharts";
import { Layers, Boxes, Crown, ChevronDown, ChevronUp } from "lucide-react";
import { apiGet } from "../lib/api";
import type { KmodesData, KmodesDistribution } from "../lib/types";
import { SummaryCard } from "./SummaryCard";

const COLORS = ["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a", "#2e7d32", "#f57c00"];

const PROVIDER_FEATURE: Record<string, string> = {
  fhvhv: "hvfhs_license_num",
  yellow: "payment_type",
  green: "payment_type",
  fhv: "payment_type",
};

const PROVIDER_LABEL: Record<string, string> = {
  fhvhv: "Proveedor de Servicio",
  yellow: "Tipo de Pago",
  green: "Tipo de Pago",
  fhv: "Tipo de Pago",
};

function featureLabel(feature: string, serviceId: string): string {
  if (feature === PROVIDER_FEATURE[serviceId]) return PROVIDER_LABEL[serviceId] ?? feature;
  if (feature === "borough_pu") return "Top Boroughs";
  if (feature === "franja_horaria") return "Franja Horaria";
  return feature;
}

function filterDistribution(
  distributions: KmodesDistribution[],
  clusterId: number,
  feature: string,
) {
  return distributions
    .filter((d) => d.cluster_id === clusterId && d.feature === feature)
    .sort((a, b) => b.count - a.count)
    .map((d) => ({ name: d.value, value: d.count, pct: d.pct }));
}

export function KmodesViewer({ serviceId }: { serviceId: string }) {
  const { data, isLoading, error } = useQuery<KmodesData>({
    queryKey: ["kmodes", serviceId],
    queryFn: () => apiGet<KmodesData>(`/ml/kmodes/${serviceId}`),
    staleTime: 5 * 60 * 1000,
  });

  const [selectedCluster, setSelectedCluster] = useState<number | null>(null);
  const [showDetail, setShowDetail] = useState(false);

  const sizes = useMemo(() => {
    if (!data?.sizes?.length) return [];
    return [...data.sizes].sort((a, b) => b.count - a.count);
  }, [data?.sizes]);

  const effectiveCluster = useMemo(() => {
    if (sizes.length === 0) return null;
    if (selectedCluster == null) return sizes[0].cluster_id;
    return sizes.some((s) => s.cluster_id === selectedCluster)
      ? selectedCluster
      : sizes[0].cluster_id;
  }, [sizes, selectedCluster]);

  if (isLoading) return <div className="py-10 text-center text-on-surface-variant">Cargando...</div>;
  if (error) return <div className="py-10 text-center text-error">Error: {(error as Error).message}</div>;
  if (!data) return null;

  const providerFeature = PROVIDER_FEATURE[serviceId] ?? "payment_type";
  const miniFeatures = ["borough_pu", "franja_horaria", providerFeature].filter((f) =>
    data.distributions.some((d) => d.feature === f),
  );

  const totalSamples = sizes.reduce((s, c) => s + c.count, 0);
  const dominant = sizes[0];
  const dominantPct = totalSamples > 0 && dominant ? (dominant.count / totalSamples) * 100 : 0;

  const funnelData = sizes.map((s, i) => ({
    name: `Clúster ${s.cluster_id}`,
    value: s.count,
    fill: COLORS[i % COLORS.length],
    cluster_id: s.cluster_id,
    pct: totalSamples > 0 ? ((s.count / totalSamples) * 100).toFixed(1) : "0",
  }));

  return (
    <div className="space-y-6">
      {/* Descripción del modelo */}
      <div className="bg-surface-container-lowest border border-border-subtle border-l-4 border-l-primary-container rounded-DEFAULT p-4 text-body-sm text-on-surface-variant leading-relaxed">
        <p>
          <strong className="text-on-surface">K-Modes</strong> agrupa viajes de{" "}
          <strong className="text-on-surface">{serviceId.toUpperCase()}</strong> según atributos
          categóricos (borough, franja horaria, proveedor, etc.) sin requerir variables numéricas.
          Cada clúster representa un perfil de viaje recurrente. Navega entre clústeres para explorar
          su composición; el embudo ordena los clústeres por tamaño poblacional de mayor a menor.
        </p>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <SummaryCard
          label="Total de Viajes"
          value={totalSamples.toLocaleString()}
          icon={<Boxes className="w-5 h-5" />}
          accent="primary"
          sublabel="Muestra clusterizada"
        />
        <SummaryCard
          label="N° de Clústeres"
          value={sizes.length}
          icon={<Layers className="w-5 h-5" />}
          accent="secondary"
          sublabel={data.variables.length > 0 ? `${data.variables.length} variables categóricas` : undefined}
        />
        <SummaryCard
          label="Clúster Dominante"
          value={dominant ? `C${dominant.cluster_id}` : "—"}
          icon={<Crown className="w-5 h-5" />}
          accent="warning"
          sublabel={dominant ? `${dominantPct.toFixed(1)}% de la muestra` : undefined}
        />
      </div>

      {/* FunnelChart */}
      {funnelData.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h3 className="text-headline-md text-secondary mb-1">Tamaño Poblacional por Clúster</h3>
          <p className="text-body-sm text-on-surface-variant mb-4">
            Ordenado de mayor a menor. Haz clic en un trapezoide para inspeccionar ese clúster.
          </p>
          <ResponsiveContainer width="100%" height={Math.max(220, funnelData.length * 70)}>
            <FunnelChart>
              <Tooltip
                formatter={(v: any, _name: any, props: any) => [
                  `${Number(v).toLocaleString()} viajes (${props?.payload?.pct}%)`,
                  props?.payload?.name,
                ]}
              />
              <Funnel
                dataKey="value"
                data={funnelData}
                isAnimationActive
                onClick={(payload: any) => {
                  if (payload?.cluster_id != null) setSelectedCluster(payload.cluster_id);
                }}
                cursor="pointer"
              >
                <LabelList
                  position="right"
                  fill="#1f2937"
                  stroke="none"
                  dataKey="name"
                  formatter={(val: any) => `${val}`}
                />
                <LabelList
                  position="center"
                  fill="#ffffff"
                  stroke="none"
                  dataKey="pct"
                  formatter={(val: any) => `${val}%`}
                />
              </Funnel>
            </FunnelChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Navegación por clúster */}
      {sizes.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4">
          <div className="text-label-md text-on-surface-variant uppercase tracking-wide mb-3">
            Clúster seleccionado
          </div>
          <div className="flex flex-wrap gap-2">
            {sizes.map((s, i) => {
              const isActive = effectiveCluster === s.cluster_id;
              return (
                <button
                  key={s.cluster_id}
                  onClick={() => setSelectedCluster(s.cluster_id)}
                  className={`px-3 py-2 rounded-DEFAULT text-label-md border transition-colors flex items-center gap-2 ${
                    isActive
                      ? "bg-secondary-container text-on-secondary-container border-secondary-container"
                      : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                  }`}
                >
                  <span
                    className="inline-block w-3 h-3 rounded-sm"
                    style={{ backgroundColor: COLORS[i % COLORS.length] }}
                  />
                  Clúster {s.cluster_id}
                  <span className="text-caption text-on-surface-variant">
                    ({totalSamples > 0 ? ((s.count / totalSamples) * 100).toFixed(1) : 0}%)
                  </span>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Mini BarCharts horizontales del clúster seleccionado */}
      {effectiveCluster != null && miniFeatures.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {miniFeatures.map((feature) => {
            const raw = filterDistribution(data.distributions, effectiveCluster, feature);
            const TOP_N = 20;
            const rows = raw.length > TOP_N
              ? [
                  ...raw.slice(0, TOP_N),
                  {
                    name: "Otros",
                    value: raw.slice(TOP_N).reduce((s, r) => s + r.value, 0),
                    pct: raw.slice(TOP_N).reduce((s, r) => s + r.pct, 0),
                  },
                ]
              : raw;
            return (
              <ChartCard key={feature} title={featureLabel(feature, serviceId)}>
                {rows.length === 0 ? (
                  <div className="text-body-sm text-on-surface-variant italic py-6 text-center">
                    Sin datos
                  </div>
                ) : (
                  <ResponsiveContainer width="100%" height={Math.max(160, rows.length * 32)}>
                    <BarChart
                      data={rows}
                      layout="vertical"
                      margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" horizontal={false} />
                      <XAxis type="number" tick={{ fontSize: 11 }} />
                      <YAxis
                        type="category"
                        dataKey="name"
                        width={110}
                        tick={{ fontSize: 11 }}
                      />
                      <Tooltip
                        formatter={(v: any, _n: any, props: any) => [
                          `${Number(v).toLocaleString()} viajes (${props?.payload?.pct ?? 0}%)`,
                          "Viajes",
                        ]}
                      />
                      <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                        {rows.map((_, i) => (
                          <Cell key={i} fill={COLORS[i % COLORS.length]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </ChartCard>
            );
          })}
        </div>
      )}

      {/* Sección de detalle técnico (colapsable) */}
      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT">
        <button
          onClick={() => setShowDetail((v) => !v)}
          className="w-full flex items-center justify-between p-4 text-left"
        >
          <h3 className="text-headline-md text-secondary">Detalle Técnico</h3>
          {showDetail ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
        </button>

        {showDetail && (
          <div className="px-6 pb-6 space-y-6">
            {/* Variables utilizadas */}
            <div>
              <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-3">
                Variables Utilizadas
              </h4>
              <div className="flex flex-wrap gap-2">
                {data.variables.length === 0 ? (
                  <span className="text-on-surface-variant italic">Sin datos</span>
                ) : (
                  data.variables.map((v) => (
                    <span
                      key={v}
                      className="px-3 py-1 bg-surface-container rounded-DEFAULT text-label-md text-on-surface border border-border-subtle"
                    >
                      {v}
                    </span>
                  ))
                )}
              </div>
            </div>

            {/* Centroides */}
            {data.centers.length > 1 && data.variables.length > 0 && (
              <div>
                <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-3">
                  Centroides de Clústeres
                </h4>
                <ResponsiveContainer width="100%" height={400}>
                  <RadarChart
                    data={
                      data.variables.slice(0, 8).map((v) => {
                        const point: Record<string, unknown> = { variable: v };
                        data.centers.forEach((c: Record<string, unknown>) => {
                          if (c[v] !== undefined) point[`Cluster ${c.cluster_id}`] = c[v];
                        });
                        return point;
                      })
                    }
                  >
                    <PolarGrid stroke="#E5E7EB" />
                    <PolarAngleAxis dataKey="variable" tick={{ fontSize: 12 }} />
                    <PolarRadiusAxis angle={30} domain={[0, "auto"]} />
                    <Tooltip />
                    {data.centers.map((c: Record<string, unknown>, i: number) => (
                      <Radar
                        key={i}
                        name={`Cluster ${c.cluster_id}`}
                        dataKey={`Cluster ${c.cluster_id}`}
                        stroke={COLORS[i % COLORS.length]}
                        fill={COLORS[i % COLORS.length]}
                        fillOpacity={0.1}
                      />
                    ))}
                    <Legend />
                  </RadarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Ajuste (Elbow) */}
            {data.tuning.length > 0 && (
              <div>
                <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-3">
                  Ajuste (Elbow)
                </h4>
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={data.tuning} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                    <XAxis dataKey="k" label={{ value: "K", position: "insideBottom", offset: -5 }} />
                    <YAxis label={{ value: "Costo", angle: -90, position: "insideLeft" }} />
                    <Tooltip />
                    <Legend />
                    {Object.keys(data.tuning[0] || {})
                      .filter((k) => k !== "k")
                      .map((key, i) => (
                        <Line
                          key={key}
                          type="monotone"
                          dataKey={key}
                          stroke={COLORS[i % COLORS.length]}
                          strokeWidth={2}
                          dot={{ r: 4 }}
                        />
                      ))}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Perfiles */}
            {data.profiles.length > 0 && (
              <div>
                <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-3">
                  Perfiles de Clústeres
                </h4>
                <div className="overflow-x-auto">
                  <table className="w-full text-body-sm">
                    <thead>
                      <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase">
                        {Object.keys(data.profiles[0]).map((col) => (
                          <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {data.profiles.map((row, i) => (
                        <tr
                          key={i}
                          className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}
                        >
                          {Object.values(row).map((val, j) => (
                            <td key={j} className="px-3 py-2 tabular-nums">{String(val ?? "")}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <p className="text-body-sm text-on-surface-variant leading-relaxed">
              El algoritmo K-Modes agrupa viajes según atributos categóricos (borough, franja horaria,
              tipo de pago, etc.) sin requerir variables numéricas. Cada clúster representa un perfil
              de viaje recurrente. El gráfico de radar compara los centroides entre clústeres; el
              gráfico de codo (elbow) muestra la reducción de costo al aumentar K, ayudando a
              seleccionar el número óptimo de clústeres. Los perfiles detallados se muestran en la
              tabla inferior.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function ChartCard({
  title,
  className,
  children,
}: {
  title: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={`bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 ${className ?? ""}`}>
      <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">{title}</h4>
      {children}
    </div>
  );
}
