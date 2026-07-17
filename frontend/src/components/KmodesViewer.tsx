import { useQuery } from "@tanstack/react-query";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Cell,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar,
  LineChart, Line, ResponsiveContainer,
} from "recharts";
import { apiGet } from "../lib/api";

interface KmodesData {
  service_id: string;
  variables: string[];
  centers: Record<string, unknown>[];
  profiles: Record<string, unknown>[];
  sizes: { cluster_id: number; count: number }[];
  tuning: Record<string, unknown>[];
}

const COLORS = ["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a", "#2e7d32", "#f57c00"];

export function KmodesViewer({ serviceId }: { serviceId: string }) {
  const { data, isLoading, error } = useQuery<KmodesData>({
    queryKey: ["kmodes", serviceId],
    queryFn: () => apiGet<KmodesData>(`/ml/kmodes/${serviceId}`),
  });

  if (isLoading) return <div className="py-10 text-center text-on-surface-variant">Cargando...</div>;
  if (error) return <div className="py-10 text-center text-error">Error: {(error as Error).message}</div>;
  if (!data) return null;

  const totalSamples = data.sizes.reduce((s, c) => s + c.count, 0);

  const sizeWithPct = data.sizes.map(s => ({
    ...s,
    pct: totalSamples > 0 ? ((s.count / totalSamples) * 100).toFixed(1) : "0",
    label: `C${s.cluster_id} (${((s.count / totalSamples) * 100).toFixed(1)}%)`,
  }));

  return (
    <div className="space-y-6">
      {/* Summary KPI */}
      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 text-body-sm text-on-surface-variant leading-relaxed">
        <p>
          Clustering con K-Modes para <strong>{serviceId.toUpperCase()}</strong>.
          {totalSamples > 0 && (
            <> Se agruparon <strong>{totalSamples.toLocaleString()}</strong> viajes en{' '}
              <strong>{data.sizes.length}</strong> clusters usando variables categóricas
              (borough, franja horaria, tipo de pago, etc.).</>
            )}
        </p>
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
        <h3 className="text-headline-md text-secondary mb-4">Variables Utilizadas</h3>
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

      {data.sizes.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h3 className="text-headline-md text-secondary mb-4">Tamaño de Clusters</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={sizeWithPct} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="label" label={{ value: "Cluster ID", position: "insideBottom", offset: -5 }} />
              <YAxis label={{ value: "Cantidad", angle: -90, position: "insideLeft" }} />
              <Tooltip />
              <Bar dataKey="count" name="Viajes" radius={[4, 4, 0, 0]}>
                {sizeWithPct.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {data.centers.length > 1 && data.variables.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h3 className="text-headline-md text-secondary mb-4">Centroides de Clusters</h3>
          <ResponsiveContainer width="100%" height={400}>
            <RadarChart data={
              data.variables.slice(0, 8).map((v) => {
                const point: Record<string, unknown> = { variable: v };
                data.centers.forEach((c: Record<string, unknown>) => {
                  if (c[v] !== undefined) point[`Cluster ${c.cluster_id}`] = c[v];
                });
                return point;
              })
            }>
              <PolarGrid stroke="#E5E7EB" />
              <PolarAngleAxis dataKey="variable" tick={{ fontSize: 12 }} />
              <PolarRadiusAxis angle={30} domain={[0, "auto"]} />
              <Tooltip />
              {data.centers.map((c: Record<string, unknown>, i: number) => (
                <Radar
                  key={i}
                  name={`Cluster ${c.cluster_id}`}
                  dataKey={`Cluster ${c.cluster_id}`}
                  stroke={["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a"][i % 5]}
                  fill={["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a"][i % 5]}
                  fillOpacity={0.1}
                />
              ))}
              <Legend />
            </RadarChart>
          </ResponsiveContainer>
        </div>
      )}

      {data.tuning.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h3 className="text-headline-md text-secondary mb-4">Ajuste (Elbow)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={data.tuning} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="k" label={{ value: "K", position: "insideBottom", offset: -5 }} />
              <YAxis label={{ value: "Costo", angle: -90, position: "insideLeft" }} />
              <Tooltip />
              <Legend />
              {Object.keys(data.tuning[0] || {}).filter((k) => k !== "k").map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={["#a00003", "#415f8e", "#004aa0"][i % 3]}
                  strokeWidth={2}
                  dot={{ r: 4 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {data.profiles.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h3 className="text-headline-md text-secondary mb-4">Perfiles de Clusters</h3>
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
                  <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}>
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

      {/* Explanatory text */}
      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 text-body-sm text-on-surface-variant leading-relaxed">
        <p>
          El algoritmo K-Modes agrupa viajes según atributos categóricos (borough, franja horaria,
          tipo de pago, etc.) sin requerir variables numéricas. Cada cluster representa un perfil
          de viaje recurrente. La gráfica de radar compara los centroides entre clusters; el gráfico
          de codo (elbow) muestra la reducción de costo al aumentar K, ayudando a seleccionar el
          número óptimo de clusters. Los perfiles detallados se muestran en la tabla inferior.
        </p>
      </div>
    </div>
  );
}
