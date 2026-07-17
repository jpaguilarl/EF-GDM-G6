import { useMemo, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { useClusterList } from "../hooks/useSpeedState";
import { SummaryCard } from "./SummaryCard";
import { Layers, BarChart4 } from "lucide-react";

const COLORS = ["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a", "#2e7d32", "#f57c00", "#6a1b9a"];

export function ClusterPanel() {
  const { data: rows = [], isLoading } = useClusterList();
  const [serviceFilter, setServiceFilter] = useState<string>("");

  const services = useMemo(() => {
    const s = new Set(rows.map((r) => r.service_id));
    return Array.from(s).sort();
  }, [rows]);

  const filtered = useMemo(
    () => (serviceFilter ? rows.filter((r) => r.service_id === serviceFilter) : rows),
    [rows, serviceFilter],
  );

  const clusterDist = useMemo(() => {
    const grouped: Record<string, number> = {};
    for (const r of filtered) {
      const k = `Cluster ${r.cluster_id}`;
      grouped[k] = (grouped[k] || 0) + 1;
    }
    return Object.entries(grouped)
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count);
  }, [filtered]);

  const columns = ["trip_id", "cluster_id", "service_id"];

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <SummaryCard label="Viajes clusterizados" value={rows.length} icon={<Layers className="w-5 h-5" />} accent="primary" />
        <SummaryCard label="Clusters distintos" value={clusterDist.length} icon={<BarChart4 className="w-5 h-5" />} accent="secondary" />
        <SummaryCard label="Servicios" value={services.length} accent="warning" />
      </div>

      {services.length > 0 && (
        <div className="flex gap-2 flex-wrap">
          <button
            onClick={() => setServiceFilter("")}
            className={`px-3 py-1.5 rounded-DEFAULT text-caption border transition-colors ${
              !serviceFilter
                ? "bg-primary-container text-on-primary-container border-primary-container"
                : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
            }`}
          >
            Todos
          </button>
          {services.map((s) => (
            <button
              key={s}
              onClick={() => setServiceFilter(s)}
              className={`px-3 py-1.5 rounded-DEFAULT text-caption border transition-colors ${
                serviceFilter === s
                  ? "bg-primary-container text-on-primary-container border-primary-container"
                  : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
              }`}
            >
              {s.toUpperCase()}
            </button>
          ))}
        </div>
      )}

      {clusterDist.length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
          <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">
            Distribución de Clusters
          </h4>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={clusterDist} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Bar dataKey="count" fill={COLORS[0]} radius={[4, 4, 0, 0]} name="Viajes" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
        <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">
          Asignaciones ({filtered.length})
        </h4>
        {isLoading ? (
          <div className="flex items-center justify-center py-16 text-on-surface-variant italic">Cargando...</div>
        ) : filtered.length === 0 ? (
          <div className="flex items-center justify-center py-16 text-on-surface-variant italic">Sin registros</div>
        ) : (
          <div className="overflow-x-auto max-h-96 overflow-y-auto">
            <table className="w-full text-body-sm">
              <thead className="sticky top-0 bg-surface-container-lowest">
                <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase tracking-wide">
                  {columns.map((col) => (
                    <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.map((row, i) => (
                  <tr key={row.trip_id} className={`border-b border-border-subtle ${i % 2 === 0 ? "" : "bg-surface-muted"}`}>
                    {columns.map((col) => {
                      const r = row as unknown as Record<string, unknown>;
                      return (
                        <td key={col} className="px-3 py-1.5 whitespace-nowrap tabular-nums">
                          {String(r[col] ?? "")}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
