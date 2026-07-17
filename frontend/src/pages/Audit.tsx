import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ClipboardList, Database, HardDrive,
  AlertTriangle, CheckCircle, Layers, BarChart3, Table2,
} from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell, ResponsiveContainer,
} from "recharts";
import { apiGet } from "../lib/api";
import type { AuditSummary, AuditLineageRow } from "../lib/types";
import { SummaryCard } from "../components/SummaryCard";
import { ChartCard } from "../components/ChartCard";
import { DataTable } from "../components/DataTable";
import { AuditFlowNodes } from "../components/AuditFlowNodes";

const LAYERS = ["bronze", "silver", "gold"] as const;
type Layer = (typeof LAYERS)[number];
const PAGE_SIZE = 50;
const COLORS = ["#a00003", "#415f8e", "#004aa0", "#5c403b", "#916f6a", "#2e7d32", "#f57c00", "#6a1b9a"];

const LAYER_EXPLANATIONS: Record<Layer, string> = {
  bronze:
    "Archivos parquet descargados desde el repositorio TLC de NYC. Cada archivo representa un mes de datos para una categoría de servicio (yellow, green, fhv, fhvhv).",
  silver:
    "Datos que pasaron el control de calidad. Las filas rechazadas se descartan por incompletitud, inconsistencias temporales, IDs de zona inválidos o duplicados exactos.",
  gold:
    "Marts analíticos y feature stores construidos sobre los datos limpios de silver. Cada mart tiene un grano agregado (fecha × bloque horario × zona) para Power BI.",
};

function formatBytes(bytes: number) {
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
  return `${(bytes / 1e3).toFixed(0)} KB`;
}

function formatSec(sec: number) {
  if (sec >= 3600) return `${(sec / 3600).toFixed(1)} h`;
  if (sec >= 60) return `${(sec / 60).toFixed(0)} min`;
  return `${sec.toFixed(0)} s`;
}

export function Audit() {
  const [focusLayer, setFocusLayer] = useState<Layer>("silver");
  const [lineagePage, setLineagePage] = useState(0);

  const { data: focusSummary, isLoading: focusLoading } = useQuery<AuditSummary>({
    queryKey: ["audit-summary", focusLayer],
    queryFn: () => apiGet<AuditSummary>(`/audit/${focusLayer}/summary`),
  });

  const { data: silverSummary } = useQuery<AuditSummary>({
    queryKey: ["audit-summary", "silver"],
    queryFn: () => apiGet<AuditSummary>("/audit/silver/summary"),
  });

  const { data: lineageData, isLoading: lineageLoading } = useQuery({
    queryKey: ["audit-lineage", lineagePage],
    queryFn: () => apiGet<{ rows: AuditLineageRow[]; total: number }>(
      `/audit/lineage?limit=${PAGE_SIZE}&offset=${lineagePage * PAGE_SIZE}`,
    ),
  });

  const rejectionData = useMemo(() => {
    if (!silverSummary?.by_month) return [];
    return silverSummary.by_month.map((m: any) => ({
      period: m.period,
      Aprobadas: m.quality_rows ?? 0,
      Rechazadas: m.quarantined_rows ?? 0,
    }));
  }, [silverSummary?.by_month]);

  const lineageColumns = useMemo(() => [
    {
      key: "audit_id",
      label: "Audit ID",
      render: (v: unknown) => String(v).substring(0, 8) + "…",
    },
    {
      key: "layer",
      label: "Capa",
      render: (v: unknown) => {
        const layer = String(v);
        const badge = {
          bronze: "bg-amber-100 text-amber-800",
          silver: "bg-slate-200 text-slate-700",
          gold: "bg-yellow-100 text-yellow-800",
        }[layer] ?? "bg-surface-muted text-on-surface-variant";
        return <span className={`px-2 py-0.5 rounded text-caption font-medium ${badge}`}>{layer}</span>;
      },
    },
    { key: "source_name", label: "Archivo / Mart" },
    {
      key: "rows_in",
      label: "Filas Entrada",
      render: (v: unknown) => (v != null ? Number(v).toLocaleString() : "—"),
    },
    {
      key: "rows_out",
      label: "Filas Salida",
      render: (v: unknown) => Number(v).toLocaleString(),
    },
    {
      key: "rows_rejected",
      label: "Rechazadas",
      render: (v: unknown) => (v != null ? Number(v).toLocaleString() : "—"),
    },
    {
      key: "duration_sec",
      label: "Duración",
      render: (v: unknown) => formatSec(Number(v)),
    },
    {
      key: "start_timestamp",
      label: "Inicio",
      render: (v: unknown) => new Date(String(v)).toLocaleString(),
    },
    {
      key: "fk_audit_id",
      label: "FK Audit",
      render: (v: unknown) => (v ? String(v).substring(0, 8) + "…" : "—"),
    },
  ], []);

  return (
    <div className="space-y-6 max-w-6xl">
      <h1 className="text-headline-lg text-primary-container font-bold flex items-center gap-3">
        <ClipboardList className="w-7 h-7" />
        Auditoría
      </h1>

      <AuditFlowNodes />

      {/* Monthly Rejection Chart (always visible, from silver data) */}
      {rejectionData.length > 0 && (
        <ChartCard title="Histórico de Rechazos Mensuales">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={rejectionData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="period" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Bar dataKey="Aprobadas" fill="#415f8e" radius={[4, 4, 0, 0]} stackId="a" />
              <Bar dataKey="Rechazadas" fill="#a00003" radius={[4, 4, 0, 0]} stackId="a" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Focus Pills */}
      <div className="flex items-center justify-between">
        <h2 className="text-headline-md text-secondary capitalize">
          Detalle: {LAYER_EXPLANATIONS[focusLayer]}
        </h2>
        <div className="flex gap-1 bg-surface-muted rounded-DEFAULT p-0.5">
          {LAYERS.map((l) => (
            <button
              key={l}
              onClick={() => setFocusLayer(l)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-DEFAULT text-label-md transition-colors capitalize ${
                focusLayer === l
                  ? "bg-surface-container-lowest text-on-surface shadow-sm"
                  : "text-on-surface-variant hover:text-on-surface"
              }`}
            >
              <Table2 className="w-4 h-4" />
              {l}
            </button>
          ))}
        </div>
      </div>

      {/* Per-layer KPI + Chart */}
      {focusLoading ? (
        <div className="text-center py-10 text-on-surface-variant">Cargando resumen...</div>
      ) : focusSummary ? (
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {focusLayer === "bronze" && (
              <>
                <SummaryCard label="Archivos" value={focusSummary.total_files ?? 0} icon={<Database className="w-5 h-5" />} accent="primary" />
                <SummaryCard label="Total Filas" value={focusSummary.total_rows ?? 0} icon={<BarChart3 className="w-5 h-5" />} accent="secondary" />
                <SummaryCard label="Volumen" value={formatBytes(focusSummary.total_bytes ?? 0)} icon={<HardDrive className="w-5 h-5" />} accent="primary" sublabel={`${formatSec(focusSummary.total_duration_sec ?? 0)} total`} />
              </>
            )}
            {focusLayer === "silver" && (
              <>
                <SummaryCard label="Filas Aprobadas" value={focusSummary.total_quality_rows ?? 0} icon={<CheckCircle className="w-5 h-5" />} accent="secondary" />
                <SummaryCard label="Filas Rechazadas" value={focusSummary.total_quarantined_rows ?? 0} icon={<AlertTriangle className="w-5 h-5" />} accent="error" />
                <SummaryCard label="Tasa de Rechazo" value={`${focusSummary.overall_reject_rate ?? 0}%`} icon={<Layers className="w-5 h-5" />} accent={focusSummary.overall_reject_rate != null && focusSummary.overall_reject_rate > 5 ? "error" : "warning"} sublabel={`Bronce: ${(focusSummary.total_bronze_rows ?? 0).toLocaleString()} filas`} />
              </>
            )}
            {focusLayer === "gold" && (
              <>
                <SummaryCard label="Ejecuciones" value={focusSummary.total_builds ?? 0} icon={<Database className="w-5 h-5" />} accent="primary" />
                <SummaryCard label="Filas Generadas" value={focusSummary.total_output_rows ?? 0} icon={<BarChart3 className="w-5 h-5" />} accent="secondary" />
                <SummaryCard label="Marts Distintos" value={focusSummary.by_mart?.length ?? 0} icon={<Layers className="w-5 h-5" />} accent="primary" sublabel={`${formatSec(focusSummary.total_duration_sec ?? 0)} total`} />
              </>
            )}
          </div>

          {/* Distribution chart */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {focusLayer === "bronze" && focusSummary.by_category && focusSummary.by_category.length > 0 && (
              <ChartCard title="Distribución por Categoría">
                <ResponsiveContainer width="100%" height={280}>
                  <PieChart>
                    <Pie
                      data={focusSummary.by_category}
                      dataKey="rows"
                      nameKey="category"
                      cx="50%" cy="50%" outerRadius={90}
                      label={(entry: any) => `${entry.category}: ${(entry.rows / 1e6).toFixed(1)}M`}
                    >
                      {focusSummary.by_category.map((_, i) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </ChartCard>
            )}
            {focusLayer === "silver" && focusSummary.by_category && focusSummary.by_category.length > 0 && (
              <ChartCard title="Tasa de Rechazo por Categoría">
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={focusSummary.by_category} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                    <XAxis dataKey="category" tick={{ fontSize: 11 }} />
                    <YAxis unit="%" tick={{ fontSize: 11 }} />
                    <Tooltip />
                    <Bar dataKey="reject_rate" fill="#a00003" radius={[4, 4, 0, 0]} name="Tasa de rechazo %" />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            )}
            {focusLayer === "gold" && focusSummary.mode_breakdown && focusSummary.mode_breakdown.length > 0 && (
              <ChartCard title="Modo de Ejecución">
                <ResponsiveContainer width="100%" height={280}>
                  <PieChart>
                    <Pie
                      data={focusSummary.mode_breakdown}
                      dataKey="count"
                      nameKey="mode"
                      cx="50%" cy="50%" outerRadius={90}
                      label={(entry: any) => `${entry.mode}: ${entry.count}`}
                    >
                      {focusSummary.mode_breakdown.map((_, i) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip />
                  </PieChart>
                </ResponsiveContainer>
              </ChartCard>
            )}
          </div>
        </div>
      ) : null}

      {/* Lineage Table */}
      <div className="flex items-center gap-2">
        <Table2 className="w-5 h-5 text-on-surface-variant" />
        <span className="text-headline-md text-secondary">Linaje de Datos</span>
      </div>
      <DataTable
        rows={(lineageData?.rows ?? []) as any}
        total={lineageData?.total ?? 0}
        pageSize={PAGE_SIZE}
        page={lineagePage}
        onPageChange={setLineagePage}
        loading={lineageLoading}
        columns={lineageColumns}
      />
    </div>
  );
}
