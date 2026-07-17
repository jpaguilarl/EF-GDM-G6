import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, ChevronRight, BarChart3, Table2 } from "lucide-react";
import { apiGet } from "../lib/api";
import type { MartSummary } from "../lib/types";
import { AbcXyzDashboard } from "./AbcXyzDashboard";
import { DemandVolumeDashboard } from "./DemandVolumeDashboard";
import { FinancialPerformanceDashboard } from "./FinancialPerformanceDashboard";
import { SupplyDemandDashboard } from "./SupplyDemandDashboard";
import { OperationalProfileDashboard } from "./OperationalProfileDashboard";
import { TippingBehaviorDashboard } from "./TippingBehaviorDashboard";

const BASE = "/api/v1/historic";

function params(filters: Record<string, string | number | undefined>): string {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(filters)) {
    if (v !== undefined) p.set(k, String(v));
  }
  return p.toString();
}

function multiParams(filters: Record<string, (string | number)[] | undefined>): string {
  const p = new URLSearchParams();
  for (const [k, vals] of Object.entries(filters)) {
    if (vals && vals.length > 0) {
      for (const v of vals) p.append(k, String(v));
    }
  }
  return p.toString();
}

async function fetchMart(mart: string, limit: number, offset: number, years?: number[], months?: number[]): Promise<Record<string, unknown>[]> {
  const q = params({ limit, offset }) + "&" + multiParams({ year: years, month: months });
  const res = await fetch(`${BASE}/${mart}?${q}`);
  if (!res.ok) throw new Error(`Fetch ${mart} failed`);
  return res.json();
}

async function fetchCount(mart: string, years?: number[], months?: number[]): Promise<number> {
  const q = multiParams({ year: years, month: months });
  const res = await fetch(`${BASE}/${mart}/count${q ? "?" + q : ""}`);
  if (!res.ok) return 0;
  const data = await res.json();
  return data.total;
}

interface MartExplorerProps {
  mart: string;
  label: string;
  pageSize?: number;
  years?: number[];
  months?: number[];
}

const martExplanations: Record<string, string> = {
  "demand-volume":
    "Este mart muestra el volumen de viajes por zona y hora. El gráfico de líneas evidencia la tendencia diaria de viajes; las zonas con mayor demanda se listan a la derecha.",
  "financial-performance":
    "KPIs de ingreso bruto, margen de plataforma y ratio de pago al conductor (Apps). El gráfico de agujas monitorea el ratio de pago; las barras agrupadas desglosan tarifa, propinas, peajes y congestión por servicio. La tabla inferior presenta el detalle mensual por año y servicio.",
  "operational-profile":
    "Perfil operacional por bloque horario y borough. Velocidad promedio, distancia y duración de viaje ayudan a identificar patrones de congestión y eficiencia.",
  "supply-demand-balance":
    "Balance de oferta y demanda por zona y hora. El flujo neto mide la diferencia entre taxis entrantes y salientes; valores negativos (rojo) indican déficit de oferta. Las tarjetas KPI muestran zonas críticas, tiempo de espera promedio y flujo neto global.",
  "abc-xyz-zones":
    "Clasificación ABC/XYZ de zonas según ingresos y variabilidad. Zonas A (alto ingreso) y X (baja variabilidad) son las más estables y rentables.",
  "tipping-behavior":
    "Comportamiento de propinas por borough y tipo de servicio. Las tarjetas KPI muestran el porcentaje promedio de propina, la propina por milla y el porcentaje de viajes sin propina. Los KPIs se calculan solo sobre pagos con tarjeta de crédito.",
};

export function MartExplorer({ mart, label, pageSize = 50, years, months }: MartExplorerProps) {
  const [view, setView] = useState<"summary" | "table">("summary");
  const [page, setPage] = useState(0);

  const martId = `mart_${mart.replace(/-/g, "_")}`;

  const summaryQuery = multiParams({ year: years, month: months });
  const { data: summary, isLoading: summaryLoading } = useQuery<MartSummary>({
    queryKey: ["mart-summary", mart, years, months],
    queryFn: () => apiGet<MartSummary>(`/marts/${martId}/summary${summaryQuery ? "?" + summaryQuery : ""}`),
  });

  const { data: rows = [], isLoading: dataLoading } = useQuery({
    queryKey: ["mart", mart, years, months, pageSize, page],
    queryFn: () => fetchMart(mart, pageSize, page * pageSize, years, months),
    enabled: view === "table",
  });

  const { data: total = 0 } = useQuery({
    queryKey: ["mart-count", mart, years, months],
    queryFn: () => fetchCount(mart, years, months),
    staleTime: 30_000,
  });

  const totalPages = Math.ceil(total / pageSize);
  const tableColumns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <div className="space-y-4">
      {/* View Toggle */}
      <div className="flex items-center justify-between">
        <h3 className="text-headline-md text-secondary capitalize">{label}</h3>
        <div className="flex gap-1 bg-surface-muted rounded-DEFAULT p-0.5">
          <button
            onClick={() => setView("summary")}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-DEFAULT text-label-md transition-colors ${
              view === "summary" ? "bg-surface-container-lowest text-on-surface shadow-sm" : "text-on-surface-variant hover:text-on-surface"
            }`}
          >
            <BarChart3 className="w-4 h-4" /> Resumen
          </button>
          <button
            onClick={() => setView("table")}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-DEFAULT text-label-md transition-colors ${
              view === "table" ? "bg-surface-container-lowest text-on-surface shadow-sm" : "text-on-surface-variant hover:text-on-surface"
            }`}
          >
            <Table2 className="w-4 h-4" /> Tabla
          </button>
        </div>
      </div>

      {view === "summary" && (
        <>
          {summaryLoading ? (
            <div className="text-center py-10 text-on-surface-variant">Cargando resumen...</div>
          ) : summary ? (
            <div className="space-y-6">
              {/* Mart-specific charts */}
              {mart === "demand-volume" && (
                <DemandVolumeDashboard summary={summary} isLoading={summaryLoading} />
              )}
              {mart === "financial-performance" && (
                <FinancialPerformanceDashboard summary={summary} isLoading={summaryLoading} />
              )}
              {mart === "operational-profile" && (
                <OperationalProfileDashboard summary={summary} isLoading={summaryLoading} />
              )}
              {mart === "supply-demand-balance" && (
                <SupplyDemandDashboard summary={summary} isLoading={summaryLoading} />
              )}
              {mart === "abc-xyz-zones" && (
                <AbcXyzDashboard summary={summary} isLoading={summaryLoading} />
              )}
              {mart === "tipping-behavior" && (
                <TippingBehaviorDashboard summary={summary} isLoading={summaryLoading} />
              )}

              {/* Explanatory Text */}
              {martExplanations[mart] && (
                <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 text-body-sm text-on-surface-variant leading-relaxed">
                  <p>{martExplanations[mart]}</p>
                </div>
              )}
            </div>
          ) : null}
        </>
      )}

      {view === "table" && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
          <div className="text-caption text-on-surface-variant">
            {total.toLocaleString()} filas
          </div>

          {dataLoading ? (
            <div className="flex items-center justify-center py-20 text-on-surface-variant">Cargando...</div>
          ) : rows.length === 0 ? (
            <div className="flex items-center justify-center py-20 text-on-surface-variant italic">Sin datos</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-body-sm">
                <thead>
                  <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase tracking-wide">
                    {tableColumns.map((col) => (
                      <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, i) => (
                    <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"} hover:bg-surface-container-low transition-colors`}>
                      {tableColumns.map((col) => (
                        <td key={col} className="px-3 py-2 whitespace-nowrap tabular-nums">
                          {String(row[col] ?? "")}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-4 pt-2">
              <button
                onClick={() => setPage(Math.max(0, page - 1))}
                disabled={page === 0}
                className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronLeft className="w-4 h-4" /> Anterior
              </button>
              <span className="text-body-md text-on-surface-variant">
                Página {page + 1} de {totalPages}
              </span>
              <button
                onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                disabled={page >= totalPages - 1}
                className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted disabled:opacity-30 disabled:cursor-not-allowed"
              >
                Siguiente <ChevronRight className="w-4 h-4" />
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}


