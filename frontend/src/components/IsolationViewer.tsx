import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, ChevronRight, Table2, ChevronDown, ChevronUp, ShieldAlert, AlertTriangle, Activity, Hash } from "lucide-react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { apiGet } from "../lib/api";
import type { IsolationSummary } from "../lib/types";
import { SummaryCard } from "../components/SummaryCard";

interface IsolationEntry {
  ratecode: string;
  metadata: Record<string, unknown>;
}

interface ScoresData {
  rows: Record<string, unknown>[];
  total: number;
}

export function IsolationViewer() {
  const [selected, setSelected] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [showTable, setShowTable] = useState(false);
  const PAGE_SIZE = 50;

  const { data: summary, isLoading: summaryLoading } = useQuery<IsolationSummary>({
    queryKey: ["isolation-summary"],
    queryFn: () => apiGet<IsolationSummary>("/ml/isolation/summary"),
  });

  const { data: ratecodes = [], isLoading } = useQuery<IsolationEntry[]>({
    queryKey: ["isolation"],
    queryFn: () => apiGet<IsolationEntry[]>("/ml/isolation"),
  });

  const { data: scores, isLoading: scoresLoading } = useQuery<ScoresData>({
    queryKey: ["isolation-scores", selected, page],
    queryFn: () => apiGet<ScoresData>(`/ml/isolation/${selected}/scores?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`),
    enabled: !!selected && showTable,
  });

  if (isLoading) return <div className="py-10 text-center text-on-surface-variant">Cargando...</div>;

  return (
    <div className="space-y-6">
      {/* Summary KPIs */}
      {summaryLoading ? (
        <div className="text-center py-4 text-on-surface-variant">Cargando resumen...</div>
      ) : summary ? (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <SummaryCard label="Viajes Evaluados" value={summary.total_scored} icon={<Activity className="w-5 h-5" />} accent="primary" />
            <SummaryCard label="Fraude Detectado" value={summary.fraud_count} icon={<ShieldAlert className="w-5 h-5" />} accent="error" />
            <SummaryCard label="Tasa de Fraude" value={`${summary.fraud_rate}%`} icon={<AlertTriangle className="w-5 h-5" />} accent={summary.fraud_rate > 5 ? "error" : "warning"} />
            <SummaryCard label="Ratecodes" value={summary.by_ratecode?.length ?? 0} icon={<Hash className="w-5 h-5" />} accent="primary" sublabel={`Score medio: ${summary.score_stats?.score_mean?.toFixed(4) ?? "-"}`} />
          </div>

          {/* Per-ratecode fraud chart */}
          {summary.by_ratecode && summary.by_ratecode.length > 0 && (
            <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
              <h3 className="text-headline-md text-secondary mb-4">Fraude por Ratecode</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={summary.by_ratecode.map(r => ({ ...r, ratecode_id: `RC ${r.ratecode_id}` }))} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                  <XAxis dataKey="ratecode_id" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="fraud" fill="#a00003" radius={[4, 4, 0, 0]} name="Fraude" stackId="a" />
                  <Bar dataKey="total" fill="#415f8e" radius={[4, 4, 0, 0]} name="Total" stackId="a" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Explanatory text */}
          <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 text-body-sm text-on-surface-variant leading-relaxed">
            <p>
              El modelo Isolation Forest evaluó <strong>{summary.total_scored.toLocaleString()}</strong> viajes,
              identificando <strong>{summary.fraud_count.toLocaleString()}</strong> como fraudulentos
              (<strong>{summary.fraud_rate}%</strong> del total). La detección opera por RatecodeID,
              etiquetando viajes con puntajes de anomalía elevados. Los ratecodes 1, 2 y 4 corresponden
              a tarifas estándar; el ratecode 99 suele asociarse a viajes promocionales o de prueba.
            </p>
          </div>
        </>
      ) : null}

      {/* Ratecode Selector */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {ratecodes.length === 0 && <p className="text-on-surface-variant italic col-span-full">Sin modelos disponibles</p>}
        {ratecodes.map((rc) => (
          <button
            key={rc.ratecode}
            onClick={() => { setSelected(rc.ratecode); setPage(0); }}
            className={`text-left p-4 rounded-DEFAULT border transition-colors ${
              selected === rc.ratecode
                ? "bg-primary-container/5 border-primary-container"
                : "bg-surface-container-lowest border-border-subtle hover:bg-surface-muted"
            }`}
          >
            <h4 className="text-label-md text-on-surface font-semibold">Ratecode {rc.ratecode}</h4>
            <div className="mt-2 space-y-1 text-caption text-on-surface-variant">
              {Object.entries(rc.metadata).map(([k, v]) => (
                <div key={k} className="flex justify-between">
                  <span>{k}:</span>
                  <span className="text-on-surface tabular-nums">{String(v)}</span>
                </div>
              ))}
            </div>
          </button>
        ))}
      </div>

      {/* Toggle Detail Table */}
      {selected && (
        <div className="flex justify-center">
          <button
            onClick={() => setShowTable(!showTable)}
            className="flex items-center gap-2 px-4 py-2 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted transition-colors"
          >
            <Table2 className="w-4 h-4" />
            {showTable ? "Ocultar tabla de scores" : "Ver tabla de scores"}
            {showTable ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          </button>
        </div>
      )}

      {/* Scores Table (collapsible) */}
      {showTable && selected && scores && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
          <h3 className="text-headline-md text-secondary">
            Scores Anómalos — Ratecode {selected}
          </h3>

          {scoresLoading ? (
            <div className="text-center py-10 text-on-surface-variant">Cargando...</div>
          ) : scores.rows.length === 0 ? (
            <div className="text-center py-10 text-on-surface-variant italic">Sin datos</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-body-sm">
                  <thead>
                    <tr className="border-b border-border-subtle text-label-md text-on-surface-variant uppercase">
                      {Object.keys(scores.rows[0]).map((col) => (
                        <th key={col} className="px-3 py-2 text-left whitespace-nowrap">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {scores.rows.map((row, i) => (
                      <tr key={i} className={`border-b border-border-subtle ${i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}>
                        {Object.values(row).map((val, j) => (
                          <td key={j} className="px-3 py-2 tabular-nums">{String(val ?? "")}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {scores.total > PAGE_SIZE && (
                <div className="flex items-center justify-center gap-4">
                  <button
                    onClick={() => setPage(Math.max(0, page - 1))}
                    disabled={page === 0}
                    className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md border border-border-subtle hover:bg-surface-muted disabled:opacity-30"
                  >
                    <ChevronLeft className="w-4 h-4" /> Anterior
                  </button>
                  <span className="text-body-md text-on-surface-variant">
                    Página {page + 1} de {Math.ceil(scores.total / PAGE_SIZE)}
                  </span>
                  <button
                    onClick={() => setPage(Math.min(Math.ceil(scores.total / PAGE_SIZE) - 1, page + 1))}
                    disabled={page >= Math.ceil(scores.total / PAGE_SIZE) - 1}
                    className="flex items-center gap-1 px-3 py-1.5 rounded-DEFAULT text-label-md border border-border-subtle hover:bg-surface-muted disabled:opacity-30"
                  >
                    Siguiente <ChevronRight className="w-4 h-4" />
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}
