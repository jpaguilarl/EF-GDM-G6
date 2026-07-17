import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  ChevronLeft, ChevronRight, Table2, ChevronDown, ChevronUp,
  ShieldAlert, AlertTriangle, Activity, TrendingDown, Gauge, ArrowUpDown, ArrowUp, ArrowDown,
} from "lucide-react";
import {
  ScatterChart, Scatter, XAxis, YAxis, ZAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import { apiGet } from "../lib/api";
import type {
  IsolationSummary, IsolationScatterData, IsolationScoresData, IsolationScoreRow,
} from "../lib/types";
import { SummaryCard } from "./SummaryCard";
import { ChartCard } from "./ChartCard";

interface IsolationEntry {
  ratecode: string;
  metadata: Record<string, unknown>;
}

const FEATURE_DESC = [
  { name: "velocidad_promedio_calculada", desc: "Razón distancia/duración (mph)" },
  { name: "costo_por_distancia", desc: "Tarifa por milla ($/mi)" },
  { name: "duracion_viaje_segundos", desc: "Tiempo del viaje (s)" },
  { name: "trip_distance", desc: "Distancia recorrida (mi)" },
  { name: "fare_amount", desc: "Tarifa cobrada ($)" },
  { name: "ratio_peaje_tarifa", desc: "Peaje / tarifa" },
];

type SortKey = "anomaly_score" | "trip_distance" | "fare_amount" | "velocidad_promedio_calculada" | "costo_por_distancia" | "ratecode_id";
type SortDir = "asc" | "desc";

const COLS: { key: SortKey; label: string; format?: (v: number | null | undefined) => string }[] = [
  { key: "ratecode_id", label: "RatecodeID" },
  { key: "trip_distance", label: "Distancia (mi)", format: (v) => v != null ? v.toFixed(2) : "-" },
  { key: "fare_amount", label: "Tarifa ($)", format: (v) => v != null ? `$${v.toFixed(2)}` : "-" },
  { key: "velocidad_promedio_calculada", label: "Velocidad (mph)", format: (v) => v != null ? v.toFixed(1) : "-" },
  { key: "costo_por_distancia", label: "Costo/Milla ($)", format: (v) => v != null ? `$${v.toFixed(2)}` : "-" },
  { key: "anomaly_score", label: "Anomaly Score", format: (v) => v != null ? v.toFixed(4) : "-" },
];

function fmtMoney(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(1)}K`;
  return `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function scatterTooltip(props: any) {
  const p = props.payload?.[0]?.payload;
  if (!p) return null;
  return (
    <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-2 text-caption shadow-lg">
      <div className="font-semibold text-on-surface">Tarifa: ${p.fare_amount?.toFixed(2) ?? "-"}</div>
      <div className="text-on-surface-variant">Distancia: {p.trip_distance?.toFixed(2) ?? "-"} mi</div>
      <div className="text-on-surface-variant">Velocidad: {p.velocidad_promedio_calculada?.toFixed(1) ?? "-"} mph</div>
      <div className="text-on-surface-variant">Score: {p.anomaly_score?.toFixed(4) ?? "-"}</div>
      <div className="text-on-surface-variant">RC: {p.ratecode_id ?? "-"}</div>
    </div>
  );
}

export function IsolationViewer() {
  const [selected, setSelected] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [showTable, setShowTable] = useState(false);
  const [sortKey, setSortKey] = useState<SortKey>("anomaly_score");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const PAGE_SIZE = 50;

  const { data: summary, isLoading: summaryLoading } = useQuery<IsolationSummary>({
    queryKey: ["isolation-summary"],
    queryFn: () => apiGet<IsolationSummary>("/ml/isolation/summary"),
  });

  const { data: ratecodes = [] } = useQuery<IsolationEntry[]>({
    queryKey: ["isolation"],
    queryFn: () => apiGet<IsolationEntry[]>("/ml/isolation"),
  });

  const { data: scatter, isLoading: scatterLoading } = useQuery<IsolationScatterData>({
    queryKey: ["isolation-scatter", selected],
    queryFn: () => apiGet<IsolationScatterData>(`/ml/isolation/scatter${selected ? `?ratecode=${selected}` : ""}`),
  });

  const { data: scores, isLoading: scoresLoading } = useQuery<IsolationScoresData>({
    queryKey: ["isolation-scores", selected, page],
    queryFn: () => apiGet<IsolationScoresData>(`/ml/isolation/${selected}/scores?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`),
    enabled: !!selected && showTable,
  });

  const legalPerMile = scatter?.legal_fare_per_mile ?? 4.12;

  const { maxX, refLine } = useMemo(() => {
    const all = scatter ? [...scatter.normal, ...scatter.fraud] : [];
    const mx = all.reduce((m, p) => Math.max(m, p.trip_distance ?? 0), 10);
    return {
      maxX: mx,
      refLine: [
        { x: 0, y: 0 },
        { x: mx, y: mx * legalPerMile },
      ] as [{ x: number; y: number }, { x: number; y: number }],
    };
  }, [scatter, legalPerMile]);

  const sortedRows = useMemo(() => {
    if (!scores?.rows) return [];
    const rows = [...scores.rows];
    rows.sort((a, b) => {
      const av = a[sortKey] ?? null;
      const bv = b[sortKey] ?? null;
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      return sortDir === "asc" ? av - bv : bv - av;
    });
    return rows;
  }, [scores, sortKey, sortDir]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  const scoreMean = summary?.score_stats?.score_mean;
  const leakage = summary?.estimated_leakage ?? 0;

  return (
    <div className="space-y-6">
      {/* KPIs */}
      {summaryLoading ? (
        <div className="text-center py-4 text-on-surface-variant">Cargando resumen...</div>
      ) : summary ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <SummaryCard
            label="Fuga Financiera Estimada"
            value={fmtMoney(leakage)}
            icon={<TrendingDown className="w-5 h-5" />}
            accent="error"
            sublabel={`Σ tarifa de ${summary.fraud_count.toLocaleString()} viajes fraudulentos`}
          />
          <SummaryCard
            label="Viajes Evaluados"
            value={summary.total_scored}
            icon={<Activity className="w-5 h-5" />}
            accent="primary"
          />
          <SummaryCard
            label="Fraude Detectado"
            value={summary.fraud_count}
            icon={<ShieldAlert className="w-5 h-5" />}
            accent="error"
          />
          <SummaryCard
            label="Score Anómalo Medio"
            value={scoreMean != null ? scoreMean.toFixed(4) : "-"}
            icon={<Gauge className="w-5 h-5" />}
            accent="secondary"
            sublabel={`Tasa de fraude: ${summary.fraud_rate}%`}
          />
        </div>
      ) : null}

      {/* Model description */}
      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-5 space-y-3">
        <div className="flex items-center gap-2">
          <AlertTriangle className="w-5 h-5 text-secondary" />
          <h3 className="text-headline-sm text-secondary">Modelo Isolation Forest — Detección de Fraude Tarifario</h3>
        </div>
        <p className="text-body-sm text-on-surface-variant leading-relaxed">
          El modelo <strong>Isolation Forest</strong> aísla observaciones anómalas mediante particiones aleatorias:
          los viajes con patrones atípicos requieren menos cortes para ser aislados, obteniendo un
          <strong> anomaly_score</strong> más alto. Se entrena <strong>un modelo independiente por RatecodeID</strong> ya
          que la estructura tarifaria (estándar, JFK, Newark, negociado) difiere por código.
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 pt-1">
          {FEATURE_DESC.map((f) => (
            <div key={f.name} className="border border-border-subtle rounded-DEFAULT p-3 bg-surface-muted/30">
              <div className="text-label-md font-mono text-on-surface">{f.name}</div>
              <div className="text-caption text-on-surface-variant">{f.desc}</div>
            </div>
          ))}
        </div>
        <p className="text-body-sm text-on-surface-variant leading-relaxed pt-1">
          Un viaje se etiqueta como <code className="px-1 bg-surface-muted rounded">is_fraud</code> cuando su score
          supera el <strong>percentil 95</strong> dentro de su ratecode. Los ratecodes 1 y 2 son tarifas estándar
          (street-hail), el 4 es Newark y el 99 suele ser promocional o de prueba. La <strong>fuga financiera</strong>
          estima el monto cobrado en viajes potencialmente fraudulentos.
        </p>
      </div>

      {/* Scatter: Tarifa vs Distancia */}
      {scatterLoading ? (
        <div className="text-center py-8 text-on-surface-variant">Cargando scatter...</div>
      ) : scatter ? (
        <ChartCard
          title="Tarifa vs Distancia — Normales vs Anomalías"
          subtitle={`Línea diagonal: tarifa legal esperada (~$${legalPerMile.toFixed(2)}/mi). ${scatter.fraud.length} anomalías (rojo) sobre ${scatter.normal.length} viajes normales (gris).`}
        >
          <ResponsiveContainer width="100%" height={420}>
            <ScatterChart margin={{ top: 10, right: 20, left: 10, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis
                type="number"
                dataKey="trip_distance"
                name="Distancia"
                unit=" mi"
                domain={[0, maxX]}
                tick={{ fontSize: 11 }}
                label={{ value: "Distancia (millas)", position: "insideBottom", offset: -10, fontSize: 12, fill: "#6b7280" }}
              />
              <YAxis
                type="number"
                dataKey="fare_amount"
                name="Tarifa"
                unit=" $"
                tick={{ fontSize: 11 }}
                label={{ value: "Tarifa ($)", angle: -90, position: "insideLeft", fontSize: 12, fill: "#6b7280" }}
              />
              <ZAxis range={[40, 40]} />
              <Tooltip content={scatterTooltip} />
              <Legend />
              <ReferenceLine
                segment={refLine}
                stroke="#16a34a"
                strokeWidth={2}
                strokeDasharray="6 4"
                ifOverflow="extendDomain"
                label={{ value: "Tarifa legal", position: "insideTopLeft", fontSize: 11, fill: "#16a34a" }}
              />
              <Scatter name="Viajes Normales" data={scatter.normal} fill="#9CA3AF" fillOpacity={0.25} />
              <Scatter name="Anomalías / Fraude" data={scatter.fraud} fill="#DC2626" fillOpacity={0.85} />
            </ScatterChart>
          </ResponsiveContainer>
        </ChartCard>
      ) : null}

      {/* Ratecode selector */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {ratecodes.length === 0 && <p className="text-on-surface-variant italic col-span-full">Sin modelos disponibles</p>}
        {ratecodes.map((rc) => (
          <button
            key={rc.ratecode}
            onClick={() => { setSelected(rc.ratecode); setPage(0); }}
            className={`text-left p-4 rounded-DEFAULT border transition-colors ${
              selected === rc.ratecode
                ? "bg-error/10 border-error"
                : "bg-surface-container-lowest border-border-subtle hover:bg-surface-muted"
            }`}
          >
            <h4 className="text-label-md text-on-surface font-semibold">Ratecode {rc.ratecode}</h4>
            <div className="mt-2 space-y-1 text-caption text-on-surface-variant">
              {Object.entries(rc.metadata).filter(([k]) => k !== "features" && k !== "trained_at").slice(0, 4).map(([k, v]) => (
                <div key={k} className="flex justify-between">
                  <span>{k}:</span>
                  <span className="text-on-surface tabular-nums">{String(v)}</span>
                </div>
              ))}
            </div>
          </button>
        ))}
      </div>

      {/* Toggle detail table */}
      {selected && (
        <div className="flex justify-center">
          <button
            onClick={() => setShowTable(!showTable)}
            className="flex items-center gap-2 px-4 py-2 rounded-DEFAULT text-label-md text-on-surface border border-border-subtle hover:bg-surface-muted transition-colors"
          >
            <Table2 className="w-4 h-4" />
            {showTable ? "Ocultar auditoría detallada" : `Ver auditoría — Ratecode ${selected}`}
            {showTable ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          </button>
        </div>
      )}

      {/* Sortable audit table */}
      {showTable && selected && scores && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
          <div className="flex items-center justify-between flex-wrap gap-2">
            <h3 className="text-headline-md text-secondary">
              Auditoría de Anomalías — Ratecode {selected}
            </h3>
            <span className="text-caption text-on-surface-variant">
              {scores.total.toLocaleString()} viajes · orden: {sortKey.replace(/_/g, " ")} ({sortDir})
            </span>
          </div>

          {scoresLoading ? (
            <div className="text-center py-10 text-on-surface-variant">Cargando...</div>
          ) : sortedRows.length === 0 ? (
            <div className="text-center py-10 text-on-surface-variant italic">Sin datos</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-body-sm">
                  <thead>
                    <tr className="border-b-2 border-border-subtle text-label-md text-on-surface-variant uppercase">
                      {COLS.map((col) => (
                        <th
                          key={col.key}
                          onClick={() => toggleSort(col.key)}
                          className="px-3 py-2 text-left whitespace-nowrap cursor-pointer hover:text-on-surface select-none"
                        >
                          <span className="inline-flex items-center gap-1">
                            {col.label}
                            {sortKey === col.key ? (
                              sortDir === "asc" ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />
                            ) : (
                              <ArrowUpDown className="w-3 h-3 opacity-40" />
                            )}
                          </span>
                        </th>
                      ))}
                      <th className="px-3 py-2 text-left whitespace-nowrap">Fraude</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedRows.map((row: IsolationScoreRow, i) => (
                      <tr
                        key={row.trip_id ?? i}
                        className={`border-b border-border-subtle ${row.is_fraud ? "bg-error/10" : i % 2 === 0 ? "bg-surface-container-lowest" : "bg-surface-muted"}`}
                      >
                        {COLS.map((col) => (
                          <td key={col.key} className="px-3 py-2 tabular-nums">
                            {col.format
                              ? col.format(row[col.key])
                              : String(row[col.key as keyof IsolationScoreRow] ?? "-")}
                          </td>
                        ))}
                        <td className="px-3 py-2">
                          {row.is_fraud ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-error/20 text-error text-caption font-semibold">
                              <AlertTriangle className="w-3 h-3" /> Sí
                            </span>
                          ) : (
                            <span className="text-on-surface-variant text-caption">No</span>
                          )}
                        </td>
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
