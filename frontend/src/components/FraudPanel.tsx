import { useState, useMemo } from "react";
import { useFraudList } from "../hooks/useSpeedState";
import { SummaryCard } from "./SummaryCard";
import { AlertTriangle, ShieldCheck, Activity, BarChart4 } from "lucide-react";

export function FraudPanel() {
  const [showFraudOnly, setShowFraudOnly] = useState(true);
  const { data: rows = [], isLoading } = useFraudList({
    is_fraud: showFraudOnly ? true : undefined,
    limit: 500,
  });

  const fraudCount = useMemo(() => rows.filter((r) => r.is_fraud).length, [rows]);
  const anomalyCandidates = useMemo(() => rows.filter((r) => r.is_anomaly_candidate).length, [rows]);
  const avgScore = useMemo(
    () => {
      const scored = rows.filter((r) => r.anomaly_score != null);
      return scored.length ? scored.reduce((s, r) => s + (r.anomaly_score ?? 0), 0) / scored.length : 0;
    },
    [rows],
  );

  const columns = ["trip_id", "service_id", "ratecode_id", "anomaly_score", "is_fraud", "is_anomaly_candidate", "timestamp"];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <label className="flex items-center gap-2 text-label-md text-on-surface-variant cursor-pointer">
          <input
            type="checkbox"
            checked={showFraudOnly}
            onChange={(e) => setShowFraudOnly(e.target.checked)}
            className="rounded border-border-subtle"
          />
          Solo fraudes
        </label>
        <span className="text-caption text-on-surface-variant">
          Actualiza cada 5s
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        <SummaryCard label="Total en ventana" value={rows.length} icon={<BarChart4 className="w-5 h-5" />} accent="primary" />
        <SummaryCard label="Fraudes" value={fraudCount} icon={<AlertTriangle className="w-5 h-5" />} accent="error" />
        <SummaryCard label="Candidatos" value={anomalyCandidates} icon={<Activity className="w-5 h-5" />} accent="warning" />
        <SummaryCard label="Score Promedio" value={avgScore.toFixed(3)} icon={<ShieldCheck className="w-5 h-5" />} accent="secondary" />
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6">
        <h4 className="text-label-md text-on-surface-variant uppercase tracking-wide mb-4">
          Registros de Fraude ({rows.length})
        </h4>
        {isLoading ? (
          <div className="flex items-center justify-center py-16 text-on-surface-variant italic">Cargando...</div>
        ) : rows.length === 0 ? (
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
                {rows.map((row, i) => (
                  <tr
                    key={row.trip_id}
                    className={`border-b border-border-subtle ${row.is_fraud ? "bg-error-container/10" : ""} ${i % 2 === 0 ? "" : "bg-surface-muted"}`}
                  >
                    {columns.map((col) => {
                      const r = row as unknown as Record<string, unknown>;
                      return (
                        <td key={col} className="px-3 py-1.5 whitespace-nowrap tabular-nums">
                          {col === "anomaly_score" && row.anomaly_score == null
                            ? "N/A"
                            : String(r[col] ?? "")}
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
