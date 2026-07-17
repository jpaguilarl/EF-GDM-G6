import { useQuery } from "@tanstack/react-query";
import { Database, Filter, BarChart3, ChevronRight } from "lucide-react";
import { apiGet } from "../lib/api";
import type { AuditSummary } from "../lib/types";

const LAYER_META = {
  bronze: { icon: Database, label: "Bronce", color: "border-l-amber-700" },
  silver: { icon: Filter, label: "Plata", color: "border-l-slate-400" },
  gold: { icon: BarChart3, label: "Oro", color: "border-l-yellow-500" },
} as const;

type Layer = keyof typeof LAYER_META;

function formatNum(n: number): string {
  return n.toLocaleString();
}

function FlowNode({
  layer,
  summary,
  isLoading,
  yieldPct,
}: {
  layer: Layer;
  summary?: AuditSummary | null;
  isLoading: boolean;
  yieldPct?: number | null;
}) {
  const meta = LAYER_META[layer];

  const retainedPct =
    layer === "silver" && summary?.total_bronze_rows
      ? ((summary.total_quality_rows ?? 0) / summary.total_bronze_rows) * 100
      : null;
  const rejectedPct =
    layer === "silver" && summary?.total_bronze_rows
      ? ((summary.total_quarantined_rows ?? 0) / summary.total_bronze_rows) * 100
      : null;

  const goldOutput = summary?.total_output_rows ?? 0;

  return (
    <div className={`flex-1 bg-surface-container-lowest border border-border-subtle border-l-4 rounded-DEFAULT p-4 ${meta.color}`}>
      <div className="flex items-center gap-2 mb-3">
        <meta.icon className="w-5 h-5 text-on-surface-variant" />
        <span className="text-label-md text-on-surface-variant uppercase tracking-wide">
          {meta.label}
        </span>
      </div>
      {isLoading ? (
        <div className="text-body-sm text-on-surface-variant animate-pulse">Cargando...</div>
      ) : summary ? (
        <div className="space-y-2">
          <div className="text-headline-lg font-bold tabular-nums text-on-surface">
            {formatNum(
              layer === "bronze"
                ? summary.total_rows ?? 0
                : layer === "silver"
                  ? summary.total_quality_rows ?? 0
                  : goldOutput,
            )}
          </div>
          <div className="text-caption text-on-surface-variant">
            {layer === "bronze" && `${summary.total_files ?? 0} archivos`}
            {layer === "silver" && `${formatNum(summary.total_quarantined_rows ?? 0)} rechazadas`}
            {layer === "gold" && `${summary.total_builds ?? 0} builds`}
          </div>

          {/* Mini retained/rejected bars (silver only) */}
          {layer === "silver" && retainedPct != null && rejectedPct != null && (
            <div className="space-y-1 pt-1">
              <div className="flex items-center gap-2 text-caption">
                <span className="w-3 h-3 rounded-sm bg-emerald-500 shrink-0" />
                <span className="text-on-surface-variant">Retenido {retainedPct.toFixed(1)}%</span>
              </div>
              <div className="w-full h-2 bg-surface-muted rounded-full overflow-hidden flex">
                <div
                  className="h-full bg-emerald-500 transition-all"
                  style={{ width: `${Math.min(retainedPct, 100)}%` }}
                />
                <div
                  className="h-full bg-red-500 transition-all"
                  style={{ width: `${Math.min(rejectedPct, 100)}%` }}
                />
              </div>
              <div className="flex items-center gap-2 text-caption">
                <span className="w-3 h-3 rounded-sm bg-red-500 shrink-0" />
                <span className="text-on-surface-variant">Rechazado {rejectedPct.toFixed(1)}%</span>
              </div>
            </div>
          )}

          {/* Yield indicator (gold only) */}
          {layer === "gold" && (
            <div className="text-caption text-on-surface-variant pt-1 space-y-0.5">
              <div>{summary.by_mart?.length ?? 0} marts distintos</div>
              {yieldPct != null && (
                <div>
                  Rendimiento: <span className="font-semibold">{yieldPct.toFixed(1)}%</span> vs silver
                </div>
              )}
            </div>
          )}
        </div>
      ) : (
        <div className="text-body-sm text-on-surface-variant italic">Sin datos</div>
      )}
    </div>
  );
}

export function AuditFlowNodes() {
  const { data: bronze, isLoading: bronzeLoading } = useQuery<AuditSummary>({
    queryKey: ["audit-summary", "bronze"],
    queryFn: () => apiGet<AuditSummary>("/audit/bronze/summary"),
  });
  const { data: silver, isLoading: silverLoading } = useQuery<AuditSummary>({
    queryKey: ["audit-summary", "silver"],
    queryFn: () => apiGet<AuditSummary>("/audit/silver/summary"),
  });
  const { data: gold, isLoading: goldLoading } = useQuery<AuditSummary>({
    queryKey: ["audit-summary", "gold"],
    queryFn: () => apiGet<AuditSummary>("/audit/gold/summary"),
  });

  const goldYield =
    silver?.total_quality_rows && gold?.total_output_rows
      ? (gold.total_output_rows / silver.total_quality_rows) * 100
      : null;

  const Arrow = () => (
    <div className="flex items-center justify-center shrink-0 px-1">
      <ChevronRight className="w-8 h-8 text-on-surface-variant/40" />
    </div>
  );

  return (
    <div className="flex items-stretch gap-0">
      <FlowNode layer="bronze" summary={bronze} isLoading={bronzeLoading} />
      <Arrow />
      <FlowNode layer="silver" summary={silver} isLoading={silverLoading} />
      <Arrow />
      <FlowNode layer="gold" summary={gold} isLoading={goldLoading} yieldPct={goldYield} />
    </div>
  );
}
