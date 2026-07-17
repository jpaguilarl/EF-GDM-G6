import { useState, useMemo } from "react";
import { BarChart3, Table, AlertTriangle, Layers } from "lucide-react";
import { IngestForm } from "../components/IngestForm";
import { RealtimeDashboard } from "../components/RealtimeDashboard";
import { FraudPanel } from "../components/FraudPanel";
import { ClusterPanel } from "../components/ClusterPanel";
import { REALTIME_VIEWS } from "../lib/realtimeViews";

const MART_TABS = REALTIME_VIEWS.map((v) => ({ key: v.key, label: v.label, config: v }));

const EXTRA_TABS = [
  { key: "fraud", label: "Fraude", icon: AlertTriangle },
  { key: "clusters", label: "Clústers", icon: Layers },
] as const;

type Tab = (typeof MART_TABS)[number]["key"] | "fraud" | "clusters";

export function Realtime() {
  const [tab, setTab] = useState<Tab>("demand-volume");
  const [serviceFilter, setServiceFilter] = useState<string>("");

  const currentConfig = useMemo(
    () => MART_TABS.find((t) => t.key === tab)?.config ?? null,
    [tab],
  );

  const filters = useMemo(() => {
    const f: Record<string, string> = {};
    if (serviceFilter) f.service_id = serviceFilter;
    return f;
  }, [serviceFilter]);

  return (
    <div className="space-y-6 max-w-6xl">
      <h1 className="text-headline-lg text-primary-container font-bold flex items-center gap-3">
        <BarChart3 className="w-7 h-7" />
        Tiempo Real (Speed Layer)
      </h1>

      {/* Tab bar */}
      <div className="flex gap-1 bg-surface-muted rounded-DEFAULT p-1 w-fit flex-wrap">
        {MART_TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key as Tab)}
            className={`px-3 py-2 rounded-DEFAULT text-label-md transition-colors ${
              tab === t.key
                ? "bg-surface-container-lowest text-on-surface shadow-sm"
                : "text-on-surface-variant hover:text-on-surface"
            }`}
          >
            <Table className="w-4 h-4 inline mr-1.5" />
            {t.label}
          </button>
        ))}
        {EXTRA_TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key as Tab)}
            className={`flex items-center gap-1.5 px-3 py-2 rounded-DEFAULT text-label-md transition-colors ${
              tab === t.key
                ? "bg-surface-container-lowest text-on-surface shadow-sm"
                : "text-on-surface-variant hover:text-on-surface"
            }`}
          >
            <t.icon className="w-4 h-4" />
            {t.label}
          </button>
        ))}
      </div>

      {/* Service filter (for mart views) */}
      {!["fraud", "clusters"].includes(tab) && (
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
          {["yellow", "green", "fhv", "fhvhv"].map((s) => (
            <button
              key={s}
              onClick={() => setServiceFilter(s)}
              className={`px-3 py-1.5 rounded-DEFAULT text-caption border transition-colors ${
                serviceFilter === s
                  ? "bg-secondary-container text-on-secondary-container border-secondary-container"
                  : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
              }`}
            >
              {s.toUpperCase()}
            </button>
          ))}
        </div>
      )}

      {/* Content: two-column layout for mart views, full-width for fraud/clusters */}
      {tab === "fraud" ? (
        <FraudPanel />
      ) : tab === "clusters" ? (
        <ClusterPanel />
      ) : currentConfig ? (
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          <div className="lg:col-span-1">
            <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 space-y-4">
              <h2 className="text-label-md text-on-surface-variant uppercase tracking-wide">
                Enviar Viaje
              </h2>
              <IngestForm />
            </div>
          </div>
          <div className="lg:col-span-3">
            <RealtimeDashboard viewConfig={currentConfig} filters={filters} />
          </div>
        </div>
      ) : null}
    </div>
  );
}
