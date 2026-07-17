import { useState } from "react";
import { BarChart3, Radar, Activity, Table, TrendingUp } from "lucide-react";
import { MartExplorer } from "../components/MartExplorer";
import { KmodesViewer } from "../components/KmodesViewer";
import { IsolationViewer } from "../components/IsolationViewer";
import { SarimaxViewer } from "../components/SarimaxViewer";

const MARTS = [
  { id: "demand-volume", label: "Demanda/Volumen" },
  { id: "financial-performance", label: "Rendimiento Financiero" },
  { id: "operational-profile", label: "Perfil Operacional" },
  { id: "supply-demand-balance", label: "Balance Oferta/Demanda" },
  { id: "abc-xyz-zones", label: "ABC/XYZ Zonas" },
  { id: "tipping-behavior", label: "Propinas" },
] as const;

const ML_TABS = [
  { id: "kmodes", label: "K-Modes", icon: Radar },
  { id: "isolation", label: "Isolation Forest", icon: Activity },
  { id: "sarimax", label: "SARIMAX", icon: TrendingUp },
] as const;

const YEARS = [2023, 2024, 2025] as const;
const MONTHS = [
  { n: 1, label: "Ene" }, { n: 2, label: "Feb" }, { n: 3, label: "Mar" },
  { n: 4, label: "Abr" }, { n: 5, label: "May" }, { n: 6, label: "Jun" },
  { n: 7, label: "Jul" }, { n: 8, label: "Ago" }, { n: 9, label: "Sep" },
  { n: 10, label: "Oct" }, { n: 11, label: "Nov" }, { n: 12, label: "Dic" },
] as const;
const SERVICES = ["yellow", "green", "fhvhv"] as const;

type Tab = "marts" | "ml";

export function GoldResults() {
  const [tab, setTab] = useState<Tab>("marts");
  const [mart, setMart] = useState<string>(MARTS[0].id);
  const [mlTab, setMlTab] = useState("kmodes");
  const [serviceId, setServiceId] = useState<string>(SERVICES[0]);
  const [selectedYears, setSelectedYears] = useState<number[]>([]);
  const [selectedMonths, setSelectedMonths] = useState<number[]>([]);

  function toggleYear(y: number) {
    setSelectedYears(prev => prev.includes(y) ? prev.filter(x => x !== y) : [...prev, y]);
  }
  function toggleMonth(m: number) {
    setSelectedMonths(prev => prev.includes(m) ? prev.filter(x => x !== m) : [...prev, m]);
  }
  function clearFilters() {
    setSelectedYears([]);
    setSelectedMonths([]);
  }

  return (
    <div className="space-y-6 max-w-6xl">
      <h1 className="text-headline-lg text-primary-container font-bold">
        Resultados Gold
      </h1>

      <div className="flex gap-1 bg-surface-muted rounded-DEFAULT p-1 w-fit">
        <button
          onClick={() => setTab("marts")}
          className={`px-4 py-2 rounded-DEFAULT text-label-md transition-colors ${
            tab === "marts" ? "bg-surface-container-lowest text-on-surface shadow-sm" : "text-on-surface-variant hover:text-on-surface"
          }`}
        >
          <Table className="w-4 h-4 inline mr-2" />Marts Power BI
        </button>
        <button
          onClick={() => setTab("ml")}
          className={`px-4 py-2 rounded-DEFAULT text-label-md transition-colors ${
            tab === "ml" ? "bg-surface-container-lowest text-on-surface shadow-sm" : "text-on-surface-variant hover:text-on-surface"
          }`}
        >
          <BarChart3 className="w-4 h-4 inline mr-2" />ML Modelos
        </button>
      </div>

      {tab === "marts" && (
        <>
          <div className="flex gap-2 flex-wrap">
            {MARTS.map((m) => (
              <button
                key={m.id}
                onClick={() => setMart(m.id)}
                className={`px-3 py-2 rounded-DEFAULT text-label-md border transition-colors ${
                  mart === m.id
                    ? "bg-primary-container text-on-primary-container border-primary-container"
                    : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                }`}
              >
                {m.label}
              </button>
            ))}
          </div>

          {/* Year / Month filter */}
          <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-4 space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-label-md text-on-surface-variant">Filtrar por período</span>
              {(selectedYears.length > 0 || selectedMonths.length > 0) && (
                <button
                  onClick={clearFilters}
                  className="text-label-sm text-primary hover:underline"
                >
                  Limpiar filtros
                </button>
              )}
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-caption text-on-surface-variant w-8">Año</span>
              {YEARS.map((y) => (
                <button
                  key={y}
                  onClick={() => toggleYear(y)}
                  className={`px-3 py-1 rounded-DEFAULT text-label-sm border transition-colors ${
                    selectedYears.includes(y)
                      ? "bg-primary text-on-primary border-primary"
                      : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                  }`}
                >
                  {y}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-caption text-on-surface-variant w-8">Mes</span>
              {MONTHS.map((m) => (
                <button
                  key={m.n}
                  onClick={() => toggleMonth(m.n)}
                  className={`px-2 py-1 rounded-DEFAULT text-label-sm border transition-colors ${
                    selectedMonths.includes(m.n)
                      ? "bg-secondary text-on-secondary border-secondary"
                      : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                  }`}
                >
                  {m.label}
                </button>
              ))}
            </div>
          </div>

          <MartExplorer
            mart={mart}
            label={MARTS.find(m => m.id === mart)?.label ?? mart}
            years={selectedYears.length > 0 ? selectedYears : undefined}
            months={selectedMonths.length > 0 ? selectedMonths : undefined}
          />
        </>
      )}

      {tab === "ml" && (
        <>
          <div className="flex gap-2 flex-wrap">
            {ML_TABS.map((t) => (
              <button
                key={t.id}
                onClick={() => setMlTab(t.id)}
                className={`flex items-center gap-2 px-3 py-2 rounded-DEFAULT text-label-md border transition-colors ${
                  mlTab === t.id
                    ? "bg-primary-container text-on-primary-container border-primary-container"
                    : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                }`}
              >
                <t.icon className="w-4 h-4" />
                {t.label}
              </button>
            ))}
          </div>

          {mlTab === "kmodes" && (
            <div className="space-y-4">
              <div className="flex gap-2">
                {SERVICES.map((s) => (
                  <button
                    key={s}
                    onClick={() => setServiceId(s)}
                    className={`px-3 py-2 rounded-DEFAULT text-label-md border transition-colors ${
                      serviceId === s
                        ? "bg-secondary-container text-on-secondary-container border-secondary-container"
                        : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
                    }`}
                  >
                    {s.toUpperCase()}
                  </button>
                ))}
              </div>
              <KmodesViewer serviceId={serviceId} />
            </div>
          )}

          {mlTab === "isolation" && <IsolationViewer />}
          {mlTab === "sarimax" && <SarimaxViewer />}
        </>
      )}
    </div>
  );
}
