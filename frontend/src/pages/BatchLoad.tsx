import { useState } from "react";
import { Play, StopCircle, Loader2, Download, Box, Beaker } from "lucide-react";
import { useJobs, useSubmitBronzeJob, useSubmitSilverJob, useSubmitGoldJob, useSubmitGoldMlJob, useStopJob } from "../hooks/useJobs";
import { JobLogViewer } from "../components/JobLogViewer";
import type { JobSummary } from "../lib/types";

const CATEGORIES = ["yellow", "green", "fhv", "fhvhv"] as const;
const SILVER_STAGES = ["quality", "schema", "load"] as const;
const GOLD_ML_MODELS = ["kmodes", "isolation", "sarimax"] as const;

const MONTH_LABELS = [
  "Ene", "Feb", "Mar", "Abr", "May", "Jun",
  "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
];

function StatusChip({ status }: { status: string }) {
  const colors: Record<string, string> = {
    running: "bg-blue-100 text-blue-800 border-blue-200",
    completed: "bg-green-100 text-green-800 border-green-200",
    failed: "bg-red-100 text-red-800 border-red-200",
    stopped: "bg-gray-100 text-gray-800 border-gray-200",
    pending: "bg-yellow-100 text-yellow-800 border-yellow-200",
  };
  return (
    <span className={`px-3 py-1 rounded-DEFAULT text-caption font-semibold border ${colors[status] || colors.pending}`}>
      {status}
    </span>
  );
}

function CategoryCheckboxes({
  value,
  onChange,
}: {
  value: string[];
  onChange: (v: string[]) => void;
}) {
  return (
    <div className="flex gap-3 flex-wrap">
      {CATEGORIES.map((c) => (
        <label key={c} className="flex items-center gap-1.5 text-body-md text-on-surface cursor-pointer">
          <input
            type="checkbox"
            checked={value.includes(c)}
            onChange={(e) => {
              if (e.target.checked) {
                onChange([...value, c]);
              } else {
                onChange(value.filter((x) => x !== c));
              }
            }}
            className="accent-primary-container"
          />
          {c}
        </label>
      ))}
    </div>
  );
}

function MonthRangeSlider({
  start,
  end,
  onChange,
}: {
  start: number;
  end: number;
  onChange: (start: number, end: number) => void;
}) {
  return (
    <div className="flex items-center gap-2">
      <label className="flex items-center gap-1 text-caption text-on-surface-variant">
        Desde
        <select
          value={start}
          onChange={(e) => onChange(Math.min(Number(e.target.value), end), end)}
          className="bg-surface-muted border border-border-subtle rounded p-1 text-body-md text-on-surface"
        >
          {Array.from({ length: end }, (_, i) => i + 1).map((m) => (
            <option key={m} value={m}>{MONTH_LABELS[m - 1]}</option>
          ))}
        </select>
      </label>
      <label className="flex items-center gap-1 text-caption text-on-surface-variant">
        Hasta
        <select
          value={end}
          onChange={(e) => onChange(start, Math.max(Number(e.target.value), start))}
          className="bg-surface-muted border border-border-subtle rounded p-1 text-body-md text-on-surface"
        >
          {Array.from({ length: 13 - start }, (_, i) => i + start).map((m) => (
            <option key={m} value={m}>{MONTH_LABELS[m - 1]}</option>
          ))}
        </select>
      </label>
    </div>
  );
}

export function BatchLoad() {
  const { data: jobs = [], isLoading } = useJobs();
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const bronzeJob = useSubmitBronzeJob();
  const silverJob = useSubmitSilverJob();
  const goldJob = useSubmitGoldJob();
  const goldMlJob = useSubmitGoldMlJob();
  const stopJob = useStopJob();

  const [bronzeForm, setBronzeForm] = useState({ categories: ["yellow"], year: 2025, monthStart: 1, monthEnd: 1 });
  const [silverForm, setSilverForm] = useState({ stage: "quality", categories: ["yellow"], year: 2025, monthStart: 1, monthEnd: 1 });
  const [goldForm, setGoldForm] = useState({ mode: "incremental", categories: ["yellow"], year: 2025, monthStart: 1, monthEnd: 1, only: "" });

  const showMessage = (msg: string) => {
    setMessage(msg);
    setTimeout(() => setMessage(null), 5000);
  };

  return (
    <div className="space-y-8 max-w-5xl">
      <h1 className="text-headline-lg text-primary-container font-bold">
        Carga por Lotes
      </h1>

      {message && (
        <div className="bg-primary-fixed border border-primary-fixed-dim rounded-DEFAULT p-4 text-body-md text-on-primary-fixed">
          {message}
        </div>
      )}

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
        <h2 className="text-headline-md text-secondary flex items-center gap-2">
          <Download className="w-5 h-5" /> Bronce (Descarga)
        </h2>
        <div className="flex gap-4 items-end flex-wrap">
          <div>
            <label className="text-label-md text-on-surface block mb-1">Categorías</label>
            <CategoryCheckboxes
              value={bronzeForm.categories}
              onChange={(v) => setBronzeForm({ ...bronzeForm, categories: v })}
            />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Año</label>
            <input
              type="number"
              value={bronzeForm.year}
              onChange={(e) => setBronzeForm({ ...bronzeForm, year: Number(e.target.value) })}
              className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface w-20"
            />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Rango de Meses</label>
            <MonthRangeSlider
              start={bronzeForm.monthStart}
              end={bronzeForm.monthEnd}
              onChange={(s, e) => setBronzeForm({ ...bronzeForm, monthStart: s, monthEnd: e })}
            />
          </div>
          <button
            onClick={() => {
              bronzeJob.mutate({
                categories: bronzeForm.categories,
                year: bronzeForm.year,
                month_start: bronzeForm.monthStart,
                month_end: bronzeForm.monthEnd,
              }, { onSuccess: (d: any) => showMessage(`Bronce lanzado: job_id=${d.job_id}`) });
            }}
            disabled={bronzeJob.isPending || bronzeForm.categories.length === 0}
            className="flex items-center gap-2 px-4 py-2 bg-primary-container text-on-primary-container rounded-DEFAULT text-label-md hover:bg-primary-container/90 disabled:opacity-50"
          >
            {bronzeJob.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            Descargar
          </button>
        </div>
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
        <h2 className="text-headline-md text-secondary flex items-center gap-2">
          <Box className="w-5 h-5" /> Silver (Calidad)
        </h2>
        <div className="flex gap-4 items-end flex-wrap">
          <div>
            <label className="text-label-md text-on-surface block mb-1">Etapa</label>
            <select
              value={silverForm.stage}
              onChange={(e) => setSilverForm({ ...silverForm, stage: e.target.value })}
              className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface"
            >
              {SILVER_STAGES.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Categorías</label>
            <CategoryCheckboxes
              value={silverForm.categories}
              onChange={(v) => setSilverForm({ ...silverForm, categories: v })}
            />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Año</label>
            <input type="number" value={silverForm.year} onChange={(e) => setSilverForm({ ...silverForm, year: Number(e.target.value) })} className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface w-20" />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Rango de Meses</label>
            <MonthRangeSlider
              start={silverForm.monthStart}
              end={silverForm.monthEnd}
              onChange={(s, e) => setSilverForm({ ...silverForm, monthStart: s, monthEnd: e })}
            />
          </div>
          <button
            onClick={() => {
              silverJob.mutate({
                stage: silverForm.stage,
                categories: silverForm.categories,
                year: silverForm.year,
                month_start: silverForm.monthStart,
                month_end: silverForm.monthEnd,
              }, { onSuccess: (d: any) => showMessage(`Silver lanzado: job_id=${d.job_id}`) });
            }}
            disabled={silverJob.isPending || silverForm.categories.length === 0}
            className="flex items-center gap-2 px-4 py-2 bg-primary-container text-on-primary-container rounded-DEFAULT text-label-md hover:bg-primary-container/90 disabled:opacity-50"
          >
            {silverJob.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            Ejecutar
          </button>
        </div>
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
        <h2 className="text-headline-md text-secondary flex items-center gap-2">
          <Beaker className="w-5 h-5" /> Gold (Marts + ML)
        </h2>
        <div className="flex gap-4 items-end flex-wrap">
          <div>
            <label className="text-label-md text-on-surface block mb-1">Modo</label>
            <select value={goldForm.mode} onChange={(e) => setGoldForm({ ...goldForm, mode: e.target.value })} className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface">
              <option value="incremental">Incremental</option>
              <option value="full">Full</option>
            </select>
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Categorías</label>
            <CategoryCheckboxes
              value={goldForm.categories}
              onChange={(v) => setGoldForm({ ...goldForm, categories: v })}
            />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Año</label>
            <input type="number" value={goldForm.year} onChange={(e) => setGoldForm({ ...goldForm, year: Number(e.target.value) })} className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface w-20" />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">Rango de Meses</label>
            <MonthRangeSlider
              start={goldForm.monthStart}
              end={goldForm.monthEnd}
              onChange={(s, e) => setGoldForm({ ...goldForm, monthStart: s, monthEnd: e })}
            />
          </div>
          <div>
            <label className="text-label-md text-on-surface block mb-1">--only (opcional, comas)</label>
            <input type="text" value={goldForm.only} onChange={(e) => setGoldForm({ ...goldForm, only: e.target.value })} placeholder="mart1,mart2" className="bg-surface-muted border border-border-subtle rounded-DEFAULT p-2 text-body-md text-on-surface w-32" />
          </div>
          <button
            onClick={() => {
              const body: Record<string, unknown> = {
                mode: goldForm.mode,
                categories: goldForm.categories,
                year: goldForm.year,
                month_start: goldForm.monthStart,
                month_end: goldForm.monthEnd,
              };
              if (goldForm.only) body.only = goldForm.only.split(",").map((s: string) => s.trim()).filter(Boolean);
              goldJob.mutate(body, { onSuccess: (d: any) => showMessage(`Gold lanzado: job_id=${d.job_id}`) });
            }}
            disabled={goldJob.isPending || goldForm.categories.length === 0}
            className="flex items-center gap-2 px-4 py-2 bg-primary-container text-on-primary-container rounded-DEFAULT text-label-md hover:bg-primary-container/90 disabled:opacity-50"
          >
            {goldJob.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            Ejecutar
          </button>
        </div>
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
        <h2 className="text-headline-md text-secondary flex items-center gap-2">
          <Beaker className="w-5 h-5" /> Gold ML (Modelos)
        </h2>
        <div className="flex gap-4 items-end flex-wrap">
          {GOLD_ML_MODELS.map((model) => (
            <button
              key={model}
              onClick={() => {
                goldMlJob.mutate({ model }, { onSuccess: (d: any) => showMessage(`${model} lanzado: job_id=${d.job_id}`) });
              }}
              disabled={goldMlJob.isPending}
              className="flex items-center gap-2 px-4 py-2 bg-surface-container text-on-surface rounded-DEFAULT text-label-md border border-border-subtle hover:bg-surface-container-high disabled:opacity-50 capitalize"
            >
              {goldMlJob.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              {model}
            </button>
          ))}
        </div>
      </div>

      <div className="bg-surface-container-lowest border border-border-subtle rounded-DEFAULT p-6 space-y-4">
        <h2 className="text-headline-md text-secondary">Trabajos Activos</h2>

        {isLoading ? (
          <div className="flex items-center gap-2 text-on-surface-variant">
            <Loader2 className="w-4 h-4 animate-spin" />
            Cargando...
          </div>
        ) : jobs.length === 0 ? (
          <p className="text-body-md text-on-surface-variant italic">Sin trabajos registrados</p>
        ) : (
          <div className="space-y-2">
            {jobs.map((job: JobSummary) => (
              <div
                key={job.id}
                className={`flex items-center gap-4 p-3 rounded-DEFAULT border border-border-subtle cursor-pointer transition-colors ${
                  selectedJobId === job.id ? "bg-primary-container/5 border-primary-container" : "bg-surface-container-lowest hover:bg-surface-muted"
                }`}
                onClick={() => setSelectedJobId(selectedJobId === job.id ? null : job.id)}
              >
                <div className="flex-1 flex items-center gap-4">
                  <span className="text-body-md text-on-surface font-mono text-caption">{job.id.slice(0, 8)}</span>
                  <span className="text-label-md text-on-surface capitalize">{job.kind}</span>
                  <StatusChip status={job.status} />
                  {job.started_at && (
                    <span className="text-caption text-on-surface-variant">{new Date(job.started_at).toLocaleString()}</span>
                  )}
                </div>
                {job.status === "running" && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      stopJob.mutate(job.id);
                    }}
                    className="p-2 text-error hover:bg-error-container rounded-DEFAULT transition-colors"
                    title="Detener"
                  >
                    <StopCircle className="w-4 h-4" />
                  </button>
                )}
              </div>
            ))}
          </div>
        )}

        {selectedJobId && <JobLogViewer jobId={selectedJobId} />}
      </div>
    </div>
  );
}
