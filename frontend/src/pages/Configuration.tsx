import { useState, useEffect, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { Save, RotateCcw, Eye, X, Check, AlertTriangle, Info } from "lucide-react";
import { apiGet, apiPut } from "../lib/api";
import type { PipelineConfig, EnvVars } from "../lib/types";

interface FieldInfo {
  type: string;
  default: string;
  description: string;
}

const FIELD_INFO: Record<string, FieldInfo> = {
  "storage.backend": {
    type: '"local" / "s3"',
    default: "local",
    description: "Sistema de archivos raíz. local usa el directorio del proyecto; s3 usa un bucket S3.",
  },
  "datasets.years": {
    type: "list[int | Module]",
    default: "[2023, 2024, 2025]",
    description: "Años a expandir en 4 categorías × 12 meses. Cada elemento puede ser un entero o un Module {category, year, month}.",
  },
  "gold.mode": {
    type: '"full" / "incremental"',
    default: "full",
    description: "full recalcula todo el histórico; incremental escribe solo particiones faltantes (respeta partitionOverwriteMode=dynamic).",
  },
  "gold.supply_demand.block_minutes": {
    type: "int",
    default: "15",
    description: "Duración de cada bloque horario para agregar viajes (minutos).",
  },
  "gold.supply_demand.deficit_threshold": {
    type: "int",
    default: "-10",
    description: "Umbral de viajes negativos para clasificar un bloque como déficit.",
  },
  "gold.abc_xyz.class_a_pct": {
    type: "float",
    default: "0.80",
    description: "Percentil acumulado para clase A (mayor volumen).",
  },
  "gold.abc_xyz.class_b_pct": {
    type: "float",
    default: "0.15",
    description: "Percentil acumulado para clase B (volumen medio).",
  },
  "gold.abc_xyz.xyz_x_max": {
    type: "float",
    default: "0.2",
    description: "Coeficiente de variación máximo para clase X (baja variabilidad).",
  },
  "gold.abc_xyz.xyz_y_max": {
    type: "float",
    default: "0.5",
    description: "Coeficiente de variación máximo para clase Y (variabilidad media).",
  },
  "gold.generosity.standard_low": {
    type: "float",
    default: "10.0",
    description: "Porcentaje de propina por debajo del cual se considera 'Baja'.",
  },
  "gold.generosity.standard_high": {
    type: "float",
    default: "18.0",
    description: "Porcentaje de propina por encima del cual se considera 'Alta'.",
  },
  "gold.isolation_fraud.contamination": {
    type: "float",
    default: "0.05",
    description: "Proporción esperada de anomalías en los datos.",
  },
  "gold.isolation_fraud.n_estimators": {
    type: "int",
    default: "100",
    description: "Número de árboles de aislamiento.",
  },
  "gold.isolation_fraud.max_samples": {
    type: "str",
    default: "auto",
    description: "Muestras por árbol ('auto' usa min(256, n_samples)).",
  },
  "gold.isolation_fraud.random_state": {
    type: "int",
    default: "42",
    description: "Semilla para reproducibilidad.",
  },
  "gold.isolation_fraud.min_rows_per_ratecode": {
    type: "int",
    default: "200",
    description: "Mínimo de filas requeridas por RatecodeID para entrenar un modelo.",
  },
  "gold.sarimax.order": {
    type: "list[int]",
    default: "[1, 1, 1]",
    description: "Orden ARIMA (p, d, q).",
  },
  "gold.sarimax.seasonal_order": {
    type: "list[int]",
    default: "[1, 1, 1, 24]",
    description: "Orden estacional (P, D, Q, s); periodo 24 para ciclo diario.",
  },
  "gold.sarimax.min_rows_per_segment": {
    type: "int",
    default: "1000",
    description: "Mínimo de filas por segmento (borough × service_id) para entrenar.",
  },
  "gold.sarimax.forecast_horizon_hours": {
    type: "int",
    default: "168",
    description: "Horizonte de pronóstico en horas (default 7 días).",
  },
  "gold.kmodes.max_k": {
    type: "int",
    default: "8",
    description: "Número máximo de clusters a evaluar (codo + silueta).",
  },
  "gold.kmodes.max_sample_per_service": {
    type: "int",
    default: "100000",
    description: "Máximo de filas muestreadas por servicio (fhvhv se muestrea al 5%).",
  },
  "gold.kmodes.n_init": {
    type: "int",
    default: "2",
    description: "Número de inicializaciones del algoritmo (elige la de menor costo).",
  },
  "gold.kmodes.init_method": {
    type: '"Cao" / "Huang"',
    default: "Cao",
    description: "Método de inicialización de centroides.",
  },
  "gold.kmodes.random_state": {
    type: "int",
    default: "42",
    description: "Semilla para reproducibilidad.",
  },
  "profiling.rules.nullability": {
    type: "dict[str, list[str]]",
    default: "—",
    description: "Columnas opcionales (nullable) por categoría. Columnas no listadas se asumen requeridas; nulos en ellas causan rechazo en Silver.",
  },
  "profiling.rules.reasonableness_ranges": {
    type: "dict[str, dict[str, [float, float]]]",
    default: "—",
    description: "Rangos aceptables [mín, máx] para variables numéricas por categoría. Usado solo por la dimensión reasonableness en profiling.",
  },
  "profiling.rules.amount_formulas": {
    type: "dict[str, {total: str, components: str[]}]",
    default: "—",
    description: "Composición del importe total a partir de componentes individuales por categoría. Usado solo por la dimensión accuracy en profiling.",
  },
  "profiling.rules.max_trip_duration_minutes": {
    type: "int",
    default: "1440",
    description: "Duración máxima razonable de un viaje (minutos). Ya no se usa para rechazar registros en Silver.",
  },
  "profiling.rules.amount_tolerance": {
    type: "float",
    default: "0.02",
    description: "Tolerancia absoluta (USD) para comparación entre total declarado y suma de componentes.",
  },
  "speed.redis_url": {
    type: "str",
    default: "redis://localhost:6379/0",
    description: "URL de conexión a Redis. En Docker Compose se sobreescribe a redis://redis:6379/0.",
  },
  "speed.state_ttl_hours": {
    type: "int",
    default: "48",
    description: "TTL en horas de las claves de estado en Redis (agregaciones, uniqueness).",
  },
  "speed.fraud_score_threshold": {
    type: "float",
    default: "-0.1",
    description: "Umbral del score de Isolation Forest para marcar un viaje como fraudulento.",
  },
  "speed.block_minutes": {
    type: "int",
    default: "15",
    description: "Duración de cada bloque horario para agregar viajes en tiempo real (minutos).",
  },
  "serving.host": {
    type: "str",
    default: "0.0.0.0",
    description: "Dirección de escucha del servidor FastAPI.",
  },
  "serving.port": {
    type: "int",
    default: "8000",
    description: "Puerto de escucha.",
  },
  "serving.query_cache_ttl_seconds": {
    type: "int",
    default: "60",
    description: "TTL en segundos del caché de consultas (reduce re-escaneos de parquet).",
  },
};

const ENV_INFO: Record<string, string> = {
  STORAGE_BACKEND: "Backend de almacenamiento (local o s3). Se sobreescribe a s3 si las variables AWS están presentes.",
  AWS_ACCESS_KEY_ID: "Clave de acceso AWS.",
  AWS_SECRET_ACCESS_KEY: "Clave secreta AWS.",
  AWS_REGION: "Región AWS (default: us-east-1).",
  S3_BUCKET: "Nombre del bucket S3.",
  S3_PREFIX: "Prefijo (carpeta virtual) dentro del bucket (default: tlc-pipeline).",
  SPARK_DRIVER_MEMORY: "Heap de la JVM de Spark (driver y executor en local mode). Bare-metal default: 10g, Docker default: 8g.",
  SPARK_MASTER_CORES: "Núcleos paralelos (local[N]). No subir sin subir el heap proporcionalmente. Bare-metal default: 10, Docker default: 8.",
  SPARK_LOCAL_DIR: "Directorio de shuffle/spill. Siempre en disco local, incluso con backend S3. Bare-metal default: data/.spark_temp, Docker default: /tmp/spark-temp.",
  REDIS_URL: "URL de conexión a Redis. En Docker Compose se sobreescribe a redis://redis:6379/0.",
  AIRFLOW_UID: "UID del usuario airflow dentro del contenedor. En Linux, usar id -u del host (default: 50000).",
  AIRFLOW__CORE__EXECUTOR: "Ejecutor de Airflow. No cambiar (el pipeline no está diseñado para Celery). Default: LocalExecutor.",
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "Cadena de conexión a la base de metadatos de Airflow.",
  AIRFLOW__CORE__FERNET_KEY: "Clave Fernet para serialización de DAGs de Airflow.",
  _AIRFLOW_WWW_USER_USERNAME: "Usuario de la interfaz web de Airflow (default: admin).",
  _AIRFLOW_WWW_USER_PASSWORD: "Contraseña de la interfaz web de Airflow (default: admin).",
};

function SectionCard({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-surface-container-lowest border border-border-subtle rounded p-6">
      <h2 className="text-lg font-semibold text-on-surface mb-4">{title}</h2>
      {children}
    </div>
  );
}

function Field({ label, children, info }: { label: string; children: React.ReactNode; info?: { type: string; default: string; description: string } | string }) {
  return (
    <label className="block stack-md">
      <span className="block text-sm font-semibold text-on-surface mb-1">{label}</span>
      {children}
      {info && (
        <p className="mt-1.5 text-xs text-on-surface-variant/70 leading-relaxed flex items-start gap-1.5">
          <Info className="w-3 h-3 mt-0.5 shrink-0" />
          {typeof info === "string" ? info : `${info.description} (${info.type}, default: ${info.default})`}
        </p>
      )}
    </label>
  );
}

function Input({
  value,
  onChange,
  type = "text",
  placeholder,
}: {
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="w-full px-3 py-2 bg-surface-muted border border-border-subtle rounded text-on-surface text-sm focus:outline-none focus:ring-2 focus:ring-primary-container/30 focus:border-primary-container"
    />
  );
}

function Select({
  value,
  options,
  onChange,
}: {
  value: string;
  options: string[];
  onChange: (v: string) => void;
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full px-3 py-2 bg-surface-muted border border-border-subtle rounded text-on-surface text-sm focus:outline-none focus:ring-2 focus:ring-primary-container/30 focus:border-primary-container"
    >
      {options.map((opt) => (
        <option key={opt} value={opt}>
          {opt}
        </option>
      ))}
    </select>
  );
}

function NumberInput({
  value,
  onChange,
}: {
  value: number;
  onChange: (v: number) => void;
}) {
  return (
    <input
      type="number"
      value={value}
      onChange={(e) => onChange(Number(e.target.value))}
      className="w-full px-3 py-2 bg-surface-muted border border-border-subtle rounded text-on-surface text-sm focus:outline-none focus:ring-2 focus:ring-primary-container/30 focus:border-primary-container"
    />
  );
}

function SecretBadge({ value }: { value: string }) {
  const isSet = value && value !== "";
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-semibold ${
        isSet
          ? "bg-green-100 text-green-800"
          : "bg-red-100 text-red-800"
      }`}
    >
      {isSet ? <Check className="w-3 h-3" /> : <X className="w-3 h-3" />}
      {isSet ? "set" : "unset"}
    </span>
  );
}

function CollapsibleSection({
  title,
  defaultOpen = false,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="border border-border-subtle rounded overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-4 py-3 bg-surface-muted text-sm font-semibold text-on-surface text-left"
      >
        {title}
        <span className={`transform transition-transform ${open ? "rotate-180" : ""}`}>
          ▾
        </span>
      </button>
      {open && <div className="p-4 space-y-4">{children}</div>}
    </div>
  );
}

function renderSubConfig(
  label: string,
  obj: Record<string, unknown> | undefined,
  prefix: string,
  draft: Record<string, string>,
  setDraft: (updater: (prev: Record<string, string>) => Record<string, string>) => void
) {
  if (!obj || typeof obj !== "object") return null;
  return (
    <CollapsibleSection key={prefix} title={label} defaultOpen={true}>
      {Object.entries(obj).map(([key, val]) => {
        const fieldKey = `${prefix}.${key}`;
        const strVal = typeof val === "object" ? JSON.stringify(val) : String(val ?? "");
        return (
          <Field key={fieldKey} label={key} info={FIELD_INFO[fieldKey]}>
            <Input
              value={draft[fieldKey] ?? strVal}
              onChange={(v) =>
                setDraft((prev) => ({ ...prev, [fieldKey]: v }))
              }
            />
          </Field>
        );
      })}
    </CollapsibleSection>
  );
}

const SECRET_KEYS = new Set([
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AIRFLOW__CORE__FERNET_KEY",
  "_AIRFLOW_WWW_USER_PASSWORD",
]);

export function Configuration() {
  const configQuery = useQuery({
    queryKey: ["config"],
    queryFn: () => apiGet<PipelineConfig>("/config"),
  });

  const envQuery = useQuery({
    queryKey: ["env"],
    queryFn: () => apiGet<EnvVars>("/env"),
  });

  const [configDraft, setConfigDraft] = useState<Record<string, string>>({});
  const [envDraft, setEnvDraft] = useState<Record<string, string>>({});
  const [showDiff, setShowDiff] = useState(false);
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState<{
    type: "success" | "error";
    message: string;
  } | null>(null);

  const config = configQuery.data;
  const env = envQuery.data;

  useEffect(() => {
    if (config && Object.keys(configDraft).length === 0) {
      const init: Record<string, string> = {};
      init["storage.backend"] = config.storage.backend;
      if (config.datasets.years) {
        init["datasets.years"] = JSON.stringify(config.datasets.years);
      }
      if (config.gold) {
        flattenObj(config.gold as unknown as Record<string, unknown>, "gold", init);
      }
      if (config.profiling) {
        flattenObj(config.profiling as unknown as Record<string, unknown>, "profiling", init);
      }
      if (config.speed) {
        flattenObj(config.speed as unknown as Record<string, unknown>, "speed", init);
      }
      if (config.serving) {
        flattenObj(config.serving as Record<string, unknown>, "serving", init);
      }
      setConfigDraft(init);
    }
  }, [config]);

  useEffect(() => {
    if (env && Object.keys(envDraft).length === 0) {
      const init: Record<string, string> = {};
      for (const [k, v] of Object.entries(env)) {
        init[k] = v;
      }
      setEnvDraft(init);
    }
  }, [env]);

  function flattenObj(
    obj: Record<string, unknown>,
    prefix: string,
    acc: Record<string, string>
  ) {
    for (const [key, val] of Object.entries(obj)) {
      const k = `${prefix}.${key}`;
      if (val !== null && typeof val === "object" && !Array.isArray(val)) {
        flattenObj(val as Record<string, unknown>, k, acc);
      } else {
        acc[k] = typeof val === "object" ? JSON.stringify(val) : String(val ?? "");
      }
    }
    return acc;
  }

  function parseValue(v: string): unknown {
    if (v === "true") return true;
    if (v === "false") return false;
    if (v === "null" || v === "") return null;
    if (!isNaN(Number(v)) && v.trim() !== "") return Number(v);
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  }

  function setNested(obj: Record<string, unknown>, dottedKey: string, value: unknown) {
    const parts = dottedKey.split(".");
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!cur[parts[i]] || typeof cur[parts[i]] !== "object") cur[parts[i]] = {};
      cur = cur[parts[i]] as Record<string, unknown>;
    }
    cur[parts[parts.length - 1]] = value;
  }

  const computeDiff = useCallback(() => {
    const changes: Record<string, { original: string; edited: string }> = {};
    for (const [k, v] of Object.entries(configDraft)) {
      const original = getOriginalConfigValue(k, config);
      const origStr = original !== undefined ? String(original) : "";
      if (String(origStr) !== String(v)) {
        changes[k] = { original: origStr, edited: v };
      }
    }
    for (const [k, v] of Object.entries(envDraft)) {
      if (env && String(env[k as keyof EnvVars] ?? "") !== String(v)) {
        changes[`env.${k}`] = {
          original: SECRET_KEYS.has(k)
            ? env[k as keyof EnvVars]
              ? "<set>"
              : "<unset>"
            : String(env[k as keyof EnvVars] ?? ""),
          edited: SECRET_KEYS.has(k) ? (v ? "<set>" : "<unset>") : v,
        };
      }
    }
    return changes;
  }, [configDraft, envDraft, config, env]);

  function getOriginalConfigValue(
    key: string,
    cfg: PipelineConfig | undefined
  ): unknown {
    if (!cfg) return undefined;
    if (key === "storage.backend") return cfg.storage.backend;
    if (key === "datasets.years") return JSON.stringify(cfg.datasets.years);
    const [group, ...rest] = key.split(".");
    const groupKey = group as keyof PipelineConfig;
    const sub = cfg[groupKey];
    if (!sub || typeof sub !== "object") return undefined;
    let current: unknown = sub;
    for (const part of rest) {
      if (current && typeof current === "object" && part in (current as Record<string, unknown>)) {
        current = (current as Record<string, unknown>)[part];
      } else {
        return undefined;
      }
    }
    return current !== undefined ? String(current) : undefined;
  }

  async function handleSave() {
    setSaving(true);
    setToast(null);
    try {
      const diff = computeDiff();
      const configUpdates: Record<string, unknown> = {};
      const envUpdates: Record<string, string> = {};

      for (const key of Object.keys(diff)) {
        if (key.startsWith("env.")) {
          const envKey = key.slice(4);
          envUpdates[envKey] = envDraft[envKey] ?? "";
        } else {
          setNested(configUpdates, key, parseValue(configDraft[key]));
        }
      }

      const promises: Promise<unknown>[] = [];
      if (Object.keys(configUpdates).length > 0) {
        promises.push(apiPut("/config", { updates: configUpdates }));
      }
      if (Object.keys(envUpdates).length > 0) {
        promises.push(apiPut("/env", { updates: envUpdates }));
      }

      await Promise.all(promises);
      setToast({ type: "success", message: "Configuración guardada correctamente" });
      setShowDiff(false);
      configQuery.refetch();
      envQuery.refetch();
    } catch (err) {
      setToast({
        type: "error",
        message: `Error al guardar: ${err instanceof Error ? err.message : String(err)}`,
      });
    } finally {
      setSaving(false);
    }
  }

  function handleReset() {
    setConfigDraft({});
    setEnvDraft({});
    setShowDiff(false);
  }

  if (configQuery.isLoading || envQuery.isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-on-surface-variant">Cargando configuración...</p>
      </div>
    );
  }

  if (configQuery.error || envQuery.error) {
    return (
      <div className="flex items-center justify-center h-64">
        <p className="text-error">Error al cargar la configuración</p>
      </div>
    );
  }

  const diff = showDiff ? computeDiff() : {};
  const hasChanges = Object.keys(computeDiff()).length > 0;

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-on-surface">Configuración</h1>
        <div className="flex items-center gap-3">
          {hasChanges && (
            <>
              <button
                type="button"
                onClick={() => setShowDiff(!showDiff)}
                className="flex items-center gap-2 px-4 py-2 border border-border-subtle rounded text-sm font-semibold text-on-surface-variant hover:bg-surface-muted transition-colors"
              >
                <Eye className="w-4 h-4" />
                {showDiff ? "Ocultar cambios" : "Revisar cambios"}
              </button>
              <button
                type="button"
                onClick={handleReset}
                className="flex items-center gap-2 px-4 py-2 border border-border-subtle rounded text-sm font-semibold text-on-surface-variant hover:bg-surface-muted transition-colors"
              >
                <RotateCcw className="w-4 h-4" />
                Restablecer
              </button>
            </>
          )}
        </div>
      </div>

      {/* Toast */}
      {toast && (
        <div
          className={`flex items-center gap-2 px-4 py-3 rounded text-sm font-semibold ${
            toast.type === "success"
              ? "bg-green-100 text-green-800"
              : "bg-red-100 text-red-800"
          }`}
        >
          {toast.type === "success" ? (
            <Check className="w-4 h-4" />
          ) : (
            <AlertTriangle className="w-4 h-4" />
          )}
          {toast.message}
          <button
            type="button"
            onClick={() => setToast(null)}
            className="ml-auto"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Diff Modal */}
      {showDiff && Object.keys(diff).length > 0 && (
        <div className="bg-surface-container-lowest border border-border-subtle rounded p-6 space-y-4">
          <h2 className="text-lg font-semibold text-on-surface">
            Cambios detectados
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border-subtle">
                  <th className="text-left py-2 pr-4 font-semibold text-on-surface">
                    Campo
                  </th>
                  <th className="text-left py-2 pr-4 font-semibold text-on-surface">
                    Original
                  </th>
                  <th className="text-left py-2 font-semibold text-on-surface">
                    Editado
                  </th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(diff).map(([key, { original, edited }]) => (
                  <tr key={key} className="border-b border-border-subtle">
                    <td className="py-2 pr-4 text-on-surface-variant font-mono text-xs">
                      {key}
                    </td>
                    <td className="py-2 pr-4 text-on-surface">{original}</td>
                    <td className="py-2 text-primary-container font-semibold">
                      {edited}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={() => setShowDiff(false)}
              className="px-4 py-2 border border-border-subtle rounded text-sm font-semibold text-on-surface-variant hover:bg-surface-muted transition-colors"
            >
              Cancelar
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving}
              className="flex items-center gap-2 px-4 py-2 bg-primary-container text-white rounded text-sm font-semibold hover:bg-primary-container/90 transition-colors disabled:opacity-50"
            >
              <Save className="w-4 h-4" />
              {saving ? "Guardando..." : "Confirmar"}
            </button>
          </div>
        </div>
      )}

      {/* Storage card */}
      <SectionCard title="Storage">
        <Field label="Backend" info={FIELD_INFO["storage.backend"]}>
          <Select
            value={configDraft["storage.backend"] ?? config?.storage.backend ?? "local"}
            options={["local", "s3"]}
            onChange={(v) =>
              setConfigDraft((prev) => ({ ...prev, "storage.backend": v }))
            }
          />
        </Field>
      </SectionCard>

      {/* Datasets card */}
      <SectionCard title="Datasets">
        <Field label="Años / Módulos" info={FIELD_INFO["datasets.years"]}>
          <div className="flex flex-wrap gap-2 mb-3">
            {(() => {
              const raw = configDraft["datasets.years"];
              let years: unknown[];
              try {
                years = raw ? JSON.parse(raw) : config?.datasets.years ?? [];
              } catch {
                years = config?.datasets.years ?? [];
              }
              return (years as unknown[]).map((y, i) => (
                <span
                  key={i}
                  className="inline-flex items-center gap-1 px-2 py-1 bg-surface-muted border border-border-subtle rounded text-xs font-mono"
                >
                  {typeof y === "object"
                    ? `${(y as { category: string }).category}/${(y as { year: number }).year}`
                    : String(y)}
                  <button
                    type="button"
                    onClick={() => {
                      const arr = [...years];
                      arr.splice(i, 1);
                      setConfigDraft((prev) => ({
                        ...prev,
                        "datasets.years": JSON.stringify(arr),
                      }));
                    }}
                    className="text-on-surface-variant hover:text-error"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </span>
              ));
            })()}
          </div>
          <div className="flex gap-2">
            <input
              type="number"
              placeholder="Añadir año (ej. 2026)"
              className="flex-1 px-3 py-2 bg-surface-muted border border-border-subtle rounded text-sm focus:outline-none focus:ring-2 focus:ring-primary-container/30 focus:border-primary-container"
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  const input = e.currentTarget;
                  const val = Number(input.value);
                  if (val) {
                    const raw = configDraft["datasets.years"];
                    let years: unknown[];
                    try {
                      years = raw ? JSON.parse(raw) : config?.datasets.years ?? [];
                    } catch {
                      years = config?.datasets.years ?? [];
                    }
                    years = [...years, val];
                    setConfigDraft((prev) => ({
                      ...prev,
                      "datasets.years": JSON.stringify(years),
                    }));
                    input.value = "";
                  }
                }
              }}
            />
          </div>
        </Field>
      </SectionCard>

      {/* Gold card */}
      <SectionCard title="Gold">
        {config?.gold
          ? Object.entries(config.gold).map(([key, val]) =>
              renderSubConfig(key, val as Record<string, unknown>, `gold.${key}`, configDraft, setConfigDraft)
            )
          : (
            <p className="text-sm text-on-surface-variant">
              No hay configuración de Gold disponible
            </p>
          )}
      </SectionCard>

      {/* Profiling card */}
      <SectionCard title="Profiling">
        {config?.profiling?.rules
          ? (
            <div className="space-y-4">
              {renderSubConfig("Nullability", config.profiling.rules.nullability as Record<string, unknown>, "profiling.rules.nullability", configDraft, setConfigDraft)}
              {renderSubConfig("Reasonableness Ranges", config.profiling.rules.reasonableness_ranges as Record<string, unknown>, "profiling.rules.reasonableness_ranges", configDraft, setConfigDraft)}
              {renderSubConfig("Amount Formulas", config.profiling.rules.amount_formulas as Record<string, unknown>, "profiling.rules.amount_formulas", configDraft, setConfigDraft)}
              <div className="grid grid-cols-2 gap-4">
                <Field label="Max Trip Duration (min)" info={FIELD_INFO["profiling.rules.max_trip_duration_minutes"]}>
                  <NumberInput
                    value={Number(configDraft["profiling.rules.max_trip_duration_minutes"] ?? config.profiling.rules.max_trip_duration_minutes ?? 1440)}
                    onChange={(v) =>
                      setConfigDraft((prev) => ({ ...prev, "profiling.rules.max_trip_duration_minutes": String(v) }))
                    }
                  />
                </Field>
                <Field label="Amount Tolerance (USD)" info={FIELD_INFO["profiling.rules.amount_tolerance"]}>
                  <Input
                    value={configDraft["profiling.rules.amount_tolerance"] ?? String(config.profiling.rules.amount_tolerance ?? 0.02)}
                    onChange={(v) =>
                      setConfigDraft((prev) => ({ ...prev, "profiling.rules.amount_tolerance": v }))
                    }
                  />
                </Field>
              </div>
            </div>
          )
          : (
            <p className="text-sm text-on-surface-variant">
              No hay configuración de Profiling disponible
            </p>
          )}
      </SectionCard>

      {/* Speed card */}
      <SectionCard title="Speed (Tiempo Real)">
        {config?.speed
          ? (
            <div className="space-y-4">
              {Object.entries(config.speed).map(([key, val]) => {
                const fieldKey = `speed.${key}`;
                return (
                  <Field key={fieldKey} label={key} info={FIELD_INFO[fieldKey]}>
                    <Input
                      value={configDraft[fieldKey] ?? String(val ?? "")}
                      onChange={(v) =>
                        setConfigDraft((prev) => ({ ...prev, [fieldKey]: v }))
                      }
                    />
                  </Field>
                );
              })}
            </div>
          )
          : (
            <p className="text-sm text-on-surface-variant">
              No hay configuración de Speed disponible
            </p>
          )}
      </SectionCard>

      {/* Serving card */}
      <SectionCard title="Serving (API)">
        {config?.serving && (
          <div className="space-y-4">
            {Object.entries(config.serving).map(([key, val]) => {
              const fieldKey = `serving.${key}`;
              return (
                <Field key={fieldKey} label={key} info={FIELD_INFO[fieldKey]}>
                  {key === "port" ? (
                    <NumberInput
                      value={Number(configDraft[fieldKey] ?? val)}
                      onChange={(v) =>
                        setConfigDraft((prev) => ({ ...prev, [fieldKey]: String(v) }))
                      }
                    />
                  ) : (
                    <Input
                      value={configDraft[fieldKey] ?? String(val ?? "")}
                      onChange={(v) =>
                        setConfigDraft((prev) => ({ ...prev, [fieldKey]: v }))
                      }
                    />
                  )}
                </Field>
              );
            })}
          </div>
        )}
      </SectionCard>

      {/* .env card */}
      <SectionCard title=".env (Variables de Entorno)">
        {env && (
          <div className="space-y-4">
            {Object.entries(env).map(([key, val]) => {
              const isSecret = SECRET_KEYS.has(key);
              return (
                <Field key={key} label={key} info={ENV_INFO[key]}>
                  <div className="flex items-center gap-3">
                    {isSecret ? (
                      <div className="flex-1 flex items-center gap-2">
                        <span className="text-sm text-on-surface-variant italic">
                          {val ? "********" : "(vacío)"}
                        </span>
                        <SecretBadge value={val} />
                      </div>
                    ) : (
                      <Input
                        value={envDraft[key] ?? val}
                        onChange={(v) =>
                          setEnvDraft((prev) => ({ ...prev, [key]: v }))
                        }
                      />
                    )}
                  </div>
                </Field>
              );
            })}
          </div>
        )}
      </SectionCard>

      {/* Global Save button */}
      {hasChanges && !showDiff && (
        <div className="flex justify-end pt-4 border-t border-border-subtle">
          <button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-6 py-3 bg-primary-container text-white rounded text-sm font-semibold hover:bg-primary-container/90 transition-colors disabled:opacity-50"
          >
            <Save className="w-4 h-4" />
            {saving ? "Guardando..." : "Guardar cambios"}
          </button>
        </div>
      )}
    </div>
  );
}
