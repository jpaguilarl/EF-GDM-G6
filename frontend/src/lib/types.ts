export interface ProfilingRules {
  nullability?: Record<string, string[]> | null;
  reasonableness_ranges?: Record<string, Record<string, [number, number]>> | null;
  amount_formulas?: Record<string, { total: string; components: string[] }> | null;
  max_trip_duration_minutes?: number | null;
  amount_tolerance?: number | null;
}

export interface ProfilingConfig {
  rules: ProfilingRules;
}

export interface PipelineConfig {
  storage: { backend: "local" | "s3" };
  datasets: { years: (number | { category: string; year: number; month?: number | null })[] };
  gold: Record<string, unknown>;
  profiling: ProfilingConfig;
  speed: Record<string, unknown>;
  serving: { host: string; port: number; query_cache_ttl_seconds: number };
}

export interface EnvVars {
  STORAGE_BACKEND: string;
  S3_BUCKET: string;
  S3_PREFIX: string;
  REDIS_URL: string;
  AIRFLOW_UID: string;
  SPARK_DRIVER_MEMORY: string;
  SPARK_MASTER_CORES: string;
  SPARK_LOCAL_DIR: string;
  AWS_ACCESS_KEY_ID: string;
  AWS_SECRET_ACCESS_KEY: string;
  AIRFLOW__CORE__FERNET_KEY: string;
  _AIRFLOW_WWW_USER_PASSWORD: string;
}

export interface JobSummary {
  id: string;
  kind: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  exit_code: number | null;
}

export interface JobDetail extends JobSummary {
  pid: number | null;
  logs_tail: string[];
}

export interface AuditResult {
  rows: Record<string, unknown>[];
  total: number;
}

export interface AuditSummary {
  total_files?: number;
  total_bytes?: number;
  total_rows?: number;
  total_duration_sec?: number;
  avg_duration_sec?: number;
  by_category?: { category: string; files: number; rows: number; bytes?: number }[];
  by_month?: Record<string, unknown>[];
  total_bronze_rows?: number;
  total_quality_rows?: number;
  total_quarantined_rows?: number;
  overall_reject_rate?: number;
  total_builds?: number;
  total_output_rows?: number;
  by_mart?: { mart_name: string; rows: number; builds: number }[];
  mode_breakdown?: { mode: string; count: number }[];
}

export interface AuditLineageRow {
  layer: string;
  audit_id: string;
  fk_audit_id: string | null;
  source_name: string;
  rows_in: number | null;
  rows_out: number;
  rows_rejected: number | null;
  start_timestamp: string;
  end_timestamp: string;
  duration_sec: number;
}

export interface MartSummary {
  timeline?: { fecha_viaje: string; viajes?: number; total_amount?: number; propina_promedio?: number; propina_total?: number; espera_total_min?: number }[];
  top_zones?: { pu_zone: string; viajes: number }[];
  by_service?: { service_id: string; viajes: number }[];
  by_hour?: { pickup_hour: number; service_id: string; viajes: number }[];
  total?: Record<string, unknown> & {
    ingreso_bruto?: number;
    margen_promedio?: number | null;
    ratio_pago_conductor?: number | null;
    pct_propina_promedio?: number;
    propina_prom_por_milla?: number | null;
    pct_viajes_sin_propina?: number;
    viajes?: number;
  };
  fare_breakdown?: Record<string, number>;
  by_block?: { bloque_horario: string; velocidad_promedio: number; distancia_promedio: number; distancia_total?: number; duracion_promedio: number; tasa_ocupacion: number | null }[];
  scatter?: { bloque_horario: string; service_id: string; duracion: number; distancia: number }[];
  shared_efficiency?: { viajes: number; viajes_match: number };
  by_borough?: { pu_borough: string; viajes: number; velocidad_promedio?: number }[];
  deficit_ratio?: number;
  total_periods?: number;
  by_zone_hour?: { location_id: number; zone: string; borough: string; hour: number; flujo_neto_oferta: number }[];
  top_deficit_zones?: { location_id: number; zone: string; borough: string; flujo_neto_oferta: number }[];
  top_surplus_zones?: { location_id: number; zone: string; borough: string; flujo_neto_oferta: number }[];
  critical_zones_count?: number;
  global_net_flow?: number;
  avg_wait_min?: number | null;
  abc_distribution?: { clase_abc: string; count: number }[];
  xyz_distribution?: { clase_xyz: string; count: number }[];
  by_borough_origin?: { pu_borough?: string; pct_propina?: number; viajes?: number }[];
  by_borough_destination?: { do_borough?: string; pct_propina?: number; viajes?: number }[];
  generosity_by_service?: { service_id?: string; categoria_generosidad?: string; viajes?: number }[];
  matrix?: { year: number; month: number; service_id: string; ingreso_total: number; margen_promedio: number | null }[];
  [key: string]: unknown;
}

export interface IsolationSummary {
  total_scored: number;
  fraud_count: number;
  fraud_rate: number;
  by_ratecode: { ratecode_id: number; total: number; fraud: number; fraud_rate: number }[];
  score_stats: { score_min: number; score_max: number; score_mean: number; score_std: number };
  estimated_leakage?: number;
}

export interface IsolationScatterPoint {
  trip_id: number;
  ratecode_id?: number;
  trip_distance: number | null;
  fare_amount: number | null;
  velocidad_promedio_calculada: number | null;
  costo_por_distancia: number | null;
  duracion_viaje_segundos?: number | null;
  anomaly_score: number;
}

export interface IsolationScatterData {
  normal: IsolationScatterPoint[];
  fraud: IsolationScatterPoint[];
  legal_fare_per_mile: number;
}

export interface IsolationScoreRow {
  trip_id: number;
  ratecode_id?: number;
  anomaly_score: number;
  is_fraud: boolean;
  trip_distance?: number | null;
  fare_amount?: number | null;
  velocidad_promedio_calculada?: number | null;
  costo_por_distancia?: number | null;
  duracion_viaje_segundos?: number | null;
}

export interface IsolationScoresData {
  rows: IsolationScoreRow[];
  total: number;
}

export interface SarimaxSummary {
  combos: { borough: string; service_id: string }[];
  total_rows: number;
  date_range: { min_dt: string; max_dt: string };
}

export interface SarimaxForecastRow {
  borough: string;
  service_id: string;
  pickup_hour: string;
  trip_count: number | null;
  yhat: number | null;
  yhat_lower: number | null;
  yhat_upper: number | null;
  model_status: string;
  forecast_type: "actual" | "forecast";
}

export interface FraudRow {
  trip_id: number;
  service_id: string;
  ratecode_id: number | null;
  anomaly_score: number | null;
  is_fraud: boolean;
  is_anomaly_candidate: boolean;
  timestamp: string;
}

export interface ClusterRow {
  trip_id: number;
  cluster_id: number;
  service_id: string;
}

export interface KmodesDistribution {
  cluster_id: number;
  feature: string;
  value: string;
  count: number;
  pct: number;
}

export interface KmodesData {
  service_id: string;
  variables: string[];
  centers: Record<string, unknown>[];
  profiles: Record<string, unknown>[];
  sizes: { cluster_id: number; count: number }[];
  tuning: Record<string, unknown>[];
  distributions: KmodesDistribution[];
}

export interface RealtimeViewConfig {
  key: string;
  mart: string;
  label: string;
  timeColumn: string | null;
  dedupKey: string[];
  valueField: string;
  categoryField: string;
  chartType: "bar" | "line" | "pie";
}
