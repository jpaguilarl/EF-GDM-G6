# Panel de Control — Reference

## Overview

The control panel is a single-page React application served by the same FastAPI
process that powers the serving layer. It provides a graphical interface to:

- View and edit pipeline configuration (`config.yaml` + `.env`)
- Manually send a trip to the speed layer
- Trigger batch pipeline runs (bronze → silver → gold → gold-ml) for
  individual months or full modes
- Live-monitor job output via SSE log streaming
- Browse gold mart data and ML model results
- Inspect audit tables (bronze / silver / gold)

---

## Access & Setup

### Prerequisites

- Python dependencies installed (`uv sync`)
- Node.js 18+ and npm (for development)

### Dev mode (hot reload)

```bash
# Terminal 1 — FastAPI serving layer
uv run main.py --serve

# Terminal 2 — Vite dev server
cd frontend && npm run dev
```

Open **http://localhost:5173/panel** (Vite proxies `/api/*` to :8000).

### Production mode

```bash
cd frontend && npm run build
uv run main.py --serve
```

FastAPI serves the built static files at `/panel`. Open
**http://localhost:8000/panel**.

---

## Page Reference

### 1. Configuración (`/panel/configuration`)

Two sub-sections displayed as cards:

| Card | Editable fields | Behavior |
|---|---|---|
| **Storage** (from `config.yaml`) | `storage.backend` select (local / s3) | Writes full `config.yaml` |
| **Datasets** | `datasets.years` — add/remove int years or Module entries | Writes full `config.yaml` |
| **Gold** | `gold.*` sub-sections: mode, supply_demand, abc_xyz, generosity, isolation_fraud, sarimax, kmodes | Writes full `config.yaml` |
| **Profiling** | `profiling.rules.*` | Writes full `config.yaml` |
| **Speed** | `redis_url`, `state_ttl_hours`, `fraud_score_threshold`, `block_minutes` | Writes full `config.yaml` |
| **Serving** | `host`, `port`, `query_cache_ttl_seconds` | Writes full `config.yaml` |
| **.env** | Editable: non-secret keys only (`STORAGE_BACKEND`, `S3_BUCKET`, `S3_PREFIX`, `REDIS_URL`, `AIRFLOW_UID`, `SPARK_DRIVER_MEMORY`, `SPARK_MASTER_CORES`, `SPARK_LOCAL_DIR`). Secrets (`AWS_SECRET_ACCESS_KEY`, `AIRFLOW__CORE__FERNET_KEY`, `_AIRFLOW_WWW_USER_PASSWORD`, `AWS_ACCESS_KEY_ID`) show as green (set) or red (unset) badges — values are never displayed. | Writes only the allowlisted keys to `.env`; unknown keys are preserved |

**Save flow**: edit → "Review changes" (JSON diff) → confirm → `PUT /config` or
`PUT /env`. Changes are written to disk immediately but **do not hot-reload**
the running pipeline — they take effect on the next `uv run main.py ...`
invocation.

### 2. Enviar Viaje (`/panel/ingest`)

A form to manually submit a single trip to the speed layer:

1. Select category (yellow / green / fhv / fhvhv)
2. The JSON editor is pre-filled with a template for the selected category
3. Edit fields as needed
4. Click "Send" → `POST /api/v1/ingest`
5. Response shows the parsed trip with its computed `trip_id` (xxhash64) or a
   rejection reason

Optionally toggle "Watch live mart updates" to open an SSE stream on
`/api/v1/realtime/demand-volume/stream` showing real-time increment events.

### 3. Carga por Lotes (`/panel/batch`)

Four job-trigger sections:

| Section | Form fields | Spawns |
|---|---|---|
| **Bronce** | category, year, month | `uv run main.py --download --cat <c> --year <y> --month <m>` |
| **Silver** | stage (quality / schema / load), optional category / year / month | `uv run main.py --silver <stage> [--cat <c> --year <y> --month <m>]` |
| **Gold** | mode (full / incremental), optional `--only` list, optional category / year / month | `uv run main.py --gold <mode> [--only <m1,m2>] [--cat <c> --year <y>]` |
| **Gold ML** | model (kmodes / isolation / sarimax) | `uv run main.py --gold-ml <model>` |

On submit the button spawns an async subprocess and immediately returns a job
ID. The job appears in the **Active Jobs** list below.

#### Active Jobs list

- Polls `GET /api/v1/panel/jobs` every 3 seconds
- Columns: job kind, status chip (color-coded), started_at, duration, stop button
- Status colors:
  - **Pending**: yellow
  - **Running**: blue
  - **Completed**: green
  - **Failed**: red
  - **Stopped**: gray

#### Log viewer

Click a job row to open the log viewer — a monospace, auto-scrolling terminal
powered by an SSE stream on `GET /api/v1/panel/jobs/{id}/logs`. Capped at 10 000
lines in the backend ring buffer.

### 4. Resultados Gold (`/panel/gold`)

Two sub-tabs: **Marts Power BI** and **ML Modelos**.

#### Marts Power BI tab

One sub-tab per mart:

| Mart | Endpoint |
|---|---|
| Demanda / Volumen | `/api/v1/historic/demand-volume` |
| Rendimiento Financiero | `/api/v1/historic/financial-performance` |
| Perfil Operacional | `/api/v1/historic/operational-profile` |
| Balance Oferta / Demanda | `/api/v1/historic/supply-demand-balance` |
| ABC / XYZ Zonas | `/api/v1/historic/abc-xyz-zones` |
| Propinas | `/api/v1/historic/tipping-behavior` |

Each mart table:
- Paginated (50 rows per page) via `limit` + `offset` query params
- Total row count fetched from `GET /api/v1/historic/{mart}/count`
- Filterable by service_id (yellow / green / fhvhv / fhv) and year / month
- Columns rendered with `tabular-nums` for numeric fields
- CSV export button (client-side, current page only)

#### ML Modelos tab

Three sub-tabs:

**K-Modes** — select service_id (yellow / green / fhvhv):

| Section | Data source | Visualization |
|---|---|---|
| Variables used | `category_mapping.json` keys | Chips list |
| Cluster sizes | Row count per `cluster_id` across `labels_*/` | Recharts `BarChart` |
| Cluster centers | `centers.parquet` (one row per cluster, one col per feature) | Recharts `RadarChart` overlay (toggle clusters on/off) |
| Profiles | `profiles.parquet` | Plain table |
| Tuning | `tuning_*.parquet` (cost vs k) | Recharts `LineChart` (elbow curve); silhouette if present |

**Isolation Forest** — lists all ratecodes found in `data/gold/models/isolation_forest/`:

| Section | Source |
|---|---|
| Metadata cards | `metadata.json` per ratecode (contamination, n_estimators, training rows) |
| Anomaly scores | `GET /api/v1/panel/ml/isolation/{rc}/scores` — paginated table |

**SARIMAX** — forecast viewer:

| Section | Source | Visualization |
|---|---|---|
| Forecast | `GET /api/v1/panel/ml/sarimax/forecast` | Recharts `LineChart` with actuals + forecast horizon; filterable by borough × service_id |

All ML viewers have a **Retrain** button that spawns a `--gold-ml` job for the
selected model and redirects to the Batch page to watch progress.

### 5. Auditoría (`/panel/audit`)

Layer selector (bronze / silver / gold):

| Layer | Audit file |
|---|---|
| Bronze | `data/bronze/audit.parquet` |
| Silver | `data/silver/audit.parquet` |
| Gold   | `data/gold/audit.parquet` |

- Paginated table (50 rows per page)
- Columns vary by layer but typically include: `audit_id`, `started_at`,
  `ended_at`, `source_file`, `rows_in`, `rows_out`, `status`, FK to parent
  layer's audit_id
- Filterable by category and year (when the layer schema supports it)

---

## API Reference

All panel endpoints are prefixed with `/api/v1/panel` and served on the same
port as the serving layer (default `:8000`).

### Configuration

| Method | Path | Request body | Response | Status |
|---|---|---|---|---|
| `GET` | `/config` | — | Full `config.yaml` as JSON object | 200 |
| `PUT` | `/config` | `{"updates": {...}}` — partial or full `SettingsSchema` | `{"status":"ok"}` | 200 / 400 |
| `GET` | `/env` | — | `.env` keys: non-secret values in plaintext, secrets are `"<set>"` or `"<unset>"` | 200 |
| `PUT` | `/env` | `{"updates": { "STORAGE_BACKEND": "s3", ... }}` | `{"status":"ok"}` | 200 / 400 |

**Validation**: `PUT /config` validates the merged result against
`SettingsSchema` (Pydantic). `PUT /env` rejects keys outside the non-secret
allowlist.

### Jobs

| Method | Path | Request body | Response | Status |
|---|---|---|---|---|
| `POST` | `/jobs/bronze` | `{"category": "yellow", "year": 2025, "month": 6}` | `{"job_id": "uuid"}` | 200 |
| `POST` | `/jobs/silver` | `{"stage": "quality", "category": "yellow", "year": 2025, "month": 6}` | `{"job_id": "uuid"}` | 200 |
| `POST` | `/jobs/gold` | `{"mode": "incremental", "only": ["mart_demand_volume"], "category": "yellow", "year": 2025}` | `{"job_id": "uuid"}` | 200 |
| `POST` | `/jobs/gold-ml` | `{"model": "kmodes"}` | `{"job_id": "uuid"}` | 200 |
| `GET` | `/jobs` | — | `[{"id": ..., "kind": ..., "status": ..., ...}]` | 200 |
| `GET` | `/jobs/{id}` | — | Full `Job` object (incl. last 200 log lines) | 200 / 404 |
| `GET` | `/jobs/{id}/logs` | — | SSE stream of log lines | 200 / 404 |
| `POST` | `/jobs/{id}/stop` | — | `{"status": "stopped"}` | 200 / 404 |

**Job statuses**: `pending` → `running` → `completed` | `failed` | `stopped`.

### Audit

| Method | Path | Query params | Response |
|---|---|---|---|
| `GET` | `/audit/{layer}` | `limit` (1–1000, default 100), `offset` (≥0, default 0), `category`, `year` | `{"rows": [...], "total": N}` |

`{layer}` is one of `bronze`, `silver`, `gold`.

### ML

| Method | Path | Query params | Response |
|---|---|---|---|
| `GET` | `/ml/kmodes/{service_id}` | — | Centers, profiles, cluster sizes, tuning data, variable list |
| `GET` | `/ml/isolation` | — | List of ratecode dirs with metadata.json contents |
| `GET` | `/ml/isolation/{ratecode}/scores` | `limit` (1–1000), `offset` (≥0) | Paginated anomaly score rows |
| `GET` | `/ml/sarimax/forecast` | `limit` (1–1000), `offset` (≥0) | Paginated forecast rows |

### Historic (reused by gold marts)

| Method | Path | Query params | Response |
|---|---|---|---|
| `GET` | `/api/v1/historic/{mart}` | `limit`, `offset`, `service_id`, `year`, `month`, `borough`, ... | Array of mart rows |
| `GET` | `/api/v1/historic/{mart}/count` | Same filters (except `limit`/`offset`) | `{"total": N}` |

### Ingest (shared with speed layer)

| Method | Path | Request body | Response |
|---|---|---|---|
| `POST` | `/api/v1/ingest` | A `RideEvent` JSON object (see templates in frontend) | Accepted ride with `trip_id` or rejection with error |

---

## Job Lifecycle

Jobs are managed by the in-memory `JobManager` (lost on server restart):

```
pending → running → completed
                 → failed
                 → stopped
```

- `submit()`: validates params and spawns `uv run main.py ...` as an async
  subprocess (stdout + stderr piped to same stream)
- `stream_logs()`: yields lines from the subprocess via an `AsyncGenerator`;
  the last 10 000 lines are kept in a ring buffer
- `stop()`: sends SIGTERM to the process group; waits 10s then SIGKILL

The job is **not persisted** — restarting the serving process clears all job
history.

### Cached-at-start limitations

- **Config changes**: take effect on the next pipeline subprocess, not the
  running serving process
- **Gold mart cache**: `PolarsQueryEngine` caches lazy scans for
  `query_cache_ttl_seconds` (default 60s). New gold data is visible after this
  TTL expires (or by restarting `--serve`)

---

## File locations

| Path | Purpose |
|---|---|
| `frontend/` | React source (Vite project) |
| `frontend/src/pages/` | One file per route: `Configuration`, `Ingest`, `BatchLoad`, `GoldResults`, `Audit` |
| `frontend/src/components/` | Reusable components: `Layout`, `MartTable`, `KmodesViewer`, `IsolationViewer`, `SarimaxViewer`, `JobLogViewer`, `JobList` |
| `frontend/src/lib/` | API client (`api.ts`), TypeScript types (`types.ts`), trip templates (`tripTemplates.ts`) |
| `frontend/src/hooks/` | TanStack Query hooks: `useJobs.ts`, `useMartData.ts` |
| `app/panel/` | Backend modules: `job_manager.py`, `config_io.py`, `audit_reader.py`, `ml_reader.py` |
| `app/serving/routes/panel.py` | 17 FastAPI endpoint definitions |
| `frontend/dist/` | Production build output (served by FastAPI at `/panel`) |
