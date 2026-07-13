# AGENTS.md

## Project

ETL + data-quality pipeline for NY TLC Trip Record Data — **medallion architecture**
(bronze → silver → gold) plus a standalone profiling stage.

Downloads parquet from `https://d37ci6vzurychx.cloudfront.net/trip-data/`, profiles each
dataset across 8 quality dimensions (accuracy, completeness, consistency, integrity,
reasonableness, timeliness, uniqueness, validity), cleans data into a star schema, builds
gold marts (Power BI, aggregated grain) + ML feature stores, and trains the ML models
(K-Modes, Isolation Forest, SARIMAX).

## Run

```bash
uv run main.py                 # bronze: download zone-lookup + all trip data
uv run main.py --profile       # profiling: evaluate quality of bronze data
uv run main.py --silver        # silver quality (default): clean bronze → stage + reject
uv run main.py --silver schema # build star-schema dimensions → data/silver/star/dims/
uv run main.py --silver load   # build star-schema facts → data/silver/star/facts/
uv run main.py --gold          # gold (full): Power BI marts + ML feature stores → data/gold/
uv run main.py --gold incremental                                       # only missing trip-grain partitions
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud # subset of builders
uv run main.py --gold-ml kmodes            # train K-Modes (default: cluster trip profiles, needs ml_feat_kmodes_trips first)
uv run main.py --gold-ml isolation         # train Isolation Forest per RatecodeID (needs ml_feat_isolation_fraud first)
uv run main.py --gold-ml sarimax           # train SARIMAX trip-count forecaster (needs ml_feat_arima_trips first)
```

Order: `bronze → --silver → --silver schema → --silver load → --gold → --gold-ml`. `--silver schema`
must precede `--silver load`; `--gold` reads `data/silver/star/` and aborts cleanly if missing.
`--gold-ml kmodes` requires `ml_feat_kmodes_trips` (run `--gold --only ml_feat_kmodes_trips` first).

**Idempotent / resumable**: every stage skips outputs that already exist (bronze validates the parquet
footer, profiling reuses report JSONs, silver skips existing stage months, star skips existing facts,
gold `incremental` skips existing partitions). To force a re-run, delete the corresponding output.
Interrupted runs resume by relaunching the same command.

## Serving layer (Lambda architecture)

- **`app.serving/`** — FastAPI app with historic GET endpoints (Polars lazy scans
  over gold marts) + real-time SSE streams (merged batch + Redis state).
- **`app.speed/`** — Speed processing engine: `POST /api/v1/ingest` → clean →
  enrich → aggregate (Redis HINCRBY) → fraud score (IsolationForest from joblib).
- **Redis** — speed layer state (aggregations, uniqueness check via SETNX trip_id).
  TTL 48h, ~50–100MB. Key prefix `rt:`.
- **trip_id parity** — `xxhash64` of `SilverCleaner.COMPOSITE_KEYS` in pure Python
  (xxhash lib) produces the same BIGINT as Spark's `F.xxhash64`. A ride ingested
  real-time has the same trip_id as when it flows through the batch silver layer.
- **No Spark in serving** — historic queries use Polars `scan_parquet()` (lazy,
  predicate pushdown); real-time uses pure Python + Redis. The serving container
  is ~500MB vs ~2GB for Spark.
- **Merge boundary** — batch gold has complete past blocks; Redis has the current
  incomplete block. `MergedViewReader` stitches them: deduplication by key tuple
  (batch wins on overlap). Old Redis keys expire after 48h (TTL) — the canonical
  data is always in silver/star facts (trip_id parity guarantees the batch
  pipeline picks it up).

### Run

```bash
uv run main.py --serve                          # start serving layer
uv run main.py --speed                          # speed engine only (stdin JSON)
docker compose up serving redis -d              # Docker
```

## `app.` package (flat, no `src/`)

| Module | Class | Role |
|---|---|---|
| `app.client.download_client` | `DownloadClient` | Async HTTP downloads (Polars), audit trail, skips valid existing parquet |
| `app.pipeline.bronze` | `BronzePipeline` | Download loop: years × categories × months |
| `app.profiling.profiling_pipeline` | `ProfilingPipeline` | 8-dimension quality profiling (PySpark), 3 files in parallel |
| `app.profiling.dataset_profiler` | `DatasetProfiler` | Runs one check per dimension class in `app/profiling/dimensions/` per file (df persisted once, unpersisted after) |
| `app.profiling.dimensions` | `BaseDimension` + 8 subcls | accuracy, completeness, consistency, integrity, reasonableness, timeliness, uniqueness, validity |
| `app.profiling.reporter` | `Reporter` | Writes JSON + `index.html` to `data/profiling/` |
| `app.pipeline.silver` (executor) | `SilverPipeline` (entry), `SilverCleaner` | Quality clean (reject + fix), 2 files in parallel + audit lock. Executor: `app/pipeline/silver.py`; impl: `app/pipeline/silver_impl/` |
| `app.pipeline.star` | `StarSchemaBuilder` | Star dims + facts (3 facts in parallel; facts carry `trip_id` + std timestamps) |
| `app.pipeline.gold` (executor) | `GoldPipeline` (entry) | Gold orchestrator: 6 marts + 3 ML feature stores. Executor: `app/pipeline/gold.py`; impl: `app/pipeline/gold_impl/` |
| `app.pipeline.gold_impl.mart_builder` | `GoldBuilder`, `TripGrainMart`, `GoldContext` | Builder bases + shared context (`get_union_facts()` lazy union) |
| `app.pipeline.gold_impl.dims.gold_dimensions` | `GoldDimensionsBuilder` | `dim_date_gold`, `dim_zone_gold`, `dim_ratecode_theoretical` |
| `app.pipeline.gold_impl.ml.isolation_forest_model` | `IsolationForestModelPipeline` | Trains sklearn IsolationForest per RatecodeID, writes scores + `model.joblib` |
| `app.pipeline.gold_impl.ml.kmodes_model` | `KModesModelPipeline` | Trains KModes per service, elbow+silhouette tuning, writes labels + centroids + profiles |
| `app.pipeline.gold_impl.ml.sarimax_model` | `SariMaxModelPipeline` | Trains SARIMAX trip-count forecaster per borough × service_id |
| `app.utils.settings` | `Settings` | Loads `config.yaml` → `SettingsSchema` (pydantic) |
| `app.utils.spark` | `SparkClient`, `target_files()` | PySpark session (`local[6]`, 6g, AQE) + coalesce sizing helper |
| `app.utils.storage` | `get_root()`, `get_backend()`, `for_spark()`, `S3Path` | Storage abstraction: local `Path` or S3 (`s3fs`), toggled by `STORAGE_BACKEND` |
| `app.utils.logger` | `Logger` | Singleton, file+console |
| `app.utils.globals` | `Globals` (instance: `globals`) | `tlc_categories`: green, yellow, fhv, fhvhv |

## Key behaviors

- **Logger** — Singleton. File: `logs/YYYY-MM-DD/HH-MM-SS.log` (DEBUG+). Console: INFO+.
  Messages in **Spanish**, code in **English**.
- **DownloadClient** — `httpx.AsyncClient` 300s timeout. Must close via `async with` or
  `await client.close()`. Skips existing files whose parquet footer reads OK (truncated files
  re-download). HTTP errors → `ERROR` (returns silently). Corruption → `CRITICAL` (re-raises).
  Outputs `data/bronze/{category}/{year}-{month:02d}.parquet`, audit at `data/bronze/audit.parquet`.
- **BronzePipeline** — downloads zone-lookup first, then iterates years × categories × months 1–12.
- **ProfilingPipeline** — reads `data/bronze/`, writes per-dataset JSON to
  `data/profiling/{category}/{year}-{month:02d}.json` + summary `index.html`. Uses
  **dynamic concurrency** (1 worker for heavy files like fhvhv/yellow, 3 workers for green/fhv); each df is persisted once so the 8 dimensions
  don't re-read the parquet. Do **not** call `catalog.clearCache()` between profiles (global —
  would evict concurrent profiles). Existing JSONs are reloaded, not recomputed.
- **SilverPipeline / SilverCleaner** — reject-only filter (no fix/imputation phase). Discards rows that
  are **incomplete** (null in a required column) or **factually incorrect** (pickup date outside the file's
  month, dropoff before pickup, zone ID not in lookup, exact duplicate). First failing reject rule wins
  (`& ~already`). Required vs. nullable columns are defined in `NULLABLE_COLUMNS` (`nullability.py`):
  columns NOT in that set are required — a null triggers rejection. The set is configurable per-category
  via `config.yaml → profiling.rules.nullability`. Source values pass through unchanged (no clamping, no
  `total_amount` recomputation, no default imputation). A light `_normalize_types` casts code columns
  (`VendorID`, `RatecodeID`, `payment_type`, `passenger_count`) to int for schema consistency.
  Uses **dynamic concurrency** (1 worker for fhvhv/yellow to avoid OOM, 2 for green/fhv), one
  `SilverCleaner` per worker, audit writes behind `_audit_lock`. Skips months already in `stage/`;
  collected failures re-raise at the end (fail loud). `--silver quality` reads `data/bronze/`;
  `--silver load` reads `data/silver/stage/`. Audit at `data/silver/audit.parquet` (FK `bronze_audit_id`).
- **StarSchemaBuilder** — builds fixed lookup dims + `dim_date` (2023–2025) + `dim_zone`, and per-category
  facts (uses **dynamic concurrency**: 1 worker for heavy files, 3 for light; skips existing monthly facts, fail-loud on collected errors). Every
  fact carries a `trip_id` (**`xxhash64` BIGINT** of `SilverCleaner.COMPOSITE_KEYS` — compact drill-through
  key; treat as long downstream) and standardized `pickup_datetime`/`dropoff_datetime` timestamps (the gold
  layer depends on this).
- **GoldPipeline** — reads silver star facts/dims, builds gold dims, then 6 Power BI marts
  (`data/gold/marts/`) + 3 ML feature stores + ML models (`data/gold/ml/`, `data/gold/models/`).
  The marts use an **aggregated grain** (1 row per fecha × hora/bloque × zona/borough — NOT per trip:
  ~940M rows was unviable for Power BI; trip detail stays in silver/star/facts; fare components stored as
  re-aggregable SUMs). Feature stores stay trip-grain; `ml_feat_kmodes_trips` samples **fhvhv at 5%**
  (seed 42). Trip-grain builders subclass `TripGrainMart` (idempotent per-partition writes via
  `partitionOverwriteMode=dynamic`); aggregate builders (supply/demand, ABC/XYZ, ARIMA) subclass
  `GoldBuilder`, recompute the whole history, and share `GoldContext.get_union_facts()` — a **lazy**
  fixed-projection union of all facts (`service_id, _file_year/_file_month, timestamps, pu/do, revenue`).
  **Never persist that union**: even `DISK_ONLY` OOM'd at ~940M rows; lazy narrow scans stream with bounded
  memory. Builder failures collect and re-raise after `release_union_cache()`. Audit at
  `data/gold/audit.parquet` (FK `silver_audit_id`).
- **Schema heterogeneity** — column names differ across categories/years (`tpep_pickup_datetime` vs
  `lpep_pickup_datetime` vs `pickup_datetime`; `PULocationID` vs `PUlocationID`). Code resolves these via
  candidate-list + `_first_match` helper. Follow this pattern; never hardcode a single column name across
  categories. Columns can also be year-gated: `cbd_congestion_fee` (MTA congestion pricing, part of
  `total_amount`) exists only from **2025+**; `_safe_select` and `if col in df.columns` guards keep it
  optional for pre-2025 files.
- **Parquet storage** — all writes use codec **`zstd` level 9** (in `SparkClient`; level 19 saved only
  ~2.5% disk at ~6x slower writes). Writes coalesce via `target_files()` (~2M rows/file, cap 32) — feed it
  a row count that's already needed for audit/log, never an extra `count()`. `trip_id` is a hashed
  **BIGINT** (`xxhash64`), not a hex-string digest, to keep facts/marts small.
- **Tooling split** — `download_client.py` + all audit writes use **Polars**; profiling, silver, star and
  gold use **PySpark**; the `--gold-ml` model pipelines use **pandas + scikit-learn/kmodes/statsmodels**.
  `pyarrow` for Parquet metadata.
- **Storage backend** — every layer (`bronze/silver/gold/profiling` + the 3 `audit.parquet`) lives on the
  local filesystem (default) or S3, toggled by **`STORAGE_BACKEND`** (`.env`) — no per-layer mixing, no code
  duplicated. `app.utils.storage.get_root()` returns a local `Path` or an `S3Path` (`s3fs`-backed, reads
  `AWS_*` from the environment, never `config.yaml`); `globals.project_root` routes through it so existing
  path composition works unchanged. `storage.for_spark(path)` rewrites `s3://` → `s3a://` for Spark
  (no-op locally); every `spark.read/write.parquet` call site wraps its path with it. Install S3 support
  with `uv sync --extra s3` (adds `s3fs`; Spark's S3 access comes from `spark.jars.packages` hadoop-aws +
  aws-sdk-bundle, added by `SparkClient` only when `STORAGE_BACKEND=s3`). **`spark.local.dir` (shuffle/spill)
  always stays on local disk**, even with S3 backend.
- **Audit chain** — `bronze_audit_id → silver_audit_id → gold_audit_id`; each layer's audit row FKs the
  previous. Polars writes all three `audit.parquet` files.
- **Reusable heuristics** — profiling rules in `app/profiling/rules/`
  (`nullability.py`, `reasonableness_ranges.py`, `amount_components.py`). `nullability.py` is shared
  between profiling and the silver cleaner (defines which columns are nullable; the complement is
  required). `reasonableness_ranges.py` and `amount_components.py` are consumed only by the profiling
  dimensions — the silver cleaner no longer uses them. Gold heuristics live in
  `app/pipeline/gold/feature_rules/` (`time_blocks.py`, `generosity.py`, `ratecode_tariff.py`,
  `passenger_groups.py`) — same rule: define there, don't inline in a mart.
- **Spark day-of-week quirk** — use `time_blocks.iso_weekday()` for day-of-week, **not**
  `date_format(ts, "u")` (`'u'` is not day-of-week in Spark's proleptic datetime patterns).
- **SparkClient** — `master=local[6]` (`local[*]` OOMs the shared 6g heap; 4 left CPU idle — don't raise
  past 6 without more heap), `spark.driver.memory=6g`, `spark.sql.shuffle.partitions=128` + **AQE enabled**
  (coalesce partitions, skew joins, 64m advisory), `spark.local.dir=data/.spark_temp` (avoids small `/tmp`
  quota during shuffle). The gold layer also sets `spark.sql.sources.partitionOverwriteMode=dynamic` for
  idempotent per-partition writes. On Windows, requires `HADOOP_HOME` pointing to a Hadoop bin dir with
  `hadoop.dll`/`winutils` (not bundled in the repo — `lib/` is gitignored).

## Config

`config.yaml` — `datasets.years` is a list of plain `int` years (expands to 4 categories × 12 months;
current default `[2023, 2024, 2025]`) or `Module` objects (`{category, year, month}`) for a single
category/year. Optional `gold:` section (`GoldConfig`) parametrizes the gold layer (block minutes, deficit
threshold, ABC/XYZ cutoffs, generosity thresholds, isolation-fraud hyperparams, kmodes params); defaults
apply if omitted.

## Docker / Airflow

The pipeline can run orchestrated via **Airflow on Docker Compose** (`docker-compose.yml` + `Dockerfile`),
while `uv run main.py ...` keeps working exactly as before for anyone not using Docker/S3.

- **`Dockerfile`** — extends `apache/airflow:2.10.5-python3.12` with JDK 17 (PySpark 4.x) and a standalone
  `uv`. Project deps install into their own venv (`uv sync --frozen --extra s3 --extra jupyter`), independent
  of Airflow's env; DAG tasks shell out `cd /opt/airflow/project && uv run main.py ...`.
- **`docker-compose.yml`** — Postgres + `airflow-webserver` + `airflow-scheduler` on **`LocalExecutor`**
  (no Celery/Redis) + optional **`jupyter`** service. Spark runs **embedded** in each task's process (no
  separate Spark container). Sets `SPARK_DRIVER_MEMORY`/`SPARK_MASTER_CORES` (lower than bare-metal default,
  VM shares RAM with Airflow+Postgres) and `SPARK_LOCAL_DIR=/tmp/spark-temp` (container disk, not the slow
  `./data` bind mount).
- **`dags/dag_01_bronze.py` … `dag_07_gold_ml.py`** — each phase is its **own DAG**, chained via
  `TriggerDagRunOperator(wait_for_completion=True)`: `dag_01_bronze → dag_02_silver_quality →
  dag_03_silver_schema → dag_04_silver_load → dag_05_gold → dag_06_profiling` (profiling last on purpose —
  read-only). `dag_07_gold_ml` (kmodes/isolation/sarimax, chained sequentially) is triggered manually from
  the UI. All tasks are thin `BashOperator`s over the existing CLI.
- **`.env`** (from `.env.example`, gitignored) — `STORAGE_BACKEND`/AWS creds/`S3_BUCKET`/`S3_PREFIX` +
  `SPARK_DRIVER_MEMORY`/`SPARK_MASTER_CORES` + Airflow bootstrap vars. Bring up: `uv lock` once (commit
  `uv.lock` — `uv sync --frozen` fails on a stale lock), `docker compose build`, `docker compose up
  airflow-init` once, then `docker compose up`. **Unpause all 7 DAGs** before triggering `dag_01_bronze`
  (the trigger chain sits queued forever behind a paused downstream DAG).
- **`.dockerignore`** keeps `data/` (huge), `.venv/` (Windows binaries would clobber the Linux venv) and
  `.env` (AWS secrets) out of the image — don't remove entries. `HADOOP_HOME`/`winutils` is only for
  bare-Windows runs; inside the Linux container that branch is inert.

## Stack

Python 3.12, managed with **uv**. **Java JDK 11+** required by PySpark (JDK 17 in the Docker image). Dependencies: `httpx`, `kmodes`,
`pandas`, `polars`, `pyarrow`, `pydantic`, `pyspark`, `pyyaml`, `scikit-learn`, `statsmodels`, `joblib`,
`ipykernel`. Optional extras: `s3` (`s3fs`, for `STORAGE_BACKEND=s3`) and `jupyter` (`jupyterlab`, for the
docker-compose `jupyter` service) — `uv sync --extra s3 --extra jupyter`.

## Tests

`pytest` (dev dependency). Run with:

```bash
PYTHONPATH=. uv run pytest                         # all tests
PYTHONPATH=. uv run pytest -m "not integration"    # fast: unit + spark (skip e2e)
PYTHONPATH=. uv run pytest -m integration          # slow: full pipeline e2e
PYTHONPATH=. uv run pytest tests/unit/             # fast pure-Python rules & schema
PYTHONPATH=. uv run pytest tests/spark/            # silver, gold, ML model tests
PYTHONPATH=. uv run pytest -k "isolation"          # subset by keyword
```

**Prerequisites**: `data/bronze/{yellow,green,fhv,fhvhv}/2025-01.parquet` must exist on
disk (the `conftest.py` session fixture samples 50 rows from each; `SAMPLE_YEAR = "2025"`). Run
`uv run main.py` once to download or use existing data. The fixture pins
`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to `sys.executable` (required on Windows).

| File | Tests | Markers |
|---|---|---|
| `tests/unit/test_rules_*.py` | Profiling-rule dictionaries (nullability, reasonableness, amount components) | — |
| `tests/unit/test_feature_rules.py` | Gold feature-rule Column functions (time blocks, generosity, ratecode tariff, passenger groups) | `spark` |
| `tests/unit/test_settings_schema.py` | Pydantic schema validation (Module, DatasetsConfig, GoldConfig defaults) | — |
| `tests/spark/test_silver_cleaner.py` | SilverCleaner reject phases (incomplete, timeliness, datetime, integrity, uniqueness), source-value preservation, _normalize_types, _first_match | `spark` |
| `tests/spark/test_star_schema.py` | StarSchemaBuilder dims/facts, trip_id (xxhash64 long), ISO weekday, heterogeneous schemas | `spark` |
| `tests/spark/test_gold_dimensions.py` | GoldDimensionsBuilder (dia_categoria, is_holiday, borough_name_es, ratecode theoretical) | `spark` |
| `tests/spark/test_gold_marts.py` | GoldBuilder infrastructure (col_or_null, with_zone, partitioning, GoldContext, flat_fare_rows) | `spark` |
| `tests/spark/test_ml_models.py` | IsolationForest, KModes, SariMax training on synthetic feature stores | `spark` |
| `tests/integration/test_pipeline_e2e.py` | Full bronze → profiling → silver → star → gold chain + audit FK integrity | `integration`, `spark` |

## Documentation

- `docs` provide config reference and guides in Markdown format (ocassionaly with HTML visualizations)
- `notebooks/revision_01..05` review each layer: bronze inventory, profiling results, silver integrity
(`bronze = stage + reject`), gold grains + no-loss proof (`SUM(viajes)` == fact rows), audit chain.
Keep them consistent when changing schemas or grains.

## Conventions

- Log/user-facing messages in **Spanish**; code, identifiers, comments in **English**.
- JFK flat fare in `dim_ratecode_theoretical` (fraude heuristic) should be verified against current TLC rules.
