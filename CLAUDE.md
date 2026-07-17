# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ETL + data-quality pipeline for NY TLC Trip Record Data, organized as a **medallion architecture**
(bronze → silver → gold) with a standalone profiling stage. Downloads parquet from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`, profiles quality across 8 dimensions, cleans
data into a star schema, builds gold marts (Power BI) + ML feature stores, and trains the ML
models (K-Modes, Isolation Forest, SARIMAX). A **Lambda-architecture serving layer**
(`app/serving/` + `app/speed/`) exposes the marts over HTTP/SSE and scores individual rides in
real time against Redis.

The four TLC categories (`app/utils/globals.py`): **green, yellow, fhv, fhvhv**.

## Commands

```bash
uv run main.py --all           # FULL pipeline: bronze → bronze-completeness check (fail-loud, 1 retry for
                               # CloudFront 403 bursts) → silver → schema → load → gold incremental → profiling.
                               # Idempotent: re-running resumes where it left off. Use this for replication.
uv run main.py                 # bronze: download zone-lookup + all trip data (years × categories × months)
uv run main.py --profile       # profiling: evaluate quality of bronze data → data/profiling/
uv run main.py --silver        # silver quality (default): clean bronze → data/silver/stage + reject
uv run main.py --silver schema # build star-schema dimension tables → data/silver/star/dims/
uv run main.py --silver load   # build star-schema fact tables → data/silver/star/facts/
uv run main.py --gold          # gold (default full): Power BI marts + ML feature stores → data/gold/
uv run main.py --gold incremental                                       # only build missing trip-grain partitions
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud # subset of builders
uv run main.py --gold-ml kmodes     # train K-Modes (needs ml_feat_kmodes_trips)
uv run main.py --gold-ml isolation  # train Isolation Forest per RatecodeID (needs ml_feat_isolation_fraud)
uv run main.py --gold-ml sarimax    # train SARIMAX forecaster (needs ml_feat_arima_trips)
uv run main.py --serve              # serving layer: FastAPI + SSE over gold marts (needs Redis + data/gold/)
uv run main.py --speed              # speed engine only, reads JSON ride events from stdin
```

Standalone scripts (not pipeline stages — they read finished outputs):

```bash
uv run scripts/export_powerbi.py                 # gold (Hive-partitioned) → one parquet per dataset in data/powerbi/
uv run scripts/export_powerbi.py --skip-heavy    # omit the two trip-grain ML feature stores (>100M rows each)
uv run scripts/export_powerbi.py --compression zstd  # default is snappy (see below)
uv run scripts/export_audit_powerbi.py           # audit trail → data/powerbi_audit/ (5 tables, dashboard-ready)
```

Pipeline order matters: `--silver schema` must run before `--silver load` (facts join against dims).
`--silver` (quality) reads from `data/bronze/`; `load` reads from `data/silver/stage/`. `--gold` reads from
`data/silver/star/` (facts + dims), so the full chain `bronze → --silver → --silver schema → --silver load →
--gold → --gold-ml` must have run; `--gold` aborts with a clear message if silver/star is missing.

**Idempotency / resumability** — every stage skips outputs that already exist: bronze skips files whose
parquet footer is readable (truncated downloads re-download), profiling reuses existing per-dataset JSONs,
silver quality skips months already in `stage/`, star skips existing monthly facts, gold `incremental` skips
existing trip-grain partitions. To force a re-run, delete the corresponding output (e.g. the month's directory
under `data/silver/stage/` or the JSON under `data/profiling/`). Spark writes commit via atomic rename, so an
existing directory is a complete output.

### Tests

`pytest` (dev dependency; markers `spark` and `integration` are strict):

```bash
PYTHONPATH=. uv run pytest                         # all tests
PYTHONPATH=. uv run pytest -m "not integration"    # fast: unit + spark (skip e2e)
PYTHONPATH=. uv run pytest tests/unit/             # pure-Python rules & schema
```

Prerequisite: `data/bronze/{yellow,green,fhv,fhvhv}/2025-01.parquet` must exist (the `conftest.py`
session fixture samples 50 rows from each — run `uv run main.py` once). The fixture pins
`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to `sys.executable` so Spark workers start on Windows.
No linter, formatter, or CI is configured.

## Stack & tooling split

Python 3.12, managed with **uv** (`uv.lock`). Key split to keep in mind:

- **Polars** — used by `DownloadClient` and all audit-trail writes (`data/*/audit.parquet`).
- **PySpark** — used by everything in `app/profiling/`, `app/pipeline/silver.py` + `silver_impl/`, and the
  whole `app/pipeline/gold.py` + `gold_impl/` package. **Not** used by the serving layer (see below).
- **pyarrow** — parquet metadata (row counts, footer validation) in the download client.
- **scikit-learn / kmodes / statsmodels / joblib** — the `--gold-ml` model pipelines (pandas-side, not Spark).
- **FastAPI / uvicorn / redis / xxhash / sse-starlette** — the serving + speed layers (`--extra serving`).

## Storage backend (local filesystem or S3)

Every layer (`bronze/silver/gold/profiling` + the 3 `audit.parquet`) can live on the local
filesystem (default) or on S3, toggled by **`STORAGE_BACKEND`** (`.env`, see `.env.example`) — no
per-layer mixing, no code duplicated between backends. The abstraction is intentionally thin:

- **`app/utils/storage.py`** — `get_root()` returns either the local project `Path` or an
  `S3Path` (a minimal Path-like wrapper: `/`, `str()`, `.exists()`, `.mkdir()` (no-op — S3 has no
  real directories), `.stat()`, `.glob()`, `.open()`/`.write_text()`/`.read_text()`, all via `s3fs`
  reading `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` from the environment — **never**
  from `config.yaml`). `app/utils/globals.py`'s `project_root` property routes through this, so the
  existing `globals.project_root / "data/silver/stage" / category`-style composition used
  throughout `silver_impl/`/`star.py`/`gold_impl/mart_builder.py`/`gold_impl/pipeline.py` keeps working unchanged for
  both backends — only the root changes, not the call sites.
- **`storage.for_spark(path)`** — Spark/hadoop-aws needs the `s3a://` scheme; Polars/pyarrow/pandas/
  s3fs need `s3://`. Every `spark.read.parquet(...)`/`...write...parquet(...)` call site wraps its
  path with this helper; Polars/pandas parquet calls pass the path through `str()` unchanged (already
  the right scheme). Local backend: `for_spark()` is a no-op (`str(path)`), so behavior is byte-identical
  to before this abstraction existed.
- **`storage.parquet_footer_readable()` / `.parquet_file()` / `.open_writable()`** — used by
  `DownloadClient` for idempotency (footer-readable check) and chunked streaming writes without
  buffering the whole file in memory, on either backend.
- Installing S3 support: `uv sync --extra s3` (adds `s3fs`; Spark's S3 access instead comes from
  `spark.jars.packages` — `hadoop-aws` + `aws-java-sdk-bundle`, added by `SparkClient` only when
  `STORAGE_BACKEND=s3` — verify the exact `hadoop-aws` version against the Hadoop bundled by your
  PySpark version before relying on it in production).
- **`spark.local.dir` (shuffle/spill) always stays on local disk**, even with `STORAGE_BACKEND=s3`:
  shuffle/spill through S3 would add network latency on top of an already-tight 6g heap — not
  something to change without deliberately re-testing memory behavior.

## Architecture

### Stages (`main.py` dispatches each)

1. **Bronze** (`app/pipeline/bronze.py`, `app/client/download_client.py`) — async HTTP downloads via
   `httpx.AsyncClient` (300s timeout). Downloads zone-lookup CSV first, then loops years × categories ×
   months 1–12. Outputs `data/bronze/{category}/{year}-{month:02d}.parquet`; audit at `data/bronze/audit.parquet`.
   Existing files with a valid parquet footer are skipped (idempotent). HTTP errors → `logger.error` + silent
   return; file corruption → `logger.critical` + re-raise. `DownloadClient` must be closed via `async with`
   or `await client.close()`.

2. **Profiling** (`app/profiling/`) — read-only quality assessment. `ProfilingPipeline` → `DatasetProfiler`
   runs one check per dimension class in `app/profiling/dimensions/` (accuracy, completeness, consistency,
   integrity, reasonableness, timeliness, uniqueness, validity). **Dynamic concurrency**: heavy categories
   (`fhvhv`, `yellow`) are profiled **sequentially** (1 worker), light ones (`green`, `fhv`) **3 in parallel**
   (`MAX_PARALLEL_PROFILES`); each file is persisted once (`MEMORY_AND_DISK`) so the 8 dimensions don't
   re-read the parquet, and unpersisted afterwards — do **not** call `catalog.clearCache()` between profiles
   (it is global and would evict concurrent profiles). Existing report JSONs are reused (idempotent).
   `Reporter` writes per-dataset JSON to `data/profiling/{category}/{year}-{month:02d}.json` plus a summary
   `index.html` (reports sorted for stable output).

3. **Silver quality** (`SilverPipeline` in `app/pipeline/silver.py`, `SilverCleaner` in
   `app/pipeline/silver_impl/cleaner.py`) — **reject-only**. There is no fix/imputation phase: source
   values pass through unchanged (no clamping, no `total_amount` recomputation, no default imputation).
   - **Reject rules** add a `_reject_reason` column: incomplete (null in a required column), timeliness
     (pickup outside the file's month), inverted/over-24h datetimes, integrity vs. zone IDs, uniqueness
     duplicates. First failing rule wins (`& ~already`). Helper columns `_pickup_dt`/`_dropoff_dt` are
     dropped **before** the persist (2 extra timestamp columns ≈ 320MB cached on an fhvhv month).
   - **Required vs. nullable** is defined by `NULLABLE_COLUMNS` (`app/profiling/rules/nullability.py`):
     a column NOT in that set is required, and a null there triggers rejection. Configurable per category
     via `config.yaml → profiling.rules.nullability`.
   - A light `_normalize_types` casts code columns (`VendorID`, `RatecodeID`, `payment_type`,
     `passenger_count`) to int for schema consistency — that is the only value-touching step.
   - **Dynamic concurrency**: heavy categories (`fhvhv`, `yellow`) run with **1 worker** (concurrent
     writes OOM'd the 6g heap), light ones (`green`, `fhv`) with `MAX_PARALLEL_FILES = 2`. One
     `SilverCleaner` per worker (its persist cache is per-instance); audit writes serialized behind
     `_audit_lock` (read-concat-write on one parquet). Months already in `stage/` are skipped; failures
     are collected and re-raised at the end (fail loud — a missing month would be silent data loss).
   - Clean rows → `data/silver/stage/{category}/`; rejected rows → `data/silver/reject/{category}/`;
     audit → `data/silver/audit.parquet` (links to the latest `bronze_audit_id`).

4. **Silver schema + load** (`StarSchemaBuilder` in `app/pipeline/silver_impl/star.py`) — star schema. `build_dimensions`
   writes fixed lookup dims (vendor, ratecode, payment_type, service), a generated `dim_date` (2023–2025),
   and `dim_zone` from zone-lookup → `data/silver/star/dims/`. `build_facts` builds **3 facts in parallel**,
   skips months whose fact already exists, and collects+re-raises failures. Per category it dispatches to
   `_FactBuilder._build_{category}` → `data/silver/star/facts/fact_{category}_trip/`. Every fact carries a
   `trip_id` (**`xxhash64` BIGINT** of the silver composite PK, from `SilverCleaner.COMPOSITE_KEYS` — a
   compact drill-through key; strict uniqueness is enforced upstream by silver's uniqueness reject) and
   standardized `pickup_datetime`/`dropoff_datetime` timestamps so the gold layer can do hour-level
   analysis — gold depends on this enrichment. Treat `trip_id` as long/bigint downstream (e.g.
   `isolation_forest_model.OUTPUT_SCHEMA` uses `LongType`).

5. **Gold** (`app/pipeline/gold/`, `GoldPipeline`) — reads silver star facts/dims, builds enriched gold dims
   (`GoldDimensionsBuilder`: `dim_date_gold`, `dim_zone_gold`, `dim_ratecode_theoretical`), then runs each
   builder. The 6 Power BI **marts** use an **aggregated grain** (1 row per fecha × hora/bloque × zona/borough
   — NOT one row per trip: at full scale that rewrote ~940M rows for dashboards that only consume counts and
   averages; trip detail stays in silver/star/facts). Fare components are stored as SUMs (re-aggregable);
   ratios are derived from the sums. **3 ML feature stores** (`ml/`) stay trip-grain; `ml_feat_kmodes_trips`
   samples **fhvhv at 5%** (seed 42 — K-Modes trains on ≤100k rows/service, emitting 240M rows was waste).
   Trip-grain builders subclass `TripGrainMart` (iterate facts, one `service_id/year/month` partition per
   pass, idempotent dynamic overwrite); aggregate builders (supply/demand, ABC/XYZ, ARIMA) subclass
   `GoldBuilder`, span the whole history, and share `GoldContext.get_union_facts()` — a **lazy** union with a
   fixed superset projection (`service_id, _file_year, _file_month, timestamps, pu/do location, revenue`).
   Do **not** persist that union: even `DISK_ONLY` OOM'd the 6g heap at ~940M rows; the lazy narrow scan
   streams with bounded memory. Builder failures are collected and re-raised after `release_union_cache()`.
   Outputs under `data/gold/{marts,ml,dims}/`; audit → `data/gold/audit.parquet` (links the latest
   `silver_audit_id`). `--gold incremental` skips existing trip-grain partitions; aggregate marts always
   recompute. `--only name1,name2` restricts which builders run.

6. **Gold ML** (`--gold-ml`, `app/pipeline/gold/ml/*_model.py`) — trains models from the feature stores
   (Spark → pandas): `KModesModelPipeline` (per-service K-Modes, elbow+silhouette tuning),
   `IsolationForestModelPipeline` (per-RatecodeID sklearn IsolationForest → fraud scores),
   `SariMaxModelPipeline` (per borough × service SARIMAX trip-count forecast). Outputs: labels/scores/forecasts
   under `data/gold/ml/`, serialized models under `data/gold/models/`.

### Serving + speed (Lambda architecture)

7. **Serving** (`app/serving/`, `--serve`) — FastAPI. `routes/historic.py` serves the 6 marts over
   `GET /api/v1/historic/*` using **Polars `scan_parquet()`** (lazy, predicate pushdown);
   `routes/realtime.py` serves `GET /api/v1/realtime/*` plus `/stream` SSE variants;
   `routes/admin.py` has `POST /api/v1/admin/reload-models`. **No Spark in serving** — the container is
   ~500MB instead of ~2GB.

8. **Speed** (`app/speed/`, `--speed`) — `POST /api/v1/ingest` → clean → enrich → aggregate
   (Redis `HINCRBY`) → fraud score (IsolationForest loaded from joblib). Redis holds speed-layer state
   (key prefix `rt:`, TTL 48h, ~50–100MB) and does the uniqueness check via `SETNX trip_id`.

- **`trip_id` parity — INTENDED BUT NOT HOLDING (known issue).** The design goal is that the speed layer's
  `trip_id` matches Spark's, but it does not: `event_processor.py:_compute_trip_id` uses
  `xxhash.xxh64(..., seed=0).intdigest()` (unsigned) while Spark's `F.xxhash64` uses **seed 42** and returns
  a **signed** BIGINT. Verified: for `"a||b"` Spark yields `422548665953921601`, the speed layer
  `1685461149381629335`. The fix is `seed=42` plus a signed conversion (`u - 2**64 if u >= 2**63 else u`),
  deliberately **not applied** — the owner decided it isn't worth the behavior change. `tests/integration/
  test_serving_e2e.py` cannot catch this: it hashes with `xxhash` on both sides, comparing Python to
  Python, never to Spark. Consequence: a real-time ride cannot be correlated with its batch fact row
  (e.g. joining `rt:fraud:{trip_id}` against silver/star facts). The merge is unaffected — see below.
- **Merge boundary** (`app/serving/merged_view.py`) — batch gold has the complete past blocks, Redis has
  the current incomplete one. `MergedViewReader` stitches them, deduplicating by **aggregation key tuple**
  (`dv_key_from_ride`, `fp_key_from_ride`, … — e.g. fecha × hora × zona), **not** by `trip_id`; **batch
  wins** on overlap. This is why the parity issue above doesn't cause double counting. Redis keys expire
  after 48h; the canonical data always lands in silver/star facts.

### Cross-cutting

- **Schema heterogeneity** — column names differ across categories/years (e.g.
  `tpep_pickup_datetime` vs `lpep_pickup_datetime` vs `pickup_datetime`; `PULocationID` vs `PUlocationID`).
  Code resolves these via candidate lists + a `_first_match` helper. Follow this pattern; never hardcode a
  single column name across categories. Columns can also appear/disappear **by year**: `cbd_congestion_fee`
  (MTA congestion pricing) exists only from **2025** onward for yellow/green/fhvhv and is part of
  `total_amount`. `_safe_select` (star) and the `available`/`if col in df.columns` guards (silver rules)
  tolerate its absence in pre-2025 files — keep new fare columns optional the same way.

- **Parquet storage** — all writes use the **`zstd`** codec at **level 9** (set in `SparkClient`; level 19
  only saved ~2.5% disk but wrote ~6x slower — measured on a real yellow month). Writes are coalesced via
  `target_files()` (`app/utils/spark.py`, ~2M rows/file, cap 32) to avoid dozens of tiny post-shuffle files;
  the row count fed to it always comes from a count already needed for audit/log — don't add extra `count()`s.
  Keep surrogate keys compact: `trip_id` is a hashed BIGINT, not a hex-string digest (a 64-char hash dominated
  fact size).

- **Shared rule modules** (`app/profiling/rules/`) — `nullability.py` is the **single source of truth**
  consumed by *both* the profiling dimensions and the silver cleaner (it defines which columns are
  nullable; the complement is required). `reasonableness_ranges.py` and `amount_components.py` are
  consumed **only** by the profiling dimensions — the silver cleaner no longer uses them (it stopped
  clamping and recomputing when it became reject-only). The gold layer has its own reusable heuristics in
  `app/pipeline/gold_impl/feature_rules/` (`time_blocks.py`, `generosity.py`, `ratecode_tariff.py`,
  `passenger_groups.py`) — same rule: define a gold heuristic there, not inline in a mart. Use
  `time_blocks.iso_weekday()` for day-of-week, **not** `date_format(ts, "u")` (`'u'` is not day-of-week in
  Spark's proleptic datetime patterns).

- **Config** (`config.yaml` → `app/schemas/settings_schema.py`) — `datasets.years` is a list of either
  plain `int` years (expands to all 4 categories × 12 months; current default `[2023, 2024, 2025]`) or
  `Module` objects (`{category, year, month}`) for targeting a single category/year. All pipeline loops handle
  both forms via a `_expand_tasks`-style expansion. An optional `gold:` section (`GoldConfig`) parametrizes
  the gold layer (supply/demand block minutes & deficit threshold, ABC/XYZ cutoffs, generosity thresholds,
  isolation-fraud & kmodes hyperparams); it has defaults, so it may be omitted.

- **Logger** (`app/utils/logger.py`) — singleton, file (`logs/YYYY-MM-DD/HH-MM-SS.log`, DEBUG+) + console
  (INFO+). **Log/user messages are in Spanish; code, identifiers, and comments are in English.** Match this.

- **Spark** (`app/utils/spark.py`) — `SparkClient` runs `local[6]` (12 logical cores on this machine;
  `local[*]` OOM'd the shared 6g heap, 4 left CPU idle — don't raise past 6 without raising the heap),
  driver memory 6g, **shuffle partitions 128 + AQE enabled** (coalesce/skew-join, ~64MB advisory), and
  `spark.local.dir` to `data/.spark_temp` (avoids small `/tmp` quota during shuffle). The gold layer also
  sets `spark.sql.sources.partitionOverwriteMode=dynamic` for idempotent per-partition writes. On **Windows**,
  requires `HADOOP_HOME` pointing to a Hadoop bin dir containing `hadoop.dll`/`winutils` (not bundled in the
  repo); native lib path is passed via `extraLibraryPath` (not `java.library.path`, which strips Windows
  backslashes). This Windows-only branch is a no-op inside the Linux Docker image (see below) — `HADOOP_HOME`
  is simply unset there. When `STORAGE_BACKEND=s3`, `SparkClient` additionally adds `spark.jars.packages`
  (hadoop-aws + aws-sdk-bundle) and s3a credentials/region config — see "Storage backend" above.

## Docker / Airflow execution mode

Running the pipeline no longer requires a bare local `uv run` — it can also run orchestrated via
**Airflow on Docker Compose** (`docker-compose.yml` + `Dockerfile`), while `uv run main.py ...`
keeps working exactly as before for anyone not using Docker/S3 (`STORAGE_BACKEND=local` is the
default with no `.env` present).

- **`Dockerfile`** — extends `apache/airflow:2.10.5-python3.12` with JDK 17 (required by PySpark
  4.x) and a standalone `uv` binary. Project dependencies install into their **own venv**
  (`uv sync --frozen --extra s3 --extra jupyter`) inside the image, independent of Airflow's own
  Python environment — DAG tasks just shell out `cd /opt/airflow/project && uv run main.py ...`, so
  nothing about the CLI itself changes.
- **`docker-compose.yml`** — Postgres (metadata DB) + `airflow-webserver` + `airflow-scheduler` on
  **`LocalExecutor`** (single worker, no Celery — deliberately simple for this project's scale) + an
  optional **`jupyter`** service (same image, `jupyter lab`) for running `notebooks/revision_01..05*`
  as living documentation against the same data (local or S3) the DAGs just produced + **`redis`**
  (speed-layer state) and **`serving`** (FastAPI, `SPARK_DRIVER_MEMORY=2g` — it doesn't run Spark).
  Spark runs **embedded in the task's own process** inside the Airflow container — no separate Spark
  cluster container, on purpose. `SPARK_DRIVER_MEMORY`/`SPARK_MASTER_CORES` (default `8g`/`8`) are
  env-tunable because the VM shares RAM with Airflow + Postgres.

  ```bash
  docker compose up serving redis -d   # solo la capa serving
  ```
- **`dags/dag_01_bronze.py` … `dag_08_serving.py`** — **8 DAGs**; each pipeline phase is its **own DAG**
  (not one DAG with many internal tasks). Six are chained via
  `TriggerDagRunOperator(wait_for_completion=True)`: `dag_01_bronze → dag_02_silver_quality →
  dag_03_silver_schema → dag_04_silver_load → dag_05_gold → dag_06_profiling`. This mirrors
  `run_full_pipeline()` in `main.py` — **profiling runs last on purpose** (read-only documentation that
  doesn't feed silver/gold), not first as an earlier draft of this refactor's brief assumed. The other
  two are `schedule=None`, triggered manually from the UI: `dag_07_gold_ml` (kmodes/isolation/sarimax,
  its three tasks chained sequentially rather than parallel — each spins up its own `SparkClient`/pandas
  process and the container's heap is already committed by one training run at a time; training is heavy,
  not wanted on every batch run) and `dag_08_serving` (`--serve`). All DAG tasks are thin `BashOperator`s
  invoking the existing CLI — none reimplement pipeline logic.
- **`.env`** (from `.env.example`, gitignored) — `STORAGE_BACKEND`/AWS credentials/`S3_BUCKET`/
  `S3_PREFIX` plus standard Airflow bootstrap vars (`AIRFLOW_UID`, `AIRFLOW__CORE__EXECUTOR`,
  `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `_AIRFLOW_WWW_USER_USERNAME`/`PASSWORD`). Bring up with
  `docker compose up airflow-init` once, then `docker compose up`.

**Operational notes (Windows / Docker Desktop):**

- **Run `uv lock` once (and commit `uv.lock`) before `docker compose build`** — the `s3`/`jupyter`
  extras were added to `pyproject.toml` and `uv sync --frozen` fails loudly on a stale lockfile.
- **Memory**: `SparkClient` demands `driver.memory=6g` — the Docker VM (WSL2 backend) needs
  **≥8 GB**. If the default (50% of host RAM) is too low, raise it in `%UserProfile%\.wslconfig`
  (`[wsl2]` → `memory=10GB`). Do NOT lower the Spark heap to fit; raise the VM. `local[6]` also
  assumes ≥6 CPUs visible to the VM (WSL2 default: all host cores).
- **`SPARK_LOCAL_DIR=/tmp/spark-temp`** (set in `docker-compose.yml`) — inside the container the
  shuffle/spill goes to container-local disk, NOT the `./data` bind mount: Windows bind mounts go
  through gRPC-FUSE and 5–15 GB of shuffle I/O over them is punishing. Outside Docker the variable
  is unset and behavior is unchanged (`data/.spark_temp`). Shuffle never goes to S3 either way.
- **Unpause the DAGs**: `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`, and the
  `TriggerDagRunOperator` chain (`wait_for_completion=True`) will sit queued forever if a
  downstream DAG is paused — unpause the chain (`dag_01`…`dag_06`) in the UI before triggering
  `dag_01_bronze`.
- **`.dockerignore` matters**: it keeps `data/` (huge), `.venv/` (Windows binaries would clobber
  the image's Linux venv) and **`.env` (AWS secrets)** out of the build context/image. Don't
  remove entries from it.
- `HADOOP_HOME`/`winutils` is only needed for **bare-Windows** runs (`uv run main.py` outside
  Docker); inside the Linux container that branch is inert.

- **Power BI export** (`scripts/export_powerbi.py`, `scripts/export_audit_powerbi.py`) — standalone, not
  pipeline stages. Gold is written **Hive-partitioned**, so `service_id`/`year`/`month` live *only in the
  folder names, not inside the parquet files*: concatenating the parts by hand silently drops them. The
  export rematerializes them as real columns. Three layouts exist and are detected, not assumed:
  Hive (marts, feature stores), keys-already-in-file (`ml_isolation_fraud_scores`,
  `ml_sarimax_trips_forecast` — applying Hive discovery there collides with the file's own column:
  `ratecode_id` int64 vs int32, `borough` large_string vs string), and unpartitioned dirs (the dims,
  which Spark writes as `dim_x.parquet/part-*.parquet` — a **directory**, not a file). Writes stream
  batch-by-batch: the two trip-grain feature stores exceed 100M rows. Defaults to **snappy, not the
  project's zstd-9**: Power Query supports snappy on every build, zstd only on recent ones.
  `kmodes_model/` is 4 datasets sharing one directory (`tuning_`/`centers_`/`labels_`/`profiles_`), not a
  valid Hive dataset — it's split by prefix.
  The audit export marks `es_ultima_corrida` instead of deduplicating: the `audit.parquet` files are
  **append logs** (a re-run adds a row), so summing them raw inflates bronze by ~43%. It also casts the
  String timestamps to real Datetime, parses `categoria`/`anio`/`mes` out of `source_file` (normalizing
  the separator first — bronze recorded `\`, silver `/`, so a raw join yields zero rows), and adds
  `filas_en_disco` because the gold audit's `rowcount_output` is 0 for incremental runs that skipped
  existing partitions (looks like an empty mart otherwise).

- **Notebooks** (`notebooks/revision_01..05`) — per-layer review notebooks (bronze inventory, profiling
  results, silver integrity `bronze = stage + reject`, gold grains + no-loss proof `SUM(viajes) == fact rows`,
  audit chain). Keep them consistent with pipeline outputs when changing schemas or grains.
