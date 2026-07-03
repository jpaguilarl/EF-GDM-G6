# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ETL + data-quality pipeline for NY TLC Trip Record Data, organized as a **medallion architecture**
(bronze → silver → gold) with a standalone profiling stage. Downloads parquet from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`, profiles quality across 8 dimensions, cleans
data into a star schema, builds gold marts (Power BI) + ML feature stores, and trains the ML
models (K-Modes, Isolation Forest, SARIMAX).

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
- **PySpark** — used by everything in `app/profiling/`, `app/pipeline/silver.py` + `star.py`, and the whole
  `app/pipeline/gold/` package.
- **pyarrow** — parquet metadata (row counts, footer validation) in the download client.
- **scikit-learn / kmodes / statsmodels / joblib** — the `--gold-ml` model pipelines (pandas-side, not Spark).

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
   integrity, reasonableness, timeliness, uniqueness, validity). Profiles **3 files in parallel**
   (`MAX_PARALLEL_PROFILES`); each file is persisted once (`MEMORY_AND_DISK`) so the 8 dimensions don't
   re-read the parquet, and unpersisted afterwards — do **not** call `catalog.clearCache()` between profiles
   (it is global and would evict concurrent profiles). Existing report JSONs are reused (idempotent).
   `Reporter` writes per-dataset JSON to `data/profiling/{category}/{year}-{month:02d}.json` plus a summary
   `index.html` (reports sorted for stable output).

3. **Silver quality** (`SilverCleaner` in `app/pipeline/silver.py`) — two-phase per file:
   - **Reject phase** adds a `_reject_reason` column (timeliness off-period, inverted/over-24h datetimes,
     integrity vs. zone IDs, uniqueness duplicates). First failing rule wins (`& ~already`). Helper columns
     `_pickup_dt`/`_dropoff_dt` are dropped **before** the persist (2 extra timestamp columns ≈ 320MB cached
     on an fhvhv month).
   - **Fix phase** runs only on non-rejected rows (completeness imputation, accuracy = recompute
     `total_amount` from components, reasonableness clamping, validity casts). Accuracy is **skipped for
     fhvhv**: it has no real passenger total, so `driver_pay` is left intact (gold needs the original value
     for `margen_plataforma` / `ratio_pago_conductor`).
   - `SilverPipeline.run_quality` processes **2 files in parallel** (`MAX_PARALLEL_FILES` — do NOT raise to 3:
     three concurrent fhvhv writes OOM'd the 6g heap), one `SilverCleaner` per worker (its persist cache is
     per-instance), audit writes serialized behind `_audit_lock` (read-concat-write on one parquet). Months
     already in `stage/` are skipped; failures are collected and re-raised at the end (fail loud — a missing
     month would be silent data loss downstream).
   - Clean rows → `data/silver/stage/{category}/`; rejected rows → `data/silver/reject/{category}/`;
     audit → `data/silver/audit.parquet` (links to the latest `bronze_audit_id`).

4. **Silver schema + load** (`StarSchemaBuilder` in `app/pipeline/star.py`) — star schema. `build_dimensions`
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

- **Shared rule modules** (`app/profiling/rules/`) — `nullability.py`, `reasonableness_ranges.py`,
  `amount_components.py` are the **single source of truth** consumed by *both* the profiling dimensions and
  the silver cleaner. Change a rule here, not in two places. The gold layer has its own reusable heuristics in
  `app/pipeline/gold/feature_rules/` (`time_blocks.py`, `generosity.py`, `ratecode_tariff.py`,
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
  backslashes).

- **Notebooks** (`notebooks/revision_01..05`) — per-layer review notebooks (bronze inventory, profiling
  results, silver integrity `bronze = stage + reject`, gold grains + no-loss proof `SUM(viajes) == fact rows`,
  audit chain). Keep them consistent with pipeline outputs when changing schemas or grains.
