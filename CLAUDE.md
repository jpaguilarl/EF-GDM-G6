# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ETL + data-quality pipeline for NY TLC Trip Record Data, organized as a **medallion architecture**
(bronze → silver → gold) with a standalone profiling stage. Downloads parquet from
`https://d37ci6vzurychx.cloudfront.net/trip-data/`, profiles quality across 8 dimensions, cleans
data into a star schema, then builds gold marts (Power BI) and ML feature stores.

The four TLC categories (`app/utils/globals.py`): **green, yellow, fhv, fhvhv**.

## Commands

```bash
uv run main.py                 # bronze: download zone-lookup + all trip data (years × categories × months)
uv run main.py --profile       # profiling: evaluate quality of bronze data → data/profiling/
uv run main.py --silver        # silver quality (default): clean bronze → data/silver/stage + reject
uv run main.py --silver schema # build star-schema dimension tables → data/silver/star/dims/
uv run main.py --silver load   # build star-schema fact tables → data/silver/star/facts/
uv run main.py --gold          # gold (default full): Power BI marts + ML feature stores → data/gold/
uv run main.py --gold incremental                                       # only build missing trip-grain partitions
uv run main.py --gold --only mart_demand_volume,ml_feat_isolation_fraud # subset of builders
```

Pipeline order matters: `--silver schema` must run before `--silver load` (facts join against dims).
`--silver` (quality) reads from `data/bronze/`; `load` reads from `data/silver/stage/`. `--gold` reads from
`data/silver/star/` (facts + dims), so the full chain `bronze → --silver → --silver schema → --silver load →
--gold` must have run; `--gold` aborts with a clear message if silver/star is missing.

No test framework, linter, formatter, or CI is configured. `test.py` referenced in older docs does not exist.

## Stack & tooling split

Python 3.12, managed with **uv** (`uv.lock`). Key split to keep in mind:

- **Polars** — used by `DownloadClient` and all audit-trail writes (`data/*/audit.parquet`).
- **PySpark** — used by everything in `app/profiling/`, `app/pipeline/silver.py` + `star.py`, and the whole
  `app/pipeline/gold/` package.
- **pyarrow** — parquet metadata (row counts) in the download client.

## Architecture

### Stages (`main.py` dispatches each)

1. **Bronze** (`app/pipeline/bronze.py`, `app/client/download_client.py`) — async HTTP downloads via
   `httpx.AsyncClient` (300s timeout). Downloads zone-lookup CSV first, then loops years × categories ×
   months 1–12. Outputs `data/bronze/{category}/{year}-{month:02d}.parquet`; audit at `data/bronze/audit.parquet`.
   HTTP errors → `logger.error` + silent return; file corruption → `logger.critical` + re-raise.
   `DownloadClient` must be closed via `async with` or `await client.close()`.

2. **Profiling** (`app/profiling/`) — read-only quality assessment. `ProfilingPipeline` → `DatasetProfiler`
   runs one check per dimension class in `app/profiling/dimensions/` (accuracy, completeness, consistency,
   integrity, reasonableness, timeliness, uniqueness, validity). `Reporter` writes per-dataset JSON to
   `data/profiling/{category}/{year}-{month:02d}.json` plus a summary `index.html`. Loads
   `data/bronze/zone-lookup/` and optional `data/bronze/dicts/` data dictionaries.

3. **Silver quality** (`SilverCleaner` in `app/pipeline/silver.py`) — two-phase per file:
   - **Reject phase** adds a `_reject_reason` column (timeliness off-period, inverted/over-24h datetimes,
     integrity vs. zone IDs, uniqueness duplicates). First failing rule wins (`& ~already`).
   - **Fix phase** runs only on non-rejected rows (completeness imputation, accuracy = recompute
     `total_amount` from components, reasonableness clamping, validity casts). Accuracy is **skipped for
     fhvhv**: it has no real passenger total, so `driver_pay` is left intact (gold needs the original value
     for `margen_plataforma` / `ratio_pago_conductor`).
   - Clean rows → `data/silver/stage/{category}/`; rejected rows → `data/silver/reject/{category}/`;
     audit → `data/silver/audit.parquet` (links to the latest `bronze_audit_id`).

4. **Silver schema + load** (`StarSchemaBuilder` in `app/pipeline/star.py`) — star schema. `build_dimensions`
   writes fixed lookup dims (vendor, ratecode, payment_type, service), a generated `dim_date` (2023–2025),
   and `dim_zone` from zone-lookup → `data/silver/star/dims/`. `build_facts` dispatches per category to
   `_FactBuilder._build_{category}` → `data/silver/star/facts/fact_{category}_trip/`. Every fact carries a
   `trip_id` (sha2 of the silver composite PK, from `SilverCleaner.COMPOSITE_KEYS`) and standardized
   `pickup_datetime`/`dropoff_datetime` timestamps so the gold layer can do hour-level analysis — gold
   depends on this enrichment.

5. **Gold** (`app/pipeline/gold/`, `GoldPipeline`) — reads silver star facts/dims, builds enriched gold dims
   (`GoldDimensionsBuilder`: `dim_date_gold`, `dim_zone_gold`, `dim_ratecode_theoretical`), then runs each
   builder: 6 wide **marts** for Power BI (`marts/`) + 3 **ML feature stores** (`ml/`). Trip-grain builders
   subclass `TripGrainMart` (iterate facts, write one `service_id/year/month` partition per file, idempotent
   dynamic overwrite); aggregate builders (supply/demand, ABC/XYZ, ARIMA) subclass `GoldBuilder` and span the
   whole history. Outputs under `data/gold/{marts,ml,dims}/`; audit → `data/gold/audit.parquet` (links the
   latest `silver_audit_id`). `--gold incremental` skips existing trip-grain partitions; aggregate marts
   always recompute. `--only name1,name2` restricts which builders run.

### Cross-cutting

- **Schema heterogeneity** — column names differ across categories/years (e.g.
  `tpep_pickup_datetime` vs `lpep_pickup_datetime` vs `pickup_datetime`; `PULocationID` vs `PUlocationID`).
  Code resolves these via candidate lists + a `_first_match` helper. Follow this pattern; never hardcode a
  single column name across categories.

- **Shared rule modules** (`app/profiling/rules/`) — `nullability.py`, `reasonableness_ranges.py`,
  `amount_components.py` are the **single source of truth** consumed by *both* the profiling dimensions and
  the silver cleaner. Change a rule here, not in two places. The gold layer has its own reusable heuristics in
  `app/pipeline/gold/feature_rules/` (`time_blocks.py`, `generosity.py`, `ratecode_tariff.py`) — same rule:
  define a gold heuristic there, not inline in a mart. Use `time_blocks.iso_weekday()` for day-of-week, **not**
  `date_format(ts, "u")` (`'u'` is not day-of-week in Spark's proleptic datetime patterns).

- **Config** (`config.yaml` → `app/schemas/settings_schema.py`) — `datasets.years` is a list of either
  plain `int` years (expands to all 4 categories × 12 months) or `Module` objects
  (`{category, year, month}`) for targeting a single category/year. All pipeline loops handle both forms.
  An optional `gold:` section (`GoldConfig`) parametrizes the gold layer (supply/demand block minutes &
  deficit threshold, ABC/XYZ cutoffs, generosity thresholds); it has defaults, so it may be omitted.

- **Logger** (`app/utils/logger.py`) — singleton, file (`logs/YYYY-MM-DD/HH-MM-SS.log`, DEBUG+) + console
  (INFO+). **Log/user messages are in Spanish; code, identifiers, and comments are in English.** Match this.

- **Spark** (`app/utils/spark.py`) — `SparkClient` runs `local[4]`, driver memory 6g, shuffle partitions 64,
  and `spark.local.dir` to `data/.spark_temp` (avoids small `/tmp` quota during shuffle). The gold layer also
  sets `spark.sql.sources.partitionOverwriteMode=dynamic` for idempotent per-partition writes. On **Windows**,
  requires `HADOOP_HOME` pointing to a Hadoop bin dir containing `hadoop.dll`/`winutils`; native lib path is
  passed via `extraLibraryPath` (not `java.library.path`, which strips Windows backslashes).
