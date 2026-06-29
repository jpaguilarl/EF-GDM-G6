# AGENTS.md

## Project

ETL + data-quality pipeline for NY TLC Trip Record Data — **medallion architecture**
(bronze → silver → gold) plus a standalone profiling stage.

Downloads parquet from `https://d37ci6vzurychx.cloudfront.net/trip-data/`, profiles each
dataset across 8 quality dimensions (accuracy, completeness, consistency, integrity,
reasonableness, timeliness, uniqueness, validity), cleans data into a star schema, then
builds gold marts (Power BI) and ML feature stores.

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
```

Order: `bronze → --silver → --silver schema → --silver load → --gold`. `--silver schema`
must precede `--silver load`; `--gold` reads `data/silver/star/` and aborts cleanly if missing.

## `app.` package (flat, no `src/`)

| Module | Class | Role |
|---|---|---|
| `app.client.download_client` | `DownloadClient` | Async HTTP downloads (Polars), audit trail |
| `app.pipeline.bronze` | `BronzePipeline` | Download loop: years × categories × months |
| `app.profiling.profiling_pipeline` | `ProfilingPipeline` | 8-dimension quality profiling (PySpark) |
| `app.profiling.dataset_profiler` | `DatasetProfiler` | Runs one check per dimension class in `app/profiling/dimensions/` per file |
| `app.profiling.dimensions` | `BaseDimension` + 8 subcls | accuracy, completeness, consistency, integrity, reasonableness, timeliness, uniqueness, validity |
| `app.profiling.reporter` | `Reporter` | Writes JSON + `index.html` to `data/profiling/` |
| `app.pipeline.silver` | `SilverPipeline`, `SilverCleaner` | Quality clean (reject + fix) + star orchestration |
| `app.pipeline.star` | `StarSchemaBuilder` | Star dims + facts (facts carry `trip_id` + std timestamps) |
| `app.pipeline.gold.gold_pipeline` | `GoldPipeline` | Gold orchestrator: 6 marts + 3 ML feature stores |
| `app.pipeline.gold.mart_builder` | `GoldBuilder`, `TripGrainMart`, `GoldContext` | Builder bases + shared context/helpers |
| `app.pipeline.gold.dims.gold_dimensions` | `GoldDimensionsBuilder` | `dim_date_gold`, `dim_zone_gold`, `dim_ratecode_theoretical` |
| `app.utils.spark` | `SparkClient` | PySpark session, `local[4]`, driver 6g, shuffles 64 |
| `app.utils.logger` | `Logger` | Singleton, file+console |
| `app.utils.globals` | `Globals` (instance: `globals`) | `tlc_categories`: green, yellow, fhv, fhvhv |

## Key behaviors

- **Logger** — Singleton. File: `logs/YYYY-MM-DD/HH-MM-SS.log` (DEBUG+). Console: INFO+.
  Messages in **Spanish**, code in **English**.
- **DownloadClient** — `httpx.AsyncClient` 300s timeout. Must close via `async with` or
  `await client.close()`. HTTP errors → `ERROR` (returns silently). Corruption → `CRITICAL` (re-raises).
  Outputs `data/bronze/{category}/{year}-{month:02d}.parquet`, audit at `data/bronze/audit.parquet`.
- **BronzePipeline** — downloads zone-lookup first, then iterates years × categories × months 1–12.
- **ProfilingPipeline** — reads `data/bronze/`, writes per-dataset JSON to
  `data/profiling/{category}/{year}-{month:02d}.json` + summary `index.html`.
- **SilverPipeline / SilverCleaner** — two-phase clean (reject then fix). First failing reject rule wins
  (`& ~already`); fix phase runs only on non-rejected rows. Accuracy (= recompute `total_amount` from
  components) is **skipped for fhvhv** so its `driver_pay` stays intact (gold needs the original value for
  `margen_plataforma` / `ratio_pago_conductor`). `--silver quality` reads `data/bronze/`; `--silver load`
  reads `data/silver/stage/`. Audit at `data/silver/audit.parquet` (FK `bronze_audit_id`).
- **StarSchemaBuilder** — builds fixed lookup dims + `dim_date` + `dim_zone`, and per-category facts. Every
  fact carries a `trip_id` (sha2 of `SilverCleaner.COMPOSITE_KEYS`) and standardized
  `pickup_datetime`/`dropoff_datetime` timestamps (the gold layer depends on this).
- **GoldPipeline** — reads silver star facts/dims, builds gold dims, then 6 wide Power BI marts
  (`data/gold/marts/`) + 3 ML feature stores (`data/gold/ml/`). Trip-grain builders subclass `TripGrainMart`
  (idempotent per-partition writes via `partitionOverwriteMode=dynamic`); aggregate builders (supply/demand,
  ABC/XYZ, ARIMA) subclass `GoldBuilder` and always recompute the whole history. Audit at
  `data/gold/audit.parquet` (FK `silver_audit_id`).
- **Schema heterogeneity** — column names differ across categories/years (`tpep_pickup_datetime` vs
  `lpep_pickup_datetime` vs `pickup_datetime`; `PULocationID` vs `PUlocationID`). Code resolves these via
  candidate-list + `_first_match` helper. Follow this pattern; never hardcode a single column name across
  categories.
- **Tooling split** — `download_client.py` + all audit writes use **Polars**; profiling, silver, star and
  gold use **PySpark**. `pyarrow` for Parquet metadata.
- **Audit chain** — `bronze_audit_id → silver_audit_id → gold_audit_id`; each layer's audit row FKs the
  previous. Polars writes all three `audit.parquet` files.
- **Reusable heuristics** — silver/profiling rules in `app/profiling/rules/`
  (`nullability.py`, `reasonableness_ranges.py`, `amount_components.py`) are the **single source of truth**
  consumed by *both* profiling dimensions and the silver cleaner — change a rule here, not in two places.
  Gold heuristics live in `app/pipeline/gold/feature_rules/` (`time_blocks.py`, `generosity.py`,
  `ratecode_tariff.py`) — same rule: define there, don't inline in a mart.
- **Spark day-of-week quirk** — use `time_blocks.iso_weekday()` for day-of-week, **not**
  `date_format(ts, "u")` (`'u'` is not day-of-week in Spark's proleptic datetime patterns).
- **SparkClient** — `master=local[4]`, `spark.driver.memory=6g`, `spark.sql.shuffle.partitions=64`,
  `spark.local.dir=data/.spark_temp` (avoids small `/tmp` quota during shuffle). The gold layer also sets
  `spark.sql.sources.partitionOverwriteMode=dynamic` for idempotent per-partition writes. On Windows,
  requires `HADOOP_HOME` pointing to a Hadoop bin dir with `hadoop.dll`/`winutils` (bundled in `lib/hadoop/`).
- **Reusable heuristics** — silver/profiling rules in `app/profiling/rules/`; gold heuristics in
  `app/pipeline/gold/feature_rules/`. Single source of truth — don't duplicate inline.

## Config

`config.yaml` — `datasets.years` is a list of plain `int` years (expands to 4 categories × 12 months) or
`Module` objects (`{category, year, month}`) for a single category/year. Optional `gold:` section
(`GoldConfig`) parametrizes the gold layer (block minutes, deficit threshold, ABC/XYZ cutoffs, generosity
thresholds); defaults apply if omitted.

## Stack

Python 3.12, managed with **uv**. **Java JDK 11+** required by PySpark. Dependencies: `httpx`, `pandas`,
`polars`, `pyarrow`, `pydantic`, `pyspark`, `pyyaml`. No test framework, linter, formatter, or CI configured.

## Conventions

- Log/user-facing messages in **Spanish**; code, identifiers, comments in **English**.
- JFK flat fare in `dim_ratecode_theoretical` (fraude heuristic) should be verified against current TLC rules.
