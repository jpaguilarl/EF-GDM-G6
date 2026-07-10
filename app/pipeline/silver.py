import concurrent.futures
import threading
import uuid
from datetime import datetime

import polars as pl
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.profiling.rules.amount_components import AMOUNT_FORMULAS
from app.profiling.rules.nullability import NULLABLE_COLUMNS
from app.profiling.rules.reasonableness_ranges import (
    MAX_TRIP_DURATION_MINUTES,
    REASONABLENESS_RANGES,
)
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils import storage
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient, target_files


class SilverCleaner:
    PICKUP_CANDIDATES = [
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "pickup_datetime",
    ]
    DROPOFF_CANDIDATES = [
        "tpep_dropoff_datetime",
        "lpep_dropoff_datetime",
        "dropoff_datetime",
        "dropOff_datetime",
    ]
    PU_CANDIDATES = ["PULocationID", "PUlocationID"]
    DO_CANDIDATES = ["DOLocationID", "DOlocationID"]

    NULLABLE_AMOUNT_COLS = {
        "yellow": {"congestion_surcharge", "airport_fee", "cbd_congestion_fee"},
        "green": {"congestion_surcharge", "ehail_fee", "cbd_congestion_fee"},
    }

    COMPOSITE_KEYS = {
        "fhv": ["dispatching_base_num", "pickup_datetime", "dropOff_datetime"],
        "fhvhv": [
            "hvfhs_license_num",
            "request_datetime",
            "pickup_datetime",
            "dropoff_datetime",
        ],
        "yellow": [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
        ],
        "green": [
            "VendorID",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
        ],
    }

    IMPUTE_DEFAULTS: dict[str, object] = {
        "passenger_count": 1,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
    }

    REFUND_TYPES = {3, 4}

    def __init__(self, spark_session) -> None:
        self.spark = spark_session
        self._cached: list[DataFrame] = []

    def clean(
        self,
        df: DataFrame,
        category: str,
        year: int,
        month: int,
        zone_ids: set[int],
    ) -> tuple[DataFrame, DataFrame]:
        df = df.withColumn("_row_idx", F.monotonically_increasing_id())
        df = df.withColumn("_reject_reason", F.lit(None).cast("string"))

        df = self._reject_timeliness(df, category, year, month)
        df = self._reject_consistency(df, category)
        df = self._reject_integrity(df, zone_ids)
        df = self._reject_uniqueness(df, category)

        # _pickup_dt/_dropoff_dt son helpers internos de _reject_consistency que
        # duplican los timestamps originales. Dropearlos ANTES del persist evita
        # cachear 2 columnas timestamp extra por fila (~320MB en un mes de fhvhv).
        df = df.drop("_pickup_dt", "_dropoff_dt")

        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        self._cached.append(df)

        reject_df = df.filter(F.col("_reject_reason").isNotNull())
        clean_df = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")

        clean_df = self._fix_completeness(clean_df, category)
        clean_df = self._fix_accuracy(clean_df, category)
        clean_df = self._fix_reasonableness(clean_df, category)
        clean_df = self._fix_validity(clean_df)

        # Columnas de trabajo de la fase de rechazo; no deben llegar al stage.
        # (_pickup_dt/_dropoff_dt ya se droparon antes del persist.)
        helper_cols = ["_row_idx", "_dup_count", "_dup_rank"]
        clean_df = clean_df.drop(*helper_cols)
        reject_df = reject_df.drop(*helper_cols)

        return clean_df, reject_df

    def cleanup(self) -> None:
        """Release cached intermediate DataFrames."""
        for cached in self._cached:
            try:
                cached.unpersist()
            except Exception:
                pass
        self._cached.clear()

    # ------------------------------------------------------------------
    # Rejection helpers
    # ------------------------------------------------------------------

    def _reject_timeliness(
        self, df: DataFrame, category: str, year: int, month: int
    ) -> DataFrame:
        pickup_col = self._first_match(df, self.PICKUP_CANDIDATES)
        if pickup_col is None:
            return df

        expected = (F.year(F.col(pickup_col)) == year) & (
            F.month(F.col(pickup_col)) == month
        )
        off_period = F.col(pickup_col).isNotNull() & ~expected
        already = F.col("_reject_reason").isNotNull()
        return df.withColumn(
            "_reject_reason",
            F.when(off_period & ~already, F.lit("timeliness_off_period")).otherwise(
                F.col("_reject_reason")
            ),
        )

    def _reject_consistency(self, df: DataFrame, category: str) -> DataFrame:
        pickup_col = self._first_match(df, self.PICKUP_CANDIDATES)
        dropoff_col = self._first_match(df, self.DROPOFF_CANDIDATES)
        if pickup_col is None or dropoff_col is None:
            return df

        already = F.col("_reject_reason").isNotNull()
        df = df.withColumn("_pickup_dt", F.to_timestamp(F.col(pickup_col)))
        df = df.withColumn("_dropoff_dt", F.to_timestamp(F.col(dropoff_col)))
        both_valid = F.col("_pickup_dt").isNotNull() & F.col("_dropoff_dt").isNotNull()
        inverted = both_valid & (F.col("_dropoff_dt") < F.col("_pickup_dt"))

        df = df.withColumn(
            "_reject_reason",
            F.when(
                inverted & ~already, F.lit("consistency_inverted_datetime")
            ).otherwise(F.col("_reject_reason")),
        )

        duration_min = (
            F.unix_timestamp(F.col("_dropoff_dt"))
            - F.unix_timestamp(F.col("_pickup_dt"))
        ) / 60
        too_long = both_valid & (duration_min > MAX_TRIP_DURATION_MINUTES)
        already = F.col("_reject_reason").isNotNull()
        return df.withColumn(
            "_reject_reason",
            F.when(too_long & ~already, F.lit("consistency_duration_gt_24h")).otherwise(
                F.col("_reject_reason")
            ),
        )

    def _reject_integrity(self, df: DataFrame, zone_ids: set[int]) -> DataFrame:
        pu_col = self._first_match(df, self.PU_CANDIDATES)
        do_col = self._first_match(df, self.DO_CANDIDATES)
        if pu_col is None and do_col is None:
            return df

        zone_list = list(zone_ids)
        for col_name in [pu_col, do_col]:
            if col_name is None:
                continue
            invalid = F.col(col_name).isNotNull() & ~F.col(col_name).isin(zone_list)
            already = F.col("_reject_reason").isNotNull()
            df = df.withColumn(
                "_reject_reason",
                F.when(
                    invalid & ~already, F.lit(f"integrity_invalid_{col_name}")
                ).otherwise(F.col("_reject_reason")),
            )
        return df

    def _reject_uniqueness(self, df: DataFrame, category: str) -> DataFrame:
        key_cols = self.COMPOSITE_KEYS.get(category, [])
        key_cols = [c for c in key_cols if c in df.columns]
        if not key_cols:
            return df

        window = Window.partitionBy(*key_cols)
        df = df.withColumn("_dup_count", F.count("*").over(window))

        order_cols = []
        for c in ["total_amount", "driver_pay"]:
            if c in df.columns:
                order_cols.append(F.col(c).desc_nulls_last())
        order_cols.append(F.col("_row_idx").asc())

        df = df.withColumn(
            "_dup_rank",
            F.row_number().over(Window.partitionBy(*key_cols).orderBy(*order_cols)),
        )

        already = F.col("_reject_reason").isNotNull()
        is_dup = (F.col("_dup_count") > 1) & (F.col("_dup_rank") > 1)
        return df.withColumn(
            "_reject_reason",
            F.when(is_dup & ~already, F.lit("uniqueness_duplicate")).otherwise(
                F.col("_reject_reason")
            ),
        )

    # ------------------------------------------------------------------
    # Fix helpers (applied after rejection)
    # ------------------------------------------------------------------

    def _fix_completeness(self, df: DataFrame, category: str) -> DataFrame:
        nullable_amounts = self.NULLABLE_AMOUNT_COLS.get(category, set())
        nullable_set = NULLABLE_COLUMNS.get(category, set())

        for col_name in df.columns:
            if col_name in self.IMPUTE_DEFAULTS:
                default = self.IMPUTE_DEFAULTS[col_name]
                df = df.withColumn(
                    col_name,
                    F.coalesce(F.col(col_name), F.lit(default)),
                )
            elif col_name in nullable_amounts:
                df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))
            elif col_name in nullable_set:
                pass
        return df

    def _fix_accuracy(self, df: DataFrame, category: str) -> DataFrame:
        formula = AMOUNT_FORMULAS.get(category)
        if formula is None:
            return df

        # fhvhv no tiene una columna 'total' real cobrada al pasajero: 'driver_pay'
        # es el pago al conductor, NO la suma de los componentes. Recalcularlo lo
        # corromperia y romperia margen_plataforma / ratio_pago_conductor en la
        # capa gold (mart financiero D1.2). Se deja el valor original intacto.
        if category == "fhvhv":
            return df

        total_col: str = formula["total"]
        components: list[str] = formula["components"]
        available = [c for c in components if c in df.columns]
        if not available:
            return df

        sum_expr = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in available)
        df = df.withColumn(total_col, F.round(sum_expr, 2))
        return df

    def _fix_reasonableness(self, df: DataFrame, category: str) -> DataFrame:
        ranges = REASONABLENESS_RANGES.get(category, {})
        if not ranges:
            return df

        has_payment_type = "payment_type" in df.columns
        refund_condition = (
            F.col("payment_type").isin(*self.REFUND_TYPES)
            if has_payment_type
            else F.lit(False)
        )

        for col_name, (low, high) in ranges.items():
            if col_name not in df.columns:
                continue

            if low >= 0:
                clamped = (
                    F.when(F.col(col_name) < F.lit(low), F.lit(low))
                    .when(F.col(col_name) > F.lit(high), F.lit(high))
                    .otherwise(F.col(col_name))
                )
            else:
                clamped = F.when(F.col(col_name) > F.lit(high), F.lit(high)).otherwise(
                    F.when(
                        refund_condition & (F.col(col_name) < F.lit(low)),
                        F.col(col_name),
                    )
                    .when(
                        ~refund_condition & (F.col(col_name) < F.lit(low)),
                        F.lit(low),
                    )
                    .otherwise(F.col(col_name))
                )

            df = df.withColumn(col_name, clamped)
        return df

    def _fix_validity(self, df: DataFrame) -> DataFrame:
        code_columns = ["VendorID", "RatecodeID", "payment_type", "passenger_count"]
        for col_name in code_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("int"))
        return df

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _first_match(df: DataFrame, candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None


class SilverPipeline:
    # 2 archivos en paralelo. NO subir a 3: con local[6] y 3 meses de fhvhv
    # (~20M filas c/u) escribiendose a la vez, los buffers de FileFormatWriter
    # reventaron los 6g de heap (OOM real 2026-07-02 19:20, cascada de 53
    # archivos). Con 2 workers los task slots igual se llenan en las fases
    # paralelas; el worker extra solo aportaba solape en fases livianas.
    MAX_PARALLEL_FILES = 2

    def __init__(self) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        # _write_audit hace read->concat->write sobre el mismo parquet; sin lock,
        # dos workers concurrentes se pisarian y perderian filas de auditoria.
        self._audit_lock = threading.Lock()

    def run_quality(self, year_span: DatasetsConfig) -> None:
        spark = self.spark_client.get_session()
        bronze_audit_id = self._get_latest_bronze_audit_id(spark)
        self.logger.info(
            f"Ejecutando silver calidad | audit_id={self.audit_id} | bronze_audit_id={bronze_audit_id}"
        )

        zone_ids = self._load_zone_ids(spark)
        tasks = self._expand_tasks(year_span)
        failures: list[str] = []

        heavy_cats = {"fhvhv", "yellow"}
        heavy_tasks = [t for t in tasks if t[0] in heavy_cats]
        light_tasks = [t for t in tasks if t[0] not in heavy_cats]

        def _run_pool(pool_tasks: list[tuple[str, int, int]], max_w: int) -> None:
            if not pool_tasks:
                return
            # Un SilverCleaner por archivo: su cache de DataFrames persistidos es por
            # instancia, y cleanup() de un worker no debe despersistir los del otro.
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
                futures_dict = {
                    executor.submit(
                        self._process_file,
                        spark,
                        SilverCleaner(spark),
                        cat,
                        year,
                        month,
                        zone_ids,
                        bronze_audit_id,
                    ): (cat, year, month)
                    for (cat, year, month) in pool_tasks
                }
                for future in concurrent.futures.as_completed(futures_dict):
                    c, y, m = futures_dict[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error procesando {c} {y}-{m:02d}: {e}")
                        failures.append(f"{c} {y}-{m:02d}")

        if heavy_tasks:
            self.logger.info("Procesando datasets pesados (fhvhv, yellow) secuencialmente")
            _run_pool(heavy_tasks, 1)

        if light_tasks:
            self.logger.info("Procesando datasets livianos (green, fhv) en paralelo")
            _run_pool(light_tasks, 2)

        if failures:
            # Fallar ruidosamente: un mes ausente en stage seria perdida de datos
            # silenciosa para star/gold.
            raise RuntimeError(
                f"Silver calidad fallo en {len(failures)} archivo(s): {', '.join(sorted(failures))}"
            )

        self.logger.info("Silver calidad completado exitosamente")

    @staticmethod
    def _expand_tasks(year_span: DatasetsConfig) -> list[tuple[str, int, int]]:
        tasks: list[tuple[str, int, int]] = []
        for year in year_span.years:
            if isinstance(year, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        tasks.append((cat, year, m))
            elif isinstance(year, Module):
                for m in range(1, 13):
                    tasks.append((year.category, year.year, m))
        return tasks

    def run_schema(self) -> None:
        from app.pipeline.star import StarSchemaBuilder

        spark = self.spark_client.get_session()
        self.logger.info("Construyendo tablas de dimension del modelo estrella")
        builder = StarSchemaBuilder(spark)
        builder.build_dimensions()
        self.logger.info(
            "Dimensiones del modelo estrella creadas en data/silver/star/dims/"
        )

    def run_load(self, year_span: DatasetsConfig) -> None:
        from app.pipeline.star import StarSchemaBuilder

        spark = self.spark_client.get_session()
        self.logger.info("Cargando tablas de hechos del modelo estrella")
        builder = StarSchemaBuilder(spark)
        builder.build_facts(year_span)
        self.logger.info("Tablas de hechos cargadas en data/silver/star/facts/")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _process_file(
        self,
        spark,
        cleaner: SilverCleaner,
        category: str,
        year: int,
        month: int,
        zone_ids: set[int],
        bronze_audit_id: str,
    ) -> None:
        source = f"data/bronze/{category}/{year}-{month:02d}.parquet"
        full_path = storage.data_path("bronze", category, f"{year}-{month:02d}.parquet")

        # Idempotencia: el mes se omite SOLO si stage Y reject tienen su marca
        # _SUCCESS (la escribe Spark al commitear el job). La sola existencia
        # del directorio NO basta: un job matado a mitad deja el directorio con
        # basura _temporary y cero parts commiteados (ocurrio 2026-07-02: OOM
        # dejo stage fhvhv 2023-09 vacio y reject 2023-07 sin commitear, y el
        # chequeo por existencia los dio por buenos). Para forzar re-limpieza,
        # borrar el directorio del mes en silver/stage.
        stage_out = (
            globals.project_root
            / "data/silver/stage"
            / category
            / f"{year}-{month:02d}.parquet"
        )
        reject_out = (
            globals.project_root
            / "data/silver/reject"
            / category
            / f"{year}-{month:02d}.parquet"
        )
        if (stage_out / "_SUCCESS").exists() and (reject_out / "_SUCCESS").exists():
            self.logger.info(
                f"  {category} {year}-{month:02d}: stage ya existe, se omite"
            )
            return

        self.logger.info(f"Procesando {source}")

        tag = f"{category} {year}-{month:02d}"
        df = spark.read.parquet(storage.for_spark(full_path)).persist(StorageLevel.MEMORY_AND_DISK)
        bronze_rowcount = df.count()
        self.logger.info(f"  {tag} | Bronce: {bronze_rowcount} filas")

        start_ts = datetime.now()
        clean_df, reject_df = cleaner.clean(df, category, year, month, zone_ids)
        df.unpersist()

        clean_count = clean_df.count()
        reject_count = reject_df.count() if reject_df is not None else 0

        silver_dir = globals.project_root / "data/silver/stage" / category
        silver_dir.mkdir(parents=True, exist_ok=True)
        clean_df.coalesce(target_files(clean_count)).write.mode("overwrite").parquet(
            storage.for_spark(silver_dir / f"{year}-{month:02d}.parquet")
        )

        if reject_count > 0:
            reject_dir = globals.project_root / "data/silver/reject" / category
            reject_dir.mkdir(parents=True, exist_ok=True)
            reject_df.coalesce(target_files(reject_count)).write.mode(
                "overwrite"
            ).parquet(storage.for_spark(reject_dir / f"{year}-{month:02d}.parquet"))

        cleaner.cleanup()

        end_ts = datetime.now()
        self._write_audit(
            bronze_audit_id,
            start_ts,
            end_ts,
            source,
            bronze_rowcount,
            clean_count,
            reject_count,
        )
        self.logger.info(f"  {tag} | Silver: {clean_count} | Rechazadas: {reject_count}")

    def _get_latest_bronze_audit_id(self, spark) -> str:
        audit_path = globals.project_root / "data/bronze/audit.parquet"
        if not audit_path.exists():
            self.logger.warning("No se encontró audit de bronce, usando 'unknown'")
            return "unknown"
        try:
            df = spark.read.parquet(storage.for_spark(audit_path))
            latest = (
                df.orderBy(F.col("start_timestamp").desc()).select("audit_id").first()
            )
            return latest["audit_id"] if latest else "unknown"
        except Exception as e:
            self.logger.warning(f"No se pudo leer audit de bronce: {e}")
            return "unknown"

    def _load_zone_ids(self, spark) -> set[int]:
        zone_path = (
            globals.project_root / "data/bronze/zone-lookup/zone-lookup-table.parquet"
        )
        if not zone_path.exists():
            self.logger.warning(
                "No se encontró zone-lookup-table, devolviendo set vacío"
            )
            return set()
        df = spark.read.parquet(storage.for_spark(zone_path))
        if "LocationID" not in df.columns:
            return set()
        return {
            row["LocationID"] for row in df.select("LocationID").distinct().collect()
        }

    def _write_audit(
        self,
        bronze_audit_id: str,
        start_ts: datetime,
        end_ts: datetime,
        source_file: str,
        bronze_rc: int,
        quality_rc: int,
        reject_rc: int,
    ) -> None:
        audit_path = globals.project_root / "data/silver/audit.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "audit_id": self.audit_id,
            "bronze_audit_id": bronze_audit_id,
            "start_timestamp": start_ts.isoformat(),
            "end_timestamp": end_ts.isoformat(),
            "source_file": source_file,
            "rowcount_bronze": bronze_rc,
            "rowcount_quality": quality_rc,
            "rowcount_quarantined": reject_rc,
        }
        # Seccion critica: read-modify-write del parquet de auditoria. Con
        # procesamiento paralelo de archivos, sin lock se perderian filas.
        with self._audit_lock:
            df_new = pl.DataFrame([row])
            if audit_path.exists():
                existing = pl.read_parquet(str(audit_path))
                df_new = pl.concat([existing, df_new])
            df_new.write_parquet(str(audit_path))
