import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from app.pipeline.silver import SilverCleaner
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils import storage
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import target_files

DIMS_DIR = globals.project_root / "data/silver/star/dims"
FACTS_DIR = globals.project_root / "data/silver/star/facts"

# ---------------------------------------------------------------------------
# Dimension lookup data
# ---------------------------------------------------------------------------

VENDOR_ROWS = [
    (1, "Creative Mobile Technologies"),
    (2, "VeriFone Inc."),
    (6, "Myle LLC"),
    (7, "Hybrid/Helix"),
    (0, "Desconocido"),
]

RATECODE_ROWS = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau/Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride"),
    (99, "Desconocido"),
]

PAYMENT_TYPE_ROWS = [
    (0, "Flex fare"),
    (1, "Tarjeta de credito"),
    (2, "Efectivo"),
    (3, "Sin cargo"),
    (4, "Disputa"),
    (5, "Desconocido"),
    (6, "Viaje anulado"),
    (99, "Desconocido"),
]

SERVICE_ROWS = [
    ("yellow", "Taxi Amarillo", True, False),
    ("green", "Taxi Verde", True, False),
    ("fhvhv", "Alta Movilidad (FHvHv)", True, True),
    ("fhv", "Alta Movilidad (FHv)", False, True),
]

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


class StarSchemaBuilder:
    def __init__(self, spark_session) -> None:
        self.spark = spark_session
        self.logger = Logger()

    # ------------------------------------------------------------------
    # Build all dimension tables
    # ------------------------------------------------------------------

    def build_dimensions(self) -> None:
        self._build_dim_date()
        self._build_dim_zone()
        self._build_lookup_dim("dim_vendor", VENDOR_ROWS, "vendor_id", "vendor_name")
        self._build_lookup_dim(
            "dim_ratecode", RATECODE_ROWS, "ratecode_id", "ratecode_name"
        )
        self._build_lookup_dim(
            "dim_payment_type",
            PAYMENT_TYPE_ROWS,
            "payment_type_id",
            "payment_type_name",
        )
        self._build_dim_service()

    def _build_dim_date(self) -> None:
        rows = []
        start = datetime(2023, 1, 1)
        end = datetime(2025, 12, 31)
        current = start
        while current <= end:
            date_key = int(current.strftime("%Y%m%d"))
            rows.append(
                (
                    date_key,
                    current.date(),
                    current.year,
                    current.month,
                    current.day,
                    (current.month - 1) // 3 + 1,
                    current.isoweekday(),
                    current.strftime("%A"),
                    current.strftime("%B"),
                    current.isoweekday() >= 6,
                )
            )
            current += timedelta(days=1)

        schema = StructType(
            [
                StructField("date_key", IntegerType(), False),
                StructField("date", DateType(), False),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
                StructField("quarter", IntegerType(), False),
                StructField("weekday", IntegerType(), False),
                StructField("day_name", StringType(), False),
                StructField("month_name", StringType(), False),
                StructField("is_weekend", BooleanType(), False),
            ]
        )
        df = self.spark.createDataFrame(rows, schema)
        path = storage.for_spark(DIMS_DIR / "dim_date.parquet")
        DIMS_DIR.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(path)
        self.logger.info(f"  dim_date: {len(rows)} registros")

    def _build_dim_zone(self) -> None:
        zone_path = (
            globals.project_root / "data/bronze/zone-lookup/zone-lookup-table.parquet"
        )
        if not zone_path.exists():
            self.logger.warning("  dim_zone: no se encontró zone-lookup-table")
            return
        read_cols = ["LocationID", "Borough", "Zone", "service_zone"]
        df = self.spark.read.parquet(storage.for_spark(zone_path))
        available = [c for c in read_cols if c in df.columns]
        if available:
            df = df.select(*available)
        path = storage.for_spark(DIMS_DIR / "dim_zone.parquet")
        DIMS_DIR.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(path)
        self.logger.info(f"  dim_zone: {df.count()} registros")

    def _build_lookup_dim(
        self,
        dim_name: str,
        rows: list[tuple],
        id_col: str,
        name_col: str,
    ) -> None:
        schema = StructType(
            [
                StructField(id_col, IntegerType(), False),
                StructField(name_col, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema)
        path = storage.for_spark(DIMS_DIR / f"{dim_name}.parquet")
        DIMS_DIR.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(path)
        self.logger.info(f"  {dim_name}: {len(rows)} registros")

    def _build_dim_service(self) -> None:
        schema = StructType(
            [
                StructField("service_id", StringType(), False),
                StructField("service_name", StringType(), False),
                StructField("has_fare", BooleanType(), False),
                StructField("has_dispatch_base", BooleanType(), False),
            ]
        )
        df = self.spark.createDataFrame(SERVICE_ROWS, schema)
        path = storage.for_spark(DIMS_DIR / "dim_service.parquet")
        DIMS_DIR.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(path)
        self.logger.info(f"  dim_service: {len(SERVICE_ROWS)} registros")

    # ------------------------------------------------------------------
    # Build fact tables
    # ------------------------------------------------------------------

    # 3 facts en paralelo (mismo razonamiento que SilverPipeline): los jobs
    # comparten los task slots de local[6]; la ganancia esta en solapar las
    # fases poco paralelas (lectura de un archivo, escritura coalesceada) de un
    # fact con el computo del otro. Sin estado compartido mutable: los dims son
    # DataFrames inmutables y aqui no se escribe auditoria.
    MAX_PARALLEL_FACTS = 3

    def build_facts(self, year_span: DatasetsConfig) -> None:
        dims = {
            "date": self._read_dim("dim_date"),
            "zone": self._read_dim("dim_zone"),
            "vendor": self._read_dim("dim_vendor"),
            "ratecode": self._read_dim("dim_ratecode"),
            "payment": self._read_dim("dim_payment_type"),
            "service": self._read_dim("dim_service"),
        }

        tasks: list[tuple[str, int, int]] = []
        for year in year_span.years:
            if isinstance(year, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        tasks.append((cat, year, m))
            elif isinstance(year, Module):
                for m in range(1, 13):
                    tasks.append((year.category, year.year, m))

        heavy_cats = {"fhvhv", "yellow"}
        heavy_tasks = [t for t in tasks if t[0] in heavy_cats]
        light_tasks = [t for t in tasks if t[0] not in heavy_cats]

        failures: list[str] = []

        def _run_pool(pool_tasks: list[tuple[str, int, int]], max_w: int) -> None:
            if not pool_tasks:
                return
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
                futures_dict = {
                    executor.submit(self._build_fact, cat, year, m, dims): (cat, year, m)
                    for (cat, year, m) in pool_tasks
                }
                for future in concurrent.futures.as_completed(futures_dict):
                    c, y, m_val = futures_dict[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"  Error en fact {c} {y}-{m_val:02d}: {e}")
                        failures.append(f"{c} {y}-{m_val:02d}")

        if heavy_tasks:
            self.logger.info("Construyendo facts pesados (fhvhv, yellow) secuencialmente")
            _run_pool(heavy_tasks, 1)
        
        if light_tasks:
            self.logger.info("Construyendo facts livianos (green, fhv) en paralelo")
            _run_pool(light_tasks, 3)

        if failures:
            raise RuntimeError(
                f"Carga de facts fallo en {len(failures)} archivo(s): {', '.join(sorted(failures))}"
            )

    def _build_fact(
        self,
        category: str,
        year: int,
        month: int,
        dims: dict[str, DataFrame],
    ) -> None:
        source = f"data/silver/stage/{category}/{year}-{month:02d}.parquet"
        full_path = globals.project_root / source
        if not full_path.exists():
            self.logger.info(f"  No se encontró {source}, saltando")
            return

        # Idempotencia: fact mensual ya construido -> no se recalcula. Se exige
        # la marca _SUCCESS (commit del job), no la mera existencia del
        # directorio: un job matado deja el directorio con solo _temporary.
        # Para reconstruir un mes, borrar su directorio en star/facts.
        fact_out = FACTS_DIR / f"fact_{category}_trip" / f"{year}-{month:02d}.parquet"
        if (fact_out / "_SUCCESS").exists():
            self.logger.info(
                f"  fact_{category}_trip/{year}-{month:02d}: ya existe, se omite"
            )
            return

        try:
            trip_df = self.spark.read.parquet(storage.for_spark(full_path))
        except Exception as e:
            self.logger.warning(f"  Error leyendo {source}: {e}")
            return

        row_count = trip_df.count()
        if row_count == 0:
            self.logger.info(f"  {category} {year}-{month:02d}: 0 filas, saltando")
            return

        pickup_col = self._first_match(trip_df, PICKUP_CANDIDATES)
        if pickup_col:
            trip_df = trip_df.withColumn("_pickup_date", F.to_date(F.col(pickup_col)))
            trip_df = trip_df.withColumn(
                "_date_key",
                F.year(F.col("_pickup_date")) * 10000
                + F.month(F.col("_pickup_date")) * 100
                + F.day(F.col("_pickup_date")),
            )
        else:
            trip_df = trip_df.withColumn(
                "_date_key", F.lit(year * 10000 + month * 100 + 1)
            )

        dropoff_col = self._first_match(trip_df, DROPOFF_CANDIDATES)
        pickup_loc = self._first_match(trip_df, ["PULocationID", "PUlocationID"])
        dropoff_loc = self._first_match(trip_df, ["DOLocationID", "DOlocationID"])

        builder = _FactBuilder(
            category,
            trip_df,
            pickup_col,
            dropoff_col,
            pickup_loc,
            dropoff_loc,
            dims,
        )
        fact_df = builder.build()

        if fact_df is None:
            self.logger.warning(
                f"  {category} {year}-{month:02d}: no se pudo construir hecho"
            )
            return

        out_dir = FACTS_DIR / f"fact_{category}_trip"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = storage.for_spark(out_dir / f"{year}-{month:02d}.parquet")
        # fact_df es proyeccion pura de trip_df (sin filtros): mismo numero de
        # filas. Reusar row_count evita un segundo count() sobre 20M filas.
        fact_df.coalesce(target_files(row_count)).write.mode("overwrite").parquet(
            out_path
        )
        self.logger.info(
            f"  fact_{category}_trip/{year}-{month:02d}.parquet: {row_count} filas"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_dim(self, name: str) -> DataFrame:
        path = storage.for_spark(DIMS_DIR / f"{name}.parquet")
        return self.spark.read.parquet(path)

    @staticmethod
    def _first_match(df: DataFrame, candidates: list[str]) -> str | None:
        cols_lower = {c.lower(): c for c in df.columns}
        for c in candidates:
            if c.lower() in cols_lower:
                return cols_lower[c.lower()]
        return None


# ======================================================================
# Fact builder per category
# ======================================================================


class _FactBuilder:
    def __init__(
        self,
        category: str,
        trip_df: DataFrame,
        pickup_col: str | None,
        dropoff_col: str | None,
        pickup_loc: str | None,
        dropoff_loc: str | None,
        dims: dict[str, DataFrame],
    ) -> None:
        self.category = category
        self.df = trip_df
        self.pickup_col = pickup_col
        self.dropoff_col = dropoff_col
        self.pickup_loc = pickup_loc
        self.dropoff_loc = dropoff_loc
        self.dims = dims

    def build(self) -> DataFrame | None:
        builder_name = f"_build_{self.category}"
        method = getattr(self, builder_name, None)
        if method is None:
            return None
        return method()

    def _compute_duration(self, df: DataFrame) -> DataFrame:
        if self.pickup_col is None or self.dropoff_col is None:
            return df.withColumn("trip_duration_minutes", F.lit(None).cast("double"))

        dur = (
            F.unix_timestamp(F.col(self.dropoff_col))
            - F.unix_timestamp(F.col(self.pickup_col))
        ) / 60
        return df.withColumn("trip_duration_minutes", F.round(dur, 2))

    def _add_trip_id(self, df: DataFrame) -> DataFrame:
        """Surrogate key = xxhash64 of the silver composite PK (single source of
        truth in ``SilverCleaner.COMPOSITE_KEYS``). Carried into gold for
        drill-through / dedup across the medallion.

        Se usa ``xxhash64`` (BIGINT de 8 bytes) en vez de ``sha2`` hex (string de
        64 chars). El hash hex es de alta entropia: no se puede codificar por
        diccionario y apenas comprime, por lo que ocupaba ~67% del tamano del
        fact y se propagaba a casi todos los marts. Un BIGINT es ~8x mas pequeno,
        ideal como clave de relacion en Power BI, y determinista (mismo PK ->
        mismo id entre corridas). La unicidad estricta ya la garantiza silver via
        COMPOSITE_KEYS; trip_id es una clave de drill-through, no de deduplicacion
        por join, asi que la probabilidad de colision de 64 bits es aceptable."""
        key_cols = SilverCleaner.COMPOSITE_KEYS.get(self.category, [])
        key_cols = [c for c in key_cols if c in df.columns]
        if not key_cols:
            return df.withColumn("trip_id", F.lit(None).cast("long"))
        concat = F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols],
        )
        return df.withColumn("trip_id", F.xxhash64(concat))

    def _prepare(self) -> DataFrame:
        """Common pre-projection enrichment: trip duration + trip_id."""
        df = self._compute_duration(self.df)
        df = self._add_trip_id(df)
        return df

    def _pickup_ts(self):
        """Standardized pickup timestamp column (heterogeneous source names)."""
        if self.pickup_col:
            return F.col(self.pickup_col).cast("timestamp").alias("pickup_datetime")
        return F.lit(None).cast("timestamp").alias("pickup_datetime")

    def _dropoff_ts(self):
        """Standardized dropoff timestamp column (heterogeneous source names)."""
        if self.dropoff_col:
            return F.col(self.dropoff_col).cast("timestamp").alias("dropoff_datetime")
        return F.lit(None).cast("timestamp").alias("dropoff_datetime")

    @staticmethod
    def _safe_select(df: DataFrame, cols: list) -> DataFrame:
        safe = []
        for c in cols:
            if isinstance(c, str):
                if c in df.columns:
                    safe.append(c)
            else:
                safe.append(c)
        return df.select(*safe)

    # ------------------------------------------------------------------
    # Category-specific builders
    # ------------------------------------------------------------------

    def _build_yellow(self) -> DataFrame:
        df = self._prepare()
        return self._safe_select(
            df,
            [
                F.col("_date_key").alias("date_key"),
                F.col("trip_id"),
                self._pickup_ts(),
                self._dropoff_ts(),
                F.col("VendorID").alias("vendor_id"),
                F.col("RatecodeID").alias("ratecode_id"),
                F.col("payment_type").alias("payment_type_id"),
                F.lit("yellow").alias("service_id"),
                F.col(self.pickup_loc).alias("pickup_location_id")
                if self.pickup_loc
                else F.lit(None).alias("pickup_location_id"),
                F.col(self.dropoff_loc).alias("dropoff_location_id")
                if self.dropoff_loc
                else F.lit(None).alias("dropoff_location_id"),
                "passenger_count",
                "trip_distance",
                "store_and_fwd_flag",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount",
                "congestion_surcharge",
                "airport_fee",
                "cbd_congestion_fee",
                "trip_duration_minutes",
            ],
        )

    def _build_green(self) -> DataFrame:
        df = self._prepare()
        return self._safe_select(
            df,
            [
                F.col("_date_key").alias("date_key"),
                F.col("trip_id"),
                self._pickup_ts(),
                self._dropoff_ts(),
                F.col("VendorID").alias("vendor_id"),
                F.col("RatecodeID").alias("ratecode_id"),
                F.col("payment_type").alias("payment_type_id"),
                F.lit("green").alias("service_id"),
                F.col(self.pickup_loc).alias("pickup_location_id")
                if self.pickup_loc
                else F.lit(None).alias("pickup_location_id"),
                F.col(self.dropoff_loc).alias("dropoff_location_id")
                if self.dropoff_loc
                else F.lit(None).alias("dropoff_location_id"),
                "passenger_count",
                "trip_distance",
                "store_and_fwd_flag",
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "ehail_fee",
                "improvement_surcharge",
                "total_amount",
                "congestion_surcharge",
                "cbd_congestion_fee",
                "trip_type",
                "trip_duration_minutes",
            ],
        )

    def _build_fhvhv(self) -> DataFrame:
        df = self._prepare()
        return self._safe_select(
            df,
            [
                F.col("_date_key").alias("date_key"),
                F.col("trip_id"),
                self._pickup_ts(),
                self._dropoff_ts(),
                F.col(self.pickup_loc).alias("pickup_location_id")
                if self.pickup_loc
                else F.lit(None).alias("pickup_location_id"),
                F.col(self.dropoff_loc).alias("dropoff_location_id")
                if self.dropoff_loc
                else F.lit(None).alias("dropoff_location_id"),
                F.lit("fhvhv").alias("service_id"),
                "hvfhs_license_num",
                "dispatching_base_num",
                "originating_base_num",
                "request_datetime",
                "on_scene_datetime",
                "trip_miles",
                "trip_time",
                "base_passenger_fare",
                "tolls",
                "bcf",
                "sales_tax",
                "congestion_surcharge",
                "airport_fee",
                "cbd_congestion_fee",
                "tips",
                "driver_pay",
                "shared_request_flag",
                "shared_match_flag",
                "access_a_ride_flag",
                "wav_request_flag",
                "wav_match_flag",
                "trip_duration_minutes",
            ],
        )

    def _build_fhv(self) -> DataFrame:
        df = self._prepare()
        return self._safe_select(
            df,
            [
                F.col("_date_key").alias("date_key"),
                F.col("trip_id"),
                self._pickup_ts(),
                self._dropoff_ts(),
                F.col(self.pickup_loc).alias("pickup_location_id")
                if self.pickup_loc
                else F.lit(None).alias("pickup_location_id"),
                F.col(self.dropoff_loc).alias("dropoff_location_id")
                if self.dropoff_loc
                else F.lit(None).alias("dropoff_location_id"),
                F.lit("fhv").alias("service_id"),
                "dispatching_base_num",
                "SR_Flag",
                "Affiliated_base_number",
                "trip_duration_minutes",
            ],
        )
