from app.pipeline.silver_impl.cleaner import SilverCleaner
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils import storage
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient, target_files


import polars as pl
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


import concurrent.futures
import threading
import uuid
from datetime import datetime


class SilverPipeline:
    MAX_PARALLEL_FILES = 2

    def __init__(self) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
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
                if year.month is not None:
                    tasks.append((year.category, year.year, year.month))
                else:
                    for m in range(1, 13):
                        tasks.append((year.category, year.year, m))
        return tasks

    def run_schema(self) -> None:
        from app.pipeline.silver_impl.star import StarSchemaBuilder

        spark = self.spark_client.get_session()
        self.logger.info("Construyendo tablas de dimension del modelo estrella")
        builder = StarSchemaBuilder(spark)
        builder.build_dimensions()
        self.logger.info(
            "Dimensiones del modelo estrella creadas en data/silver/star/dims/"
        )

    def run_load(self, year_span: DatasetsConfig) -> None:
        from app.pipeline.silver_impl.star import StarSchemaBuilder

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

        if not full_path.exists():
            self.logger.info(f"  {source}: bronce no existe, se omite")
            return

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
        with self._audit_lock:
            df_new = pl.DataFrame([row])
            if audit_path.exists():
                existing = pl.read_parquet(str(audit_path))
                df_new = pl.concat([existing, df_new])
            df_new.write_parquet(str(audit_path))