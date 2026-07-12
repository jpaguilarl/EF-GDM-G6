import concurrent.futures
import gc
import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from app.profiling.dataset_profiler import DatasetProfiler
from app.profiling.reporter import Reporter
from app.profiling.schemas.profiling_schema import ProfilingReport
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils import storage
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient


class ProfilingPipeline:
    def __init__(self, output_dir: str | Path | None = None) -> None:
        self.logger = Logger()
        self.spark = SparkClient()
        self.profiler = DatasetProfiler(self.spark)
        self.reporter = Reporter(output_dir=output_dir)
        self.zone_ids: set[int] = set()
        self.dicts: dict[str, DataFrame] = {}

    # 3 perfiles en paralelo: el tiempo por archivo esta dominado por el
    # overhead de scheduling de Spark (8 dimensiones x varias acciones = ~100
    # jobs pequenos por archivo; medido: ~2 min incluso en green con ~65k
    # filas), no por el volumen de datos. Solapar perfiles llena esos huecos
    # con los task slots de local[6]. Misma estrategia que silver/star.
    MAX_PARALLEL_PROFILES = 3

    async def run(self, year_span: DatasetsConfig) -> None:
        self.logger.info("Iniciando pipeline de profiling (8 dimensiones)")

        self._load_zone_lookup()
        self._load_data_dictionaries()

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

        # NO llamar catalog.clearCache() entre perfiles: es global y
        # despersistiria los df cacheados de los perfiles concurrentes. Cada
        # perfil ya libera su propio cache (unpersist en DatasetProfiler).
        all_reports: list[ProfilingReport] = []

        def _run_pool(pool_tasks: list[tuple[str, int, int]], max_w: int) -> None:
            if not pool_tasks:
                return
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
                futures = [
                    executor.submit(self._profile_one, cat, y, m)
                    for (cat, y, m) in pool_tasks
                ]
                for future in concurrent.futures.as_completed(futures):
                    report = future.result()
                    if report:
                        all_reports.append(report)

        if heavy_tasks:
            self.logger.info("Perfilando datasets pesados (fhvhv, yellow) secuencialmente")
            _run_pool(heavy_tasks, 1)

        if light_tasks:
            self.logger.info("Perfilando datasets livianos (green, fhv) en paralelo")
            _run_pool(light_tasks, 3)

        gc.collect()

        # as_completed devuelve en orden de finalizacion; ordenar para que el
        # index.html quede estable por categoria/mes.
        all_reports.sort(key=lambda r: (r.meta.category, r.meta.year, r.meta.month))

        if all_reports:
            self.reporter.build_index_html(all_reports)
            self.logger.info(
                f"Pipeline de profiling completado: {len(all_reports)} datasets procesados"
            )
        else:
            self.logger.warning(
                "No se encontraron datasets para perfilar en data/bronze/"
            )

    def _profile_one(
        self, category: str, year: int, month: int
    ) -> ProfilingReport | None:
        file_path = storage.data_path("bronze", category, f"{year}-{month:02d}.parquet")

        if not file_path.exists():
            self.logger.warning(f"Dataset no encontrado, se omite: {file_path}")
            return None

        # Idempotencia: si el reporte JSON de este dataset ya existe, se reusa
        # (profiling es solo-lectura y deterministico sobre el mismo bronce).
        # Se devuelve el reporte cargado para que el index.html lo incluya.
        # Para re-perfilar un dataset, borrar su JSON en data/profiling/.
        report_json = self.reporter.output_dir / category / f"{year}-{month:02d}.json"
        if report_json.exists():
            try:
                report = ProfilingReport(
                    **json.loads(report_json.read_text(encoding="utf-8"))
                )
                self.logger.info(f"Perfil ya existe, se reutiliza: {report_json}")
                return report
            except Exception:
                self.logger.warning(
                    f"Perfil existente ilegible, se re-perfila: {report_json}"
                )

        empty_schema = StructType([
            StructField("nombre_campo", StringType()),
            StructField("tipo_dato", StringType()),
            StructField("valor", StringType()),
        ])
        dict_df = self.dicts.get(
            category,
            self.spark.get_session().createDataFrame([], empty_schema),
        )

        try:
            report = self.profiler.profile(
                file_path=file_path,
                category=category,
                year=year,
                month=month,
                dict_df=dict_df,
                zone_ids=self.zone_ids,
            )
            self.reporter.write_json(report)
            return report
        except Exception as e:
            self.logger.critical(f"Error critico perfilando {file_path}: {e}")
            return None

    def _load_zone_lookup(self) -> None:
        zone_path = storage.data_path("bronze", "zone-lookup", "zone-lookup-table.parquet")
        if not zone_path.exists():
            self.logger.warning(
                "Tabla de zonas no encontrada, integridad referencial omitida"
            )
            return

        df = self.spark.get_session().read.parquet(storage.for_spark(zone_path))
        ids = (
            df.filter(F.col("LocationID").isNotNull())
            .select("LocationID")
            .distinct()
            .collect()
        )
        self.zone_ids = set(int(row["LocationID"]) for row in ids)
        self.logger.info(f"Zonas cargadas: {len(self.zone_ids)} LocationIDs validos")

    def _load_data_dictionaries(self) -> None:
        dict_dir = storage.data_path("bronze", "dicts")
        if not dict_dir.exists():
            self.logger.warning("Directorio de diccionarios no encontrado")
            return

        for cat in globals.tlc_categories:
            dict_path = dict_dir / f"data_dictionary_trip_records_{cat}.parquet"
            if dict_path.exists():
                self.dicts[cat] = self.spark.get_session().read.parquet(
                    storage.for_spark(dict_path)
                )
                count = self.dicts[cat].count()
                self.logger.info(
                    f"Diccionario cargado: {cat} ({count} entradas)"
                )
            else:
                self.logger.warning(f"Diccionario no encontrado para {cat}")
