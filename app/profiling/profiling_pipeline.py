import gc
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from app.profiling.dataset_profiler import DatasetProfiler
from app.profiling.reporter import Reporter
from app.profiling.schemas.profiling_schema import ProfilingReport
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient


class ProfilingPipeline:
    def __init__(self, output_dir: str = "data/profiling") -> None:
        self.logger = Logger()
        self.spark = SparkClient()
        self.profiler = DatasetProfiler(self.spark)
        self.reporter = Reporter(output_dir=output_dir)
        self.zone_ids: set[int] = set()
        self.dicts: dict[str, DataFrame] = {}

    async def run(self, year_span: DatasetsConfig) -> None:
        self.logger.info("Iniciando pipeline de profiling (8 dimensiones)")

        self._load_zone_lookup()
        self._load_data_dictionaries()

        all_reports: list[ProfilingReport] = []

        for year in year_span.years:
            if isinstance(year, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        report = self._profile_one(cat, year, m)
                        if report:
                            all_reports.append(report)
                        self._free_memory()
            elif isinstance(year, Module):
                for m in range(1, 13):
                    report = self._profile_one(year.category, year.year, m)
                    if report:
                        all_reports.append(report)
                    self._free_memory()

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
        file_path = Path(f"data/bronze/{category}/{year}-{month:02d}.parquet")

        if not file_path.exists():
            self.logger.warning(f"Dataset no encontrado, se omite: {file_path}")
            return None

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

    def _free_memory(self) -> None:
        """Libera DataFrames cacheados en la JVM y fuerza GC de Python entre archivos.

        Evita la acumulacion de memoria del catalogo de Spark a lo largo del loop.
        """
        try:
            self.spark.get_session().catalog.clearCache()
        except Exception:
            pass
        gc.collect()

    def _load_zone_lookup(self) -> None:
        zone_path = Path("data/bronze/zone-lookup/zone-lookup-table.parquet")
        if not zone_path.exists():
            self.logger.warning(
                "Tabla de zonas no encontrada, integridad referencial omitida"
            )
            return

        df = self.spark.get_session().read.parquet(str(zone_path))
        ids = (
            df.filter(F.col("LocationID").isNotNull())
            .select("LocationID")
            .distinct()
            .collect()
        )
        self.zone_ids = set(int(row["LocationID"]) for row in ids)
        self.logger.info(f"Zonas cargadas: {len(self.zone_ids)} LocationIDs validos")

    def _load_data_dictionaries(self) -> None:
        dict_dir = Path("data/bronze/dicts")
        if not dict_dir.exists():
            self.logger.warning("Directorio de diccionarios no encontrado")
            return

        for cat in globals.tlc_categories:
            dict_path = dict_dir / f"data_dictionary_trip_records_{cat}.parquet"
            if dict_path.exists():
                self.dicts[cat] = self.spark.get_session().read.parquet(
                    str(dict_path)
                )
                count = self.dicts[cat].count()
                self.logger.info(
                    f"Diccionario cargado: {cat} ({count} entradas)"
                )
            else:
                self.logger.warning(f"Diccionario no encontrado para {cat}")
