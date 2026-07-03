from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.profiling.dimensions.accuracy import Accuracy
from app.profiling.dimensions.base import Dimension
from app.profiling.dimensions.completeness import Completeness
from app.profiling.dimensions.consistency import Consistency
from app.profiling.dimensions.integrity import Integrity
from app.profiling.dimensions.reasonableness import Reasonableness
from app.profiling.dimensions.timeliness import Timeliness
from app.profiling.dimensions.uniqueness import Uniqueness
from app.profiling.dimensions.validity import Validity
from app.profiling.schemas.profiling_schema import DatasetMeta, ProfilingReport
from app.utils.logger import Logger
from app.utils.spark import SparkClient


class DatasetProfiler:
    def __init__(self, spark_client: SparkClient) -> None:
        self.logger = Logger()
        self.spark = spark_client
        self.dimensions: list[Dimension] = [
            Accuracy(),
            Completeness(),
            Consistency(),
            Integrity(),
            Reasonableness(),
            Timeliness(),
            Uniqueness(),
            Validity(),
        ]

    def profile(
        self,
        file_path: Path,
        category: str,
        year: int,
        month: int,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> ProfilingReport:
        self.logger.info(f"Perfilando dataset: {category} {year}-{month:02d}")

        pf = pq.ParquetFile(file_path)
        rowcount = pf.metadata.num_rows
        columns = pf.schema_arrow.names

        session = self.spark.get_session()
        df = session.read.parquet(str(file_path))
        # Una sola lectura fisica por archivo: cada una de las 8 dimensiones
        # dispara sus propias acciones y sin persist el parquet se releia >=8
        # veces desde disco (dominaba el tiempo de profiling en HDD). Lo que no
        # cabe en heap derrama a spark.local.dir; se libera en el finally.
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        time_span = self._extract_time_span(df, category)

        meta = DatasetMeta(
            name=f"{category}_{year}-{month:02d}",
            category=category,
            year=year,
            month=month,
            rowcount=rowcount,
            columns=columns,
            time_span=time_span,
            file_path=str(file_path),
            generated_at=datetime.now().isoformat(),
        )

        dimension_results = []
        for dim in self.dimensions:
            try:
                result = dim.evaluate(df, meta, dict_df, zone_ids)
                dimension_results.append(result)
                status = "APROBADA" if result.passed else "FALLIDA"
                self.logger.info(
                    f"  Dimension {dim.name}: score={result.score:.4f} - {status}"
                )
            except Exception as e:
                self.logger.error(f"  Error en dimension {dim.name}: {e}")
                from app.profiling.schemas.profiling_schema import DimensionResult

                dimension_results.append(
                    DimensionResult(
                        dimension=dim.name,
                        score=0.0,
                        passed=False,
                        metrics=[],
                        failures_sample=[{"error": str(e)}],
                    )
                )

        # Las dimensiones son los unicos consumidores del df cacheado; si una
        # falla, el except del loop ya lo capturo y aqui se libera igual. Un
        # fallo antes del loop lo limpia _free_memory (clearCache) del pipeline.
        df.unpersist()

        overall_score = round(
            sum(d.score for d in dimension_results) / max(len(dimension_results), 1), 4
        )

        report = ProfilingReport(
            meta=meta,
            dimensions=dimension_results,
            overall_score=overall_score,
        )

        summary = "APROBADO" if all(d.passed for d in dimension_results) else "FALLIDO"
        self.logger.info(
            f"Perfilado completado: {meta.name} — score global: {overall_score:.4f} — {summary}"
        )

        return report

    def _extract_time_span(
        self, df: DataFrame, category: str
    ) -> tuple[str, str] | None:
        pickup_candidates = [
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "pickup_datetime",
            "request_datetime",
        ]
        pickup_col = None
        for c in pickup_candidates:
            if c in df.columns:
                pickup_col = c
                break

        if pickup_col is None:
            return None

        ts_col = F.to_timestamp(F.col(pickup_col))
        stats = df.filter(ts_col.isNotNull()).agg(
            F.min(ts_col).alias("_min"), F.max(ts_col).alias("_max")
        ).collect()[0]

        if stats["_min"] is None:
            return None

        return (str(stats["_min"]), str(stats["_max"]))
