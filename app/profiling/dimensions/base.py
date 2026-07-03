from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult


class Dimension(ABC):
    name: str

    @abstractmethod
    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult: ...

    @staticmethod
    def collect_sample_as_strings(df: DataFrame, limit: int = 10) -> list[dict]:
        """Colecta una muestra de filas con TODAS las columnas como string.

        Colectar filas crudas revienta en Windows cuando hay timestamps
        pre-1970 (p.ej. dropoffs "1900-01-01" en fhv 2025-01): PySpark los
        convierte con datetime.fromtimestamp(epoch negativo) -> OSError 22.
        La muestra solo alimenta el reporte JSON del profiling, asi que
        string basta y ademas es JSON-serializable directo.
        """
        stringified = df.select(
            [F.col(c).cast("string").alias(c) for c in df.columns]
        )
        return [row.asDict() for row in stringified.limit(limit).collect()]
