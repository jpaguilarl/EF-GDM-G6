from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

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
