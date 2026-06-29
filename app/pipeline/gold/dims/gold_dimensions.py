"""Dimensiones gold: enriquecen las dimensiones silver para los dashboards.

- ``dim_date_gold``: ``dim_date`` + ``dia_categoria`` + ``is_holiday``.
- ``dim_zone_gold``: ``dim_zone`` + ``borough_name_es`` (Power BI en español).
- ``dim_ratecode_theoretical``: tarifa teorica por RatecodeID + año fiscal (D3.3).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from app.pipeline.gold.feature_rules import ratecode_tariff as rt
from app.pipeline.gold.feature_rules import time_blocks as tb
from app.pipeline.gold.mart_builder import GOLD_DIMS_DIR, SILVER_DIMS_DIR
from app.utils.logger import Logger

# date_keys (YYYYMMDD) de feriados federales observados en NYC, 2023-2025.
# Alimenta is_holiday (variable exogena para ARIMA y filtros de dashboards).
HOLIDAY_DATE_KEYS: set[int] = {
    # 2023
    20230101, 20230116, 20230220, 20230529, 20230619, 20230704,
    20230904, 20231009, 20231111, 20231123, 20231225,
    # 2024
    20240101, 20240115, 20240219, 20240527, 20240619, 20240704,
    20240902, 20241014, 20241111, 20241128, 20241225,
    # 2025
    20250101, 20250120, 20250217, 20250526, 20250619, 20250704,
    20250901, 20251013, 20251111, 20251127, 20251225,
}

# Borough (ingles, fuente TLC) -> nombre para Power BI en español.
BOROUGH_ES = {
    "Bronx": "El Bronx",
    "EWR": "Newark (EWR)",
    "Unknown": "Desconocido",
    "N/A": "Desconocido",
}


class GoldDimensionsBuilder:
    def __init__(self, spark, logger: Logger | None = None) -> None:
        self.spark = spark
        self.logger = logger or Logger()

    def build_all(self) -> dict[str, DataFrame]:
        GOLD_DIMS_DIR.mkdir(parents=True, exist_ok=True)
        dims = {
            "date": self._build_dim_date_gold(),
            "zone": self._build_dim_zone_gold(),
            "ratecode": self._build_dim_ratecode_theoretical(),
        }
        return dims

    # ------------------------------------------------------------------
    def _build_dim_date_gold(self) -> DataFrame:
        src = SILVER_DIMS_DIR / "dim_date.parquet"
        df = self.spark.read.parquet(str(src))
        df = df.withColumn("dia_categoria", tb.dia_categoria(F.col("weekday")))
        df = df.withColumn(
            "is_holiday", F.col("date_key").isin(list(HOLIDAY_DATE_KEYS))
        )
        self._write(df, "dim_date_gold")
        self.logger.info(f"  dim_date_gold: {df.count()} registros")
        return df

    def _build_dim_zone_gold(self) -> DataFrame:
        src = SILVER_DIMS_DIR / "dim_zone.parquet"
        df = self.spark.read.parquet(str(src))

        borough_es = F.col("Borough")
        for eng, es in BOROUGH_ES.items():
            borough_es = F.when(F.col("Borough") == eng, F.lit(es)).otherwise(
                borough_es
            )
        df = df.withColumn("borough_name_es", borough_es)
        self._write(df, "dim_zone_gold")
        self.logger.info(f"  dim_zone_gold: {df.count()} registros")
        return df

    def _build_dim_ratecode_theoretical(self) -> DataFrame:
        schema = StructType(
            [
                StructField("ratecode_id", IntegerType(), False),
                StructField("fiscal_year", IntegerType(), False),
                StructField("flat_fare", DoubleType(), True),
                StructField("ratecode_name", StringType(), False),
            ]
        )
        df = self.spark.createDataFrame(rt.flat_fare_rows(), schema)
        self._write(df, "dim_ratecode_theoretical")
        self.logger.info(f"  dim_ratecode_theoretical: {df.count()} registros")
        return df

    # ------------------------------------------------------------------
    def _write(self, df: DataFrame, name: str) -> None:
        path = str(GOLD_DIMS_DIR / f"{name}.parquet")
        df.coalesce(1).write.mode("overwrite").parquet(path)
