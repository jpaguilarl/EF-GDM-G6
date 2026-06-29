from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Uniqueness(Dimension):
    name = "uniqueness"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        composite_keys: dict[str, list[str]] = {
            "fhv": [
                "dispatching_base_num",
                "pickup_datetime",
                "dropOff_datetime",
            ],
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

        key_cols = composite_keys.get(meta.category, df.columns[:4])
        available_cols = [c for c in key_cols if c in df.columns]

        if not available_cols:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="uniqueness_check",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": "No hay columnas clave disponibles"
                        },
                    )
                ],
            )

        df = df.withColumn("_row_idx", F.monotonically_increasing_id())

        key_window = Window.partitionBy(*available_cols)
        df = df.withColumn("_dup_count", F.count("*").over(key_window))
        dup_mask = F.col("_dup_count") > 1

        total_rows = df.count()
        dup_count = df.filter(dup_mask).count()
        unique_pct = round((1 - dup_count / max(total_rows, 1)) * 100, 2)

        score = round(unique_pct / 100, 4)
        passed = dup_count == 0

        failures_sample = []
        if dup_count > 0:
            failures_sample = [
                row.asDict()
                for row in df.filter(dup_mask).limit(10).collect()
            ]

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=[
                Metric(
                    name="unique_rows_pct",
                    value=unique_pct,
                    passed=passed,
                    detail={
                        "total_rows": total_rows,
                        "duplicated_rows": dup_count,
                        "key_columns": available_cols,
                    },
                )
            ],
            failures_sample=failures_sample,
        )
