from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Timeliness(Dimension):
    name = "timeliness"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        pickup_candidates = [
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "pickup_datetime",
        ]
        pickup_col = None
        for c in pickup_candidates:
            if c in df.columns:
                pickup_col = c
                break

        if pickup_col is None:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="timeliness_check",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": "No se encontro columna de pickup datetime"
                        },
                    )
                ],
            )

        pickup_dt = F.to_timestamp(F.col(pickup_col))
        valid = pickup_dt.isNotNull()
        pickup_valid = df.filter(valid).select(pickup_dt.alias("_pickup_ts"))

        total_valid = pickup_valid.count()

        if total_valid == 0:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="timeliness_check",
                        value="no_aplica",
                        passed=True,
                        detail={"reason": "No hay pickups validos"},
                    )
                ],
            )

        expected_year = meta.year
        expected_month = meta.month

        same_month = (F.year(F.col("_pickup_ts")) == expected_year) & (
            F.month(F.col("_pickup_ts")) == expected_month
        )
        off_period_count = pickup_valid.filter(~same_month).count()

        match_pct = round(
            (1 - off_period_count / max(total_valid, 1)) * 100, 2
        )

        top_off = []
        if off_period_count > 0:
            off_periods = (
                pickup_valid.filter(~same_month)
                .withColumn("_period", F.date_format(F.col("_pickup_ts"), "yyyy-MM"))
                .groupBy("_period")
                .count()
                .orderBy(F.desc("count"))
                .limit(5)
                .collect()
            )
            top_off = sorted(
                [
                    {"period": row["_period"], "count": row["count"]}
                    for row in off_periods
                ],
                key=lambda x: x["count"],
                reverse=True,
            )

        score = round(match_pct / 100, 4)
        passed = off_period_count == 0

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=[
                Metric(
                    name="pickup_in_period_pct",
                    value=match_pct,
                    passed=passed,
                    detail={
                        "expected": f"{expected_year}-{expected_month:02d}",
                        "total_valid_pickups": total_valid,
                        "off_period_count": off_period_count,
                        "top_off_periods": top_off[:5],
                        "pickup_col": pickup_col,
                    },
                )
            ],
            failures_sample=top_off[:10],
        )
