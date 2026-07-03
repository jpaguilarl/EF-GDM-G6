from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Consistency(Dimension):
    name = "consistency"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        df = df.withColumn("_row_idx", F.monotonically_increasing_id())
        metrics: list[Metric] = []
        total_rows = df.count()

        datetime_cols = self._detect_datetime_columns(df)
        pickups, dropoffs = self._resolve_datetime_pair(datetime_cols, meta.category)

        if pickups and dropoffs:
            df = df.withColumn("_pickup_dt", F.to_timestamp(F.col(pickups)))
            df = df.withColumn("_dropoff_dt", F.to_timestamp(F.col(dropoffs)))

            valid = F.col("_pickup_dt").isNotNull() & F.col("_dropoff_dt").isNotNull()
            inverted = valid & (F.col("_dropoff_dt") < F.col("_pickup_dt"))
            inverted_count = df.filter(inverted).count()
            pct = round(inverted_count / max(total_rows, 1) * 100, 4)

            df = df.withColumn("_failure", F.coalesce(inverted, F.lit(False)))
            metrics.append(
                Metric(
                    name="pickup_before_dropoff_pct",
                    value=round(100 - pct, 2),
                    passed=inverted_count == 0,
                    detail={
                        "inverted_count": inverted_count,
                        "inverted_pct": pct,
                        "pickup_col": pickups,
                        "dropoff_col": dropoffs,
                    },
                )
            )

            if pickups == "request_datetime":
                request_dt = F.to_timestamp(F.col("request_datetime"))
                req_valid = request_dt.isNotNull() & F.col("_pickup_dt").isNotNull()
                req_late = req_valid & (F.col("_pickup_dt") < request_dt)
                req_late_count = df.filter(req_late).count()
                df = df.withColumn(
                    "_failure",
                    F.when(F.coalesce(req_late, F.lit(False)), True).otherwise(
                        F.col("_failure")
                    ),
                )
                metrics.append(
                    Metric(
                        name="request_before_pickup_pct",
                        value=round(
                            (1 - req_late_count / max(total_rows, 1)) * 100, 2
                        ),
                        passed=req_late_count == 0,
                        detail={"request_late_count": req_late_count},
                    )
                )

            duration_minutes = (
                F.unix_timestamp(F.col("_dropoff_dt"))
                - F.unix_timestamp(F.col("_pickup_dt"))
            ) / 60
            too_long = duration_minutes > 1440
            too_long_count = df.filter(
                F.col("_pickup_dt").isNotNull()
                & F.col("_dropoff_dt").isNotNull()
                & too_long
            ).count()
            df = df.withColumn(
                "_failure",
                F.when(
                    F.col("_pickup_dt").isNotNull()
                    & F.col("_dropoff_dt").isNotNull()
                    & too_long,
                    True,
                ).otherwise(F.col("_failure")),
            )
            max_dur = 0
            valid_dur = df.filter(
                F.col("_pickup_dt").isNotNull() & F.col("_dropoff_dt").isNotNull()
            ).agg(F.max(duration_minutes)).collect()[0][0]
            if valid_dur is not None:
                max_dur = round(float(valid_dur), 2)
            metrics.append(
                Metric(
                    name="trip_duration_lt_24h_pct",
                    value=round(
                        (1 - too_long_count / max(total_rows, 1)) * 100, 2
                    ),
                    passed=too_long_count == 0,
                    detail={
                        "too_long_count": too_long_count,
                        "max_duration_minutes": max_dur,
                    },
                )
            )
        else:
            df = df.withColumn("_failure", F.lit(False))

        trip_distance_col = self._find_col(df, ["trip_distance", "trip_miles"])
        if trip_distance_col:
            neg_dist = F.col(trip_distance_col) < 0
            neg_count = df.filter(
                F.col(trip_distance_col).isNotNull() & neg_dist
            ).count()
            df = df.withColumn(
                "_failure",
                F.when(
                    F.col(trip_distance_col).isNotNull() & neg_dist, True
                ).otherwise(F.col("_failure")),
            )
            metrics.append(
                Metric(
                    name=f"{trip_distance_col}_non_negative_pct",
                    value=round((1 - neg_count / max(total_rows, 1)) * 100, 2),
                    passed=neg_count == 0,
                    detail={"negative_count": neg_count},
                )
            )

        passenger_col = self._find_col(df, ["passenger_count"])
        if passenger_col:
            neg_pass = F.col(passenger_col) < 0
            neg_pass_count = df.filter(
                F.col(passenger_col).isNotNull() & neg_pass
            ).count()
            df = df.withColumn(
                "_failure",
                F.when(
                    F.col(passenger_col).isNotNull() & neg_pass, True
                ).otherwise(F.col("_failure")),
            )
            metrics.append(
                Metric(
                    name="passenger_count_non_negative_pct",
                    value=round(
                        (1 - neg_pass_count / max(total_rows, 1)) * 100, 2
                    ),
                    passed=neg_pass_count == 0,
                    detail={"negative_count": neg_pass_count},
                )
            )

        if not metrics:
            score = 1.0
            passed = True
        else:
            scores = [1.0]
            for m in metrics:
                if isinstance(m.value, (int, float)):
                    scores.append(float(m.value) / 100)
            score = round(sum(scores) / len(scores), 4)
            passed = df.filter(F.col("_failure")).count() == 0

        failures_sample = []
        if not passed:
            # Via helper: filas con timestamps pre-1970 (dropoffs "1900-01-01"
            # reales en fhv) rompen el collect crudo en Windows (OSError 22).
            failures_sample = self.collect_sample_as_strings(
                df.filter(F.col("_failure"))
            )

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=metrics
            or [
                Metric(
                    name="consistency_check",
                    value="no_aplica",
                    passed=True,
                    detail={},
                )
            ],
            failures_sample=failures_sample,
        )

    def _detect_datetime_columns(self, df: DataFrame) -> list[str]:
        datetime_patterns = ["datetime", "_datetime", "date", "time"]
        cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(p in col_lower for p in datetime_patterns):
                try:
                    df.select(F.to_timestamp(F.col(col))).limit(100).collect()
                    cols.append(col)
                except Exception:
                    pass
        return cols

    def _resolve_datetime_pair(
        self, cols: list[str], category: str
    ) -> tuple[str | None, str | None]:
        col_lower = [c.lower() for c in cols]
        col_map = {c.lower(): c for c in cols}

        pickup_candidates = [
            "pickup_datetime",
            "lpep_pickup_datetime",
            "tpep_pickup_datetime",
        ]
        dropoff_candidates = [
            "dropoff_datetime",
            "lpep_dropoff_datetime",
            "tpep_dropoff_datetime",
            "dropoff_datetime",
        ]

        pickup = None
        dropoff = None

        for c in pickup_candidates:
            if c in col_lower:
                pickup = col_map[c]
                break

        for c in dropoff_candidates:
            if c in col_lower:
                dropoff = col_map[c]
                break

        return pickup, dropoff

    def _find_col(self, df: DataFrame, candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None
