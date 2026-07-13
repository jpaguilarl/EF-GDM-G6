from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.profiling.rules.nullability import NULLABLE_COLUMNS


class SilverCleaner:
    PICKUP_CANDIDATES = [
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "pickup_datetime",
    ]
    DROPOFF_CANDIDATES = [
        "tpep_dropoff_datetime",
        "lpep_dropoff_datetime",
        "dropoff_datetime",
        "dropOff_datetime",
    ]
    PU_CANDIDATES = ["PULocationID", "PUlocationID"]
    DO_CANDIDATES = ["DOLocationID", "DOlocationID"]

    COMPOSITE_KEYS = {
        "fhv": ["dispatching_base_num", "pickup_datetime", "dropOff_datetime"],
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

    def __init__(self, spark_session) -> None:
        self.spark = spark_session
        self._cached: list[DataFrame] = []

    def clean(
        self,
        df: DataFrame,
        category: str,
        year: int,
        month: int,
        zone_ids: set[int],
    ) -> tuple[DataFrame, DataFrame]:
        df = df.withColumn("_row_idx", F.monotonically_increasing_id())
        df = df.withColumn("_reject_reason", F.lit(None).cast("string"))

        # -- Rejection phase (factual correctness + incompleteness only) --
        df = self._reject_incomplete(df, category)
        df = self._reject_timeliness(df, category, year, month)
        df = self._reject_datetime_invalid(df, category)
        df = self._reject_integrity(df, zone_ids)
        df = self._reject_uniqueness(df, category)

        df = df.drop("_pickup_dt", "_dropoff_dt")

        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        self._cached.append(df)

        reject_df = df.filter(F.col("_reject_reason").isNotNull())
        clean_df = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")

        # -- Normalize types (no value changes, just schema consistency) --
        clean_df = self._normalize_types(clean_df)

        helper_cols = ["_row_idx", "_dup_count", "_dup_rank"]
        clean_df = clean_df.drop(*helper_cols)
        reject_df = reject_df.drop(*helper_cols)

        return clean_df, reject_df

    def cleanup(self) -> None:
        for cached in self._cached:
            try:
                cached.unpersist()
            except Exception:
                pass
        self._cached.clear()

    # ------------------------------------------------------------------
    # Rejection helpers
    # ------------------------------------------------------------------

    def _reject_incomplete(self, df: DataFrame, category: str) -> DataFrame:
        """Reject rows with null values in required columns.

        Required columns = all columns in the DataFrame minus those listed in
        ``NULLABLE_COLUMNS`` for this category.  Internal helper columns
        (prefixed with ``_``) are excluded from the check.
        """
        nullable_set = NULLABLE_COLUMNS.get(category, set())
        required_cols = [
            c
            for c in df.columns
            if c not in nullable_set and not c.startswith("_")
        ]

        already = F.col("_reject_reason").isNotNull()
        for col_name in required_cols:
            has_null = F.col(col_name).isNull()
            df = df.withColumn(
                "_reject_reason",
                F.when(has_null & ~already, F.lit(f"incomplete_{col_name}")).otherwise(
                    F.col("_reject_reason")
                ),
            )
            already = F.col("_reject_reason").isNotNull()
        return df

    def _reject_timeliness(
        self, df: DataFrame, category: str, year: int, month: int
    ) -> DataFrame:
        pickup_col = self._first_match(df, self.PICKUP_CANDIDATES)
        if pickup_col is None:
            return df

        expected = (F.year(F.col(pickup_col)) == year) & (
            F.month(F.col(pickup_col)) == month
        )
        off_period = F.col(pickup_col).isNotNull() & ~expected
        already = F.col("_reject_reason").isNotNull()
        return df.withColumn(
            "_reject_reason",
            F.when(off_period & ~already, F.lit("timeliness_off_period")).otherwise(
                F.col("_reject_reason")
            ),
        )

    def _reject_datetime_invalid(self, df: DataFrame, category: str) -> DataFrame:
        """Reject rows where dropoff is strictly before pickup (inverted dates)."""
        pickup_col = self._first_match(df, self.PICKUP_CANDIDATES)
        dropoff_col = self._first_match(df, self.DROPOFF_CANDIDATES)
        if pickup_col is None or dropoff_col is None:
            return df

        already = F.col("_reject_reason").isNotNull()
        df = df.withColumn("_pickup_dt", F.to_timestamp(F.col(pickup_col)))
        df = df.withColumn("_dropoff_dt", F.to_timestamp(F.col(dropoff_col)))
        both_valid = F.col("_pickup_dt").isNotNull() & F.col("_dropoff_dt").isNotNull()
        inverted = both_valid & (F.col("_dropoff_dt") < F.col("_pickup_dt"))

        return df.withColumn(
            "_reject_reason",
            F.when(
                inverted & ~already, F.lit("datetime_inverted")
            ).otherwise(F.col("_reject_reason")),
        )

    def _reject_integrity(self, df: DataFrame, zone_ids: set[int]) -> DataFrame:
        pu_col = self._first_match(df, self.PU_CANDIDATES)
        do_col = self._first_match(df, self.DO_CANDIDATES)
        if pu_col is None and do_col is None:
            return df

        zone_list = list(zone_ids)
        for col_name in [pu_col, do_col]:
            if col_name is None:
                continue
            invalid = F.col(col_name).isNotNull() & ~F.col(col_name).isin(zone_list)
            already = F.col("_reject_reason").isNotNull()
            df = df.withColumn(
                "_reject_reason",
                F.when(
                    invalid & ~already, F.lit(f"integrity_invalid_{col_name}")
                ).otherwise(F.col("_reject_reason")),
            )
        return df

    def _reject_uniqueness(self, df: DataFrame, category: str) -> DataFrame:
        key_cols = self.COMPOSITE_KEYS.get(category, [])
        key_cols = [c for c in key_cols if c in df.columns]
        if not key_cols:
            return df

        window = Window.partitionBy(*key_cols)
        df = df.withColumn("_dup_count", F.count("*").over(window))

        order_cols = []
        for c in ["total_amount", "driver_pay"]:
            if c in df.columns:
                order_cols.append(F.col(c).desc_nulls_last())
        order_cols.append(F.col("_row_idx").asc())

        df = df.withColumn(
            "_dup_rank",
            F.row_number().over(Window.partitionBy(*key_cols).orderBy(*order_cols)),
        )

        already = F.col("_reject_reason").isNotNull()
        is_dup = (F.col("_dup_count") > 1) & (F.col("_dup_rank") > 1)
        return df.withColumn(
            "_reject_reason",
            F.when(is_dup & ~already, F.lit("uniqueness_duplicate")).otherwise(
                F.col("_reject_reason")
            ),
        )

    # ------------------------------------------------------------------
    # Type normalization (no value changes)
    # ------------------------------------------------------------------

    def _normalize_types(self, df: DataFrame) -> DataFrame:
        """Cast code columns to int for schema consistency downstream."""
        code_columns = ["VendorID", "RatecodeID", "payment_type", "passenger_count"]
        for col_name in code_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("int"))
        return df

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _first_match(df: DataFrame, candidates: list[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None
