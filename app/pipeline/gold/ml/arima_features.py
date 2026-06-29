"""D3.1 — Feature store ARIMA: serie temporal univariada de viajes por borough x hora."""

from pyspark.sql import functions as F

from app.pipeline.gold.feature_rules import time_blocks as tb
from app.pipeline.gold.mart_builder import PU_LOC, GoldBuilder, GoldContext


class ArimaFeatures(GoldBuilder):
    name = "ml_feat_arima_trips"
    subdir = "ml"
    partition_keys = ["borough", "year", "month"]

    def build(self, ctx: GoldContext) -> int:
        def select_fn(fact, category):
            if PU_LOC not in fact.columns:
                return None
            return fact.select(
                F.col("pickup_datetime"),
                F.col(PU_LOC).alias("location_id"),
                F.col("service_id"),
            )

        df = ctx.read_union(select_fn)
        if df is None:
            self.logger.warning(f"  {self.name}: sin facts disponibles")
            return -1
        df = df.filter(F.col("pickup_datetime").isNotNull())

        zone_dim = ctx.gold_dims["zone"].select(
            F.col("LocationID").alias("_zid"), F.col("Borough").alias("borough")
        )
        df = (
            df.join(F.broadcast(zone_dim), F.col("location_id") == F.col("_zid"), "left")
            .drop("_zid")
            .withColumn("borough", F.coalesce(F.col("borough"), F.lit("Unknown")))
            .withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_datetime")))
        )

        agg = df.groupBy("borough", "service_id", "pickup_hour").agg(
            F.count(F.lit(1)).alias("trip_count")
        )

        dow = tb.iso_weekday(F.col("pickup_hour"))
        agg = (
            agg.withColumn("hour_of_day", F.hour("pickup_hour"))
            .withColumn("dow", dow)
            .withColumn("is_weekend", dow >= 6)
            .withColumn(
                "date_key",
                F.year("pickup_hour") * 10000
                + F.month("pickup_hour") * 100
                + F.dayofmonth("pickup_hour"),
            )
        )

        date_dim = ctx.gold_dims["date"].select(
            F.col("date_key").alias("_dk"), F.col("is_holiday")
        )
        agg = (
            agg.join(F.broadcast(date_dim), F.col("date_key") == F.col("_dk"), "left")
            .drop("_dk")
            .withColumn("is_holiday", F.coalesce(F.col("is_holiday"), F.lit(False)))
        )

        out = agg.select(
            F.col("borough"),
            F.col("service_id"),
            F.col("pickup_hour"),
            F.col("trip_count"),
            F.col("hour_of_day"),
            F.col("dow"),
            F.col("is_weekend"),
            F.col("is_holiday"),
            F.year("pickup_hour").alias("year"),
            F.month("pickup_hour").alias("month"),
        )
        n = out.count()
        self._write(out)
        self.logger.info(f"  {self.name}: {n} filas (serie borough x hora)")
        return n
