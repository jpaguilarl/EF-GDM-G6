"""D2.3 — Mart de Comportamiento de Propinas (tabla ancha, 1 fila por viaje)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.feature_rules import generosity
from app.pipeline.gold.mart_builder import (
    DO_LOC,
    PU_LOC,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)


class TippingBehaviorMart(TripGrainMart):
    name = "mart_tipping_behavior"
    subdir = "marts"
    applies_to = {"yellow", "green", "fhvhv"}  # fhv no tiene tarifa/propina

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        zone = ctx.gold_dims["zone"]
        df = with_zone(fact, zone, PU_LOC, "pu")
        df = with_zone(df, zone, DO_LOC, "do")
        cfg = ctx.config.generosity
        null_d = F.lit(None).cast("double")

        payment = col_or_null(df, "payment_type_id", "int")
        is_credit = (
            F.when(payment == 1, F.lit(True))
            .when(payment.isNotNull(), F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )

        if category in ("yellow", "green"):
            fare = col_or_null(df, "fare_amount")
            propina = col_or_null(df, "tip_amount")
            # Solo tarjeta (payment_type==1): el efectivo no registra propina y
            # distorsionaria el analisis (D2.3).
            pct = F.when(
                (payment == 1) & (fare > 0), F.round(propina / fare * 100, 2)
            ).otherwise(null_d)
            miles = col_or_null(df, "trip_distance")
        elif category == "fhvhv":
            bpf = col_or_null(df, "base_passenger_fare")
            propina = col_or_null(df, "tips")
            pct = F.when(bpf > 0, F.round(propina / bpf * 100, 2)).otherwise(null_d)
            miles = col_or_null(df, "trip_miles")
        else:
            propina = null_d
            pct = null_d
            miles = null_d

        ppm = F.when(miles > 0, F.round(propina / miles, 4)).otherwise(null_d)
        categoria = generosity.categoria_generosidad(
            pct, cfg.standard_low, cfg.standard_high
        )

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            F.col("date_key"),
            F.col("pu_borough"),
            F.col("do_borough"),
            payment.alias("payment_type_id"),
            is_credit.alias("is_credit_card"),
            col_or_null(df, "fare_amount").alias("fare_amount"),
            col_or_null(df, "base_passenger_fare").alias("base_passenger_fare"),
            col_or_null(df, "tip_amount").alias("tip_amount"),
            col_or_null(df, "tips").alias("tips"),
            col_or_null(df, "trip_distance").alias("trip_distance"),
            col_or_null(df, "trip_miles").alias("trip_miles"),
            pct.alias("porcentaje_propina"),
            ppm.alias("propina_por_milla"),
            categoria.alias("categoria_generosidad"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
