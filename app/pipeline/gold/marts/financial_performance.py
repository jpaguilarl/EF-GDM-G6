"""D1.2 — Mart de Rendimiento Financiero (tabla ancha, 1 fila por viaje)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.mart_builder import (
    GoldContext,
    TripGrainMart,
    col_or_null,
)


class FinancialPerformanceMart(TripGrainMart):
    name = "mart_financial_performance"
    subdir = "marts"
    applies_to = {"yellow", "green", "fhvhv"}  # fhv no tiene datos de tarifa

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        df = fact
        null_d = F.lit(None).cast("double")

        if category in ("yellow", "green"):
            dist = col_or_null(df, "trip_distance")
            total = col_or_null(df, "total_amount")
            ingreso = F.when(dist > 0, F.round(total / dist, 4)).otherwise(null_d)
            margen = null_d
            ratio = null_d
        elif category == "fhvhv":
            miles = col_or_null(df, "trip_miles")
            bpf = col_or_null(df, "base_passenger_fare")
            driver = col_or_null(df, "driver_pay")
            ingreso = F.when(miles > 0, F.round(bpf / miles, 4)).otherwise(null_d)
            margen = F.round(bpf - driver, 2)  # ganancia bruta de la plataforma
            ratio = F.when(bpf > 0, F.round(driver / bpf, 4)).otherwise(null_d)
        else:
            ingreso = margen = ratio = null_d

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            F.col("date_key"),
            F.col("pickup_datetime"),
            # --- componentes tarifa taxis (yellow/green) ---
            col_or_null(df, "fare_amount").alias("fare_amount"),
            col_or_null(df, "extra").alias("extra"),
            col_or_null(df, "mta_tax").alias("mta_tax"),
            col_or_null(df, "tip_amount").alias("tip_amount"),
            col_or_null(df, "tolls_amount").alias("tolls_amount"),
            col_or_null(df, "improvement_surcharge").alias("improvement_surcharge"),
            col_or_null(df, "congestion_surcharge").alias("congestion_surcharge"),
            col_or_null(df, "airport_fee").alias("airport_fee"),
            col_or_null(df, "ehail_fee").alias("ehail_fee"),
            col_or_null(df, "total_amount").alias("total_amount"),
            # --- componentes HVFHV ---
            col_or_null(df, "base_passenger_fare").alias("base_passenger_fare"),
            col_or_null(df, "tolls").alias("tolls"),
            col_or_null(df, "bcf").alias("bcf"),
            col_or_null(df, "sales_tax").alias("sales_tax"),
            col_or_null(df, "tips").alias("tips"),
            col_or_null(df, "driver_pay").alias("driver_pay"),
            # --- distancia ---
            col_or_null(df, "trip_distance").alias("trip_distance"),
            col_or_null(df, "trip_miles").alias("trip_miles"),
            # --- computadas ---
            ingreso.alias("ingreso_bruto_por_milla"),
            margen.alias("margen_plataforma"),
            ratio.alias("ratio_pago_conductor"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
