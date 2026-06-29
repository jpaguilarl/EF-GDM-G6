"""D1.1 — Mart de Volumen y Demanda (tabla ancha, 1 fila por viaje)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.feature_rules import time_blocks as tb
from app.pipeline.gold.mart_builder import (
    DO_LOC,
    PU_LOC,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)


class DemandVolumeMart(TripGrainMart):
    name = "mart_demand_volume"
    subdir = "marts"

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        zone = ctx.gold_dims["zone"]
        df = with_zone(fact, zone, PU_LOC, "pu")
        df = with_zone(df, zone, DO_LOC, "do")

        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        dow = tb.iso_weekday(pickup)  # 1=Lunes .. 7=Domingo

        # tiempo_espera_minutos: exclusivo HVFHV (on_scene_datetime - request_datetime)
        if {"on_scene_datetime", "request_datetime"} <= set(fact.columns):
            espera = F.round(
                (
                    F.unix_timestamp("on_scene_datetime")
                    - F.unix_timestamp("request_datetime")
                )
                / 60.0,
                2,
            )
        else:
            espera = F.lit(None).cast("double")

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            pickup.alias("pickup_datetime"),
            F.col("dropoff_datetime"),
            F.col("date_key"),
            F.to_date(pickup).alias("fecha_viaje"),
            hour.alias("pickup_hour"),
            tb.bloque_horario(hour).alias("bloque_horario"),
            dow.alias("dia_semana"),
            tb.is_weekend(dow).alias("is_weekend"),
            col_or_null(fact, PU_LOC, "int").alias("pu_location_id"),
            col_or_null(fact, DO_LOC, "int").alias("do_location_id"),
            F.col("pu_borough"),
            F.col("pu_zone"),
            F.col("do_borough"),
            F.col("do_zone"),
            col_or_null(fact, "hvfhs_license_num", "string").alias("hvfhs_license_num"),
            espera.alias("tiempo_espera_minutos"),
            F.lit(1).alias("trip_count"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
