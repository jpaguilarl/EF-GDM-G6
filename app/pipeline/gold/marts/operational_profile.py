"""D1.3 — Mart de Perfil Operativo (tabla ancha, 1 fila por viaje)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.mart_builder import (
    DO_LOC,
    PU_LOC,
    GoldContext,
    TripGrainMart,
    col_or_null,
)


class OperationalProfileMart(TripGrainMart):
    name = "mart_operational_profile"
    subdir = "marts"
    applies_to = {"yellow", "green", "fhvhv"}  # fhv no tiene distancia

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        df = fact
        null_d = F.lit(None).cast("double")

        duracion = col_or_null(df, "trip_duration_minutes")
        distancia = (
            col_or_null(df, "trip_miles")
            if category == "fhvhv"
            else col_or_null(df, "trip_distance")
        )
        # velocidad: null si duracion<=0 o distancia==0 (evita division por cero)
        velocidad = F.when(
            (duracion > 0) & (distancia > 0),
            F.round(distancia / (duracion / 60.0), 2),
        ).otherwise(null_d)

        match_flag = col_or_null(df, "shared_match_flag", "string")
        is_shared = (
            F.when(match_flag == "Y", F.lit(True))
            .when(match_flag.isNotNull(), F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            F.col("date_key"),
            F.col("pickup_datetime"),
            F.col("dropoff_datetime"),
            col_or_null(df, PU_LOC, "int").alias("pu_location_id"),
            col_or_null(df, DO_LOC, "int").alias("do_location_id"),
            duracion.alias("duracion_viaje_minutos"),
            col_or_null(df, "trip_distance").alias("trip_distance"),
            col_or_null(df, "trip_miles").alias("trip_miles"),
            velocidad.alias("velocidad_promedio_mph"),
            col_or_null(df, "shared_request_flag", "string").alias("shared_request_flag"),
            match_flag.alias("shared_match_flag"),
            is_shared.alias("is_shared_match"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
