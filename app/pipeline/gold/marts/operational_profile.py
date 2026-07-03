"""D1.3 — Mart de Perfil Operativo (agregado: 1 fila por fecha x bloque x zona).

Grano agregado: eficiencia de flota como promedios/tasas por periodo y zona.
La velocidad promedio se calcula ponderada (suma de millas validas / suma de
horas validas), no como promedio de promedios. El detalle por viaje permanece
integro en silver/star/facts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.feature_rules import time_blocks as tb
from app.pipeline.gold.mart_builder import (
    PU_LOC,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)


class OperationalProfileMart(TripGrainMart):
    name = "mart_operational_profile"
    subdir = "marts"
    applies_to = {"yellow", "green", "fhvhv"}  # fhv no tiene distancia

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        null_d = F.lit(None).cast("double")

        duracion = col_or_null(fact, "trip_duration_minutes")
        distancia = (
            col_or_null(fact, "trip_miles")
            if category == "fhvhv"
            else col_or_null(fact, "trip_distance")
        )
        # Solo viajes con duracion y distancia validas aportan a la velocidad
        # (mismo criterio que el grano viaje: evita division por cero).
        valido = (duracion > 0) & (distancia > 0)

        match_flag = col_or_null(fact, "shared_match_flag", "string")
        request_flag = col_or_null(fact, "shared_request_flag", "string")

        df = fact.select(
            F.to_date(pickup).alias("fecha_viaje"),
            tb.bloque_horario(hour).alias("bloque_horario"),
            col_or_null(fact, PU_LOC, "int").alias(PU_LOC),
            duracion.alias("_duracion_min"),
            distancia.alias("_millas"),
            F.when(valido, distancia).otherwise(null_d).alias("_millas_validas"),
            F.when(valido, duracion / 60.0).otherwise(null_d).alias("_horas_validas"),
            (match_flag == "Y").cast("int").alias("_match_compartido"),
            (request_flag == "Y").cast("int").alias("_solicitud_compartida"),
        )

        agg = df.groupBy("fecha_viaje", "bloque_horario", PU_LOC).agg(
            F.count(F.lit(1)).alias("viajes"),
            F.round(F.sum("_duracion_min"), 2).alias("duracion_total_min"),
            F.round(F.avg("_duracion_min"), 2).alias("duracion_promedio_min"),
            F.round(F.sum("_millas"), 2).alias("distancia_total_millas"),
            F.round(F.avg("_millas"), 4).alias("distancia_promedio_millas"),
            F.sum("_millas_validas").alias("_sum_millas_validas"),
            F.sum("_horas_validas").alias("_sum_horas_validas"),
            F.sum("_solicitud_compartida").alias("viajes_solicitud_compartida"),
            F.sum("_match_compartido").alias("viajes_match_compartido"),
        )

        velocidad = F.when(
            F.col("_sum_horas_validas") > 0,
            F.round(F.col("_sum_millas_validas") / F.col("_sum_horas_validas"), 2),
        ).otherwise(null_d)
        # tasa_ocupacion_compartida: exclusivo HVFHV (unico con shared flags)
        tasa_compartida = (
            F.round(F.col("viajes_match_compartido") / F.col("viajes"), 4)
            if category == "fhvhv"
            else null_d
        )

        agg = with_zone(agg, ctx.gold_dims["zone"], PU_LOC, "pu")

        return agg.select(
            F.lit(category).alias("service_id"),
            F.col("fecha_viaje"),
            F.col("bloque_horario"),
            F.col(PU_LOC).alias("pu_location_id"),
            F.col("pu_borough"),
            F.col("pu_zone"),
            F.col("viajes"),
            F.col("duracion_total_min"),
            F.col("duracion_promedio_min"),
            F.col("distancia_total_millas"),
            F.col("distancia_promedio_millas"),
            velocidad.alias("velocidad_promedio_mph"),
            F.col("viajes_solicitud_compartida"),
            F.col("viajes_match_compartido"),
            tasa_compartida.alias("tasa_ocupacion_compartida"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
