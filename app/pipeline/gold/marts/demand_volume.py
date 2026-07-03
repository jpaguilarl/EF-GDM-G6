"""D1.1 — Mart de Volumen y Demanda (agregado: 1 fila por fecha x hora x zona).

Grano agregado en lugar de 1 fila por viaje: el dashboard consume conteos y
promedios por fecha/hora/zona/servicio, nunca viajes individuales, y el grano
viaje reescribia los ~940M de registros de silver (inviable para Power BI).
El detalle por viaje permanece integro en silver/star/facts.
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


class DemandVolumeMart(TripGrainMart):
    name = "mart_demand_volume"
    subdir = "marts"

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        dow = tb.iso_weekday(pickup)  # 1=Lunes .. 7=Domingo

        # tiempo_espera_minutos: exclusivo HVFHV (on_scene_datetime - request_datetime)
        if {"on_scene_datetime", "request_datetime"} <= set(fact.columns):
            espera = (
                F.unix_timestamp("on_scene_datetime")
                - F.unix_timestamp("request_datetime")
            ) / 60.0
        else:
            espera = F.lit(None).cast("double")

        df = fact.select(
            F.to_date(pickup).alias("fecha_viaje"),
            hour.alias("pickup_hour"),
            tb.bloque_horario(hour).alias("bloque_horario"),
            dow.alias("dia_semana"),
            tb.is_weekend(dow).alias("is_weekend"),
            col_or_null(fact, PU_LOC, "int").alias(PU_LOC),
            col_or_null(fact, "hvfhs_license_num", "string").alias("hvfhs_license_num"),
            espera.alias("_espera_min"),
        )

        # bloque/dia_semana/is_weekend son funcion de fecha+hora: no suben la
        # cardinalidad del groupBy, solo evitan recalcularlos en Power BI.
        agg = df.groupBy(
            "fecha_viaje",
            "pickup_hour",
            "bloque_horario",
            "dia_semana",
            "is_weekend",
            PU_LOC,
            "hvfhs_license_num",
        ).agg(
            F.count(F.lit(1)).alias("viajes"),
            F.round(F.sum("_espera_min"), 2).alias("espera_total_min"),
            F.count("_espera_min").alias("viajes_con_espera"),
            F.round(F.avg("_espera_min"), 2).alias("espera_promedio_min"),
        )

        # Enriquecer zona DESPUES de agregar: join sobre miles de filas, no millones.
        agg = with_zone(agg, ctx.gold_dims["zone"], PU_LOC, "pu")

        return agg.select(
            F.lit(category).alias("service_id"),
            F.col("fecha_viaje"),
            F.col("pickup_hour"),
            F.col("bloque_horario"),
            F.col("dia_semana"),
            F.col("is_weekend"),
            F.col(PU_LOC).alias("pu_location_id"),
            F.col("pu_borough"),
            F.col("pu_zone"),
            F.col("hvfhs_license_num"),
            F.col("viajes"),
            F.col("espera_total_min"),
            F.col("viajes_con_espera"),
            F.col("espera_promedio_min"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
