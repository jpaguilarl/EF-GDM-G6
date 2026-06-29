"""D3.2 — Feature store K-Modes: viaje a viaje, SOLO variables categoricas nominales.

K-Modes calcula proximidad por coincidencia de modas: se excluyen explicitamente
variables continuas (distancia, tarifa). Las ubicaciones se emiten como string para
tratarse como categorias nominales.
"""

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


class KModesFeatures(TripGrainMart):
    name = "ml_feat_kmodes_trips"
    subdir = "ml"
    applies_to = {"yellow", "green", "fhvhv"}

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        zone = ctx.gold_dims["zone"]
        df = with_zone(fact, zone, PU_LOC, "pu")
        df = with_zone(df, zone, DO_LOC, "do")

        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        dow = tb.iso_weekday(pickup)  # 1=Lunes .. 7=Domingo

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            F.col("date_key"),
            col_or_null(fact, PU_LOC, "int").cast("string").alias("pu_location_id"),
            col_or_null(fact, DO_LOC, "int").cast("string").alias("do_location_id"),
            F.col("pu_borough").alias("borough_pu"),
            F.col("do_borough").alias("borough_do"),
            tb.franja_horaria(hour).alias("franja_horaria"),
            tb.dia_categoria(dow).alias("dia_categoria"),
            col_or_null(fact, "hvfhs_license_num", "string").alias("hvfhs_license_num"),
            col_or_null(fact, "vendor_id", "int").cast("string").alias("vendor_id"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
