"""D1.2 — Mart de Rendimiento Financiero (agregado: 1 fila por fecha x bloque x borough).

Grano agregado: el dashboard monitorea ingresos, costos y margenes como sumas
y ratios por periodo/servicio, no viajes sueltos. Los componentes de tarifa se
guardan como SUM (re-agregables en Power BI) y los ratios se precalculan desde
las sumas. El detalle por viaje permanece integro en silver/star/facts.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold_impl.feature_rules import time_blocks as tb
from app.pipeline.gold_impl.mart_builder import (
    PU_LOC,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)

# Componentes de tarifa a sumar: taxis (yellow/green) y HVFHV. Los que no
# existen en la categoria salen como NULL via col_or_null.
_SUM_COLS = [
    # --- taxis ---
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "cbd_congestion_fee",
    "airport_fee",
    "ehail_fee",
    "total_amount",
    # --- HVFHV ---
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "tips",
    "driver_pay",
    # --- distancia ---
    "trip_distance",
    "trip_miles",
]


class FinancialPerformanceMart(TripGrainMart):
    name = "mart_financial_performance"
    subdir = "marts"
    applies_to = {"yellow", "green", "fhvhv"}  # fhv no tiene datos de tarifa

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        null_d = F.lit(None).cast("double")

        df = fact.select(
            F.to_date(pickup).alias("fecha_viaje"),
            tb.bloque_horario(hour).alias("bloque_horario"),
            col_or_null(fact, PU_LOC, "int").alias(PU_LOC),
            *[col_or_null(fact, c).alias(c) for c in _SUM_COLS],
        )

        agg = df.groupBy("fecha_viaje", "bloque_horario", PU_LOC).agg(
            F.count(F.lit(1)).alias("viajes"),
            *[F.round(F.sum(c), 2).alias(c) for c in _SUM_COLS],
        )

        # Ratios desde las sumas (equivale al promedio ponderado por viaje).
        if category in ("yellow", "green"):
            dist = F.col("trip_distance")
            ingreso = F.when(
                dist > 0, F.round(F.col("total_amount") / dist, 4)
            ).otherwise(null_d)
            margen = null_d
            ratio = null_d
        else:  # fhvhv
            miles = F.col("trip_miles")
            bpf = F.col("base_passenger_fare")
            driver = F.col("driver_pay")
            ingreso = F.when(miles > 0, F.round(bpf / miles, 4)).otherwise(null_d)
            margen = F.round(bpf - driver, 2)  # ganancia bruta de la plataforma
            ratio = F.when(bpf > 0, F.round(driver / bpf, 4)).otherwise(null_d)

        # Borough despues de agregar: join sobre miles de filas, no millones.
        agg = with_zone(agg, ctx.gold_dims["zone"], PU_LOC, "pu")

        return agg.select(
            F.lit(category).alias("service_id"),
            F.col("fecha_viaje"),
            F.col("bloque_horario"),
            F.col(PU_LOC).alias("pu_location_id"),
            F.col("pu_borough"),
            F.col("pu_zone"),
            F.col("viajes"),
            *[F.col(c) for c in _SUM_COLS],
            ingreso.alias("ingreso_bruto_por_milla"),
            margen.alias("margen_plataforma"),
            ratio.alias("ratio_pago_conductor"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
