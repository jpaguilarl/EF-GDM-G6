"""D2.3 — Mart de Comportamiento de Propinas (agregado: 1 fila por fecha x
borough origen/destino x tipo de pago x categoria de generosidad).

Grano agregado: la categoria de generosidad pasa de atributo por viaje a
dimension del groupBy, asi el dashboard grafica la distribucion de viajes
tacanos/estandar/generosos directamente. El porcentaje de propina ponderado se
deriva de las sumas. El detalle por viaje permanece integro en silver/star/facts.
"""

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
        cfg = ctx.config.generosity
        null_d = F.lit(None).cast("double")

        payment = col_or_null(fact, "payment_type_id", "int")
        is_credit = (
            F.when(payment == 1, F.lit(True))
            .when(payment.isNotNull(), F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )

        if category in ("yellow", "green"):
            tarifa = col_or_null(fact, "fare_amount")
            propina = col_or_null(fact, "tip_amount")
            # Solo tarjeta (payment_type==1): el efectivo no registra propina y
            # distorsionaria el analisis (D2.3).
            pct = F.when(
                (payment == 1) & (tarifa > 0), propina / tarifa * 100
            ).otherwise(null_d)
            # La tarifa base del % ponderado sigue el mismo filtro que pct.
            tarifa_base = F.when((payment == 1) & (tarifa > 0), tarifa).otherwise(
                null_d
            )
            propina_base = F.when((payment == 1) & (tarifa > 0), propina).otherwise(
                null_d
            )
            millas = col_or_null(fact, "trip_distance")
        else:  # fhvhv
            tarifa = col_or_null(fact, "base_passenger_fare")
            propina = col_or_null(fact, "tips")
            pct = F.when(tarifa > 0, propina / tarifa * 100).otherwise(null_d)
            tarifa_base = F.when(tarifa > 0, tarifa).otherwise(null_d)
            propina_base = F.when(tarifa > 0, propina).otherwise(null_d)
            millas = col_or_null(fact, "trip_miles")

        # Boroughs ANTES de agregar (join broadcast con la dim de 265 zonas):
        # agrupar por zona x zona explotaria la cardinalidad (265x265 combos);
        # borough x borough son 7x7.
        df = with_zone(fact, zone, PU_LOC, "pu")
        df = with_zone(df, zone, DO_LOC, "do")
        df = df.select(
            F.to_date("pickup_datetime").alias("fecha_viaje"),
            F.col("pu_borough"),
            F.col("do_borough"),
            payment.alias("payment_type_id"),
            is_credit.alias("is_credit_card"),
            generosity.categoria_generosidad(
                F.round(pct, 2), cfg.standard_low, cfg.standard_high
            ).alias("categoria_generosidad"),
            propina.alias("_propina"),
            pct.alias("_pct"),
            tarifa_base.alias("_tarifa_base"),
            propina_base.alias("_propina_base"),
            millas.alias("_millas"),
        )

        agg = df.groupBy(
            "fecha_viaje",
            "pu_borough",
            "do_borough",
            "payment_type_id",
            "is_credit_card",
            "categoria_generosidad",
        ).agg(
            F.count(F.lit(1)).alias("viajes"),
            F.sum((F.col("_propina") > 0).cast("int")).alias("viajes_con_propina"),
            F.round(F.sum("_propina"), 2).alias("propina_total"),
            F.round(F.avg("_pct"), 2).alias("porcentaje_propina_promedio"),
            F.sum("_tarifa_base").alias("_sum_tarifa_base"),
            F.sum("_propina_base").alias("_sum_propina_base"),
            F.sum("_millas").alias("_sum_millas"),
        )

        pct_ponderado = F.when(
            F.col("_sum_tarifa_base") > 0,
            F.round(F.col("_sum_propina_base") / F.col("_sum_tarifa_base") * 100, 2),
        ).otherwise(null_d)
        propina_por_milla = F.when(
            F.col("_sum_millas") > 0,
            F.round(F.col("propina_total") / F.col("_sum_millas"), 4),
        ).otherwise(null_d)

        return agg.select(
            F.lit(category).alias("service_id"),
            F.col("fecha_viaje"),
            F.col("pu_borough"),
            F.col("do_borough"),
            F.col("payment_type_id"),
            F.col("is_credit_card"),
            F.col("categoria_generosidad"),
            F.col("viajes"),
            F.col("viajes_con_propina"),
            F.col("propina_total"),
            F.col("porcentaje_propina_promedio"),
            pct_ponderado.alias("porcentaje_propina_ponderado"),
            propina_por_milla.alias("propina_por_milla"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
