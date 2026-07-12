"""D3.3 — Feature store deteccion de anomalias/fraude en taximetros (yellow/green).

Emite features para un pipeline no supervisado (Isolation Forest) o supervisado
(XGBoost). ``is_anomaly_candidate`` es una bandera heuristica por RatecodeID, NO el
score final del modelo.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold_impl.feature_rules import ratecode_tariff as rt
from app.pipeline.gold_impl.mart_builder import (
    GoldContext,
    TripGrainMart,
    col_or_null,
)


class IsolationFraudFeatures(TripGrainMart):
    name = "ml_feat_isolation_fraud"
    subdir = "ml"
    applies_to = {"yellow", "green"}

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        ratecode_dim = (
            ctx.gold_dims["ratecode"]
            .filter(F.col("fiscal_year") == year)
            .select(
                F.col("ratecode_id").alias("_rc"),
                F.col("flat_fare"),
                F.col("ratecode_name"),
            )
        )
        df = fact.join(
            F.broadcast(ratecode_dim), F.col("ratecode_id") == F.col("_rc"), "left"
        ).drop("_rc")

        fare = col_or_null(df, "fare_amount")
        dist = col_or_null(df, "trip_distance")
        ratecode = col_or_null(df, "ratecode_id", "int")
        flat_fare = F.col("flat_fare")
        tolls = col_or_null(df, "tolls_amount")

        dur_seg = F.unix_timestamp("dropoff_datetime") - F.unix_timestamp(
            "pickup_datetime"
        )
        velocidad = F.when(
            (dur_seg > 0) & (dist > 0), F.round(dist / (dur_seg / 3600.0), 2)
        ).otherwise(F.lit(None).cast("double"))
        costo = F.round(fare / (dist + F.lit(0.001)), 4)
        desviacion = rt.desviacion_tarifa_teorica(fare, flat_fare)
        candidate = rt.is_anomaly_candidate(ratecode, fare, flat_fare, velocidad, costo)
        ratio_peaje = F.when(
            fare > 0, F.round(tolls / fare, 4)
        ).otherwise(F.lit(None).cast("double"))

        return df.select(
            F.col("trip_id"),
            F.lit(category).alias("service_id"),
            F.col("date_key"),
            ratecode.alias("ratecode_id"),
            F.col("ratecode_name"),
            F.col("pickup_datetime"),
            F.col("dropoff_datetime"),
            dur_seg.alias("duracion_viaje_segundos"),
            dist.alias("trip_distance"),
            fare.alias("fare_amount"),
            tolls.alias("tolls_amount"),
            col_or_null(df, "extra").alias("extra"),
            col_or_null(df, "mta_tax").alias("mta_tax"),
            col_or_null(df, "improvement_surcharge").alias("improvement_surcharge"),
            velocidad.alias("velocidad_promedio_calculada"),
            costo.alias("costo_por_distancia"),
            ratio_peaje.alias("ratio_peaje_tarifa"),
            desviacion.alias("desviacion_tarifa_teorica"),
            candidate.alias("is_anomaly_candidate"),
            F.lit(year).alias("year"),
            F.lit(month).alias("month"),
        )
