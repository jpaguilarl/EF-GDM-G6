"""D3.2 — Feature store K-Modes: viaje a viaje, SOLO variables categoricas nominales.

K-Modes calcula proximidad por coincidencia de modas: se excluyen explicitamente
variables continuas (distancia, tarifa). Las ubicaciones se emiten como string para
tratarse como categorias nominales.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.pipeline.gold.feature_rules import passenger_groups as pg
from app.pipeline.gold.feature_rules import time_blocks as tb
from app.pipeline.gold.mart_builder import (
    DO_LOC,
    PU_LOC,
    SILVER_DIMS_DIR,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)
from app.utils import storage


class KModesFeatures(TripGrainMart):
    name = "ml_feat_kmodes_trips"
    subdir = "ml"
    applies_to = {"yellow", "green", "fhvhv"}

    # KModesModelPipeline entrena con max_sample_per_service=100k filas: emitir
    # los ~240M de viajes fhvhv al feature store es trabajo que nadie consume.
    # Un 5% de fhvhv (~12M filas, muestreo uniforme reproducible) sigue siendo
    # 120x lo que el modelo usa. yellow/green se emiten completos (son chicos y
    # los comparte el analisis exploratorio). Silver conserva el 100% de los
    # viajes; esto solo dimensiona el insumo de entrenamiento.
    FHVHV_SAMPLE_FRACTION = 0.05
    SAMPLE_SEED = 42

    def transform(
        self, fact: DataFrame, category: str, year: int, month: int, ctx: GoldContext
    ) -> DataFrame | None:
        if category == "fhvhv":
            fact = fact.sample(
                withReplacement=False,
                fraction=self.FHVHV_SAMPLE_FRACTION,
                seed=self.SAMPLE_SEED,
            )
        zone = ctx.gold_dims["zone"]
        df = with_zone(fact, zone, PU_LOC, "pu")
        df = with_zone(df, zone, DO_LOC, "do")
        pickup = F.col("pickup_datetime")
        hour = F.hour(pickup)
        dow = tb.iso_weekday(pickup)

        base_cols = {
            "trip_id": F.col("trip_id"),
            "service_id": F.lit(category),
            "date_key": F.col("date_key"),
            "pu_location_id": col_or_null(fact, PU_LOC, "int").cast("string"),
            "do_location_id": col_or_null(fact, DO_LOC, "int").cast("string"),
            "borough_pu": F.col("pu_borough"),
            "borough_do": F.col("do_borough"),
            "franja_horaria": tb.franja_horaria(hour),
            "dia_categoria": tb.dia_categoria(dow),
            "hvfhs_license_num": col_or_null(fact, "hvfhs_license_num", "string"),
            "vendor_id": col_or_null(fact, "vendor_id", "int").cast("string"),
            "year": F.lit(year),
            "month": F.lit(month),
        }

        if category == "fhvhv":
            base_cols["payment_type"] = F.lit(None).cast("string")
            base_cols["ratecode"] = F.lit(None).cast("string")
            base_cols["passenger_group"] = F.lit(None).cast("string")
        else:
            payment_type = ctx.spark.read.parquet(
                storage.for_spark(SILVER_DIMS_DIR / "dim_payment_type.parquet")
            ).select(
                F.col("payment_type_id").alias("_pt_id"),
                F.col("payment_type_name"),
            )
            ratecode = ctx.spark.read.parquet(
                storage.for_spark(SILVER_DIMS_DIR / "dim_ratecode.parquet")
            ).select(
                F.col("ratecode_id").alias("_rc_id"),
                F.col("ratecode_name"),
            )
            df = df.join(
                F.broadcast(payment_type),
                col_or_null(fact, "payment_type_id", "int") == F.col("_pt_id"),
                "left",
            ).drop("_pt_id")
            df = df.join(
                F.broadcast(ratecode),
                col_or_null(fact, "ratecode_id", "int") == F.col("_rc_id"),
                "left",
            ).drop("_rc_id")
            base_cols["payment_type"] = F.col("payment_type_name")
            base_cols["ratecode"] = F.col("ratecode_name")
            base_cols["passenger_group"] = pg.passenger_group(
                col_or_null(fact, "passenger_count", "int")
            )

        return df.select(*[v.alias(k) for k, v in base_cols.items()])
