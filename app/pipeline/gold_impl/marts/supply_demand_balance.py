"""D2.1 — Mart de Desequilibrio Oferta-Demanda (agregado zona x bloque temporal).

Granularidad: 1 fila por (zona, bloque_temporal_t). El bloque es parametrizable
(15/30 min via config). Pre-computa el flujo neto para no unir contra los facts en
Power BI:
    flujo_neto_oferta(z, t) = entrantes(z, t) - salientes(z, t+1)
donde entrantes = dropoffs hacia z en t, salientes = pickups desde z en t+1.
"""

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.pipeline.gold_impl.mart_builder import (
    GoldBuilder,
    GoldContext,
)


class SupplyDemandBalanceMart(GoldBuilder):
    name = "mart_supply_demand_balance"
    subdir = "marts"
    partition_keys = ["year", "month"]

    def build(self, ctx: GoldContext) -> int:
        block = ctx.config.supply_demand.block_minutes
        threshold = ctx.config.supply_demand.deficit_threshold
        step = block * 60  # segundos por bloque

        # Cache unificado compartido con ABC/XYZ y ARIMA (una sola lectura de
        # todos los facts). Ya esta persistido: los dos escaneos (entrantes +
        # salientes) lo reusan sin re-leer parquet.
        union = ctx.get_union_facts()
        if union is None:
            self.logger.warning(f"  {self.name}: sin facts con columnas de ubicación")
            return -1
        df = union.select(
            "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"
        )

        drop_block = (F.floor(F.unix_timestamp("dropoff_datetime") / step) * step).cast(
            "long"
        )
        pick_block = (F.floor(F.unix_timestamp("pickup_datetime") / step) * step).cast(
            "long"
        )

        entrantes = (
            df.filter(
                F.col("do_location_id").isNotNull()
                & F.col("dropoff_datetime").isNotNull()
            )
            .groupBy(
                F.col("do_location_id").alias("location_id"), drop_block.alias("t_unix")
            )
            .agg(F.count(F.lit(1)).alias("entrantes"))
        )
        salientes = (
            df.filter(
                F.col("pu_location_id").isNotNull()
                & F.col("pickup_datetime").isNotNull()
            )
            .groupBy(
                F.col("pu_location_id").alias("location_id"), pick_block.alias("s_unix")
            )
            .agg(F.count(F.lit(1)).alias("salientes"))
        )
        # alinear salientes(t+1) al bloque t: una fila en s corresponde a t = s - step
        salientes_aligned = salientes.select(
            "location_id",
            (F.col("s_unix") - F.lit(step)).alias("t_unix"),
            "salientes",
        )

        joined = (
            entrantes.join(salientes_aligned, ["location_id", "t_unix"], "full_outer")
            .withColumn("entrantes", F.coalesce(F.col("entrantes"), F.lit(0)))
            .withColumn("salientes", F.coalesce(F.col("salientes"), F.lit(0)))
            .withColumn(
                "flujo_neto_oferta", F.col("entrantes") - F.col("salientes")
            )
            .withColumn(
                "deficit_severo_flag", F.col("flujo_neto_oferta") < F.lit(threshold)
            )
            .withColumn(
                "bloque_temporal_t", F.to_timestamp(F.from_unixtime(F.col("t_unix")))
            )
            .withColumn(
                "bloque_temporal_t_plus_1",
                F.to_timestamp(F.from_unixtime(F.col("t_unix") + F.lit(step))),
            )
        ).filter(F.col("bloque_temporal_t").isNotNull())

        zone_dim = ctx.gold_dims["zone"].select(
            F.col("LocationID").alias("_zid"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
        )
        out = (
            joined.join(
                F.broadcast(zone_dim), F.col("location_id") == F.col("_zid"), "left"
            )
            .drop("_zid")
            .select(
                F.col("location_id"),
                F.col("borough"),
                F.col("zone"),
                F.col("bloque_temporal_t"),
                F.col("bloque_temporal_t_plus_1"),
                F.col("entrantes").alias("taxis_entrantes_zona_t"),
                F.col("salientes").alias("taxis_salientes_zona_t_plus_1"),
                F.col("flujo_neto_oferta"),
                F.col("deficit_severo_flag"),
                F.year("bloque_temporal_t").alias("year"),
                F.month("bloque_temporal_t").alias("month"),
            )
        )

        # Persistir el AGREGADO (zona x bloque, pequeño): sin esto, count() y la
        # escritura recomputarian todo el DAG (2 pasadas extra sobre el cache).
        out = out.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            n = out.count()
            self._write(out)
            self.logger.info(f"  {self.name}: {n} filas (bloques de {block} min)")
            return n
        finally:
            out.unpersist()
