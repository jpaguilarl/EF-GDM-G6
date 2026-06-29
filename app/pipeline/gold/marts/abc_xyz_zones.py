"""D2.2 — Mart de Analisis ABC/XYZ de zonas de origen (agregado por zona x año).

- ABC (Pareto de ingresos): A = 80% acumulado, B = siguiente 15%, C = ultimo 5%.
- XYZ (coeficiente de variacion de viajes diarios): X<0.2, Y 0.2-0.5, Z>0.5.
"""

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.pipeline.gold.mart_builder import PU_LOC, GoldBuilder, GoldContext

# columna de ingresos por categoria (taxis usan total_amount; fhvhv base_passenger_fare)
REVENUE_COL = {
    "yellow": "total_amount",
    "green": "total_amount",
    "fhvhv": "base_passenger_fare",
}


class AbcXyzZonesMart(GoldBuilder):
    name = "mart_abc_xyz_zones"
    subdir = "marts"
    partition_keys = ["service_id", "year"]

    def build(self, ctx: GoldContext) -> int:
        cfg = ctx.config.abc_xyz
        a_thr = cfg.class_a_pct
        b_thr = cfg.class_a_pct + cfg.class_b_pct

        pairs = sorted(
            {(c, y) for (c, y, _m) in ctx.targets if c in REVENUE_COL}
        )
        total = 0
        wrote_any = False
        for (cat, year) in pairs:
            data = self._load_year(ctx, cat, year)
            if data is None:
                continue
            # data se escanea varias veces (ingresos, viajes diarios, Pareto)
            data = data.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                out = self._classify(data, cat, year, ctx, a_thr, b_thr, cfg)
                n = out.count()
                if n == 0:
                    continue
                self._write(out)
                total += n
                wrote_any = True
                self.logger.info(f"  {self.name} | {cat} {year}: {n} zonas")
            finally:
                data.unpersist()
        return total if wrote_any else -1

    def _load_year(
        self, ctx: GoldContext, cat: str, year: int
    ) -> DataFrame | None:
        rc = REVENUE_COL[cat]
        dfs: list[DataFrame] = []
        for (c, y, m) in ctx.target_months([cat]):
            if y != year:
                continue
            fact = ctx.read_fact(c, y, m)
            if fact is None or rc not in fact.columns or PU_LOC not in fact.columns:
                continue
            dfs.append(
                fact.select(
                    F.col(PU_LOC).alias("location_id"),
                    F.col(rc).alias("revenue"),
                    F.to_date("pickup_datetime").alias("trip_date"),
                )
            )
        if not dfs:
            return None
        data = dfs[0]
        for d in dfs[1:]:
            data = data.unionByName(d)
        return data.filter(F.col("location_id").isNotNull())

    def _classify(
        self, data, cat, year, ctx, a_thr, b_thr, cfg
    ) -> DataFrame:
        ingresos = data.groupBy("location_id").agg(
            F.sum("revenue").alias("ingresos_totales_zona")
        )
        daily = (
            data.filter(F.col("trip_date").isNotNull())
            .groupBy("location_id", "trip_date")
            .agg(F.count(F.lit(1)).alias("viajes_dia"))
        )
        stats = daily.groupBy("location_id").agg(
            F.avg("viajes_dia").alias("viajes_diarios_promedio"),
            F.stddev_samp("viajes_dia").alias("viajes_diarios_std"),
        )
        zonas = ingresos.join(stats, "location_id", "left")

        zonas = zonas.withColumn(
            "coeficiente_variacion_xyz",
            F.when(
                F.col("viajes_diarios_promedio") > 0,
                F.col("viajes_diarios_std") / F.col("viajes_diarios_promedio"),
            ).otherwise(F.lit(None).cast("double")),
        )
        zonas = zonas.withColumn(
            "clase_xyz",
            F.when(F.col("coeficiente_variacion_xyz").isNull(), F.lit(None).cast("string"))
            .when(F.col("coeficiente_variacion_xyz") < cfg.xyz_x_max, F.lit("X"))
            .when(F.col("coeficiente_variacion_xyz") <= cfg.xyz_y_max, F.lit("Y"))
            .otherwise(F.lit("Z")),
        )

        # Pareto ABC: orden global por ingresos desc + % acumulado
        total_ingresos = zonas.agg(
            F.sum("ingresos_totales_zona")
        ).first()[0] or 0.0
        w_cum = Window.orderBy(F.col("ingresos_totales_zona").desc()).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        zonas = zonas.withColumn("_cum", F.sum("ingresos_totales_zona").over(w_cum))
        zonas = zonas.withColumn(
            "porcentaje_acumulado_ingresos",
            F.when(
                F.lit(total_ingresos) > 0,
                F.round(F.col("_cum") / F.lit(total_ingresos), 4),
            ).otherwise(F.lit(None).cast("double")),
        )
        zonas = zonas.withColumn(
            "clase_abc",
            F.when(F.col("porcentaje_acumulado_ingresos").isNull(), F.lit(None).cast("string"))
            .when(F.col("porcentaje_acumulado_ingresos") <= a_thr, F.lit("A"))
            .when(F.col("porcentaje_acumulado_ingresos") <= b_thr, F.lit("B"))
            .otherwise(F.lit("C")),
        )

        zone_dim = ctx.gold_dims["zone"].select(
            F.col("LocationID").alias("_zid"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
        )
        return (
            zonas.join(F.broadcast(zone_dim), F.col("location_id") == F.col("_zid"), "left")
            .drop("_zid", "_cum")
            .select(
                F.col("location_id").alias("pu_location_id"),
                F.col("borough"),
                F.col("zone"),
                F.lit(cat).alias("service_id"),
                F.lit(year).alias("year"),
                F.round("ingresos_totales_zona", 2).alias("ingresos_totales_zona"),
                F.round("viajes_diarios_promedio", 2).alias("viajes_diarios_promedio"),
                F.round("viajes_diarios_std", 2).alias("viajes_diarios_std"),
                F.round("coeficiente_variacion_xyz", 4).alias("coeficiente_variacion_xyz"),
                F.col("clase_xyz"),
                F.col("porcentaje_acumulado_ingresos"),
                F.col("clase_abc"),
            )
        )
