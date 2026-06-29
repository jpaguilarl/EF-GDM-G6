from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.rules.amount_components import AMOUNT_FORMULAS, AMOUNT_TOLERANCE
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Accuracy(Dimension):
    name = "accuracy"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        formula = AMOUNT_FORMULAS.get(meta.category)

        if formula is None:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="total_amount_check",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": f"La categoria {meta.category} no tiene campos de monto para verificar"
                        },
                    )
                ],
            )

        total_col = formula["total"]
        component_cols = formula["components"]

        available_cols = [c for c in component_cols if c in df.columns]
        if not available_cols:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="total_amount_check",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": "No se encontraron columnas de componentes en el dataset"
                        },
                    )
                ],
            )

        df_work = df.select(total_col, *available_cols)
        for c in available_cols:
            df_work = df_work.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
        df_work = df_work.filter(F.col(total_col).isNotNull())

        computed_expr = sum(F.col(c) for c in available_cols)
        df_work = df_work.withColumn("_computed", computed_expr)
        df_work = df_work.withColumn("_diff", F.abs(F.col(total_col) - F.col("_computed")))
        df_work = df_work.withColumn("_mismatch", F.col("_diff") > F.lit(AMOUNT_TOLERANCE))

        total_rows = df_work.count()
        mismatch_count = df_work.filter(F.col("_mismatch")).count()
        match_pct = round((1 - mismatch_count / max(total_rows, 1)) * 100, 2)
        max_diff = round(
            df_work.agg(F.max("_diff")).collect()[0][0] or 0.0, 4
        ) if total_rows > 0 else 0.0

        score = round(match_pct / 100, 4)

        failures_sample = []
        if mismatch_count > 0:
            sample_rows = (
                df_work.filter(F.col("_mismatch"))
                .orderBy(F.desc("_diff"))
                .select(
                    *[F.col(c) for c in available_cols],
                    F.col(total_col),
                    F.round(F.col("_computed"), 4).alias("_computed"),
                    F.round(F.col("_diff"), 4).alias("_diff"),
                )
                .limit(10)
                .collect()
            )
            failures_sample = [row.asDict() for row in sample_rows]

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=mismatch_count == 0,
            metrics=[
                Metric(
                    name="total_amount_match_pct",
                    value=match_pct,
                    passed=mismatch_count == 0,
                    detail={
                        "formula": f"{total_col} = {' + '.join(available_cols)}",
                        "tolerance": AMOUNT_TOLERANCE,
                    },
                ),
                Metric(
                    name="total_amount_mismatch_count",
                    value=mismatch_count,
                    passed=mismatch_count == 0,
                    detail={},
                ),
                Metric(
                    name="total_amount_max_diff",
                    value=max_diff,
                    passed=mismatch_count == 0,
                    detail={},
                ),
            ],
            failures_sample=failures_sample,
        )
