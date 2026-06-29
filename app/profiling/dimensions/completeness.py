from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.rules.nullability import NULLABLE_COLUMNS
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Completeness(Dimension):
    name = "completeness"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        nullable_set = NULLABLE_COLUMNS.get(meta.category, set())
        total_rows = df.count()
        total_cells = total_rows * len(df.columns)

        null_counts_expr = [
            F.sum(F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(c)
            for c in df.columns
        ]
        null_counts_row = df.select(*null_counts_expr).collect()[0].asDict()
        total_nulls_sum = sum(null_counts_row.values())

        metrics: list[Metric] = []
        total_unallowed_nulls = 0
        column_details: dict[str, dict[str, float | int | bool]] = {}

        for col in df.columns:
            null_count = null_counts_row[col]
            null_pct = round(null_count / max(total_rows, 1) * 100, 4)
            is_nullable = col in nullable_set

            if is_nullable:
                unallowed_nulls = 0
            else:
                unallowed_nulls = null_count

            total_unallowed_nulls += unallowed_nulls

            column_details[col] = {
                "null_count": null_count,
                "null_pct": null_pct,
                "nullable": is_nullable,
                "unallowed_nulls": unallowed_nulls,
            }

            metrics.append(
                Metric(
                    name=f"null_pct_{col}",
                    value=null_pct,
                    passed=not is_nullable and null_pct == 0 or is_nullable,
                    detail={
                        "null_count": null_count,
                        "nullable": is_nullable,
                    },
                )
            )

        score = round(1 - (total_unallowed_nulls / max(total_cells, 1)), 4)
        passed = total_unallowed_nulls == 0

        problematic_cols = sorted(
            [(col, d) for col, d in column_details.items() if d["null_pct"] > 0],
            key=lambda x: x[1]["null_pct"],
            reverse=True,
        )
        failures = [{"column": col, **detail} for col, detail in problematic_cols[:15]]

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=[
                Metric(
                    name="completeness_score",
                    value=round(score * 100, 2),
                    passed=passed,
                    detail={
                        "total_cells": total_cells,
                        "total_nulls": total_nulls_sum,
                        "unallowed_nulls": total_unallowed_nulls,
                    },
                ),
                *metrics,
            ],
            failures_sample=failures,
        )
