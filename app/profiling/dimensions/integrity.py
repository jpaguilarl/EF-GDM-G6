from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Integrity(Dimension):
    name = "integrity"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        pu_candidates = ["PULocationID", "PUlocationID"]
        do_candidates = ["DOLocationID", "DOlocationID"]

        pu_col = None
        do_col = None
        for c in pu_candidates:
            if c in df.columns:
                pu_col = c
                break
        for c in do_candidates:
            if c in df.columns:
                do_col = c
                break

        if pu_col is None and do_col is None:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="zone_integrity",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": "No se encontraron columnas de zona en el dataset"
                        },
                    )
                ],
            )

        metrics: list[Metric] = []
        zone_list = list(zone_ids)

        for col_name in [pu_col, do_col]:
            if col_name is None:
                continue

            non_null_df = df.filter(F.col(col_name).isNotNull())
            total_non_null = non_null_df.count()

            if total_non_null == 0:
                metrics.append(
                    Metric(
                        name=f"{col_name}_valid_pct",
                        value=100.0,
                        passed=True,
                        detail={
                            "total_non_null": 0,
                            "valid_references": 0,
                            "invalid_references": 0,
                            "valid_zone_count": len(zone_ids),
                        },
                    )
                )
                continue

            valid_count = non_null_df.filter(F.col(col_name).isin(zone_list)).count()
            invalid_count = total_non_null - valid_count

            pct = round(valid_count / max(total_non_null, 1) * 100, 2)
            metrics.append(
                Metric(
                    name=f"{col_name}_valid_pct",
                    value=pct,
                    passed=invalid_count == 0,
                    detail={
                        "total_non_null": total_non_null,
                        "valid_references": valid_count,
                        "invalid_references": invalid_count,
                        "valid_zone_count": len(zone_ids),
                    },
                )
            )

        all_ratios = [
            float(m.value) / 100 for m in metrics if isinstance(m.value, (int, float))
        ]
        score = round(sum(all_ratios) / max(len(all_ratios), 1), 4)

        all_passed = all(m.passed for m in metrics)
        failures_sample: list[dict] = []
        if not all_passed:
            for col_name in [pu_col, do_col]:
                if col_name is None:
                    continue
                bad = (
                    df.filter(
                        F.col(col_name).isNotNull() & ~F.col(col_name).isin(zone_list)
                    )
                    .select(col_name)
                    .limit(5)
                    .collect()
                )
                if bad:
                    for row in bad:
                        d = row.asDict()
                        d["_reason"] = f"{col_name} no existe en zone-lookup"
                        failures_sample.append(d)
                    break

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=all_passed,
            metrics=metrics,
            failures_sample=failures_sample[:10],
        )
