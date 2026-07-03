from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.rules.reasonableness_ranges import REASONABLENESS_RANGES
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Reasonableness(Dimension):
    name = "reasonableness"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        ranges = REASONABLENESS_RANGES.get(meta.category, {})
        metrics: list[Metric] = []
        has_failures = False

        for col, (low, high) in ranges.items():
            if col not in df.columns:
                continue

            col_clean = df.filter(F.col(col).isNotNull())
            total_clean = col_clean.count()
            if total_clean == 0:
                continue

            too_low = F.col(col) < low
            too_high = F.col(col) > high
            out_of_range = too_low | too_high
            oor_count = col_clean.filter(out_of_range).count()

            if oor_count > 0:
                has_failures = True

            pct_in_range = round(
                (1 - oor_count / max(total_clean, 1)) * 100, 2
            )
            min_val = col_clean.agg(F.min(col)).collect()[0][0] or 0.0
            max_val = col_clean.agg(F.max(col)).collect()[0][0] or 0.0
            too_low_count = col_clean.filter(too_low).count()
            too_high_count = col_clean.filter(too_high).count()

            metrics.append(
                Metric(
                    name=f"{col}_in_range_pct",
                    value=pct_in_range,
                    passed=oor_count == 0,
                    detail={
                        "range": [low, high],
                        "out_of_range_count": oor_count,
                        "too_low_count": too_low_count,
                        "too_high_count": too_high_count,
                        "min_actual": float(min_val),
                        "max_actual": float(max_val),
                    },
                )
            )

        if not metrics:
            return DimensionResult(
                dimension=self.name,
                score=1.0,
                passed=True,
                metrics=[
                    Metric(
                        name="reasonableness_check",
                        value="no_aplica",
                        passed=True,
                        detail={
                            "reason": f"No hay rangos definidos para {meta.category}"
                        },
                    )
                ],
            )

        scores = [
            float(m.value) / 100
            for m in metrics
            if isinstance(m.value, (int, float))
        ]
        score = round(sum(scores) / len(scores), 4)
        passed = not has_failures

        failures_sample = []
        if has_failures:
            active_ranges = {
                c: (lo, hi) for c, (lo, hi) in ranges.items() if c in df.columns
            }
            oor_conditions = [
                F.col(c).isNotNull() & ((F.col(c) < lo) | (F.col(c) > hi))
                for c, (lo, hi) in active_ranges.items()
            ]
            if oor_conditions:
                combined = oor_conditions[0]
                for cond in oor_conditions[1:]:
                    combined = combined | cond
                # Via helper: timestamps pre-1970 rompen el collect crudo en
                # Windows (OSError 22); la muestra va como strings al JSON.
                failures_sample = self.collect_sample_as_strings(
                    df.filter(combined)
                )

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=metrics,
            failures_sample=failures_sample,
        )
