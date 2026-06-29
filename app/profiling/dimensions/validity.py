import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.profiling.dimensions.base import Dimension
from app.profiling.schemas.profiling_schema import DatasetMeta, DimensionResult, Metric


class Validity(Dimension):
    name = "validity"

    def evaluate(
        self,
        df: DataFrame,
        meta: DatasetMeta,
        dict_df: DataFrame,
        zone_ids: set[int],
    ) -> DimensionResult:
        metrics: list[Metric] = []

        dict_cols = set(
            row["nombre_campo"]
            for row in dict_df.select("nombre_campo").distinct().collect()
        )
        df_cols = set(df.columns)

        unknown_cols = df_cols - dict_cols

        if unknown_cols:
            metrics.append(
                Metric(
                    name="unknown_columns",
                    value=True,
                    passed=False,
                    detail={
                        "reason": "Columnas no encontradas en el diccionario de datos",
                        "columns": sorted(unknown_cols),
                    },
                )
            )

        valid_pcts: list[float] = []
        all_failures: list[dict] = []

        for col in df.columns:
            dict_entries = dict_df.filter(F.col("nombre_campo") == col)
            if dict_entries.count() == 0:
                continue

            entries_with_values = dict_entries.filter(
                F.col("valor").isNotNull() & (F.col("valor") != "")
            )
            if entries_with_values.count() == 0:
                continue

            valid_values = self._extract_valid_values(entries_with_values)
            if not valid_values:
                continue

            valid_list = list(valid_values)

            col_data = df.filter(F.col(col).isNotNull())
            total_non_null = col_data.count()
            if total_non_null == 0:
                continue

            valid_count = col_data.filter(
                F.col(col).cast("string").isin(valid_list)
            ).count()
            invalid_count = total_non_null - valid_count

            pct = round(valid_count / max(total_non_null, 1) * 100, 2)
            valid_pcts.append(pct)

            bad_samples = []
            if invalid_count > 0:
                bad = (
                    df.filter(
                        F.col(col).isNotNull()
                        &                         ~F.col(col).cast("string").isin(valid_list)
                    )
                    .select(col)
                    .groupBy(col)
                    .count()
                    .orderBy(F.desc("count"))
                    .limit(5)
                    .collect()
                )
                bad_samples = [
                    {"valor": str(row[col]), "ocurrencias": row["count"]}
                    for row in bad
                ]
                all_failures.append(
                    {"column": col, "invalid_values": bad_samples}
                )

            metrics.append(
                Metric(
                    name=f"{col}_valid_pct",
                    value=pct,
                    passed=invalid_count == 0,
                    detail={
                        "valid_set": sorted(valid_values),
                        "total_non_null": total_non_null,
                        "invalid_count": invalid_count,
                        "top_invalid": bad_samples,
                    },
                )
            )

        for col in unknown_cols:
            metrics.append(
                Metric(
                    name=f"{col}_in_dict",
                    value=False,
                    passed=False,
                    detail={
                        "reason": f"Columna '{col}' no aparece en el diccionario"
                    },
                )
            )

        if not valid_pcts and not unknown_cols:
            score = 1.0
            passed = True
        else:
            score = round(sum(valid_pcts) / max(len(valid_pcts), 1) / 100, 4)
            passed = len(unknown_cols) == 0 and all(
                isinstance(m.passed, bool) and m.passed
                for m in metrics
                if m.passed is not None
            )

        return DimensionResult(
            dimension=self.name,
            score=score,
            passed=passed,
            metrics=metrics,
            failures_sample=all_failures[:10],
        )

    def _extract_valid_values(self, dict_entries: DataFrame) -> set:
        values = set()
        for row in dict_entries.collect():
            val = str(row["valor"])
            match = re.search(r"(.+?)\s*=", val)
            if match:
                values.add(match.group(1).strip())
            text_match = re.search(r"^([YN])\b", val.strip())
            if text_match:
                values.add(text_match.group(1))
        return values
