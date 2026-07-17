from datetime import datetime

import polars as pl

from app.utils.globals import globals

AUDIT_PATHS = {
    "bronze": "data/bronze/audit.parquet",
    "silver": "data/silver/audit.parquet",
    "gold": "data/gold/audit.parquet",
}


def _lf(layer: str) -> pl.LazyFrame | None:
    rel_path = AUDIT_PATHS.get(layer)
    if not rel_path:
        return None
    full_path = globals.project_root / rel_path
    if not full_path.exists():
        return None
    return pl.scan_parquet(str(full_path))


def read_audit(
    layer: str,
    limit: int = 100,
    offset: int = 0,
    category: str | None = None,
    year: int | None = None,
) -> dict:
    lf = _lf(layer)
    if lf is None:
        return {"rows": [], "total": 0}

    schema = lf.collect_schema()
    names = schema.names()

    name_col = None
    for col in ("source_file", "mart_name"):
        if col in names:
            name_col = col
            break

    if category and name_col:
        lf = lf.filter(pl.col(name_col).str.contains(category))
    if year is not None and name_col:
        lf = lf.filter(pl.col(name_col).str.contains(f"{year}-"))

    total = lf.select(pl.len()).collect().item()
    rows = lf.slice(offset, limit).collect().to_dicts()

    return {"rows": rows, "total": total}


LINEAGE_COLS = [
    "layer", "audit_id", "fk_audit_id", "source_name",
    "rows_in", "rows_out", "rows_rejected",
    "start_timestamp", "end_timestamp", "duration_sec",
]


def _layer_lf(layer: str) -> pl.LazyFrame | None:
    lf = _lf(layer)
    if lf is None:
        return None
    schema = lf.collect_schema().names()

    if layer == "bronze":
        name_col = "source_file" if "source_file" in schema else "name"
        return lf.select(
            pl.lit("bronze").alias("layer"),
            pl.col("audit_id"),
            pl.lit(None, dtype=pl.Utf8).alias("fk_audit_id"),
            pl.col(name_col).alias("source_name"),
            pl.lit(None, dtype=pl.Int64).alias("rows_in"),
            pl.col("rowcount").alias("rows_out"),
            pl.lit(None, dtype=pl.Int64).alias("rows_rejected"),
            pl.col("start_timestamp"),
            pl.col("end_timestamp"),
        )
    elif layer == "silver":
        return lf.select(
            pl.lit("silver").alias("layer"),
            pl.col("audit_id"),
            pl.col("bronze_audit_id").alias("fk_audit_id"),
            pl.col("source_file").alias("source_name"),
            pl.col("rowcount_bronze").alias("rows_in"),
            pl.col("rowcount_quality").alias("rows_out"),
            pl.col("rowcount_quarantined").alias("rows_rejected"),
            pl.col("start_timestamp"),
            pl.col("end_timestamp"),
        )
    elif layer == "gold":
        return lf.select(
            pl.lit("gold").alias("layer"),
            pl.col("gold_audit_id").alias("audit_id"),
            pl.col("silver_audit_id").alias("fk_audit_id"),
            pl.col("mart_name").alias("source_name"),
            pl.lit(None, dtype=pl.Int64).alias("rows_in"),
            pl.col("rowcount_output").alias("rows_out"),
            pl.lit(None, dtype=pl.Int64).alias("rows_rejected"),
            pl.col("start_timestamp"),
            pl.col("end_timestamp"),
        )
    return None


def read_audit_lineage(
    limit: int = 100,
    offset: int = 0,
    layer: str | None = None,
) -> dict:
    layers = [layer] if layer else ["bronze", "silver", "gold"]
    parts = [_layer_lf(l) for l in layers]
    parts = [p for p in parts if p is not None]
    if not parts:
        return {"rows": [], "total": 0}

    lf = pl.concat(parts)
    lf = lf.with_columns(
        pl.col("start_timestamp").str.strptime(pl.Datetime).alias("_start"),
        pl.col("end_timestamp").str.strptime(pl.Datetime).alias("_end"),
    ).with_columns(
        (pl.col("_end") - pl.col("_start")).dt.total_seconds().alias("duration_sec"),
    ).drop("_start", "_end")

    lf = lf.sort("start_timestamp", descending=True)

    total = lf.select(pl.len()).collect().item()
    rows = lf.slice(offset, limit).collect().to_dicts()

    for r in rows:
        for tcol in ("start_timestamp", "end_timestamp"):
            if isinstance(r.get(tcol), str):
                r[tcol] = r[tcol]
            elif hasattr(r.get(tcol), "isoformat"):
                r[tcol] = r[tcol].isoformat()
            elif r.get(tcol) is not None:
                r[tcol] = str(r[tcol])

    return {"rows": rows, "total": total}


def _parse_period(name: str) -> str | None:
    parts = name.split("_")
    if len(parts) >= 2:
        candidate = parts[-1]
        if len(candidate) == 7 and candidate[4] == "-":
            return candidate
    return None


def read_audit_summary(layer: str) -> dict:
    lf = _lf(layer)
    if lf is None:
        return {"total_rows": 0, "charts": {}}
    df = lf.collect()
    names = df.columns
    result: dict = {}

    if layer == "bronze":
        result["total_files"] = len(df)
        result["total_bytes"] = df["bytecount"].sum()
        result["total_rows"] = df["rowcount"].sum()

        # rowcount distribution
        stats = df["rowcount"].describe()
        result["avg_rows"] = round(stats["mean"], 0) if "mean" in stats else 0

        # by category
        cat_df = df.with_columns(
            pl.col("name").str.split("_").list.first().alias("category")
        )
        by_cat = (
            cat_df.group_by("category")
            .agg(
                pl.len().alias("files"),
                pl.col("rowcount").sum().alias("rows"),
                pl.col("bytecount").sum().alias("bytes"),
            )
            .sort("category")
            .to_dicts()
        )
        result["by_category"] = by_cat

        # by month (timeline)
        month_df = cat_df.with_columns(
            pl.col("name")
            .map_elements(_parse_period, return_dtype=pl.String)
            .alias("period")
        ).filter(pl.col("period").is_not_null())
        if len(month_df) > 0:
            by_month = (
                month_df.group_by("period")
                .agg(
                    pl.col("rowcount").sum().alias("rows"),
                    pl.len().alias("files"),
                )
                .sort("period")
                .to_dicts()
            )
            result["by_month"] = by_month

        # duration stats
        ts_df = df.with_columns(
            pl.col("end_timestamp").str.strptime(pl.Datetime),
            pl.col("start_timestamp").str.strptime(pl.Datetime),
        ).with_columns(
            (pl.col("end_timestamp") - pl.col("start_timestamp"))
            .dt.total_seconds()
            .alias("duration_sec")
        )
        dur = ts_df["duration_sec"]
        result["total_duration_sec"] = int(dur.sum()) if len(dur) > 0 else 0
        result["avg_duration_sec"] = round(dur.mean(), 1) if len(dur) > 0 else 0

    elif layer == "silver":
        result["total_files"] = len(df)
        result["total_bronze_rows"] = int(df["rowcount_bronze"].sum())
        result["total_quality_rows"] = int(df["rowcount_quality"].sum())
        result["total_quarantined_rows"] = int(df["rowcount_quarantined"].sum())
        total_bronze = result["total_bronze_rows"]
        result["overall_reject_rate"] = (
            round(result["total_quarantined_rows"] / total_bronze * 100, 2)
            if total_bronze > 0
            else 0
        )

        # by category
        cat_df = df.with_columns(
            pl.col("source_file")
            .str.split("/")
            .list.get(pl.lit(2))
            .alias("category")
        )
        by_cat = (
            cat_df.group_by("category")
            .agg(
                pl.len().alias("files"),
                pl.col("rowcount_bronze").sum().alias("bronze_rows"),
                pl.col("rowcount_quality").sum().alias("quality_rows"),
                pl.col("rowcount_quarantined").sum().alias("quarantined_rows"),
            )
            .sort("category")
            .to_dicts()
        )
        for c in by_cat:
            total = c["bronze_rows"]
            c["reject_rate"] = (
                round(c["quarantined_rows"] / total * 100, 2) if total > 0 else 0
            )
        result["by_category"] = by_cat

        # by month (timeline)
        month_df = cat_df.with_columns(
            pl.col("source_file")
            .str.split("/")
            .list.get(pl.lit(3))
            .str.replace(".parquet", "")
            .alias("period")
        ).filter(pl.col("period").is_not_null())
        if len(month_df) > 0:
            by_month = (
                month_df.group_by("period")
                .agg(
                    pl.col("rowcount_bronze").sum().alias("bronze_rows"),
                    pl.col("rowcount_quality").sum().alias("quality_rows"),
                    pl.col("rowcount_quarantined").sum().alias("quarantined_rows"),
                    pl.len().alias("files"),
                )
                .sort("period")
                .to_dicts()
            )
            result["by_month"] = by_month

        # duration stats
        ts_df = df.with_columns(
            pl.col("end_timestamp").str.strptime(pl.Datetime),
            pl.col("start_timestamp").str.strptime(pl.Datetime),
        ).with_columns(
            (pl.col("end_timestamp") - pl.col("start_timestamp"))
            .dt.total_seconds()
            .alias("duration_sec")
        )
        dur = ts_df["duration_sec"]
        result["total_duration_sec"] = int(dur.sum()) if len(dur) > 0 else 0
        result["avg_duration_sec"] = round(dur.mean(), 1) if len(dur) > 0 else 0

    elif layer == "gold":
        result["total_builds"] = len(df)
        result["total_output_rows"] = int(df["rowcount_output"].sum())

        by_mart = (
            df.group_by("mart_name")
            .agg(
                pl.col("rowcount_output").sum().alias("rows"),
                pl.len().alias("builds"),
            )
            .sort("mart_name")
            .to_dicts()
        )
        result["by_mart"] = by_mart

        mode_bd = (
            df.group_by("mode").agg(pl.len().alias("count")).to_dicts()
        )
        result["mode_breakdown"] = mode_bd

        ts_df = df.with_columns(
            pl.col("end_timestamp").str.strptime(pl.Datetime),
            pl.col("start_timestamp").str.strptime(pl.Datetime),
        ).with_columns(
            (pl.col("end_timestamp") - pl.col("start_timestamp"))
            .dt.total_seconds()
            .alias("duration_sec")
        )
        dur = ts_df["duration_sec"]
        result["total_duration_sec"] = int(dur.sum()) if len(dur) > 0 else 0
        result["avg_duration_sec"] = round(dur.mean(), 1) if len(dur) > 0 else 0

    return result
