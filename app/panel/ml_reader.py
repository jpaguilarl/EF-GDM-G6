import json

import polars as pl

from app.utils.globals import globals

MODELS_DIR = globals.project_root / "data/gold/models"
ML_DIR = globals.project_root / "data/gold/ml"


def kmodes_summary(service_id: str) -> dict:
    model_dir = MODELS_DIR / "kmodes" / service_id
    ml_base = ML_DIR / "kmodes_model"

    result = {
        "service_id": service_id,
        "variables": [],
        "centers": [],
        "profiles": [],
        "sizes": [],
        "tuning": [],
    }

    mapping_file = model_dir / "category_mapping.json"
    if mapping_file.exists():
        with open(mapping_file) as f:
            mapping = json.load(f)
        result["variables"] = list(mapping.get("encoding", mapping).keys())

    centers_glob = sorted(ml_base.glob(f"centers_service_id={service_id}/centers*.parquet"))
    if centers_glob:
        result["centers"] = pl.scan_parquet(str(centers_glob[0])).collect().to_dicts()

    profiles_glob = sorted(ml_base.glob(f"profiles_service_id={service_id}/profiles*.parquet"))
    if profiles_glob:
        result["profiles"] = pl.scan_parquet(str(profiles_glob[0])).collect().to_dicts()

    labels_glob = sorted(ml_base.glob(f"labels_service_id={service_id}/labels*.parquet"))
    if labels_glob:
        sizes = (
            pl.scan_parquet(str(labels_glob[0]))
            .group_by("cluster_id")
            .agg(pl.len().alias("count"))
            .sort("cluster_id")
            .collect()
        )
        result["sizes"] = sizes.to_dicts()

    tuning_glob = sorted(ml_base.glob(f"tuning_service_id={service_id}/tuning*.parquet"))
    if tuning_glob:
        result["tuning"] = pl.scan_parquet(str(tuning_glob[0])).collect().to_dicts()

    return result


def isolation_list() -> list[dict]:
    model_base = MODELS_DIR / "isolation_forest"
    result = []
    if not model_base.exists():
        return result
    for ratecode_dir in sorted(model_base.iterdir()):
        if not ratecode_dir.is_dir():
            continue
        meta_file = ratecode_dir / "metadata.json"
        metadata = {}
        if meta_file.exists():
            with open(meta_file) as f:
                metadata = json.load(f)
        result.append({
            "ratecode": ratecode_dir.name,
            "metadata": metadata,
        })
    return result


def isolation_summary() -> dict:
    scores_dir = ML_DIR / "ml_isolation_fraud_scores"
    if not scores_dir.exists():
        return {"total_scored": 0, "fraud_count": 0, "fraud_rate": 0, "by_ratecode": []}
    lf = pl.scan_parquet(str(scores_dir / "**" / "*.parquet"))
    total = lf.select(pl.len()).collect().item()
    fraud_count = lf.filter(pl.col("is_fraud") == True).select(pl.len()).collect().item()
    fraud_rate = round(fraud_count / total * 100, 2) if total > 0 else 0

    by_ratecode = (
        lf.group_by("ratecode_id")
        .agg(
            pl.len().alias("total"),
            pl.col("is_fraud").sum().alias("fraud"),
        )
        .sort("ratecode_id")
        .collect()
        .to_dicts()
    )
    for rc in by_ratecode:
        rc["fraud_rate"] = round(rc["fraud"] / rc["total"] * 100, 2) if rc["total"] > 0 else 0

    # score distribution histogram (20 bins)
    score_stats = lf.select(
        pl.col("anomaly_score").min().alias("score_min"),
        pl.col("anomaly_score").max().alias("score_max"),
        pl.col("anomaly_score").mean().alias("score_mean"),
        pl.col("anomaly_score").std().alias("score_std"),
    ).collect().to_dicts()

    return {
        "total_scored": total,
        "fraud_count": int(fraud_count),
        "fraud_rate": fraud_rate,
        "by_ratecode": by_ratecode,
        "score_stats": score_stats[0] if score_stats else {},
    }


def isolation_scores(ratecode: str, limit: int = 100, offset: int = 0) -> dict:
    scores_dir = ML_DIR / "ml_isolation_fraud_scores"
    if not scores_dir.exists():
        return {"rows": [], "total": 0}
    lf = pl.scan_parquet(str(scores_dir / "**" / "*.parquet"))
    names = lf.collect_schema().names()
    if "ratecode_id" in names:
        lf = lf.filter(pl.col("ratecode_id") == int(ratecode))
    total = lf.select(pl.len()).collect().item()
    rows = lf.sort("anomaly_score", descending=True).slice(offset, limit).collect().to_dicts()
    return {"rows": rows, "total": total}


def sarimax_summary() -> dict:
    forecast_dir = ML_DIR / "ml_sarimax_trips_forecast"
    if not forecast_dir.exists():
        return {"combos": [], "total_rows": 0}
    lf = pl.scan_parquet(str(forecast_dir / "**" / "*.parquet"))
    total = lf.select(pl.len()).collect().item()
    combos = (
        lf.select(["borough", "service_id"])
        .unique()
        .sort(["borough", "service_id"])
        .collect()
        .to_dicts()
    )
    date_range = lf.select(
        pl.col("pickup_hour").min().alias("min_dt"),
        pl.col("pickup_hour").max().alias("max_dt"),
    ).collect().to_dicts()
    return {
        "combos": combos,
        "total_rows": total,
        "date_range": date_range[0] if date_range else {},
    }


def sarimax_forecast(
    limit: int = 100,
    offset: int = 0,
    borough: str | None = None,
    service_id: str | None = None,
) -> dict:
    forecast_dir = ML_DIR / "ml_sarimax_trips_forecast"
    if not forecast_dir.exists():
        return {"rows": [], "total": 0}
    lf = pl.scan_parquet(str(forecast_dir / "**" / "*.parquet"))
    if borough:
        lf = lf.filter(pl.col("borough") == borough)
    if service_id:
        lf = lf.filter(pl.col("service_id") == service_id)
    total = lf.select(pl.len()).collect().item()
    rows = lf.sort("pickup_hour").slice(offset, limit).collect().to_dicts()
    return {"rows": rows, "total": total}
