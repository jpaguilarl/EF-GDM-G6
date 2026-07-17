import json
from datetime import datetime

import polars as pl

from app.panel._cache import ttl_cache
from app.utils.globals import globals

MODELS_DIR = globals.project_root / "data/gold/models"
ML_DIR = globals.project_root / "data/gold/ml"
SCORES_DIR = ML_DIR / "ml_isolation_fraud_scores"
FEAT_DIR = ML_DIR / "ml_feat_isolation_fraud"

FEATURE_COLS = [
    "trip_distance",
    "fare_amount",
    "velocidad_promedio_calculada",
    "costo_por_distancia",
    "duracion_viaje_segundos",
]

LEGAL_FARE_PER_MILE = 4.12


def _scan_scores() -> pl.LazyFrame:
    files = sorted(SCORES_DIR.rglob("*.parquet"))
    if not files:
        return pl.LazyFrame()  # empty — caller will handle
    return pl.scan_parquet([str(f) for f in files], hive_partitioning=True)


def _scan_feat() -> pl.LazyFrame:
    files = sorted(FEAT_DIR.rglob("*.parquet"))
    if not files:
        return pl.LazyFrame()  # empty — caller will handle
    return pl.scan_parquet([str(f) for f in files], hive_partitioning=True)


@ttl_cache(ttl_seconds=300)
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
        "distributions": [],
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

    labels_glob = sorted(ml_base.glob(f"labels_service_id={service_id}/*.parquet"))
    if labels_glob:
        sizes = (
            pl.scan_parquet([str(p) for p in labels_glob])
            .group_by("cluster_id")
            .agg(pl.len().alias("count"))
            .sort("cluster_id")
            .collect()
        )
        result["sizes"] = sizes.to_dicts()

    tuning_glob = sorted(ml_base.glob(f"tuning_service_id={service_id}/tuning*.parquet"))
    if tuning_glob:
        result["tuning"] = pl.scan_parquet(str(tuning_glob[0])).collect().to_dicts()

    result["distributions"] = _kmodes_distributions(service_id)

    return result


_PROVIDER_FEATURE = {
    "fhvhv": "hvfhs_license_num",
    "yellow": "payment_type",
    "green": "payment_type",
    "fhv": "payment_type",
}

_DIST_FEATURES = ("borough_pu", "franja_horaria")


def _kmodes_distributions(service_id: str) -> list[dict]:
    """Categorical-value distributions per cluster for the dashboard mini-charts.

    Joins labels (cluster_id) with the trip-grain feature store and computes, per
    cluster and feature, the value counts + share. Returns long-form rows
    {cluster_id, feature, value, count, pct}.
    """
    ml_base = ML_DIR / "kmodes_model"
    if not ml_base.exists():
        return []
    labels_glob = sorted(ml_base.glob(f"labels_service_id={service_id}/*.parquet"))
    if not labels_glob:
        return []

    feat_dir = ML_DIR / "ml_feat_kmodes_trips" / f"service_id={service_id}"
    if not feat_dir.exists():
        return []

    try:
        labels = pl.scan_parquet([str(p) for p in labels_glob]).select([
            pl.col("trip_id").cast(pl.Int64, strict=False).alias("trip_id"),
            "cluster_id",
        ])
    except pl.exceptions.PolarsError:
        return []

    features = _DIST_FEATURES
    provider_feat = _PROVIDER_FEATURE.get(service_id)
    if provider_feat:
        features = (*features, provider_feat)

    try:
        feat = pl.scan_parquet(str(feat_dir / "**" / "*.parquet"))
    except pl.exceptions.PolarsError:
        return []

    feat_schema = feat.collect_schema().names()
    select_cols = ["trip_id"] + [c for c in features if c in feat_schema]
    if len(select_cols) <= 1:
        return []

    feat = feat.select(select_cols)
    joined = labels.join(feat, on="trip_id", how="inner")

    rows: list[dict] = []
    for feat_name in select_cols[1:]:
        per = (
            joined.group_by(["cluster_id", feat_name])
            .agg(pl.len().alias("count"))
            .sort(["cluster_id", "count"], descending=[False, True])
        )
        totals = per.group_by("cluster_id").agg(pl.sum("count").alias("total"))
        per = per.join(totals, on="cluster_id").with_columns(
            (pl.col("count") / pl.col("total") * 100).round(2).alias("pct")
        )
        for r in per.select(["cluster_id", feat_name, "count", "pct"]).collect().to_dicts():
            rows.append({
                "cluster_id": int(r["cluster_id"]),
                "feature": feat_name,
                "value": str(r[feat_name]) if r[feat_name] is not None else "N/A",
                "count": int(r["count"]),
                "pct": float(r["pct"]),
            })
    return rows


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


@ttl_cache(ttl_seconds=300)
def isolation_summary() -> dict:
    if not SCORES_DIR.exists():
        return {
            "total_scored": 0,
            "fraud_count": 0,
            "fraud_rate": 0,
            "by_ratecode": [],
            "score_stats": {},
            "estimated_leakage": 0,
        }
    lf = _scan_scores()
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

    score_stats = lf.select(
        pl.col("anomaly_score").min().alias("score_min"),
        pl.col("anomaly_score").max().alias("score_max"),
        pl.col("anomaly_score").mean().alias("score_mean"),
        pl.col("anomaly_score").std().alias("score_std"),
    ).collect().to_dicts()

    estimated_leakage = _estimated_fraud_leakage(lf)

    return {
        "total_scored": total,
        "fraud_count": int(fraud_count),
        "fraud_rate": fraud_rate,
        "by_ratecode": by_ratecode,
        "score_stats": score_stats[0] if score_stats else {},
        "estimated_leakage": estimated_leakage,
    }


def _estimated_fraud_leakage(scores_lf) -> float:
    """Sum fare_amount of trips flagged as fraud (join with the feature store)."""
    if not FEAT_DIR.exists():
        return 0.0
    try:
        feat = _scan_feat()
        feat_schema = feat.collect_schema().names()
        if "fare_amount" not in feat_schema:
            return 0.0
        fraud = scores_lf.filter(pl.col("is_fraud") == True).select("trip_id")
        joined = (
            fraud.join(feat.select(["trip_id", "fare_amount"]), on="trip_id", how="inner")
            .select(pl.col("fare_amount").sum().alias("total"))
            .collect()
            .item()
        )
        return round(float(joined), 2) if joined is not None else 0.0
    except pl.exceptions.PolarsError:
        return 0.0


@ttl_cache(ttl_seconds=300)
def isolation_scores(ratecode: str, limit: int = 100, offset: int = 0) -> dict:
    if not SCORES_DIR.exists():
        return {"rows": [], "total": 0}
    lf = _scan_scores()
    names = lf.collect_schema().names()
    if "ratecode_id" in names:
        lf = lf.filter(pl.col("ratecode_id") == int(ratecode))
    total = lf.select(pl.len()).collect().item()
    keep = ["trip_id", "ratecode_id", "anomaly_score", "is_fraud"]
    scores_small = (
        lf.sort("anomaly_score", descending=True)
        .slice(offset, limit)
        .select([c for c in keep if c in names])
        .collect()
    )
    if FEAT_DIR.exists() and scores_small.height > 0:
        try:
            feat = _scan_feat()
            feat_schema = feat.collect_schema().names()
            proj = [c for c in FEATURE_COLS if c in feat_schema]
            if proj:
                trip_ids = scores_small.select("trip_id").lazy()
                feat_rows = (
                    feat.select(["trip_id", *proj])
                    .join(trip_ids, on="trip_id", how="inner")
                    .unique(subset=["trip_id"], keep="first")
                    .collect()
                )
                scores_small = scores_small.join(feat_rows, on="trip_id", how="left")
        except pl.exceptions.PolarsError:
            pass
    rows = scores_small.to_dicts()
    for r in rows:
        if "is_fraud" in r and r["is_fraud"] is not None:
            r["is_fraud"] = bool(r["is_fraud"])
    return {"rows": rows, "total": total}


@ttl_cache(ttl_seconds=300)
def isolation_scatter(ratecode: str | None = None, limit: int = 500) -> dict:
    """Return two point series (normal, fraud) for the fare-vs-distance scatter.

    Joins the fraud scores with the feature store by trip_id, sampling down to
    `limit` points per series for chart performance.
    """
    if not SCORES_DIR.exists():
        return {"normal": [], "fraud": [], "legal_fare_per_mile": LEGAL_FARE_PER_MILE}
    lf = _scan_scores()
    if ratecode:
        names = lf.collect_schema().names()
        if "ratecode_id" in names:
            lf = lf.filter(pl.col("ratecode_id") == int(ratecode))
    lf = _join_features(lf)

    keep = ["trip_id", "ratecode_id", "trip_distance", "fare_amount",
            "velocidad_promedio_calculada", "costo_por_distancia", "anomaly_score"]

    schema_names = lf.collect_schema().names()
    cols = [c for c in keep if c in schema_names]

    def _sample(fraud_flag: bool) -> list[dict]:
        sub = lf.filter(pl.col("is_fraud") == fraud_flag)
        try:
            df = sub.select(cols).collect()
        except pl.exceptions.PolarsError:
            return []
        if df.height > limit:
            df = df.sample(n=limit, seed=42)
        return df.to_dicts()

    return {
        "normal": _sample(False),
        "fraud": _sample(True),
        "legal_fare_per_mile": LEGAL_FARE_PER_MILE,
    }


def _join_features(scores_lf):
    """Left-join scores with the isolation feature store by trip_id."""
    if not FEAT_DIR.exists():
        return scores_lf
    try:
        feat = _scan_feat()
        feat_schema = feat.collect_schema().names()
        proj = [c for c in FEATURE_COLS if c in feat_schema]
        if not proj:
            return scores_lf
        return scores_lf.join(feat.select(["trip_id", *proj]), on="trip_id", how="left")
    except pl.exceptions.PolarsError:
        return scores_lf


@ttl_cache(ttl_seconds=300)
def sarimax_summary() -> dict:
    forecast_dir = ML_DIR / "ml_sarimax_trips_forecast"
    if not forecast_dir.exists():
        return {"combos": [], "total_rows": 0}
    lf = pl.scan_parquet(str(forecast_dir / "**" / "*.parquet"), hive_partitioning=True)
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


@ttl_cache(ttl_seconds=300)
def sarimax_forecast(
    limit: int = 100,
    offset: int = 0,
    borough: str | None = None,
    service_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    grain: str = "hourly",
) -> dict:
    forecast_dir = ML_DIR / "ml_sarimax_trips_forecast"
    if not forecast_dir.exists():
        return {"rows": [], "total": 0}
    lf = pl.scan_parquet(str(forecast_dir / "**" / "*.parquet"), hive_partitioning=True)
    if borough:
        lf = lf.filter(pl.col("borough") == borough)
    if service_id:
        lf = lf.filter(pl.col("service_id") == service_id)
    if start:
        start_dt = datetime.fromisoformat(start)
        lf = lf.filter(pl.col("pickup_hour") >= start_dt)
    if end:
        end_dt = datetime.fromisoformat(end)
        if "T" not in end:
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
        lf = lf.filter(pl.col("pickup_hour") <= end_dt)

    if grain == "daily":
        lf = (
            lf.with_columns(pl.col("pickup_hour").cast(pl.Date).alias("__date"))
            .group_by(["__date", "forecast_type"])
            .agg(
                pl.when(pl.col("trip_count").is_null().all())
                .then(None)
                .otherwise(pl.col("trip_count").sum())
                .alias("trip_count"),
                pl.col("yhat").sum(),
                pl.col("yhat_lower").min(),
                pl.col("yhat_upper").max(),
                pl.col("model_status").first(),
            )
            .with_columns(
                pl.col("__date").cast(pl.Datetime).alias("pickup_hour"),
                pl.lit(borough or "All").alias("borough"),
                pl.lit(service_id or "All").alias("service_id"),
            )
            .select(["borough", "service_id", "pickup_hour", "trip_count", "yhat", "yhat_lower", "yhat_upper", "model_status", "forecast_type"])
            .sort("pickup_hour", "forecast_type")
        )

    total = lf.select(pl.len()).collect().item()
    rows = lf.sort("pickup_hour").slice(offset, limit).collect().to_dicts()
    return {"rows": rows, "total": total}
