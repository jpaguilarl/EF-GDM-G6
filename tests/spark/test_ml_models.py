import math
from datetime import datetime, timedelta
from pathlib import Path
from random import uniform

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import Row


# ============================================================================
# IsolationForestModelPipeline
# ============================================================================


def _patch_isolation_forest(monkeypatch, tmp_gold, tmp_ml):
    import app.pipeline.gold_impl.mart_builder as mb
    monkeypatch.setattr(mb, "ML_DIR", tmp_ml)
    monkeypatch.setattr(mb, "GOLD_DIR", tmp_gold)

    import app.pipeline.gold_impl.ml.isolation_forest_model as m
    monkeypatch.setattr(m, "ML_DIR", tmp_ml)
    monkeypatch.setattr(m, "GOLD_DIR", tmp_gold)
    monkeypatch.setattr(m, "SCORES_DIR", tmp_ml / "ml_isolation_fraud_scores")
    monkeypatch.setattr(m, "MODELS_DIR", tmp_gold / "models" / "isolation_forest")


def test_isolation_forest_normal_training(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_isolation_forest(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.isolation_forest_model import (
        IsolationForestModelPipeline,
    )

    rows = []
    for i in range(10):
        rows.append(
            Row(
                trip_id=i,  # xxhash64 BIGINT en produccion; aqui entero de prueba
                ratecode_id=1,
                service_id="yellow",
                year=2024,
                month=1,
                is_anomaly_candidate=True,
                velocidad_promedio_calculada=float(30 + i),
                costo_por_distancia=float(2.5 + i * 0.1),
                duracion_viaje_segundos=float(600 + i * 10),
                trip_distance=float(5.0 + i * 0.5),
                fare_amount=float(20.0 + i * 2),
                ratio_peaje_tarifa=float(0.05 + i * 0.01),
            )
        )
    for i in range(10, 20):
        rows.append(
            Row(
                trip_id=i,  # xxhash64 BIGINT en produccion; aqui entero de prueba
                ratecode_id=2,
                service_id="yellow",
                year=2024,
                month=1,
                is_anomaly_candidate=True,
                velocidad_promedio_calculada=float(45 + i),
                costo_por_distancia=float(3.1 + i * 0.05),
                duracion_viaje_segundos=float(900 + i * 15),
                trip_distance=float(8.0 + i * 0.3),
                fare_amount=float(35.0 + i),
                ratio_peaje_tarifa=float(0.08 + i * 0.005),
            )
        )

    feat_dir = tmp_ml / "ml_feat_isolation_fraud"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = IsolationForestModelPipeline(settings)
    result = pipeline.run()

    assert result > 0
    scores_base = tmp_ml / "ml_isolation_fraud_scores"
    models_base = tmp_gold / "models" / "isolation_forest"
    assert (scores_base / "ratecode_id=1").exists()
    assert (scores_base / "ratecode_id=2").exists()
    assert (models_base / "1" / "model.joblib").exists()
    assert (models_base / "2" / "model.joblib").exists()


def test_isolation_forest_missing_feature_store(monkeypatch, settings, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    _patch_isolation_forest(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.isolation_forest_model import IsolationForestModelPipeline

    pipeline = IsolationForestModelPipeline(settings)
    result = pipeline.run()
    assert result == -1


def test_isolation_forest_low_rows_skipped(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_isolation_forest(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.isolation_forest_model import (
        IsolationForestModelPipeline,
    )

    rows = []
    for i in range(3):
        rows.append(
            Row(
                trip_id=i,  # xxhash64 BIGINT en produccion; aqui entero de prueba
                ratecode_id=1,
                service_id="yellow",
                year=2024,
                month=1,
                is_anomaly_candidate=True,
                velocidad_promedio_calculada=float(30 + i),
                costo_por_distancia=2.5,
                duracion_viaje_segundos=600.0,
                trip_distance=5.0,
                fare_amount=20.0,
                ratio_peaje_tarifa=0.05,
            )
        )
    for i in range(3, 6):
        rows.append(
            Row(
                trip_id=i,  # xxhash64 BIGINT en produccion; aqui entero de prueba
                ratecode_id=2,
                service_id="yellow",
                year=2024,
                month=1,
                is_anomaly_candidate=True,
                velocidad_promedio_calculada=45.0,
                costo_por_distancia=3.1,
                duracion_viaje_segundos=900.0,
                trip_distance=8.0,
                fare_amount=35.0,
                ratio_peaje_tarifa=0.08,
            )
        )

    feat_dir = tmp_ml / "ml_feat_isolation_fraud"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = IsolationForestModelPipeline(settings)
    result = pipeline.run()

    assert result > 0

    scores_base = tmp_ml / "ml_isolation_fraud_scores"
    models_base = tmp_gold / "models" / "isolation_forest"
    scores_df = spark.read.parquet(str(scores_base))
    statuses = [r["model_status"] for r in scores_df.select("model_status").distinct().collect()]
    assert "skipped_low_rows" in statuses
    assert not (models_base / "1" / "model.joblib").exists()
    assert not (models_base / "2" / "model.joblib").exists()


def test_make_train_score_fn_picklable():
    from pyspark.cloudpickle import cloudpickle

    from app.pipeline.gold_impl.ml.isolation_forest_model import _make_train_score_fn

    fn = _make_train_score_fn(10, 0.05, 10, "auto", 42)
    data = cloudpickle.dumps(fn)
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_isolation_forest_output_schema(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_isolation_forest(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.isolation_forest_model import (
        OUTPUT_SCHEMA,
        IsolationForestModelPipeline,
    )

    expected_names = set(f.name for f in OUTPUT_SCHEMA.fields)
    rows = []
    for i in range(10):
        rows.append(
            Row(
                trip_id=i,  # xxhash64 BIGINT en produccion; aqui entero de prueba
                ratecode_id=1,
                service_id="yellow",
                year=2024,
                month=1,
                is_anomaly_candidate=True,
                velocidad_promedio_calculada=float(30 + i),
                costo_por_distancia=2.5,
                duracion_viaje_segundos=600.0,
                trip_distance=5.0,
                fare_amount=20.0,
                ratio_peaje_tarifa=0.05,
            )
        )

    feat_dir = tmp_ml / "ml_feat_isolation_fraud"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = IsolationForestModelPipeline(settings)
    pipeline.run()

    scores_base = tmp_ml / "ml_isolation_fraud_scores"
    scores_df = spark.read.parquet(str(scores_base))
    assert set(scores_df.columns) == expected_names


# ============================================================================
# KModesModelPipeline
# ============================================================================


def _patch_kmodes(monkeypatch, tmp_gold, tmp_ml):
    import app.pipeline.gold_impl.mart_builder as mb
    monkeypatch.setattr(mb, "ML_DIR", tmp_ml)
    monkeypatch.setattr(mb, "GOLD_DIR", tmp_gold)

    import app.pipeline.gold_impl.ml.kmodes_model as m
    monkeypatch.setattr(m, "ML_DIR", tmp_ml)
    monkeypatch.setattr(m, "GOLD_DIR", tmp_gold)
    monkeypatch.setattr(m, "KMODELS_DIR", tmp_ml / "kmodes_model")


def _kmodes_rows(rng, n_yellow, n_fhvhv):
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx"]
    franjas = ["manana", "tarde", "noche"]
    dias = ["laborable", "finde"]
    payments = ["tarjeta", "efectivo"]
    ratecodes = ["1", "2"]
    pgroups = ["1", "2", "3+"]
    licenses = ["HV0001", "HV0002"]

    rows = []
    for i in range(n_yellow):
        rows.append(
            Row(
                trip_id=f"trip_y_{i}",
                service_id="yellow",
                year=2024,
                month=1,
                borough_pu=str(rng.choice(boroughs)),
                borough_do=str(rng.choice(boroughs)),
                franja_horaria=str(rng.choice(franjas)),
                dia_categoria=str(rng.choice(dias)),
                payment_type=str(rng.choice(payments)),
                ratecode=str(rng.choice(ratecodes)),
                passenger_group=str(rng.choice(pgroups)),
                hvfhs_license_num="NA",
            )
        )
    for i in range(n_fhvhv):
        rows.append(
            Row(
                trip_id=f"trip_f_{i}",
                service_id="fhvhv",
                year=2024,
                month=1,
                borough_pu=str(rng.choice(boroughs)),
                borough_do=str(rng.choice(boroughs)),
                franja_horaria=str(rng.choice(franjas)),
                dia_categoria=str(rng.choice(dias)),
                payment_type="NA",
                ratecode="NA",
                passenger_group="NA",
                hvfhs_license_num=str(rng.choice(licenses)),
            )
        )
    return rows


def test_kmodes_normal_training(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_kmodes(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.kmodes_model import KModesModelPipeline

    rng = np.random.default_rng(42)
    rows = _kmodes_rows(rng, 60, 40)

    feat_dir = tmp_ml / "ml_feat_kmodes_trips"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = KModesModelPipeline(settings)
    result = pipeline.run()

    assert result > 0

    km_base = tmp_ml / "kmodes_model"
    assert (km_base / "tuning_service_id=yellow").exists()
    assert (km_base / "centers_service_id=yellow").exists()
    assert (km_base / "profiles_service_id=yellow").exists()
    assert (km_base / "labels_service_id=yellow").exists()
    assert (tmp_gold / "models" / "kmodes" / "yellow" / "model.joblib").exists()


def test_kmodes_missing_feature_store(monkeypatch, settings, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    _patch_kmodes(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.kmodes_model import KModesModelPipeline

    pipeline = KModesModelPipeline(settings)
    result = pipeline.run()
    assert result == -1


def test_matching_dissim():
    from app.pipeline.gold_impl.ml.kmodes_model import _matching_dissim

    a = np.array([0, 1, 2])
    b = np.array([0, 1, 2])
    assert _matching_dissim(a, b) == 0

    a = np.array([0, 1, 2])
    b = np.array([3, 4, 5])
    assert _matching_dissim(a, b) == 3

    a = np.array([0, 1, 2])
    b = np.array([0, 9, 2])
    assert _matching_dissim(a, b) == 1


def test_kmodes_output_parquet_structure(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_kmodes(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.kmodes_model import KModesModelPipeline

    rng = np.random.default_rng(42)
    rows = _kmodes_rows(rng, 60, 40)

    feat_dir = tmp_ml / "ml_feat_kmodes_trips"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = KModesModelPipeline(settings)
    pipeline.run()

    km_base = tmp_ml / "kmodes_model"
    centers = pd.read_parquet(km_base / "centers_service_id=yellow" / "centers.parquet")
    assert "cluster_id" in centers.columns
    assert "borough_pu" in centers.columns
    assert "payment_type" in centers.columns

    profiles = pd.read_parquet(km_base / "profiles_service_id=yellow" / "profiles.parquet")
    assert "cluster_id" in profiles.columns
    assert "feature" in profiles.columns
    assert "top_value" in profiles.columns
    assert "top_pct" in profiles.columns
    assert "n_unique" in profiles.columns


# ============================================================================
# SariMaxModelPipeline
# ============================================================================


def _patch_sarimax(monkeypatch, tmp_gold, tmp_ml):
    import app.pipeline.gold_impl.mart_builder as mb
    monkeypatch.setattr(mb, "ML_DIR", tmp_ml)
    monkeypatch.setattr(mb, "GOLD_DIR", tmp_gold)

    import app.pipeline.gold_impl.ml.sarimax_model as m
    monkeypatch.setattr(m, "ML_DIR", tmp_ml)
    monkeypatch.setattr(m, "GOLD_DIR", tmp_gold)
    monkeypatch.setattr(m, "FORECAST_DIR", tmp_ml / "ml_sarimax_trips_forecast")
    monkeypatch.setattr(m, "MODELS_DIR", tmp_gold / "models" / "sarimax")


def _sarimax_rows(n, borough, service_id, start_hour=0):
    base = datetime(2024, 1, 7, 0, 0, 0)
    rows = []
    for i in range(n):
        t = base + timedelta(hours=start_hour + i)
        rows.append(
            Row(
                borough=borough,
                service_id=service_id,
                pickup_hour=t,
                trip_count=float(100 + i),
            )
        )
    return rows


def test_sarimax_normal_training(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_sarimax(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline

    rows = _sarimax_rows(100, "Manhattan", "yellow")
    rows.extend(_sarimax_rows(50, "Brooklyn", "green"))

    feat_dir = tmp_ml / "ml_feat_arima_trips"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = SariMaxModelPipeline(settings)
    result = pipeline.run()

    assert result > 0
    forecast_dir = tmp_ml / "ml_sarimax_trips_forecast"
    assert (forecast_dir / "borough=Manhattan" / "service_id=yellow").exists()
    assert (tmp_gold / "models" / "sarimax" / "Manhattan__yellow" / "model.joblib").exists()


def test_sarimax_missing_feature_store(monkeypatch, settings, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    _patch_sarimax(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline

    pipeline = SariMaxModelPipeline(settings)
    result = pipeline.run()
    assert result == -1


def test_sarimax_low_rows_skipped(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_sarimax(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline

    rows = _sarimax_rows(5, "Manhattan", "yellow")

    feat_dir = tmp_ml / "ml_feat_arima_trips"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = SariMaxModelPipeline(settings)
    result = pipeline.run()

    assert result > 0

    forecast_dir = tmp_ml / "ml_sarimax_trips_forecast"
    scores_df = spark.read.parquet(str(forecast_dir))
    statuses = [r["model_status"] for r in scores_df.select("model_status").distinct().collect()]
    assert "skipped_low_rows" in statuses


def test_compute_exog_from_timestamps():
    from app.pipeline.gold_impl.ml.sarimax_model import _compute_exog_from_timestamps

    timestamps = pd.date_range("2024-01-01", periods=168, freq="h")
    result = _compute_exog_from_timestamps(timestamps, "Manhattan")

    assert "is_holiday" in result.columns
    assert "is_weekend" in result.columns
    assert "is_rush_hour" in result.columns
    assert "is_airport_borough" in result.columns

    jan1_0 = timestamps[0]
    jan6_0 = timestamps[120]
    jan7_0 = timestamps[144]

    assert result.loc[jan1_0, "is_holiday"] == 1.0
    assert result.loc[jan6_0, "is_holiday"] == 0.0
    assert result.loc[jan7_0, "is_holiday"] == 0.0

    jan6_sat = jan6_0
    assert result.loc[jan6_sat, "is_weekend"] == 1.0
    assert result.loc[jan1_0, "is_weekend"] == 0.0

    hour7 = timestamps[7]
    hour12 = timestamps[12]
    hour17 = timestamps[17]
    assert result.loc[hour7, "is_rush_hour"] == 1.0
    assert result.loc[hour12, "is_rush_hour"] == 0.0
    assert result.loc[hour17, "is_rush_hour"] == 1.0

    assert result.loc[jan1_0, "is_airport_borough"] == 0.0

    result_ewr = _compute_exog_from_timestamps(timestamps, "EWR")
    assert result_ewr.loc[jan1_0, "is_airport_borough"] == 1.0

    result_queens = _compute_exog_from_timestamps(timestamps, "Queens")
    assert result_queens.loc[jan1_0, "is_airport_borough"] == 1.0


def test_sarimax_fallback_order(spark, settings, monkeypatch, tmp_path):
    tmp_gold = tmp_path
    tmp_ml = tmp_gold / "ml"
    tmp_ml.mkdir(parents=True)
    _patch_sarimax(monkeypatch, tmp_gold, tmp_ml)

    from app.pipeline.gold_impl.ml.sarimax_model import SariMaxModelPipeline

    rows = _sarimax_rows(15, "Manhattan", "yellow")

    feat_dir = tmp_ml / "ml_feat_arima_trips"
    feat_dir.mkdir(parents=True)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(feat_dir))

    pipeline = SariMaxModelPipeline(settings)
    result = pipeline.run()

    assert result > 0

    forecast_dir = tmp_ml / "ml_sarimax_trips_forecast"
    scores_df = spark.read.parquet(str(forecast_dir))
    statuses = set(
        r["model_status"] for r in scores_df.select("model_status").distinct().collect()
    )
    assert statuses & {"ok", "fallback_order"}, (
        f"Expected ok or fallback_order, got {statuses}"
    )
