"""SARIMAX trip-count forecaster per borough x hora.

Lee ml_feat_arima_trips, enriquece con regresores exogenos rush-hour y
airport-borough, entrena un SARIMAX(1,1,1)(1,1,1,24) por segmento
(borough, service_id) y escribe predicciones in-sample.

Output:
- data/gold/ml/ml_sarimax_trips_forecast/ (particionado por borough, service_id)
- data/gold/models/sarimax/{borough}__{service_id}/model.joblib + metadata.json
"""

import json
import uuid
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from statsmodels.tsa.statespace.sarimax import SARIMAX

from app.pipeline.gold.dims.gold_dimensions import HOLIDAY_DATE_KEYS
from app.pipeline.gold.mart_builder import GOLD_DIR, ML_DIR
from app.utils.logger import Logger
from app.utils.spark import SparkClient

# --- Constantes ------------------------------------------------------------
EXOG_COLS: list[str] = [
    "is_holiday",
    "is_weekend",
    "is_rush_hour",
    "is_airport_borough",
]

RUSH_HOURS: set[int] = {7, 8, 9, 16, 17, 18, 19}
AIRPORT_BOROUGHS: set[str] = {"EWR", "Queens"}

OUTPUT_SCHEMA = StructType(
    [
        StructField("borough", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("pickup_hour", TimestampType(), True),
        StructField("trip_count", DoubleType(), True),
        StructField("yhat", DoubleType(), True),
        StructField("yhat_lower", DoubleType(), True),
        StructField("yhat_upper", DoubleType(), True),
        StructField("model_status", StringType(), True),
    ]
)

# --- Rutas de salida -------------------------------------------------------
FORECAST_DIR: Path = ML_DIR / "ml_sarimax_trips_forecast"
MODELS_DIR: Path = GOLD_DIR / "models" / "sarimax"


def _compute_exog_from_timestamps(
    timestamps: pd.DatetimeIndex, borough: str
) -> pd.DataFrame:
    """Recompone las columnas exogenas a partir de timestamps y borough.

    Todas las columnas de EXOG_COLS son deterministas dados el timestamp
    y el borough, por lo que estan disponibles para cualquier horizonte futuro.
    """
    hour = timestamps.hour
    dow = timestamps.dayofweek  # pandas: Monday=0 .. Sunday=6
    date_keys = timestamps.year * 10000 + timestamps.month * 100 + timestamps.day

    return pd.DataFrame(
        {
            "is_holiday": pd.Series(date_keys, index=timestamps)
            .isin(HOLIDAY_DATE_KEYS)
            .astype(float)
            .values,
            "is_weekend": (dow >= 5).astype(float),
            "is_rush_hour": pd.Series(hour, index=timestamps)
            .isin(RUSH_HOURS)
            .astype(float)
            .values,
            "is_airport_borough": float(borough in AIRPORT_BOROUGHS),
        },
        index=timestamps,
    )


def _make_train_score_fn(
    min_rows: int,
    order: tuple,
    seasonal_order: tuple,
    forecast_horizon: int,
):
    """Devuelve una funcion picklable para ``applyInPandas``.

    Captura los hiperparametros como closure. En Spark ``local[4]`` los
    side effects (joblib.dump) escriben al disco local compartido.
    """

    def _train_score(pdf: pd.DataFrame) -> pd.DataFrame:
        borough = str(pdf["borough"].iloc[0])
        service_id = str(pdf["service_id"].iloc[0])

        # --- Ordenar y reindexar a grilla horaria completa (sin gaps) ---
        pdf = pdf.sort_values("pickup_hour").reset_index(drop=True)
        pdf["pickup_hour"] = pd.to_datetime(pdf["pickup_hour"])

        full_hours = pd.date_range(
            pdf["pickup_hour"].min(),
            pdf["pickup_hour"].max(),
            freq="h",
        )
        full = pd.DataFrame({"pickup_hour": full_hours})
        merged = full.merge(pdf, on="pickup_hour", how="left")
        merged["trip_count"] = merged["trip_count"].fillna(0.0).astype(float)

        exog = _compute_exog_from_timestamps(
            pd.DatetimeIndex(merged["pickup_hour"]), borough
        )

        n = len(merged)

        # --- Saltar segmentos con pocos datos --------------------------
        if n < min_rows:
            merged["yhat"] = np.nan
            merged["yhat_lower"] = np.nan
            merged["yhat_upper"] = np.nan
            merged["model_status"] = "skipped_low_rows"
            merged["borough"] = borough
            merged["service_id"] = service_id
            return merged[list(OUTPUT_SCHEMA.fieldNames())]

        # --- Split train / backtest ------------------------------------
        y = merged["trip_count"].values
        if forecast_horizon > 0 and n > forecast_horizon:
            train_end = n - forecast_horizon
            y_train = y[:train_end]
            exog_train = exog.iloc[:train_end]
            y_test = y[train_end:]
            exog_test = exog.iloc[train_end:]
        else:
            y_train = y
            exog_train = exog
            y_test = None
            exog_test = None

        # --- Ajustar SARIMAX -------------------------------------------
        model_status = "ok"
        result = None
        actual_order = order
        actual_seasonal = seasonal_order

        def _fit(order_args, seasonal_args, label):
            m = SARIMAX(
                y_train,
                exog=exog_train.values,
                order=order_args,
                seasonal_order=seasonal_args,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            return m.fit(disp=False), label, order_args, seasonal_args

        try:
            result, model_status, actual_order, actual_seasonal = _fit(
                order, seasonal_order, "ok"
            )
        except Exception:
            try:
                result, model_status, actual_order, actual_seasonal = _fit(
                    (1, 0, 1), (0, 1, 1, 24), "fallback_order"
                )
            except Exception:
                merged["yhat"] = np.nan
                merged["yhat_lower"] = np.nan
                merged["yhat_upper"] = np.nan
                merged["model_status"] = "fit_failed"
                merged["borough"] = borough
                merged["service_id"] = service_id
                return merged[list(OUTPUT_SCHEMA.fieldNames())]

        aic = float(result.aic)

        # --- Prediccion in-sample + forecast de backtest ---------------
        in_sample = result.get_prediction()
        yhat_ins = in_sample.predicted_mean
        ci_ins = np.asarray(in_sample.conf_int(alpha=0.05))
        ci_ins_lower = ci_ins[:, 0]
        ci_ins_upper = ci_ins[:, 1]

        mae = None
        mape = None

        if y_test is not None and len(y_test) > 0:
            forec = result.get_forecast(steps=len(y_test), exog=exog_test.values)
            yhat_fc = forec.predicted_mean
            ci_fc = np.asarray(forec.conf_int(alpha=0.05))
            ci_fc_lower = ci_fc[:, 0]
            ci_fc_upper = ci_fc[:, 1]

            yhat_full = np.concatenate([yhat_ins, yhat_fc])
            yhat_lower_full = np.concatenate([ci_ins_lower, ci_fc_lower])
            yhat_upper_full = np.concatenate([ci_ins_upper, ci_fc_upper])

            mae = float(np.mean(np.abs(y_test - yhat_fc)))
            with np.errstate(divide="ignore", invalid="ignore"):
                mape = float(
                    np.mean(
                        np.abs((y_test - yhat_fc) / np.maximum(np.abs(y_test), 1.0))
                    )
                    * 100
                )
        else:
            yhat_full = yhat_ins
            yhat_lower_full = ci_ins_lower
            yhat_upper_full = ci_ins_upper

        merged["yhat"] = yhat_full
        merged["yhat_lower"] = yhat_lower_full
        merged["yhat_upper"] = yhat_upper_full
        merged["model_status"] = model_status
        merged["borough"] = borough
        merged["service_id"] = service_id

        # --- Serializar modelo y metadata ------------------------------
        model_dir = MODELS_DIR / f"{borough}__{service_id}"
        model_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(result, model_dir / "model.joblib")

        with (model_dir / "metadata.json").open("w") as f:
            json.dump(
                {
                    "borough": borough,
                    "service_id": service_id,
                    "order": list(actual_order),
                    "seasonal_order": list(actual_seasonal),
                    "model_status": model_status,
                    "n_rows": n,
                    "exog_cols": EXOG_COLS,
                    "aic": aic,
                    "mae": mae,
                    "mape": mape,
                    "trained_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
            )

        return merged[list(OUTPUT_SCHEMA.fieldNames())]

    return _train_score


class SariMaxModelPipeline:
    """Entrena y evalua SARIMAX(1,1,1)(1,1,1,24) por borough y service_id.

    Uso:
        pipeline = SariMaxModelPipeline(config)
        total = pipeline.run()
    """

    def __init__(self, config) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        self.sarimax_config = config.gold.sarimax

    def run(self) -> int:
        spark = self.spark_client.get_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.logger.info(
            f"Iniciando SARIMAX trip-count forecaster | audit_id={self.audit_id}"
        )

        feature_store_dir = ML_DIR / "ml_feat_arima_trips"
        if not feature_store_dir.exists():
            self.logger.error(
                "Feature store ml_feat_arima_trips no encontrado en "
                f"{feature_store_dir}. Ejecuta "
                "'uv run main.py --gold --only ml_feat_arima_trips' "
                "primero. Abortando."
            )
            return -1

        self.logger.info("Leyendo ml_feat_arima_trips")
        df = spark.read.parquet(str(feature_store_dir))

        cols_needed = ["borough", "service_id", "pickup_hour", "trip_count"]
        available = [c for c in cols_needed if c in df.columns]
        missing = set(cols_needed) - set(available)
        if missing:
            self.logger.warning(f"Columnas faltantes en origen: {missing}")
            return -1

        data = df.select(*available).filter(
            F.col("pickup_hour").isNotNull() & F.col("borough").isNotNull()
        )

        order = tuple(self.sarimax_config.order)
        seasonal_order = tuple(self.sarimax_config.seasonal_order)
        min_rows = self.sarimax_config.min_rows_per_segment
        forecast_horizon = self.sarimax_config.forecast_horizon_hours

        train_fn = _make_train_score_fn(
            min_rows=min_rows,
            order=order,
            seasonal_order=seasonal_order,
            forecast_horizon=forecast_horizon,
        )

        scored = data.groupby("borough", "service_id").applyInPandas(
            train_fn, schema=OUTPUT_SCHEMA
        )

        n = scored.count()
        if n == 0:
            self.logger.warning("Sin datos para modelar")
            return -1

        FORECAST_DIR.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Escribiendo {n} registros de prediccion a {FORECAST_DIR}")
        scored.write.mode("overwrite").partitionBy("borough", "service_id").parquet(
            str(FORECAST_DIR)
        )

        self.logger.info("Resumen de modelos SARIMAX por segmento:")
        for row in (
            scored.groupby("borough", "service_id", "model_status")
            .agg(F.count("*").alias("cnt"))
            .orderBy("borough", "service_id")
            .collect()
        ):
            self.logger.info(
                f"  {row['borough']:>15} | {row['service_id']:>8} | "
                f"{row['model_status']:>18} | {row['cnt']:>8} horas"
            )

        self._write_audit(n)
        self.logger.info("SARIMAX trip-count forecaster completado exitosamente")
        return n

    def _write_audit(self, rowcount: int) -> None:
        audit_path = ML_DIR / "audit.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "ml_audit_id": self.audit_id,
            "pipeline": "sarimax",
            "order": str(tuple(self.sarimax_config.order)),
            "seasonal_order": str(tuple(self.sarimax_config.seasonal_order)),
            "exog_cols": str(EXOG_COLS),
            "min_rows_per_segment": self.sarimax_config.min_rows_per_segment,
            "forecast_horizon_hours": self.sarimax_config.forecast_horizon_hours,
            "rowcount_output": rowcount,
            "started_at": datetime.now().isoformat(),
        }
        df_new = pl.DataFrame([row])
        if audit_path.exists():
            existing = pl.read_parquet(str(audit_path))
            df_new = pl.concat([existing, df_new])
        df_new.write_parquet(str(audit_path))
