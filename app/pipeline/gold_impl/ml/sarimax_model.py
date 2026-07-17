"""SARIMAX trip-count forecaster per borough x hora.

Lee ml_feat_arima_trips, enriquece con regresores exogenos rush-hour y
airport-borough, entrena un SARIMAX(1,1,1)(1,1,1,24) por segmento
(borough, service_id) y escribe predicciones in-sample.

Output:
- data/gold/ml/ml_sarimax_trips_forecast/ (particionado por borough, service_id)
- data/gold/models/sarimax/{borough}__{service_id}/model.pkl + metadata.json
"""

import json
import uuid
import re
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

from app.pipeline.gold_impl.dims.gold_dimensions import HOLIDAY_DATE_KEYS
from app.pipeline.gold_impl.mart_builder import GOLD_DIR, ML_DIR
from app.utils import storage
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

# --- Rutas de salida -------------------------------------------------------
FORECAST_DIR: Path = ML_DIR / "ml_sarimax_trips_forecast"
MODELS_DIR: Path = GOLD_DIR / "models" / "sarimax"


def _fs_safe(value: str) -> str:
    """Sanitiza un valor de segmento para rutas de archivo/directorio.

    El zone-lookup trae boroughs como "N/A": la barra convierte el nombre en
    un subdirectorio inexistente y la escritura revienta con FileNotFoundError
    (fallo real 2026-07-03 en el segmento N/A|fhv). El parquet conserva el
    valor original en su columna borough; solo la ruta usa el sanitizado.
    """
    return re.sub(r'[<>:"/\\|?*]', "_", value)


def _compute_exog_from_timestamps(
    timestamps: pd.DatetimeIndex, borough: str
) -> pd.DataFrame:
    hour = timestamps.hour
    dow = timestamps.dayofweek
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


def _train_and_score_segment(
    pdf: pd.DataFrame,
    min_rows: int,
    order: tuple,
    seasonal_order: tuple,
    forecast_horizon: int,
    forecast_until_year: int | None = None,
) -> pd.DataFrame:
    borough = str(pdf["borough"].iloc[0])
    service_id = str(pdf["service_id"].iloc[0])

    pdf = pdf.sort_values("pickup_hour").reset_index(drop=True)
    pdf["pickup_hour"] = pd.to_datetime(pdf["pickup_hour"])
    if hasattr(pdf["pickup_hour"].dt, "tz") and pdf["pickup_hour"].dt.tz is not None:
        pdf["pickup_hour"] = pdf["pickup_hour"].dt.tz_localize(None)

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

    if n < min_rows:
        out = merged[["pickup_hour", "trip_count"]].copy()
        out["yhat"] = np.nan
        out["yhat_lower"] = np.nan
        out["yhat_upper"] = np.nan
        out["model_status"] = "skipped_low_rows"
        out["borough"] = borough
        out["service_id"] = service_id
        out["forecast_type"] = "actual"
        return out

    if forecast_until_year is not None:
        y_train = merged["trip_count"].values
        exog_train = exog
        y_test = None
        exog_test = None
    else:
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
            out = merged[["pickup_hour", "trip_count"]].copy()
            out["yhat"] = np.nan
            out["yhat_lower"] = np.nan
            out["yhat_upper"] = np.nan
            out["model_status"] = "fit_failed"
            out["borough"] = borough
            out["service_id"] = service_id
            out["forecast_type"] = "actual"
            return out

    aic = float(result.aic)

    in_sample = result.get_prediction()
    yhat_ins = in_sample.predicted_mean
    ci_ins = np.asarray(in_sample.conf_int(alpha=0.05))
    ci_ins_lower = ci_ins[:, 0]
    ci_ins_upper = ci_ins[:, 1]

    mae = None
    mape = None

    if forecast_until_year is not None:
        historical = merged[["pickup_hour", "trip_count"]].copy()
        historical["yhat"] = yhat_ins
        historical["yhat_lower"] = ci_ins_lower
        historical["yhat_upper"] = ci_ins_upper
        historical["model_status"] = model_status
        historical["borough"] = borough
        historical["service_id"] = service_id
        historical["forecast_type"] = "actual"

        future_steps_count = 0
        end_date = pd.Timestamp(year=forecast_until_year, month=12, day=31, hour=23)
        last_hour = merged["pickup_hour"].max()
        future_hours = pd.date_range(
            last_hour + pd.Timedelta(hours=1), end_date, freq="h"
        )
        future_steps_count = len(future_hours)
        if future_steps_count > 0:
            future_exog = _compute_exog_from_timestamps(
                pd.DatetimeIndex(future_hours), borough
            )
            forec = result.get_forecast(
                steps=future_steps_count, exog=future_exog.values
            )
            yhat_fc = forec.predicted_mean
            ci_fc = np.asarray(forec.conf_int(alpha=0.05))
            ci_fc_lower = ci_fc[:, 0]
            ci_fc_upper = ci_fc[:, 1]

            future_df = pd.DataFrame({"pickup_hour": future_hours})
            future_df["trip_count"] = np.nan
            future_df["yhat"] = yhat_fc
            future_df["yhat_lower"] = ci_fc_lower
            future_df["yhat_upper"] = ci_fc_upper
            future_df["model_status"] = model_status
            future_df["borough"] = borough
            future_df["service_id"] = service_id
            future_df["forecast_type"] = "forecast"

            out = pd.concat([historical, future_df], ignore_index=True)
        else:
            out = historical
    else:
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
                        np.abs(
                            (y_test - yhat_fc) / np.maximum(np.abs(y_test), 1.0)
                        )
                    )
                    * 100
                )
        else:
            yhat_full = yhat_ins
            yhat_lower_full = ci_ins_lower
            yhat_upper_full = ci_ins_upper

        out = merged[["pickup_hour", "trip_count"]].copy()
        out["yhat"] = yhat_full
        out["yhat_lower"] = yhat_lower_full
        out["yhat_upper"] = yhat_upper_full
        out["model_status"] = model_status
        out["borough"] = borough
        out["service_id"] = service_id
        out["forecast_type"] = "actual"

    model_dir = MODELS_DIR / f"{_fs_safe(borough)}__{_fs_safe(service_id)}"
    model_dir.mkdir(parents=True, exist_ok=True)
    result.remove_data()
    with storage.open_writable(model_dir / "model.pkl") as f:
        result.save(f, remove_data=True)

    metadata = {
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
    }
    if forecast_until_year is not None:
        metadata["forecast_until_year"] = forecast_until_year
        metadata["future_steps"] = future_steps_count
    with (model_dir / "metadata.json").open("w") as f:
        json.dump(metadata, f, indent=2)

    return out[["pickup_hour", "trip_count", "yhat", "yhat_lower", "yhat_upper", "model_status", "borough", "service_id", "forecast_type"]]


class SariMaxModelPipeline:
    def __init__(self, config) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        self.sarimax_config = config.gold.sarimax

    def run(self) -> int:
        spark = self.spark_client.get_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
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
        df = spark.read.parquet(storage.for_spark(feature_store_dir))

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
        forecast_until_year = self.sarimax_config.forecast_until_year

        segments = (
            data.select("borough", "service_id")
            .distinct()
            .orderBy("borough", "service_id")
            .collect()
        )

        self.logger.info(f"Segmentos a procesar: {len(segments)}")

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
                StructField("forecast_type", StringType(), True),
            ]
        )

        FORECAST_DIR.parent.mkdir(parents=True, exist_ok=True)
        total_rows = 0

        for seg in segments:
            borough_val = seg["borough"]
            service_val = seg["service_id"]
            self.logger.info(f"  Segmento: {borough_val:>15} | {service_val:>8}")

            # Idempotencia por segmento: un SARIMAX tarda ~5-8 min; si el
            # forecast del segmento ya existe se reusa (borrar el archivo
            # para re-entrenar). Permite reanudar tras un corte sin repetir
            # los segmentos ya entrenados.
            # En modo future-forecast (forecast_until_year set), el skip
            # valida que el archivo existente cubra hasta el año destino.
            safe_b, safe_s = _fs_safe(str(borough_val)), _fs_safe(str(service_val))
            seg_dir = FORECAST_DIR / f"borough={safe_b}" / f"service_id={safe_s}"
            part_path = seg_dir / f"forecast_{safe_b}_{safe_s}.zstd.parquet"
            skip = False
            prev_rows = 0
            if part_path.exists():
                if forecast_until_year is not None:
                    existing_df = pl.read_parquet(str(part_path))
                    existing_max = existing_df.select(
                        pl.col("pickup_hour").max()
                    ).item()
                    prev_rows = existing_df.height
                    if existing_max is not None and existing_max.year >= forecast_until_year:
                        skip = True
                    else:
                        self.logger.info(
                            "    existe pero no cubre hasta "
                            f"{forecast_until_year}, se re-entrena"
                        )
                else:
                    skip = True
                    prev_rows = pl.read_parquet(str(part_path)).height
            if skip:
                total_rows += prev_rows
                self.logger.info(
                    f"    ya existe ({prev_rows} horas), se omite"
                )
                continue

            pdf = (
                data.filter(
                    (F.col("borough") == borough_val)
                    & (F.col("service_id") == service_val)
                )
                .toPandas()
            )

            result_pdf = _train_and_score_segment(
                pdf,
                min_rows=min_rows,
                order=order,
                seasonal_order=seasonal_order,
                forecast_horizon=forecast_horizon,
                forecast_until_year=forecast_until_year,
            )

            if result_pdf is not None and len(result_pdf) > 0:
                result_pl = pl.from_pandas(result_pdf)
                result_pl = result_pl.with_columns(
                    pl.col("pickup_hour").cast(pl.Datetime),
                )
                # seg_dir/part_path ya calculados arriba con _fs_safe (el
                # borough "N/A" contiene una barra que rompia la ruta).
                seg_dir.mkdir(parents=True, exist_ok=True)
                result_pl.write_parquet(str(part_path), compression="zstd", compression_level=9)
                total_rows += len(result_pdf)
                status = result_pdf["model_status"].iloc[0]
                self.logger.info(
                    f"    {status:>18} | {len(result_pdf):>8} horas"
                )

        self.logger.info(f"Total registros escritos: {total_rows}")

        spark.catalog.clearCache()

        self._write_audit(total_rows)
        self.logger.info("SARIMAX trip-count forecaster completado exitosamente")
        return total_rows

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
            "forecast_until_year": self.sarimax_config.forecast_until_year,
            "rowcount_output": rowcount,
            "started_at": datetime.now().isoformat(),
        }
        df_new = pl.DataFrame([row])
        if audit_path.exists():
            existing = pl.read_parquet(str(audit_path))
            df_new = pl.concat([existing, df_new], how="diagonal")
        df_new.write_parquet(str(audit_path))
