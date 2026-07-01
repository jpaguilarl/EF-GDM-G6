"""D3.4 — Isolation Forest por RatecodeID sobre ml_feat_isolation_fraud.

Lee el feature store de fraude (trip-grain, yellow/green), agrupa por ratecode_id,
entrena un sklearn IsolationForest por grupo, emite scores + etiquetas y serializa
artefactos de cada modelo.

Output:
- data/gold/ml/ml_isolation_fraud_scores/ (particionado por ratecode_id)
- data/gold/models/isolation_forest/{ratecode_id}/model.joblib + metadata.json
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
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from sklearn.ensemble import IsolationForest

from app.pipeline.gold.mart_builder import GOLD_DIR, ML_DIR
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient

# --- Columnas de entrada y salida ------------------------------------------
FEATURE_COLS: list[str] = [
    "velocidad_promedio_calculada",
    "costo_por_distancia",
    "duracion_viaje_segundos",
    "trip_distance",
    "fare_amount",
    "ratio_peaje_tarifa",
]

IDENTIFIER_COLS: list[str] = [
    "trip_id",
    "ratecode_id",
    "service_id",
    "year",
    "month",
    "is_anomaly_candidate",
]

OUTPUT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("service_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("anomaly_score", DoubleType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("model_status", StringType(), True),
        StructField("is_anomaly_candidate", BooleanType(), True),
    ]
)

# --- Rutas de salida -------------------------------------------------------
SCORES_DIR: Path = ML_DIR / "ml_isolation_fraud_scores"
MODELS_DIR: Path = GOLD_DIR / "models" / "isolation_forest"


def _make_train_score_fn(
    min_rows: int,
    contamination: float,
    n_estimators: int,
    max_samples: str | int,
    random_state: int,
):
    """Devuelve una funcion picklable para ``applyInPandas``.

    Captura los hiperparametros y las constantes de modulo (Path, listas) como
    closure. En Spark ``local[4]`` los side effects (joblib.dump) escriben al
    disco local compartido.
    """
    feature_cols = FEATURE_COLS
    id_cols = IDENTIFIER_COLS
    models_dir = MODELS_DIR

    def _train_score(pdf: pd.DataFrame) -> pd.DataFrame:
        rc = int(pdf["ratecode_id"].iloc[0])
        n_rows = len(pdf)

        # Imputar NaN con mediana, y si toda la columna es NaN -> 0
        X = pdf[feature_cols].copy()
        for col in feature_cols:
            if col in X.columns:
                X[col] = X[col].fillna(X[col].median()).fillna(0.0)

        result = pdf[id_cols].copy()

        if n_rows < min_rows:
            result["anomaly_score"] = np.nan
            result["is_fraud"] = False
            result["model_status"] = "skipped_low_rows"
            return result

        model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            max_samples=max_samples,
            random_state=random_state,
            n_jobs=-1,
        )
        model.fit(X.values)

        scores = -model.decision_function(X.values)
        preds = model.predict(X.values)

        model_dir = models_dir / str(rc)
        model_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, model_dir / "model.joblib")

        with (model_dir / "metadata.json").open("w") as f:
            json.dump(
                {
                    "ratecode_id": rc,
                    "n_rows": n_rows,
                    "n_features": len(feature_cols),
                    "features": feature_cols,
                    "contamination": contamination,
                    "n_estimators": n_estimators,
                    "max_samples": max_samples,
                    "random_state": random_state,
                    "trained_at": datetime.now().isoformat(),
                },
                f,
                indent=2,
            )

        result["anomaly_score"] = scores
        result["is_fraud"] = (preds == -1).astype(bool)
        result["model_status"] = "ok"
        return result[list(OUTPUT_SCHEMA.fieldNames())]

    return _train_score


class IsolationForestModelPipeline:
    """Entrena y evalua un Isolation Forest por RatecodeID.

    Uso:
        pipeline = IsolationForestModelPipeline(config)
        total = pipeline.run()
    """

    def __init__(self, config) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        self.if_config = config.gold.isolation_fraud

    def run(self) -> int:
        spark = self.spark_client.get_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.logger.info(
            f"Iniciando Isolation Forest | audit_id={self.audit_id}"
        )

        feature_store_dir = ML_DIR / "ml_feat_isolation_fraud"
        if not feature_store_dir.exists():
            self.logger.error(
                "Feature store ml_feat_isolation_fraud no encontrado en "
                f"{feature_store_dir}. Ejecuta "
                "'uv run main.py --gold --only ml_feat_isolation_fraud' "
                "primero. Abortando."
            )
            return -1

        self.logger.info("Leyendo ml_feat_isolation_fraud")
        df = spark.read.parquet(str(feature_store_dir))

        cols_needed = IDENTIFIER_COLS + FEATURE_COLS
        available = [c for c in cols_needed if c in df.columns]
        missing = set(cols_needed) - set(available)
        if missing:
            self.logger.warning(f"Columnas faltantes en origen: {missing}")
            return -1

        data = df.select(*available).filter(
            F.col("ratecode_id").isNotNull()
        )

        n_estimators = self.if_config.n_estimators
        contamination = self.if_config.contamination
        max_samples = self.if_config.max_samples
        random_state = self.if_config.random_state
        min_rows = self.if_config.min_rows_per_ratecode

        train_fn = _make_train_score_fn(
            min_rows=min_rows,
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=max_samples,
            random_state=random_state,
        )

        scored = data.groupby("ratecode_id").applyInPandas(
            train_fn, schema=OUTPUT_SCHEMA
        )

        n = scored.count()
        if n == 0:
            self.logger.warning("Sin datos para modelar")
            return -1

        SCORES_DIR.parent.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            f"Escribiendo {n} viajes puntuados a {SCORES_DIR}"
        )
        scored.write.mode("overwrite").partitionBy("ratecode_id").parquet(
            str(SCORES_DIR)
        )

        self.logger.info("Resumen de modelos por RatecodeID:")
        for row in (
            scored.groupby("ratecode_id", "model_status")
            .agg(F.count("*").alias("cnt"))
            .orderBy("ratecode_id")
            .collect()
        ):
            self.logger.info(
                f"  RatecodeID {row['ratecode_id']:>2} | "
                f"{row['model_status']:>18} | {row['cnt']:>8} viajes"
            )

        self._write_audit(n)
        self.logger.info("Isolation Forest completado exitosamente")
        return n

    def _write_audit(self, rowcount: int) -> None:
        audit_path = ML_DIR / "audit.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "ml_audit_id": self.audit_id,
            "pipeline": "isolation_forest",
            "contamination": self.if_config.contamination,
            "n_estimators": self.if_config.n_estimators,
            "min_rows_per_ratecode": self.if_config.min_rows_per_ratecode,
            "rowcount_output": rowcount,
            "started_at": datetime.now().isoformat(),
        }
        df_new = pl.DataFrame([row])
        if audit_path.exists():
            existing = pl.read_parquet(str(audit_path))
            df_new = pl.concat([existing, df_new])
        df_new.write_parquet(str(audit_path))
