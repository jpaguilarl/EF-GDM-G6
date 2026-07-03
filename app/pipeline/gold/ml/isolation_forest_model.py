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
    LongType,
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
        StructField("trip_id", LongType(), True),
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

MAX_SAMPLE_PER_RATECODE = 200_000


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
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
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

        ratecode_ids = [
            r.ratecode_id
            for r in data.select("ratecode_id").distinct().orderBy("ratecode_id").collect()
        ]
        self.logger.info(f"Ratecodes encontrados: {ratecode_ids}")

        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        SCORES_DIR.mkdir(parents=True, exist_ok=True)
        total_scored = 0

        for rc in ratecode_ids:
            rc_data = data.filter(F.col("ratecode_id") == rc)
            n_rows = rc_data.count()
            self.logger.info(f"=== RatecodeID={rc} | filas={n_rows:,} ===")

            if n_rows < min_rows:
                self.logger.info(f"  Saltado: solo {n_rows} filas (min_rows={min_rows})")
                continue

            if n_rows > MAX_SAMPLE_PER_RATECODE:
                fraction = MAX_SAMPLE_PER_RATECODE / n_rows
                sampled = rc_data.sample(fraction=fraction, seed=42).limit(MAX_SAMPLE_PER_RATECODE)
                self.logger.info(f"  Muestreo: {n_rows:,} -> {MAX_SAMPLE_PER_RATECODE:,} (frac={fraction:.4f})")
            else:
                sampled = rc_data

            pdf = sampled.toPandas()
            id_part = pdf[IDENTIFIER_COLS].copy()
            X = pdf[FEATURE_COLS].copy()
            for col in FEATURE_COLS:
                if col in X.columns:
                    X[col] = X[col].fillna(X[col].median()).fillna(0.0)

            model = IsolationForest(
                n_estimators=n_estimators,
                contamination=contamination,
                max_samples=max_samples,
                random_state=random_state,
                n_jobs=-1,
            )
            model.fit(X.values)
            scores = -model.decision_function(X.values)

            model_dir = MODELS_DIR / str(rc)
            model_dir.mkdir(parents=True, exist_ok=True)
            joblib.dump(model, model_dir / "model.joblib")
            with (model_dir / "metadata.json").open("w") as f:
                json.dump(
                    {
                        "ratecode_id": int(rc),
                        "n_rows_source": n_rows,
                        "n_rows_trained": len(pdf),
                        "n_features": len(FEATURE_COLS),
                        "features": FEATURE_COLS,
                        "contamination": contamination,
                        "n_estimators": n_estimators,
                        "max_samples": max_samples,
                        "random_state": random_state,
                        "trained_at": datetime.now().isoformat(),
                    },
                    f,
                    indent=2,
                )

            out = id_part.copy()
            out["anomaly_score"] = scores
            out["is_fraud"] = (scores > np.percentile(scores, 95)).astype(bool)
            out["model_status"] = "ok"

            out = out[list(OUTPUT_SCHEMA.fieldNames())]
            n_scored = len(out)
            total_scored += n_scored

            temp_path = str(SCORES_DIR / f"_tmp_{rc}")
            spark.createDataFrame(out).coalesce(1).write.mode("overwrite").parquet(temp_path)

            final_path = str(SCORES_DIR / f"ratecode_id={rc}")
            spark.read.parquet(temp_path).write.mode("overwrite").parquet(final_path)

            self.logger.info(f"  Modelo guardado: RatecodeID={rc} | {n_scored:,} scores escritos")

        if total_scored == 0:
            self.logger.warning("Sin datos para modelar")
            return -1

        self.logger.info(f"Total viajes puntuados: {total_scored:,}")

        self._write_audit(total_scored)
        self.logger.info("Isolation Forest completado exitosamente")
        return total_scored

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
            common = [c for c in df_new.columns if c in existing.columns]
            if common:
                df_new = pl.concat([existing.select(common), df_new.select(common)])
            else:
                df_new = pl.concat([existing, df_new], how="diagonal")
        df_new.write_parquet(str(audit_path))
