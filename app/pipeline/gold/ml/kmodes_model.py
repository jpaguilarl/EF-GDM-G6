"""K-Modes clustering de perfiles de viaje por service_id.

Lee ml_feat_kmodes_trips, entrena un KModes por service_id con seleccion de
columnas categoricas especifica por categoria. Realiza tuning de k (elbow +
silhouette categorica), escribe etiquetas, centroides (modas) y perfiles.

Output:
- data/gold/ml/kmodes_model/tuning_{svc}/   curvas costo + silhouette por k
- data/gold/ml/kmodes_model/centers_{svc}/  modas de cada cluster
- data/gold/ml/kmodes_model/labels_{svc}/   trip_id -> cluster_id
- data/gold/ml/kmodes_model/profiles_{svc}/ distribucion por feature dentro de cluster
"""

import json
import uuid
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from kmodes.kmodes import KModes
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from sklearn.metrics import silhouette_score

from app.pipeline.gold.mart_builder import GOLD_DIR, ML_DIR
from app.utils import storage
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient

# --- Columnas de feature por servicio ---------------------------------------
YELLOW_GREEN_FEATURES: list[str] = [
    "borough_pu",
    "borough_do",
    "franja_horaria",
    "dia_categoria",
    "payment_type",
    "ratecode",
    "passenger_group",
]

FHVHV_FEATURES: list[str] = [
    "borough_pu",
    "borough_do",
    "franja_horaria",
    "dia_categoria",
    "hvfhs_license_num",
]

IDENTIFIER_COLS: list[str] = [
    "trip_id",
    "service_id",
    "year",
    "month",
]

LABEL_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("cluster_id", IntegerType(), True),
    ]
)

# --- Rutas de salida --------------------------------------------------------
KMODELS_DIR: Path = ML_DIR / "kmodes_model"


def _matching_dissim(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Distancia de coincidencia (0 = match exacto, n_cats = max diff).

    Compatible con sklearn silhouette: recibe dos vectores 1D.
    """
    return (a != b).sum()


class KModesModelPipeline:
    """Entrena KModes por service_id sobre el feature store categorico.

    Uso:
        pipeline = KModesModelPipeline(config)
        total = pipeline.run()
    """

    def __init__(self, config) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        self.kmodes_config = config.gold.kmodes

    def run(self) -> int:
        spark = self.spark_client.get_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        feature_store_dir = ML_DIR / "ml_feat_kmodes_trips"
        if not feature_store_dir.exists():
            self.logger.error(
                "Feature store ml_feat_kmodes_trips no encontrado en "
                f"{feature_store_dir}. Ejecuta "
                "'uv run main.py --gold --only ml_feat_kmodes_trips' primero. Abortando."
            )
            return -1

        self.logger.info("Leyendo ml_feat_kmodes_trips")
        df = spark.read.parquet(storage.for_spark(feature_store_dir))

        available = set(df.columns)
        all_features = (
            set(YELLOW_GREEN_FEATURES) | set(FHVHV_FEATURES) | set(IDENTIFIER_COLS)
        )
        missing = all_features - available
        if missing:
            self.logger.warning(f"Columnas faltantes en origen: {missing}")
            return -1

        services = [
            row["service_id"]
            for row in df.select("service_id")
            .distinct()
            .orderBy("service_id")
            .collect()
        ]

        self.logger.info(f"Servicios encontrados: {services}")
        total_labeled = 0

        for svc in services:
            self.logger.info(f"=== Procesando service_id={svc} ===")
            features = (
                FHVHV_FEATURES if svc == "fhvhv" else YELLOW_GREEN_FEATURES
            )
            n = self._train_service(spark, df, svc, features)
            if n < 0:
                self.logger.warning(f"  {svc}: sin datos suficientes, omitido")
                continue
            total_labeled += n

        self._write_audit(total_labeled)
        self.logger.info("K-Modes completado exitosamente")
        return total_labeled

    # ------------------------------------------------------------------
    def _train_service(
        self,
        spark,
        df_all,
        service_id: str,
        feature_cols: list[str],
    ) -> int:
        cfg = self.kmodes_config
        svc_dir = KMODELS_DIR / f"service_id={service_id}"
        svc_dir.mkdir(parents=True, exist_ok=True)

        subset = df_all.filter(F.col("service_id") == service_id).select(
            *IDENTIFIER_COLS, *feature_cols
        )
        total_rows = subset.count()
        self.logger.info(f"  Filas totales: {total_rows}")

        # --- Spark-sample antes de colectar al driver ----------------------
        sample_size = cfg.max_sample_per_service
        if total_rows > sample_size:
            fraction = min(1.0, sample_size / total_rows)
            subset = subset.sample(withReplacement=False, fraction=fraction, seed=cfg.random_state)
            self.logger.info(
                f"  Muestreo Spark con fraccion={fraction:.6f} (max_sample={sample_size})"
            )

        pdf: pd.DataFrame = subset.toPandas()

        # --- Drop de nulos -------------------------------------------------
        before = len(pdf)
        pdf = pdf.dropna(subset=feature_cols).reset_index(drop=True)
        dropped = before - len(pdf)
        if dropped:
            pct = 100.0 * dropped / before
            self.logger.info(f"  Filas con nulos eliminadas: {dropped} ({pct:.1f}%)")

        if len(pdf) == 0:
            return -1

        # --- Ajuste post-sample (approx por si el sample aun excede) --------
        if len(pdf) > sample_size:
            rng = np.random.default_rng(cfg.random_state)
            pdf = pdf.iloc[
                rng.choice(len(pdf), size=sample_size, replace=False)
            ].reset_index(drop=True)
            self.logger.info(
                f"  Ajuste post-drop a {sample_size} filas"
            )

        # --- Factorizar strings -> ints ------------------------------------
        encoded_cols: list[np.ndarray] = []
        mappings: dict[str, dict[int, str]] = {}
        for col in feature_cols:
            codes, uniques = pd.factorize(pdf[col].astype(str))
            encoded_cols.append(codes)
            mappings[col] = dict(enumerate(uniques.tolist()))

        X = np.column_stack(encoded_cols).astype(np.int32)
        n_rows = X.shape[0]
        self.logger.info(
            f"  Matriz: {n_rows} filas x {len(feature_cols)} features"
        )

        # --- Tuning: elbow + silhouette ------------------------------------
        max_k = min(cfg.max_k, n_rows - 1)
        if max_k < 2:
            self.logger.warning("  Menos de 2 filas distintas: saltando modelo")
            return -1

        # Submuestra para silhouette (O(n^2) inviable en >5K filas)
        sil_sample_size = min(5000, n_rows)
        rng_sil = np.random.default_rng(cfg.random_state)
        sil_idx = rng_sil.choice(n_rows, size=sil_sample_size, replace=False)
        X_sil = X[sil_idx]

        costs: list[float] = []
        sil_scores: list[float] = []
        best_k = 2
        best_sil = -1.0

        for k in range(2, max_k + 1):
            km = KModes(
                n_clusters=k,
                init=cfg.init_method,
                n_init=1,
                random_state=cfg.random_state,
                verbose=0,
            )
            labels = km.fit_predict(X)
            cost = float(km.cost_)
            costs.append(cost)

            labels_sil = labels[sil_idx]
            if len(set(labels_sil)) > 1:
                s = silhouette_score(
                    X_sil,
                    labels_sil,
                    metric=_matching_dissim,
                    random_state=cfg.random_state,
                )
            else:
                s = -1.0
            sil_scores.append(s)

            if s > best_sil:
                best_sil = s
                best_k = k

            self.logger.info(
                f"    k={k:>2} | cost={cost:>12,.0f} | silhouette={s:.4f}"
            )

        # --- Guardar tuning ------------------------------------------------
        tuning_df = pd.DataFrame(
            {
                "k": list(range(2, max_k + 1)),
                "cost": costs,
                "silhouette": sil_scores,
            }
        )
        tuning_path = KMODELS_DIR / f"tuning_service_id={service_id}" / "tuning.parquet"
        tuning_path.parent.mkdir(parents=True, exist_ok=True)
        tuning_df.to_parquet(str(tuning_path), index=False)

        self.logger.info(f"  Mejor k elegido: {best_k} (silhouette={best_sil:.4f})")

        # --- Fit final con best_k ------------------------------------------
        km = KModes(
            n_clusters=best_k,
            init=cfg.init_method,
            n_init=cfg.n_init,
            random_state=cfg.random_state,
            verbose=0,
        )
        final_labels = km.fit_predict(X)

        # --- Centroides (modas) --------------------------------------------
        centers = km.cluster_centroids_.astype(np.int32)
        center_rows: list[dict] = []
        for cid in range(best_k):
            row: dict = {"cluster_id": int(cid), "n_rows": int((final_labels == cid).sum())}
            for j, col in enumerate(feature_cols):
                raw_val = centers[cid, j]
                label = mappings[col].get(int(raw_val), str(raw_val))
                row[col] = label
            center_rows.append(row)

        centers_df = pd.DataFrame(center_rows)
        centers_path = KMODELS_DIR / f"centers_service_id={service_id}" / "centers.parquet"
        centers_path.parent.mkdir(parents=True, exist_ok=True)
        centers_df.to_parquet(str(centers_path), index=False)

        # --- Perfiles de cluster -------------------------------------------
        pdf_labeled = pdf[IDENTIFIER_COLS].copy()
        pdf_labeled["cluster_id"] = final_labels
        prof_rows: list[dict] = []
        for cid in range(best_k):
            mask = final_labels == cid
            n_in_cluster = int(mask.sum())
            for col in feature_cols:
                val_counts = pdf.loc[mask, col].astype(str).value_counts()
                top_val = val_counts.index[0]
                top_pct = float(100.0 * val_counts.iloc[0] / n_in_cluster)
                prof_rows.append(
                    {
                        "cluster_id": cid,
                        "feature": col,
                        "top_value": top_val,
                        "top_pct": round(top_pct, 2),
                        "n_unique": int(val_counts.count()),
                    }
                )

        profiles_df = pd.DataFrame(prof_rows)
        profiles_path = KMODELS_DIR / f"profiles_service_id={service_id}" / "profiles.parquet"
        profiles_path.parent.mkdir(parents=True, exist_ok=True)
        profiles_df.to_parquet(str(profiles_path), index=False)

        # --- Escribir labels (todas las filas, no solo sample) --------------
        # Las labels solo existen para el sample; para el resto del dataset
        # necesitariamos predecir. Mejor re-procesar todo con el modelo final.
        # En esta version escribimos las labels del sample.
        out_labels_dir = KMODELS_DIR / f"labels_service_id={service_id}"
        n_labeled = len(pdf_labeled)
        spark_labels = spark.createDataFrame(pdf_labeled, schema=LABEL_SCHEMA)
        spark_labels.write.mode("overwrite").parquet(storage.for_spark(out_labels_dir))
        self.logger.info(f"  Labels escritas: {n_labeled} viajes")

        # --- Serializar modelo -------------------------------------------
        model_dir = GOLD_DIR / "models" / "kmodes" / str(service_id)
        model_dir.mkdir(parents=True, exist_ok=True)

        model_meta = {
            "service_id": service_id,
            "n_clusters": best_k,
            "n_rows_trained": int(n_rows),
            "feature_cols": feature_cols,
            "init_method": cfg.init_method,
            "n_init": cfg.n_init,
            "random_state": cfg.random_state,
            "best_silhouette": round(float(best_sil), 4),
            "trained_at": datetime.now().isoformat(),
        }
        with (model_dir / "metadata.json").open("w") as f:
            json.dump(model_meta, f, indent=2)

        # Guardar mapping de categorias
        mapping_dict = {
            col: {str(k): v for k, v in cats.items()}
            for col, cats in mappings.items()
        }
        with (model_dir / "category_mapping.json").open("w") as f:
            json.dump(mapping_dict, f, indent=2)

        # joblib serializa el objeto KModes completo (centroids + params)
        import joblib

        with storage.open_writable(model_dir / "model.joblib") as f:
            joblib.dump(km, f)

        self.logger.info(
            f"  Modelo guardado en {model_dir} | k={best_k} | silhouette={best_sil:.4f}"
        )

        return n_labeled

    # ------------------------------------------------------------------
    def _write_audit(self, rowcount: int) -> None:
        audit_path = ML_DIR / "audit.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "ml_audit_id": self.audit_id,
            "pipeline": "kmodes",
            "max_k": self.kmodes_config.max_k,
            "max_sample_per_service": self.kmodes_config.max_sample_per_service,
            "n_init": self.kmodes_config.n_init,
            "rowcount_output": rowcount,
            "started_at": datetime.now().isoformat(),
        }
        df_new = pl.DataFrame([row])
        if audit_path.exists():
            existing = pl.read_parquet(str(audit_path))
            df_new = pl.concat([existing, df_new])
        df_new.write_parquet(str(audit_path))
