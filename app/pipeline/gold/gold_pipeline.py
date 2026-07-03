"""Orquestador de la capa gold (Bronze -> Silver -> Gold).

Lee los facts/dims del modelo estrella silver, construye las dimensiones gold y
ejecuta cada builder (marts Power BI + feature store ML). Registra trazabilidad en
``data/gold/audit.parquet`` enlazando con ``silver_audit_id``.

Modos:
- ``full``: reconstruye todas las particiones objetivo.
- ``incremental``: los marts a nivel viaje omiten particiones ya existentes; los
  marts agregados (que abarcan todo el historico) siempre se recomputan.
"""

import hashlib
import uuid
from datetime import datetime

import polars as pl
from pyspark.sql import functions as F

from app.pipeline.gold.dims.gold_dimensions import GoldDimensionsBuilder
from app.pipeline.gold.mart_builder import (
    FACTS_DIR,
    GOLD_DIR,
    SILVER_DIMS_DIR,
    GoldContext,
)
from app.pipeline.gold.marts.abc_xyz_zones import AbcXyzZonesMart
from app.pipeline.gold.marts.demand_volume import DemandVolumeMart
from app.pipeline.gold.marts.financial_performance import FinancialPerformanceMart
from app.pipeline.gold.marts.operational_profile import OperationalProfileMart
from app.pipeline.gold.marts.supply_demand_balance import SupplyDemandBalanceMart
from app.pipeline.gold.marts.tipping_behavior import TippingBehaviorMart
from app.pipeline.gold.ml.arima_features import ArimaFeatures
from app.pipeline.gold.ml.isolation_fraud_features import IsolationFraudFeatures
from app.pipeline.gold.ml.kmodes_features import KModesFeatures
from app.schemas.settings_schema import DatasetsConfig, Module, SettingsSchema
from app.utils.globals import globals
from app.utils.logger import Logger
from app.utils.spark import SparkClient


class GoldPipeline:
    BUILDER_CLASSES = [
        DemandVolumeMart,
        FinancialPerformanceMart,
        OperationalProfileMart,
        SupplyDemandBalanceMart,
        AbcXyzZonesMart,
        TippingBehaviorMart,
        ArimaFeatures,
        KModesFeatures,
        IsolationFraudFeatures,
    ]

    def __init__(self, mode: str = "full", only: list[str] | None = None) -> None:
        self.audit_id = str(uuid.uuid4())
        self.logger = Logger()
        self.spark_client = SparkClient()
        self.mode = mode if mode in ("full", "incremental") else "full"
        self.only = set(only) if only else None

    def run(self, settings: SettingsSchema) -> None:
        spark = self.spark_client.get_session()
        # overwrite dinamico: 'overwrite' solo reemplaza las particiones presentes
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.logger.info(
            f"Iniciando capa gold | audit_id={self.audit_id} | modo={self.mode}"
        )

        if not self._silver_ready():
            self.logger.error(
                "Capa silver/star no encontrada en data/silver/star/. Ejecuta "
                "'uv run main.py --silver schema' y '--silver load' antes de gold. Abortando."
            )
            return

        silver_audit_id = self._latest_silver_audit_id(spark)
        targets = self._expand_targets(settings.datasets)
        self.logger.info(f"Targets: {len(targets)} combinaciones (categoría, año, mes)")
        if self.only:
            self.logger.info(f"Filtro --only activo: {sorted(self.only)}")

        self.logger.info("Construyendo dimensiones gold")
        gold_dims = GoldDimensionsBuilder(spark, self.logger).build_all()

        ctx = GoldContext(
            spark=spark,
            logger=self.logger,
            config=settings.gold,
            targets=targets,
            gold_dims=gold_dims,
            silver_audit_id=silver_audit_id,
            mode=self.mode,
        )
        config_md5 = self._config_md5(settings.gold)

        failures: list[str] = []
        for cls in self.BUILDER_CLASSES:
            builder = cls()
            if self.only and builder.name not in self.only:
                continue
            self.logger.info(f"Construyendo {builder.name} ({builder.subdir})")
            start = datetime.now()
            try:
                rowcount = builder.build(ctx)
            except Exception as e:
                self.logger.critical(f"Error construyendo {builder.name}: {e}")
                failures.append(builder.name)
                continue
            end = datetime.now()
            if rowcount < 0:
                self.logger.warning(
                    f"  {builder.name}: sin datos de entrada, omitido (sin audit)"
                )
                continue
            self._write_audit(builder, rowcount, start, end, silver_audit_id, config_md5)

        ctx.release_union_cache()
        if failures:
            # Fallar ruidosamente: sin esto un builder caido (p.ej. por muerte de
            # la JVM) dejaba un gold incompleto reportado como exitoso.
            raise RuntimeError(
                f"Capa gold fallo en {len(failures)} builder(s): {', '.join(failures)}"
            )
        self.logger.info("Capa gold completada exitosamente")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _silver_ready(self) -> bool:
        facts_ok = FACTS_DIR.exists() and any(FACTS_DIR.glob("fact_*_trip"))
        dims_ok = (SILVER_DIMS_DIR / "dim_date.parquet").exists() and (
            SILVER_DIMS_DIR / "dim_zone.parquet"
        ).exists()
        return facts_ok and dims_ok

    def _expand_targets(self, datasets: DatasetsConfig) -> list[tuple[str, int, int]]:
        targets: list[tuple[str, int, int]] = []
        for y in datasets.years:
            if isinstance(y, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        targets.append((cat, y, m))
            elif isinstance(y, Module):
                for m in range(1, 13):
                    targets.append((y.category, y.year, m))
        return targets

    def _latest_silver_audit_id(self, spark) -> str:
        audit_path = globals.project_root / "data/silver/audit.parquet"
        if not audit_path.exists():
            self.logger.warning("No se encontró audit de silver, usando 'unknown'")
            return "unknown"
        try:
            df = spark.read.parquet(str(audit_path))
            latest = (
                df.orderBy(F.col("start_timestamp").desc()).select("audit_id").first()
            )
            return latest["audit_id"] if latest else "unknown"
        except Exception as e:
            self.logger.warning(f"No se pudo leer audit de silver: {e}")
            return "unknown"

    def _config_md5(self, gold_config) -> str:
        try:
            payload = gold_config.model_dump_json()
        except Exception:
            payload = str(gold_config)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    def _write_audit(
        self, builder, rowcount, start, end, silver_audit_id, config_md5
    ) -> None:
        audit_path = GOLD_DIR / "audit.parquet"
        audit_path.parent.mkdir(parents=True, exist_ok=True)

        row = {
            "gold_audit_id": self.audit_id,
            "silver_audit_id": silver_audit_id,
            "mart_name": builder.name,
            "subdir": builder.subdir,
            "mode": self.mode,
            "start_timestamp": start.isoformat(),
            "end_timestamp": end.isoformat(),
            "rowcount_output": rowcount,
            "partition_keys": ",".join(builder.partition_keys),
            "config_snapshot_md5": config_md5,
        }
        df_new = pl.DataFrame([row])
        if audit_path.exists():
            existing = pl.read_parquet(str(audit_path))
            df_new = pl.concat([existing, df_new])
        df_new.write_parquet(str(audit_path))
