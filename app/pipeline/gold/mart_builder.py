"""Abstracciones base de la capa gold.

- ``GoldContext``: estado compartido (sesion Spark, config, targets, dims gold).
- ``GoldBuilder``: contrato base de cualquier producto gold (mart o feature store).
- ``TripGrainMart``: base para marts a nivel viaje (1 fila por viaje) que iteran
  los facts (categoria, año, mes) y escriben particiones idempotentes.
- Helpers reusables (``col_or_null``, ``with_zone``).

Reutiliza ``SparkClient``/``Logger``/``globals`` existentes. Origen de datos:
``data/silver/star/facts`` + dimensiones gold derivadas de ``data/silver/star/dims``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from app.utils.globals import globals
from app.utils.logger import Logger

# --- Rutas estandar -------------------------------------------------------
GOLD_DIR = globals.project_root / "data/gold"
FACTS_DIR = globals.project_root / "data/silver/star/facts"
SILVER_DIMS_DIR = globals.project_root / "data/silver/star/dims"
GOLD_DIMS_DIR = GOLD_DIR / "dims"
MARTS_DIR = GOLD_DIR / "marts"
ML_DIR = GOLD_DIR / "ml"

# Nombres estandarizados que los facts enriquecidos exponen (ver star.py).
PU_LOC = "pickup_location_id"
DO_LOC = "dropoff_location_id"


# --- Helpers de columnas --------------------------------------------------
def col_or_null(df: DataFrame, name: str, dtype: str = "double"):
    """Devuelve la columna si existe en ``df``; si no, un literal NULL tipado.

    Permite que marts con esquemas heterogeneos (taxis vs fhvhv) produzcan
    siempre el MISMO esquema de salida (clave para que Power BI lea el folder).
    """
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(dtype)


def with_zone(df: DataFrame, zone_dim: DataFrame, loc_col: str, prefix: str) -> DataFrame:
    """Enriquece ``df`` con ``{prefix}_borough`` y ``{prefix}_zone`` por join a la
    dimension de zonas (broadcast: la dim tiene ~265 filas)."""
    z = zone_dim.select(
        F.col("LocationID").alias(f"_{prefix}_loc"),
        F.col("Borough").alias(f"{prefix}_borough"),
        F.col("Zone").alias(f"{prefix}_zone"),
    )
    return df.join(
        F.broadcast(z), F.col(loc_col) == F.col(f"_{prefix}_loc"), "left"
    ).drop(f"_{prefix}_loc")


# --- Contexto compartido --------------------------------------------------
class GoldContext:
    def __init__(
        self,
        spark,
        logger: Logger,
        config,
        targets: list[tuple[str, int, int]],
        gold_dims: dict[str, DataFrame],
        silver_audit_id: str,
        mode: str = "full",
    ) -> None:
        self.spark = spark
        self.logger = logger
        self.config = config  # GoldConfig (pydantic)
        self.targets = targets  # [(category, year, month), ...]
        self.gold_dims = gold_dims  # {"zone","date","ratecode"}
        self.silver_audit_id = silver_audit_id
        self.mode = mode

    def fact_path(self, category: str, year: int, month: int) -> Path:
        return FACTS_DIR / f"fact_{category}_trip" / f"{year}-{month:02d}.parquet"

    def read_fact(self, category: str, year: int, month: int) -> DataFrame | None:
        path = self.fact_path(category, year, month)
        if not path.exists():
            return None
        try:
            return self.spark.read.parquet(str(path))
        except Exception as e:  # parquet vacio / corrupto: no abortar el mart
            self.logger.warning(f"  No se pudo leer {path.name}: {e}")
            return None

    def target_months(
        self, categories: list[str] | None = None
    ) -> list[tuple[str, int, int]]:
        if categories is None:
            return list(self.targets)
        cats = set(categories)
        return [(c, y, m) for (c, y, m) in self.targets if c in cats]

    def read_union(self, select_fn, categories: list[str] | None = None) -> DataFrame | None:
        """Lee y une (unionByName) las proyecciones de varios facts.

        ``select_fn(fact_df, category) -> DataFrame | None`` debe devolver columnas
        homogeneas para poder unirse. Util en marts agregados (supply/demand, ARIMA).
        """
        dfs: list[DataFrame] = []
        for (cat, year, month) in self.target_months(categories):
            fact = self.read_fact(cat, year, month)
            if fact is None:
                continue
            sel = select_fn(fact, cat)
            if sel is not None:
                dfs.append(sel)
        if not dfs:
            return None
        out = dfs[0]
        for d in dfs[1:]:
            out = out.unionByName(d, allowMissingColumns=True)
        return out


# --- Builder base ---------------------------------------------------------
class GoldBuilder(ABC):
    name: str = "gold_builder"
    subdir: str = "marts"  # "marts" o "ml"
    partition_keys: list[str] = ["service_id", "year", "month"]

    def __init__(self) -> None:
        self.logger = Logger()

    @property
    def output_dir(self) -> Path:
        return GOLD_DIR / self.subdir / self.name

    def _write(self, df: DataFrame) -> None:
        """Escritura idempotente. Con ``partitionOverwriteMode=dynamic`` (lo fija
        ``GoldPipeline``) ``overwrite`` solo reemplaza las particiones presentes."""
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        writer = df.write.mode("overwrite")
        if self.partition_keys:
            writer = writer.partitionBy(*self.partition_keys)
        writer.parquet(str(self.output_dir))

    @abstractmethod
    def build(self, ctx: GoldContext) -> int:
        """Construye y escribe la tabla. Devuelve nº de filas (-1 si no hay datos)."""
        ...


class TripGrainMart(GoldBuilder):
    """Mart a nivel viaje: itera los facts y escribe una particion por archivo."""

    applies_to: set[str] | None = None  # categorias soportadas; None = todas

    def _partition_exists(self, category: str, year: int, month: int) -> bool:
        path = (
            self.output_dir
            / f"service_id={category}"
            / f"year={year}"
            / f"month={month}"
        )
        return path.exists()

    def build(self, ctx: GoldContext) -> int:
        total = 0
        wrote_any = False
        for (cat, year, month) in ctx.targets:
            if self.applies_to and cat not in self.applies_to:
                continue
            if ctx.mode == "incremental" and self._partition_exists(cat, year, month):
                self.logger.info(
                    f"  {self.name} | {cat} {year}-{month:02d}: partición ya existe, omitido (incremental)"
                )
                wrote_any = True
                continue
            fact = ctx.read_fact(cat, year, month)
            if fact is None:
                continue
            part = self.transform(fact, cat, year, month, ctx)
            if part is None:
                continue
            part = part.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                n = part.count()
                if n == 0:
                    continue
                self._write(part)
                total += n
                wrote_any = True
                self.logger.info(f"  {self.name} | {cat} {year}-{month:02d}: {n} filas")
            finally:
                part.unpersist()
        return total if wrote_any else -1

    @abstractmethod
    def transform(
        self,
        fact: DataFrame,
        category: str,
        year: int,
        month: int,
        ctx: GoldContext,
    ) -> DataFrame | None:
        """Proyecta un fact a la tabla ancha del mart. Debe incluir las columnas de
        particion (service_id, year, month) y un esquema identico entre categorias."""
        ...
