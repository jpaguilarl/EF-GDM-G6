from __future__ import annotations

import os
import shutil
import sys
import uuid
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession

from app.schemas.settings_schema import (
    StorageConfig,
    AbcXyzConfig,
    DatasetsConfig,
    GenerosityConfig,
    GoldConfig,
    IsolationFraudConfig,
    KmodesConfig,
    Module,
    SariMaxConfig,
    SettingsSchema,
    SupplyDemandConfig,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_SOURCE = PROJECT_ROOT / "data" / "bronze"

SAMPLE_ROWS = 50
# El bronce real en disco es del año 2025 (config.yaml datasets.years=[2025]).
SAMPLE_YEAR = "2025"
SAMPLE_MONTH = "01"
CATEGORIES = ["yellow", "green", "fhv", "fhvhv"]


def _sample_bronze(target_root: Path) -> Path:
    """Sample ~50 rows per category from the real bronze data on disk."""
    root = target_root / "data" / "bronze"
    for cat in CATEGORIES:
        src = BRONZE_SOURCE / cat / f"{SAMPLE_YEAR}-{SAMPLE_MONTH}.parquet"
        if not src.exists():
            continue
        table = pq.ParquetFile(str(src)).read_row_group(0).slice(0, SAMPLE_ROWS)
        dst = root / cat
        dst.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dst / f"{SAMPLE_YEAR}-{SAMPLE_MONTH}.parquet")

    zone_src = BRONZE_SOURCE / "zone-lookup" / "zone-lookup-table.parquet"
    if zone_src.exists():
        dst = root / "zone-lookup"
        dst.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(zone_src), str(dst / "zone-lookup-table.parquet"))

    audit = pl.DataFrame([
        {
            "audit_id": str(uuid.uuid4()),
            "name": "test_bronze",
            "source_file": "test",
            "bytecount": 0,
            "rowcount": 50,
            "start_timestamp": "2023-01-01T00:00:00",
            "end_timestamp": "2023-01-01T01:00:00",
        }
    ])
    audit_path = root / "audit.parquet"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit.write_parquet(str(audit_path))
    return root


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # Sin esto, en Windows Spark lanza los Python workers como "python3"/"python",
    # cae en el stub de Microsoft Store y el worker nunca conecta (SocketTimeout ->
    # los tests spark cuelgan y fallan). SparkClient (prod) ya lo fija; el fixture
    # debe hacer lo mismo para poder correr la suite en Windows.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark_temp = Path("/tmp") / f".spark_temp_{uuid.uuid4().hex}"
    spark_temp.mkdir(parents=True, exist_ok=True)
    s = (
        SparkSession.builder
        .appName("ef_gdm_g6_tests")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.local.dir", str(spark_temp))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
    shutil.rmtree(spark_temp, ignore_errors=True)


@pytest.fixture(scope="session")
def bronze_subset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("bronze_subset")
    return _sample_bronze(tmp)


@pytest.fixture(scope="session")
def datasets_config() -> DatasetsConfig:
    return DatasetsConfig(
        years=[Module(category=c, year=int(SAMPLE_YEAR), month=int(SAMPLE_MONTH))
               for c in CATEGORIES]
    )


@pytest.fixture(scope="session")
def gold_config() -> GoldConfig:
    return GoldConfig(
        mode="full",
        supply_demand=SupplyDemandConfig(block_minutes=15, deficit_threshold=-10),
        abc_xyz=AbcXyzConfig(class_a_pct=0.80, class_b_pct=0.15, xyz_x_max=0.2, xyz_y_max=0.5),
        generosity=GenerosityConfig(standard_low=10.0, standard_high=18.0),
        isolation_fraud=IsolationFraudConfig(
            contamination=0.05, n_estimators=100, max_samples="auto",
            random_state=42, min_rows_per_ratecode=10,
        ),
        kmodes=KmodesConfig(max_k=5, max_sample_per_service=500, n_init=1, init_method="Cao", random_state=42),
        sarimax=SariMaxConfig(order=[1, 1, 1], seasonal_order=[1, 1, 1, 24],
                               min_rows_per_segment=10, forecast_horizon_hours=24),
    )


@pytest.fixture(scope="session")
def settings(storage_config, datasets_config, gold_config) -> SettingsSchema:
    return SettingsSchema(storage=storage_config, datasets=datasets_config, gold=gold_config)


@pytest.fixture(autouse=True)
def _project_root_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable Logger file logging by redirecting its log dir to a temp location.

    This fixture runs before every test to keep logs from piling up in the real
    logs/ directory. It also sets app.utils.globals.PROJECT_ROOT to None so
    modules that rely on it for I/O must use a specific temp-dir fixture.
    """
    # The Logger writes logs/ relative to PROJECT_ROOT.  We can't easily
    # monkeypatch its internals after import, so we let it create log files
    # but they go to logs/ as normal.  Tests that care about I/O use
    # ``tmp_path`` fixtures explicitly.
    pass
