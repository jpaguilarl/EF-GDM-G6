from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from app.pipeline.silver import SilverCleaner
from app.pipeline.silver_impl.star import StarSchemaBuilder
from app.profiling.rules.reasonableness_ranges import MAX_TRIP_DURATION_MINUTES
from app.utils.globals import globals


# ---------------------------------------------------------------------------
# Module-scoped setup: bronze + silver stage under a temp dir
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def star_setup(tmp_path_factory, spark, bronze_subset):
    tmp = tmp_path_factory.mktemp("star_test")
    shutil.copytree(
        str(bronze_subset.parent),
        str(tmp / "data"),
        dirs_exist_ok=True,
    )
    zone_df = spark.read.parquet(
        str(tmp / "data" / "bronze" / "zone-lookup" / "zone-lookup-table.parquet")
    )
    zone_ids = {
        r["LocationID"]
        for r in zone_df.select("LocationID").distinct().collect()
    }
    cleaner = SilverCleaner(spark)
    for cat in ["yellow", "green", "fhv", "fhvhv"]:
        df = spark.read.parquet(str(tmp / "data" / "bronze" / cat / "2025-01.parquet"))
        clean_df, reject_df = cleaner.clean(df, cat, 2025, 1, zone_ids)
        stage_dir = tmp / "data" / "silver" / "stage" / cat
        stage_dir.mkdir(parents=True, exist_ok=True)
        clean_df.write.mode("overwrite").parquet(str(stage_dir / "2025-01.parquet"))
        cleaner.cleanup()
    return tmp


@pytest.fixture(autouse=True)
def _patches(monkeypatch, star_setup):
    monkeypatch.setattr("app.utils.globals.PROJECT_ROOT", star_setup)
    monkeypatch.setattr(
        "app.pipeline.star.DIMS_DIR",
        star_setup / "data/silver/star/dims",
    )
    monkeypatch.setattr(
        "app.pipeline.star.FACTS_DIR",
        star_setup / "data/silver/star/facts",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_dimensions(spark, star_setup):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    if DIMS_DIR.exists():
        shutil.rmtree(DIMS_DIR)

    builder = StarSchemaBuilder(spark)
    builder.build_dimensions()

    expected = [
        "dim_date.parquet",
        "dim_zone.parquet",
        "dim_vendor.parquet",
        "dim_ratecode.parquet",
        "dim_payment_type.parquet",
        "dim_service.parquet",
    ]
    for name in expected:
        p = DIMS_DIR / name
        assert p.exists(), f"Missing dimension: {p}"

    dim_date_files = list((DIMS_DIR / "dim_date.parquet").rglob("*.parquet"))
    dim_date_cols = {f.name for f in pq.read_schema(dim_date_files[0])}
    assert "date_key" in dim_date_cols
    assert "date" in dim_date_cols
    assert "year" in dim_date_cols
    assert "month" in dim_date_cols
    assert "day" in dim_date_cols
    assert "quarter" in dim_date_cols
    assert "weekday" in dim_date_cols
    assert "day_name" in dim_date_cols
    assert "month_name" in dim_date_cols
    assert "is_weekend" in dim_date_cols


def test_dim_date_weekday_is_iso(spark, star_setup):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    if not (DIMS_DIR / "dim_date.parquet").exists():
        StarSchemaBuilder(spark).build_dimensions()

    df = spark.read.parquet(str(DIMS_DIR / "dim_date.parquet"))
    df.createOrReplaceTempView("_dim_date")

    rows = df.select("date_key", "date", "weekday").collect()
    for r in rows:
        assert 1 <= r["weekday"] <= 7

    monday = df.filter(F.col("date") == "2023-01-02").select("weekday").first()
    assert monday is not None
    assert monday["weekday"] == 1

    saturday = df.filter(F.col("date") == "2023-01-07").select("weekday").first()
    assert saturday is not None
    assert saturday["weekday"] == 6


def test_dim_lookup_counts(spark, star_setup):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    if not (DIMS_DIR / "dim_vendor.parquet").exists():
        StarSchemaBuilder(spark).build_dimensions()

    vendor = spark.read.parquet(str(DIMS_DIR / "dim_vendor.parquet"))
    assert vendor.count() == 5

    ratecode = spark.read.parquet(str(DIMS_DIR / "dim_ratecode.parquet"))
    assert ratecode.count() == 7

    payment = spark.read.parquet(str(DIMS_DIR / "dim_payment_type.parquet"))
    assert payment.count() == 8

    service = spark.read.parquet(str(DIMS_DIR / "dim_service.parquet"))
    assert service.count() == 4


def test_build_facts_smoke(spark, star_setup, datasets_config):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    FACTS_DIR = star_setup / "data/silver/star/facts"
    if not (DIMS_DIR / "dim_date.parquet").exists():
        StarSchemaBuilder(spark).build_dimensions()

    if FACTS_DIR.exists():
        shutil.rmtree(FACTS_DIR)

    builder = StarSchemaBuilder(spark)
    builder.build_facts(datasets_config)

    for cat in ["yellow", "green", "fhv", "fhvhv"]:
        p = FACTS_DIR / f"fact_{cat}_trip" / "2025-01.parquet"
        assert p.exists(), f"Missing fact: {p}"


def test_fact_has_trip_id(spark, star_setup, datasets_config):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    FACTS_DIR = star_setup / "data/silver/star/facts"
    if not (FACTS_DIR / "fact_yellow_trip" / "2025-01.parquet").exists():
        if not (DIMS_DIR / "dim_date.parquet").exists():
            StarSchemaBuilder(spark).build_dimensions()
        StarSchemaBuilder(spark).build_facts(datasets_config)

    for cat in ["yellow", "green", "fhv", "fhvhv"]:
        p = FACTS_DIR / f"fact_{cat}_trip" / "2025-01.parquet"
        df = spark.read.parquet(str(p))
        assert df.filter(F.col("trip_id").isNull()).count() == 0
        sample = df.select("trip_id").first()
        assert sample is not None
        tid = sample["trip_id"]
        # trip_id es xxhash64 -> BIGINT (ver star._add_trip_id), no string sha2
        assert isinstance(tid, int)


def test_fact_standardized_timestamps(spark, star_setup, datasets_config):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    FACTS_DIR = star_setup / "data/silver/star/facts"
    if not (FACTS_DIR / "fact_yellow_trip" / "2025-01.parquet").exists():
        if not (DIMS_DIR / "dim_date.parquet").exists():
            StarSchemaBuilder(spark).build_dimensions()
        StarSchemaBuilder(spark).build_facts(datasets_config)

    for cat in ["yellow", "green", "fhv", "fhvhv"]:
        p = FACTS_DIR / f"fact_{cat}_trip" / "2025-01.parquet"
        df = spark.read.parquet(str(p))
        col_types = dict(df.dtypes)
        assert "pickup_datetime" in col_types, f"{cat} missing pickup_datetime"
        assert "dropoff_datetime" in col_types, f"{cat} missing dropoff_datetime"
        assert "timestamp" in col_types["pickup_datetime"].lower()
        assert "timestamp" in col_types["dropoff_datetime"].lower()


def test_fact_schema_by_category(spark, star_setup, datasets_config):
    DIMS_DIR = star_setup / "data/silver/star/dims"
    FACTS_DIR = star_setup / "data/silver/star/facts"
    if not (FACTS_DIR / "fact_yellow_trip" / "2025-01.parquet").exists():
        if not (DIMS_DIR / "dim_date.parquet").exists():
            StarSchemaBuilder(spark).build_dimensions()
        StarSchemaBuilder(spark).build_facts(datasets_config)

    yellow_cols = set(
        spark.read.parquet(
            str(FACTS_DIR / "fact_yellow_trip" / "2025-01.parquet")
        ).columns
    )
    for c in ["fare_amount", "vendor_id", "trip_id", "pickup_datetime",
              "dropoff_datetime", "passenger_count", "total_amount",
              "service_id"]:
        assert c in yellow_cols, f"yellow missing {c}"

    fhvhv_cols = set(
        spark.read.parquet(
            str(FACTS_DIR / "fact_fhvhv_trip" / "2025-01.parquet")
        ).columns
    )
    for c in ["driver_pay", "hvfhs_license_num", "trip_id", "pickup_datetime",
              "dropoff_datetime", "trip_miles", "service_id"]:
        assert c in fhvhv_cols, f"fhvhv missing {c}"


def test_first_match_case_insensitive(spark):
    schema = StructType([
        StructField("PUlocationID", StringType()),
        StructField("dropOff_datetime", StringType()),
    ])
    df = spark.createDataFrame([("x", "y")], schema)

    result = StarSchemaBuilder._first_match(df, ["PULocationID"])
    assert result == "PUlocationID"

    result = StarSchemaBuilder._first_match(df, ["dropoff_datetime"])
    assert result == "dropOff_datetime"

    result = StarSchemaBuilder._first_match(df, ["nonexistent"])
    assert result is None
