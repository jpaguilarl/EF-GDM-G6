from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from app.pipeline.silver import SilverCleaner, SilverPipeline


def _zone_ids(spark, bronze_subset):
    zone_df = spark.read.parquet(
        str(bronze_subset / "zone-lookup/zone-lookup-table.parquet")
    )
    return {r["LocationID"] for r in zone_df.select("LocationID").distinct().collect()}


def _make_row(spark, category, **overrides):
    base = {
        "yellow": {
            "VendorID": 1,
            "tpep_pickup_datetime": "2023-01-15 08:00:00",
            "tpep_dropoff_datetime": "2023-01-15 08:30:00",
            "passenger_count": 1,
            "trip_distance": 3.0,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 1,
            "DOLocationID": 1,
            "payment_type": 1,
            "fare_amount": 10.0,
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 10.8,
            "congestion_surcharge": 0.0,
            "airport_fee": 0.0,
        },
        "green": {
            "VendorID": 1,
            "lpep_pickup_datetime": "2023-01-15 08:00:00",
            "lpep_dropoff_datetime": "2023-01-15 08:30:00",
            "passenger_count": 1,
            "trip_distance": 3.0,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 1,
            "DOLocationID": 1,
            "payment_type": 1,
            "fare_amount": 10.0,
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 0.0,
            "tolls_amount": 0.0,
            "ehail_fee": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": 10.8,
            "congestion_surcharge": 0.0,
            "trip_type": 1,
        },
        "fhv": {
            "dispatching_base_num": "B00001",
            "pickup_datetime": "2023-01-15 08:00:00",
            "dropOff_datetime": "2023-01-15 08:30:00",
            "PULocationID": 1,
            "DOLocationID": 1,
            "SR_Flag": 0,
        },
        "fhvhv": {
            "hvfhs_license_num": "HV0001",
            "dispatching_base_num": "B00001",
            "originating_base_num": "B00001",
            "request_datetime": "2023-01-15 07:55:00",
            "on_scene_datetime": "2023-01-15 08:00:00",
            "pickup_datetime": "2023-01-15 08:00:00",
            "dropoff_datetime": "2023-01-15 08:30:00",
            "PULocationID": 1,
            "DOLocationID": 1,
            "trip_miles": 3.0,
            "trip_time": 1800,
            "base_passenger_fare": 10.0,
            "tolls": 0.0,
            "bcf": 0.5,
            "sales_tax": 0.3,
            "congestion_surcharge": 2.5,
            "airport_fee": 0.0,
            "tips": 1.5,
            "driver_pay": 15.0,
            "shared_request_flag": "N",
            "shared_match_flag": "N",
            "access_a_ride_flag": "N",
            "wav_request_flag": "N",
            "wav_match_flag": "N",
        },
    }
    data = base[category].copy()
    data.update(overrides)
    return data


def _make_single_row_df(spark, category, **overrides):
    d = _make_row(spark, category, **overrides)

    def _spark_type(v):
        if v is None:
            return StringType()
        if isinstance(v, bool):
            return StringType()
        if isinstance(v, int):
            return IntegerType()
        if isinstance(v, float):
            return DoubleType()
        return StringType()

    fields = [StructField(k, _spark_type(v), True) for k, v in d.items()]
    schema = StructType(fields)
    return spark.createDataFrame([list(d.values())], schema=schema)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_clean_basic_invocation(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    cleaner = SilverCleaner(spark)

    for cat in ["yellow", "green", "fhv", "fhvhv"]:
        df = spark.read.parquet(
            str(bronze_subset / f"{cat}/2025-01.parquet")
        )
        total = df.count()
        clean_df, reject_df = cleaner.clean(df, cat, 2025, 1, zone_ids)
        clean_count = clean_df.count()
        reject_count = reject_df.count() if reject_df is not None else 0
        assert clean_count + reject_count == total, (
            f"{cat}: {clean_count} + {reject_count} != {total}"
        )
        cleaner.cleanup()


def test_reject_timeliness_off_period(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    df = _make_single_row_df(
        spark, "yellow",
        tpep_pickup_datetime="2022-06-15 08:00:00",
        tpep_dropoff_datetime="2022-06-15 08:30:00",
        PULocationID=next(iter(zone_ids)),
        DOLocationID=next(iter(zone_ids)),
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert "timeliness_off_period" in reasons
    cleaner.cleanup()


def test_reject_datetime_invalid_inverted(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        tpep_pickup_datetime="2023-01-15 08:30:00",
        tpep_dropoff_datetime="2023-01-15 08:00:00",
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert "datetime_inverted" in reasons
    cleaner.cleanup()


def test_long_trip_not_rejected(spark, bronze_subset):
    """A 25-hour trip is unusual but NOT factually incorrect — should pass."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        tpep_pickup_datetime="2023-01-15 08:00:00",
        tpep_dropoff_datetime="2023-01-16 09:00:00",
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    clean_df, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    assert reject_df.count() == 0
    cleaner.cleanup()


def test_reject_integrity_invalid_zone(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    df = _make_single_row_df(
        spark, "yellow",
        PULocationID=999999,
        DOLocationID=999999,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert any("integrity_invalid" in r for r in reasons)
    cleaner.cleanup()


def test_reject_uniqueness_duplicate(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    row = _make_row(
        spark, "yellow",
        VendorID=1,
        tpep_pickup_datetime="2023-01-15 08:00:00",
        tpep_dropoff_datetime="2023-01-15 08:30:00",
        PULocationID=zid,
        DOLocationID=zid,
        total_amount=10.0,
    )
    df = spark.createDataFrame([row, row])
    cleaner = SilverCleaner(spark)
    clean_df, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    assert reject_df.count() == 1
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert "uniqueness_duplicate" in reasons
    cleaner.cleanup()


def test_first_match(spark):
    from app.pipeline.silver import SilverCleaner
    from app.pipeline.silver_impl.star import StarSchemaBuilder

    schema = StructType([
        StructField("foo", StringType()),
        StructField("bar", StringType()),
    ])
    df = spark.createDataFrame([("a", "b")], schema)

    assert SilverCleaner._first_match(df, ["foo", "bar"]) == "foo"
    assert SilverCleaner._first_match(df, ["baz", "bar"]) == "bar"
    assert SilverCleaner._first_match(df, ["baz", "qux"]) is None

    assert SilverCleaner._first_match(df, ["FOO"]) is None

    schema2 = StructType([
        StructField("PUlocationID", StringType()),
        StructField("pickup_datetime", StringType()),
    ])
    df2 = spark.createDataFrame([("x", "y")], schema2)
    result = StarSchemaBuilder._first_match(df2, ["PULocationID"])
    assert result == "PUlocationID"


def test_reject_incomplete_required_null(spark, bronze_subset):
    """A null in a required column (VendorID for yellow) should reject the row."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        VendorID=None,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert reject_df.count() == 1
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert any("incomplete_" in r for r in reasons)
    cleaner.cleanup()


def test_nullable_col_passes_through(spark, bronze_subset):
    """A null in a nullable column (congestion_surcharge for yellow) should NOT
    reject the row and the null should be preserved as-is."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        congestion_surcharge=None,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    clean_df, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    assert reject_df.count() == 0
    row = clean_df.select("congestion_surcharge").first()
    assert row["congestion_surcharge"] is None
    cleaner.cleanup()


def test_no_imputation_nulls_preserved(spark, bronze_subset):
    """Nullable columns that were previously imputed (passenger_count→1) should
    now cause rejection since passenger_count is required by default."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        passenger_count=None,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert reject_df.count() == 1
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert any("incomplete_passenger_count" in r for r in reasons)
    cleaner.cleanup()


def test_source_values_preserved(spark, bronze_subset):
    """Values that were previously clamped or recomputed should now pass through
    unchanged: no clamping, no total_amount recomputation."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        trip_distance=1000.0,
        total_amount=99.99,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    clean_df, _ = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    row = clean_df.select("trip_distance", "total_amount").first()
    # Values preserved as-is: no clamping of trip_distance, no recomputation of total_amount
    assert row["trip_distance"] == 1000.0
    assert row["total_amount"] == 99.99
    cleaner.cleanup()


def test_fhvhv_driver_pay_preserved(spark, bronze_subset):
    """driver_pay must pass through unchanged (no accuracy recomputation)."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    original_driver_pay = 100.0
    df = _make_single_row_df(
        spark, "fhvhv",
        driver_pay=original_driver_pay,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    clean_df, _ = cleaner.clean(df, "fhvhv", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    row = clean_df.select("driver_pay").first()
    assert row["driver_pay"] == original_driver_pay
    cleaner.cleanup()


def test_normalize_types_cast(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        RatecodeID="2",
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    clean_df, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert clean_df.count() == 1
    row = clean_df.select("RatecodeID").first()
    assert isinstance(row["RatecodeID"], int)
    assert row["RatecodeID"] == 2
    cleaner.cleanup()


def test_composite_keys_all_categories():
    keys = SilverCleaner.COMPOSITE_KEYS
    assert "yellow" in keys
    assert "green" in keys
    assert "fhv" in keys
    assert "fhvhv" in keys
    assert isinstance(keys["yellow"], list)
    assert isinstance(keys["green"], list)
    assert isinstance(keys["fhv"], list)
    assert isinstance(keys["fhvhv"], list)
    assert len(keys["yellow"]) >= 3
    assert len(keys["green"]) >= 3
    assert len(keys["fhv"]) >= 2
    assert len(keys["fhvhv"]) >= 3


def test_zone_ids_loaded_correctly(spark, bronze_subset):
    zone_ids = _zone_ids(spark, bronze_subset)
    assert len(zone_ids) > 0
    assert all(isinstance(z, int) for z in zone_ids)


def test_reject_incomplete_trip_distance(spark, bronze_subset):
    """trip_distance is required for yellow — null should reject."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "yellow",
        trip_distance=None,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "yellow", 2023, 1, zone_ids)
    assert reject_df.count() == 1
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert any("incomplete_trip_distance" in r for r in reasons)
    cleaner.cleanup()


def test_reject_incomplete_fhvhv_trip_miles(spark, bronze_subset):
    """trip_miles is required for fhvhv — null should reject."""
    zone_ids = _zone_ids(spark, bronze_subset)
    zid = next(iter(zone_ids))
    df = _make_single_row_df(
        spark, "fhvhv",
        trip_miles=None,
        PULocationID=zid,
        DOLocationID=zid,
    )
    cleaner = SilverCleaner(spark)
    _, reject_df = cleaner.clean(df, "fhvhv", 2023, 1, zone_ids)
    assert reject_df.count() == 1
    reasons = [
        r["_reject_reason"]
        for r in reject_df.select("_reject_reason").distinct().collect()
    ]
    assert any("incomplete_trip_miles" in r for r in reasons)
    cleaner.cleanup()
