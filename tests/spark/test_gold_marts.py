from pyspark.sql import Row, functions as F

from app.pipeline.gold import mart_builder
from app.pipeline.gold.mart_builder import (
    GoldBuilder,
    GoldContext,
    TripGrainMart,
    col_or_null,
    with_zone,
)
from app.pipeline.gold.feature_rules.ratecode_tariff import FLAT_FARES, flat_fare_rows
from app.schemas.settings_schema import GoldConfig
from app.utils.logger import Logger


class TestColOrNull:
    def test_existing_column(self, spark):
        df = spark.createDataFrame([Row(fare_amount=10.0), Row(fare_amount=20.0)])
        col = col_or_null(df, "fare_amount")
        assert [r.v for r in df.select(col.alias("v")).collect()] == [10.0, 20.0]

    def test_missing_column(self, spark):
        df = spark.createDataFrame([Row(fare_amount=10.0)])
        col = col_or_null(df, "driver_pay")
        assert df.select(col.alias("v")).collect()[0].v is None


class TestWithZone:
    def test_enrichment(self, spark):
        fact = spark.createDataFrame([
            Row(pickup_location_id=1, trip_id="a"),
            Row(pickup_location_id=2, trip_id="b"),
        ])
        zone_dim = spark.createDataFrame([
            Row(LocationID=1, Borough="Manhattan", Zone="Midtown", service_zone="Boro Zone"),
            Row(LocationID=2, Borough="Queens", Zone="Astoria", service_zone="Boro Zone"),
        ])
        result = with_zone(fact, zone_dim, "pickup_location_id", "pickup")
        assert "pickup_borough" in result.columns
        assert "pickup_zone" in result.columns
        rows = result.orderBy("trip_id").select("pickup_borough", "pickup_zone").collect()
        assert rows[0].pickup_borough == "Manhattan"
        assert rows[0].pickup_zone == "Midtown"
        assert rows[1].pickup_borough == "Queens"
        assert rows[1].pickup_zone == "Astoria"


class TestGoldBuilderWrite:
    def test_write_partitioning(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(mart_builder, "GOLD_DIR", tmp_path)

        class TestBuilder(GoldBuilder):
            name = "test_builder"
            subdir = "marts_test"

            def build(self, ctx):
                return 0

        builder = TestBuilder()
        df = spark.createDataFrame([
            Row(service_id="yellow", year=2023, month=1, trip_id="a"),
            Row(service_id="green", year=2023, month=1, trip_id="b"),
        ])
        builder._write(df)
        for cat in ("yellow", "green"):
            part = builder.output_dir / f"service_id={cat}" / "year=2023" / "month=1"
            assert part.exists()


class TestTripGrainMart:
    def test_applies_to_filter(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(mart_builder, "GOLD_DIR", tmp_path)
        monkeypatch.setattr(mart_builder, "FACTS_DIR", tmp_path / "facts")

        class TrackingMart(TripGrainMart):
            name = "tracking_mart"
            subdir = "marts_test"
            applies_to = {"yellow", "green"}

            def transform(self, fact, category, year, month, ctx):
                self._track.append((category, year, month))
                return fact.withColumn("service_id", F.lit(category)).withColumn("year", F.lit(year)).withColumn("month", F.lit(month))

        targets = [
            ("yellow", 2023, 1),
            ("green", 2023, 1),
            ("fhv", 2023, 1),
            ("fhvhv", 2023, 1),
        ]
        facts_dir = tmp_path / "facts"
        for cat, y, m in targets:
            p = facts_dir / f"fact_{cat}_trip" / f"{y}-{m:02d}.parquet"
            p.parent.mkdir(parents=True, exist_ok=True)
            spark.createDataFrame([Row(trip_id="x")]).write.mode("overwrite").parquet(str(p))

        gold_dims = {
            "zone": spark.createDataFrame(
                [Row(LocationID=1, Borough="X", Zone="Z", service_zone="Boro")]
            )
        }
        ctx = GoldContext(
            spark, Logger(), GoldConfig(), targets, gold_dims, "audit123", "full"
        )
        mart = TrackingMart()
        mart._track = []
        mart.build(ctx)
        assert ("yellow", 2023, 1) in mart._track
        assert ("green", 2023, 1) in mart._track
        assert ("fhv", 2023, 1) not in mart._track
        assert ("fhvhv", 2023, 1) not in mart._track

    def test_partition_exists(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(mart_builder, "GOLD_DIR", tmp_path)

        class TestMart(TripGrainMart):
            name = "test_partition"
            subdir = "marts_test"

            def transform(self, fact, category, year, month, ctx):
                return fact

        mart = TestMart()
        part_dir = mart.output_dir / "service_id=yellow" / "year=2023" / "month=1"
        part_dir.mkdir(parents=True, exist_ok=True)
        assert mart._partition_exists("yellow", 2023, 1) is True
        assert mart._partition_exists("green", 2023, 1) is False


class TestGoldContext:
    def test_read_fact(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(mart_builder, "FACTS_DIR", tmp_path / "facts")
        facts_dir = tmp_path / "facts"
        cat, y, m = "yellow", 2023, 1
        fact_path = facts_dir / f"fact_{cat}_trip" / f"{y}-{m:02d}.parquet"
        fact_path.parent.mkdir(parents=True, exist_ok=True)
        expected = spark.createDataFrame([Row(trip_id="abc", fare_amount=10.0)])
        expected.write.mode("overwrite").parquet(str(fact_path))

        ctx = GoldContext(spark, Logger(), GoldConfig(), [], {}, "audit123", "full")
        result = ctx.read_fact(cat, y, m)
        assert result is not None
        assert result.collect() == expected.collect()

    def test_read_fact_missing(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(mart_builder, "FACTS_DIR", tmp_path / "facts")
        ctx = GoldContext(spark, Logger(), GoldConfig(), [], {}, "audit123", "full")
        assert ctx.read_fact("yellow", 2099, 1) is None

    def test_target_months_all(self):
        targets = [("yellow", 2023, 1), ("green", 2024, 2)]
        ctx = GoldContext(None, Logger(), GoldConfig(), targets, {}, "audit123", "full")
        assert ctx.target_months() == targets

    def test_target_months_filtered(self):
        targets = [
            ("yellow", 2023, 1),
            ("green", 2023, 2),
            ("fhv", 2023, 3),
            ("fhvhv", 2024, 1),
        ]
        ctx = GoldContext(None, Logger(), GoldConfig(), targets, {}, "audit123", "full")
        assert ctx.target_months(["yellow", "fhv"]) == [
            ("yellow", 2023, 1),
            ("fhv", 2023, 3),
        ]


class TestFlatFareRows:
    def test_jfk_across_years(self):
        rows = flat_fare_rows()
        for y in (2023, 2024, 2025):
            assert (2, y, 70.0, "JFK") in rows

    def test_flat_fares_dict(self):
        assert FLAT_FARES[2][2023] == 70.0
        assert FLAT_FARES[2][2024] == 70.0
        assert FLAT_FARES[2][2025] == 70.0
