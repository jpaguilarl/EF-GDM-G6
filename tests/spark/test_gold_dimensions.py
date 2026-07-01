import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from app.pipeline.gold import mart_builder
from app.pipeline.gold.dims import gold_dimensions as gd_module
from app.pipeline.gold.dims.gold_dimensions import GoldDimensionsBuilder


@pytest.fixture
def builder(spark, tmp_path, monkeypatch):
    silver_dims = tmp_path / "silver_dims"
    gold_dims = tmp_path / "gold_dims"
    silver_dims.mkdir(parents=True, exist_ok=True)
    gold_dims.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(mart_builder, "SILVER_DIMS_DIR", silver_dims)
    monkeypatch.setattr(mart_builder, "GOLD_DIMS_DIR", gold_dims)
    monkeypatch.setattr(gd_module, "SILVER_DIMS_DIR", silver_dims)
    monkeypatch.setattr(gd_module, "GOLD_DIMS_DIR", gold_dims)

    date_schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("weekday", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("month_name", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
    ])
    date_rows = [
        (20230101, datetime.date(2023, 1, 1), 2023, 1, 1, 1, 7, "Sunday", "January", True),
        (20230102, datetime.date(2023, 1, 2), 2023, 1, 2, 1, 1, "Monday", "January", False),
        (20230103, datetime.date(2023, 1, 3), 2023, 1, 3, 1, 2, "Tuesday", "January", False),
        (20240101, datetime.date(2024, 1, 1), 2024, 1, 1, 1, 1, "Monday", "January", False),
    ]
    spark.createDataFrame(date_rows, date_schema).write.mode("overwrite").parquet(
        str(silver_dims / "dim_date.parquet")
    )

    zone_schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), False),
        StructField("Zone", StringType(), False),
        StructField("service_zone", StringType(), False),
    ])
    zone_rows = [
        (1, "Bronx", "Some Zone", "Boro Zone"),
        (2, "EWR", "Newark Airport", "EWR"),
        (3, "Unknown", "Unknown", "N/A"),
        (4, "Brooklyn", "Brooklyn Zone", "Boro Zone"),
    ]
    spark.createDataFrame(zone_rows, zone_schema).write.mode("overwrite").parquet(
        str(silver_dims / "dim_zone.parquet")
    )

    return GoldDimensionsBuilder(spark)


class TestGoldDimensionsBuilder:
    def test_gold_dimensions_all_built(self, builder, tmp_path):
        dims = builder.build_all()
        assert set(dims.keys()) == {"date", "zone", "ratecode"}
        assert (tmp_path / "gold_dims" / "dim_date_gold.parquet").exists()
        assert (tmp_path / "gold_dims" / "dim_zone_gold.parquet").exists()
        assert (tmp_path / "gold_dims" / "dim_ratecode_theoretical.parquet").exists()

    def test_dim_date_gold_dia_categoria(self, builder):
        dims = builder.build_all()
        rows = dims["date"].select("weekday", "dia_categoria").collect()
        for r in rows:
            if r.weekday >= 6:
                assert r.dia_categoria == "Fin de Semana"
            else:
                assert r.dia_categoria == "Día Laborable"

    def test_dim_date_gold_is_holiday(self, builder):
        dims = builder.build_all()
        df = dims["date"]
        holiday_row = df.filter(F.col("date_key") == 20240101).select("is_holiday").collect()[0]
        assert holiday_row.is_holiday is True
        non_holiday_row = df.filter(F.col("date_key") == 20230102).select("is_holiday").collect()[0]
        assert non_holiday_row.is_holiday is False

    def test_dim_zone_gold_borough_name_es(self, builder):
        dims = builder.build_all()
        rows = {
            r.LocationID: r.borough_name_es
            for r in dims["zone"].select("LocationID", "borough_name_es").collect()
        }
        assert rows[1] == "El Bronx"
        assert rows[2] == "Newark (EWR)"
        assert rows[3] == "Desconocido"
        assert rows[4] == "Brooklyn"

    def test_dim_ratecode_theoretical_schema(self, builder):
        dims = builder.build_all()
        assert set(dims["ratecode"].columns) == {
            "ratecode_id", "fiscal_year", "flat_fare", "ratecode_name"
        }

    def test_dim_ratecode_theoretical_jfk_fare(self, builder):
        dims = builder.build_all()
        row = dims["ratecode"].filter(
            (F.col("ratecode_id") == 2) & (F.col("fiscal_year") == 2023)
        ).select("flat_fare").collect()[0]
        assert row.flat_fare == 70.0

    def test_dim_ratecode_theoretical_no_flat_fare(self, builder):
        dims = builder.build_all()
        rows = dims["ratecode"].filter(F.col("ratecode_id") == 1).select("flat_fare").collect()
        for r in rows:
            assert r.flat_fare is None
